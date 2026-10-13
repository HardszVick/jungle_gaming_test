package postgres

import (
	"context"
	"errors"
	"time"
)

func (s *Store) ClaimPendingReferences(ctx context.Context, claimedBy string, limit, maxAttempts int, lease time.Duration) ([]PendingReferenceClaim, error) {
	if claimedBy == "" || limit <= 0 || maxAttempts <= 0 || lease <= 0 {
		return nil, errors.New("postgres: invalid pending-reference claim")
	}
	rows, err := s.db.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM wager_transactions
			WHERE state = 'PENDING_REFERENCE'
			  AND (reference_claim_until IS NULL OR reference_claim_until <= CURRENT_TIMESTAMP)
			  AND (reference_next_attempt_at <= CURRENT_TIMESTAMP OR reference_expires_at <= CURRENT_TIMESTAMP OR reference_attempts >= $2)
			ORDER BY reference_next_attempt_at, created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE wager_transactions AS transactions
		SET reference_claimed_by = $3,
		    reference_claim_until = CURRENT_TIMESTAMP + $4::interval,
		    reference_attempts = CASE
		        WHEN reference_attempts < $2 THEN reference_attempts + 1
		        ELSE reference_attempts
		    END
		FROM candidates
		WHERE transactions.id = candidates.id
		RETURNING `+claimedTransactionColumns,
		limit, maxAttempts, claimedBy, lease.String())
	if err != nil {
		return nil, TranslateError(err)
	}
	defer rows.Close()
	claims := make([]PendingReferenceClaim, 0, limit)
	for rows.Next() {
		record, scanErr := scanTransaction(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		claims = append(claims, PendingReferenceClaim{Record: record})
	}
	return claims, TranslateError(rows.Err())
}

func (s *Store) ReschedulePendingReference(ctx context.Context, transactionID, claimedBy string, nextAttemptAt time.Time) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE wager_transactions
		SET reference_next_attempt_at = $1, reference_claimed_by = NULL,
		    reference_claim_until = NULL, updated_at = GREATEST(updated_at, CURRENT_TIMESTAMP)
		WHERE id = $2 AND state = 'PENDING_REFERENCE' AND reference_claimed_by = $3`,
		nextAttemptAt, transactionID, claimedBy)
	if err != nil {
		return TranslateError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrClaimLost
	}
	return nil
}
