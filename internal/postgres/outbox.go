package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func (s *Store) InsertOutboxEvent(ctx context.Context, event domain.DomainEvent) error {
	if event == nil {
		return errors.New("postgres: event is nil")
	}
	envelope, err := json.Marshal(event)
	if err != nil {
		return err
	}
	causationID, hasCausation := event.CausationID()
	var causation any
	if hasCausation {
		causation = causationID
	}
	_, err = s.db.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, aggregate_id, correlation_id, causation_id,
			version, envelope
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		event.ID(), string(event.Type()), event.AggregateID(), event.CorrelationID(),
		causation, event.Version(), envelope)
	return TranslateError(err)
}

func (s *Store) ClaimOutbox(ctx context.Context, claimedBy string, limit int, lease time.Duration) ([]OutboxClaim, error) {
	if claimedBy == "" || limit <= 0 || lease <= 0 {
		return nil, errors.New("postgres: invalid outbox claim")
	}
	rows, err := s.db.Query(ctx, `
		WITH candidates AS (
			SELECT event_id
			FROM outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= CURRENT_TIMESTAMP
			  AND (claim_until IS NULL OR claim_until <= CURRENT_TIMESTAMP)
			ORDER BY next_attempt_at, created_at, event_id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox_events AS events
		SET claimed_by = $2, claim_until = CURRENT_TIMESTAMP + $3::interval
		FROM candidates
		WHERE events.event_id = candidates.event_id
		RETURNING events.event_id::text, events.event_type, events.aggregate_id::text,
		          events.envelope, events.attempt_count, events.claimed_by,
		          events.claim_until, events.created_at`,
		limit, claimedBy, lease.String())
	if err != nil {
		return nil, TranslateError(err)
	}
	defer rows.Close()
	claims := make([]OutboxClaim, 0, limit)
	for rows.Next() {
		var claim OutboxClaim
		if err := rows.Scan(&claim.EventID, &claim.EventType, &claim.AggregateID, &claim.Envelope,
			&claim.AttemptCount, &claim.ClaimedBy, &claim.ClaimUntil, &claim.CreatedAt); err != nil {
			return nil, TranslateError(err)
		}
		claims = append(claims, claim)
	}
	return claims, TranslateError(rows.Err())
}

func (s *Store) MarkOutboxPublished(ctx context.Context, eventID, claimedBy string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = CURRENT_TIMESTAMP, claimed_by = NULL, claim_until = NULL, last_error = NULL
		WHERE event_id = $1 AND claimed_by = $2 AND published_at IS NULL`,
		eventID, claimedBy)
	if err != nil {
		return TranslateError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrClaimLost
	}
	return nil
}

func (s *Store) RescheduleOutbox(ctx context.Context, eventID, claimedBy string, delay time.Duration, lastError string) error {
	if delay <= 0 || lastError == "" {
		return errors.New("postgres: positive outbox delay and failure reason are required")
	}
	tag, err := s.db.Exec(ctx, `
		UPDATE outbox_events
		SET attempt_count = attempt_count + 1,
		    next_attempt_at = CURRENT_TIMESTAMP + $1::interval,
		    claimed_by = NULL, claim_until = NULL, last_error = $2
		WHERE event_id = $3 AND claimed_by = $4 AND published_at IS NULL`,
		delay.String(), lastError, eventID, claimedBy)
	if err != nil {
		return TranslateError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrClaimLost
	}
	return nil
}
