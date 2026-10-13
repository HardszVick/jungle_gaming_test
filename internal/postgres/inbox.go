package postgres

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Store) BeginInbox(ctx context.Context, consumerName, messageID string, payloadHash [32]byte, receivedAt time.Time) (InboxResult, error) {
	var inserted int
	err := s.db.QueryRow(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING
		RETURNING 1`, consumerName, messageID, payloadHash[:], receivedAt).Scan(&inserted)
	if err == nil {
		return InboxResult{Status: InboxStarted}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return InboxResult{}, TranslateError(err)
	}
	var storedHash []byte
	var transactionID *string
	var completedAt *time.Time
	err = s.db.QueryRow(ctx, `
		SELECT payload_hash, transaction_id::text, completed_at
		FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2`, consumerName, messageID).
		Scan(&storedHash, &transactionID, &completedAt)
	if err != nil {
		return InboxResult{}, TranslateError(err)
	}
	if !bytes.Equal(storedHash, payloadHash[:]) {
		return InboxResult{}, ErrInboxHashConflict
	}
	if completedAt != nil && transactionID != nil {
		return InboxResult{Status: InboxCompleted, TransactionID: *transactionID}, nil
	}
	return InboxResult{Status: InboxPending}, nil
}

func (s *Store) CompleteInbox(ctx context.Context, consumerName, messageID, transactionID string, completedAt time.Time) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE inbox_messages
		SET transaction_id = $1, completed_at = $2
		WHERE consumer_name = $3 AND message_id = $4 AND completed_at IS NULL`,
		transactionID, completedAt, consumerName, messageID)
	if err != nil {
		return TranslateError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConcurrentUpdate
	}
	return nil
}
