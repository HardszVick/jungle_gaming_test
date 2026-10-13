package messaging

import (
	"context"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type OutboxRepository interface {
	ClaimOutbox(context.Context, string, int, time.Duration) ([]postgres.OutboxClaim, error)
	MarkOutboxPublished(context.Context, string, string) error
	RescheduleOutbox(context.Context, string, string, time.Duration, string) error
}

// PostgresOutbox adapts Database.Store without exposing it to the publisher.
type PostgresOutbox struct{ database *postgres.Database }

func NewPostgresOutbox(database *postgres.Database) (*PostgresOutbox, error) {
	if database == nil {
		return nil, errors.New("messaging: PostgreSQL database is required")
	}
	return &PostgresOutbox{database: database}, nil
}

func (repository *PostgresOutbox) ClaimOutbox(ctx context.Context, claimedBy string, limit int, lease time.Duration) ([]postgres.OutboxClaim, error) {
	return repository.database.Store().ClaimOutbox(ctx, claimedBy, limit, lease)
}

func (repository *PostgresOutbox) MarkOutboxPublished(ctx context.Context, eventID, claimedBy string) error {
	return repository.database.Store().MarkOutboxPublished(ctx, eventID, claimedBy)
}

func (repository *PostgresOutbox) RescheduleOutbox(ctx context.Context, eventID, claimedBy string, delay time.Duration, reason string) error {
	return repository.database.Store().RescheduleOutbox(ctx, eventID, claimedBy, delay, reason)
}
