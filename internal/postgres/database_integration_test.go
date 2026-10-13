//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

func TestDatabaseUnitOfWorkAndRepositories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	environment, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer environment.Close(context.Background())
	resource, err := environment.NewDatabase(ctx, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close(context.Background())
	database, err := Open(ctx, resource.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}

	brl, _ := domain.ParseCurrency("BRL")
	balance, _ := domain.NewMoney(10000, brl)
	now := time.Now().UTC()
	walletID := uuid.NewString()
	wallet, _ := domain.NewWallet(walletID, "player-1", balance, now)
	wantRollback := errors.New("rollback")
	err = database.ReadCommitted(ctx, func(store *Store) error {
		if err := store.InsertWallet(ctx, wallet); err != nil {
			return err
		}
		return wantRollback
	})
	if !errors.Is(err, wantRollback) {
		t.Fatalf("rollback error = %v", err)
	}
	if _, err := database.Store().GetWallet(ctx, walletID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back wallet lookup = %v", err)
	}
	if err := database.WithinTx(ctx, pgx.TxOptions{}, func(store *Store) error {
		return store.InsertWallet(ctx, wallet)
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.Store().GetWallet(ctx, walletID)
	if err != nil || loaded.Balance().MinorUnits() != 10000 {
		t.Fatalf("committed wallet = %#v, %v", loaded, err)
	}
	canceled, cancelTransaction := context.WithCancel(ctx)
	cancelTransaction()
	if err := database.ReadCommitted(canceled, func(*Store) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transaction = %v", err)
	}
	eventID := uuid.NewString()
	aggregateID := uuid.NewString()
	if _, err := database.Pool().Exec(ctx, `
		INSERT INTO outbox_events (event_id, event_type, aggregate_id, correlation_id, version, envelope)
		VALUES ($1, 'WagerTransactionProcessed', $2, 'clock-skew-test', 1, '{}'::jsonb)`, eventID, aggregateID); err != nil {
		t.Fatal(err)
	}
	claims, err := database.Store().ClaimOutbox(ctx, "publisher-a", 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("initial outbox claim = %d, %v", len(claims), err)
	}
	competing, err := database.Store().ClaimOutbox(ctx, "publisher-with-skewed-clock", 1, time.Minute)
	if err != nil || len(competing) != 0 {
		t.Fatalf("active database lease was reclaimed: claims=%d err=%v", len(competing), err)
	}
	var databaseTimestamp, claimUntil time.Time
	if err := database.Pool().QueryRow(ctx, `SELECT CURRENT_TIMESTAMP, claim_until FROM outbox_events WHERE event_id = $1`, eventID).Scan(&databaseTimestamp, &claimUntil); err != nil {
		t.Fatal(err)
	}
	if claimUntil.Before(databaseTimestamp.Add(59*time.Second)) || claimUntil.After(databaseTimestamp.Add(61*time.Second)) {
		t.Fatalf("claim_until = %v, database timestamp = %v", claimUntil, databaseTimestamp)
	}

	futureEvent := databaseClockEvent{
		id: uuid.NewString(), aggregateID: uuid.NewString(), occurredAt: now.Add(24 * time.Hour),
	}
	if err := database.Store().InsertOutboxEvent(ctx, futureEvent); err != nil {
		t.Fatal(err)
	}
	futureClaims, err := database.Store().ClaimOutbox(ctx, "future-clock-publisher", 1, time.Minute)
	if err != nil || len(futureClaims) != 1 || futureClaims[0].EventID != futureEvent.id {
		t.Fatalf("future-clock event claim = %+v, %v", futureClaims, err)
	}
	if err := database.Store().MarkOutboxPublished(ctx, futureEvent.id, "future-clock-publisher"); err != nil {
		t.Fatal(err)
	}
	var futureDatabaseTimestamp, futureNextAttempt, futureCreatedAt, futurePublishedAt time.Time
	if err := database.Pool().QueryRow(ctx, `
		SELECT CURRENT_TIMESTAMP, next_attempt_at, created_at, published_at
		FROM outbox_events WHERE event_id = $1`, futureEvent.id).Scan(
		&futureDatabaseTimestamp, &futureNextAttempt, &futureCreatedAt, &futurePublishedAt,
	); err != nil {
		t.Fatal(err)
	}
	for name, timestamp := range map[string]time.Time{
		"next_attempt_at": futureNextAttempt,
		"created_at":      futureCreatedAt,
		"published_at":    futurePublishedAt,
	} {
		if timestamp.Before(futureDatabaseTimestamp.Add(-2*time.Second)) || timestamp.After(futureDatabaseTimestamp.Add(2*time.Second)) {
			t.Fatalf("%s = %v, database timestamp = %v", name, timestamp, futureDatabaseTimestamp)
		}
	}

	rescheduledEventID := uuid.NewString()
	if _, err := database.Pool().Exec(ctx, `
		INSERT INTO outbox_events (event_id, event_type, aggregate_id, correlation_id, version, envelope)
		VALUES ($1, 'WagerTransactionProcessed', $2, 'reschedule-clock-test', 1, '{}'::jsonb)`,
		rescheduledEventID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	rescheduledClaims, err := database.Store().ClaimOutbox(ctx, "reschedule-publisher", 1, time.Minute)
	if err != nil || len(rescheduledClaims) != 1 || rescheduledClaims[0].EventID != rescheduledEventID {
		t.Fatalf("reschedule event claim = %+v, %v", rescheduledClaims, err)
	}
	if err := database.Store().RescheduleOutbox(ctx, rescheduledEventID, "reschedule-publisher", 2*time.Minute, "temporary failure"); err != nil {
		t.Fatal(err)
	}
	var rescheduleDatabaseTimestamp, nextAttemptAt time.Time
	if err := database.Pool().QueryRow(ctx, `
		SELECT CURRENT_TIMESTAMP, next_attempt_at FROM outbox_events WHERE event_id = $1`,
		rescheduledEventID).Scan(&rescheduleDatabaseTimestamp, &nextAttemptAt); err != nil {
		t.Fatal(err)
	}
	if nextAttemptAt.Before(rescheduleDatabaseTimestamp.Add(119*time.Second)) || nextAttemptAt.After(rescheduleDatabaseTimestamp.Add(121*time.Second)) {
		t.Fatalf("next_attempt_at = %v, database timestamp = %v", nextAttemptAt, rescheduleDatabaseTimestamp)
	}
	database.Close()
	if err := database.Health(ctx); err == nil {
		t.Fatal("closed database reported healthy")
	}
}

type databaseClockEvent struct {
	id, aggregateID string
	occurredAt      time.Time
}

func (event databaseClockEvent) ID() string { return event.id }
func (event databaseClockEvent) Type() domain.EventType {
	return domain.EventTypeWagerTransactionProcessed
}
func (event databaseClockEvent) AggregateID() string         { return event.aggregateID }
func (event databaseClockEvent) CorrelationID() string       { return "future-clock-test" }
func (event databaseClockEvent) CausationID() (string, bool) { return "", false }
func (event databaseClockEvent) OccurredAt() time.Time       { return event.occurredAt }
func (event databaseClockEvent) Version() int                { return domain.DomainEventVersion }
func (event databaseClockEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"eventId": event.id, "occurredAt": event.occurredAt.Format(time.RFC3339Nano)})
}
