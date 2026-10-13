//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/junglegaming/backend-challenge-go/internal/bootstrap"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

func TestPendingReferenceWinRemainsRehydratableAcrossMigrationDownAndUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "win_migration_round_trip")

	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	wallet := openWallet(t, ctx, service, "win-migration-player", "100.00")
	pending := process(t, ctx, service, commandInput(
		"provider-a", "win-migration", "win-migration-player", wallet.ID(), "WIN", "10.00", "missing-bet",
	), "win-migration")
	if pending.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("seeded WIN state = %s, want %s", pending.Transaction.State(), domain.TransactionStatePendingReference)
	}
	database.Close()

	directory := testsupport.ProjectRoot() + "/migrations"
	if err := bootstrap.RunMigrationsFromDirectory(ctx, resource.URL, "down", directory); err != nil {
		t.Fatalf("remove financial invariants migration: %v", err)
	}
	if err := bootstrap.RunMigrationsFromDirectory(ctx, resource.URL, "down", directory); err != nil {
		t.Fatalf("migrate to version one: %v", err)
	}
	assertRehydratableRejectedWin(t, ctx, resource.URL, pending.Transaction.ID())

	if err := bootstrap.RunMigrationsFromDirectory(ctx, resource.URL, "up", directory); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	assertRehydratableRejectedWin(t, ctx, resource.URL, pending.Transaction.ID())
}

func assertRehydratableRejectedWin(t *testing.T, ctx context.Context, databaseURL, transactionID string) {
	t.Helper()
	database := openDatabase(t, ctx, databaseURL)
	defer database.Close()
	record, err := database.Store().GetTransaction(ctx, transactionID)
	if err != nil {
		t.Fatalf("rehydrate downgraded WIN: %v", err)
	}
	if record.Transaction.State() != domain.TransactionStateRejected || record.Transaction.FailureCode() != domain.FailureReferenceNotFound {
		t.Fatalf(
			"downgraded WIN terminal result = %s/%s, want %s/%s",
			record.Transaction.State(), record.Transaction.FailureCode(),
			domain.TransactionStateRejected, domain.FailureReferenceNotFound,
		)
	}
	if record.Transaction.Details().ReferenceExternalTransactionID != "" || record.ReferenceTransactionID != "" {
		t.Fatalf("downgraded WIN retained a reference: %#v", record)
	}
}

func TestFinancialInvariantsRejectDirectSQLViolations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "financial_invariants")
	pool := openPool(t, ctx, resource.URL)
	defer pool.Close()

	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	wallet := openWallet(t, ctx, service, "financial-invariants-player", "100.00")
	bet := process(t, ctx, service, commandInput(
		"provider-a", "financial-invariants-bet", "financial-invariants-player", wallet.ID(), "BET", "20.00", "",
	), "financial-invariants-bet")
	rollback := process(t, ctx, service, commandInput(
		"provider-a", "financial-invariants-rollback", "financial-invariants-player", wallet.ID(), "ROLLBACK", "20.00", "financial-invariants-bet",
	), "financial-invariants-rollback")
	loss := process(t, ctx, service, commandInput(
		"provider-a", "financial-invariants-loss", "financial-invariants-player", wallet.ID(), "LOSS", "0.00", "",
	), "financial-invariants-loss")

	otherWalletID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor)
		VALUES ($1, 'other-player', 'BRL', 10000)`, otherWalletID); err != nil {
		t.Fatal(err)
	}
	assertDatabaseConstraint(t, ctx, pool, "23514", "wager_transactions_wallet_identity_consistent", `
		INSERT INTO wager_transactions (
			id, wallet_id, player_id, provider_id, external_transaction_id,
			idempotency_key, request_hash, round_id, game_id, kind, amount_minor, amount_currency
		) VALUES ($1, $2, 'financial-invariants-player', 'provider-a', $3, $3,
			decode(repeat('01', 32), 'hex'), 'round', 'game', 'BET', 100, 'BRL')`,
		uuid.NewString(), otherWalletID, "mismatched-wallet")

	pendingID := insertPendingBet(t, ctx, pool, wallet.ID(), "financial-invariants-player", "pending-ledger")
	assertLedgerInsertConstraint(t, ctx, pool, wallet.ID(), pendingID, "DEBIT", 100, 10000, 9900)
	assertLedgerInsertConstraint(t, ctx, pool, wallet.ID(), loss.Transaction.ID(), "CREDIT", 1, 10000, 10001)
	assertLedgerInsertConstraint(t, ctx, pool, wallet.ID(), bet.Transaction.ID(), "CREDIT", 2000, 0, 2000)
	assertLedgerInsertConstraint(t, ctx, pool, wallet.ID(), bet.Transaction.ID(), "DEBIT", 1, 10000, 9999)
	assertLedgerInsertConstraint(t, ctx, pool, wallet.ID(), rollback.Transaction.ID(), "DEBIT", 2000, 2000, 0)

	assertDatabaseConstraint(t, ctx, pool, "55000", "wallets_identity_immutable",
		`UPDATE wallets SET player_id = 'tampered-player' WHERE id = $1`, wallet.ID())
	assertDatabaseConstraint(t, ctx, pool, "55000", "wager_transactions_identity_immutable",
		`UPDATE wager_transactions SET amount_minor = amount_minor + 1 WHERE id = $1`, pendingID)
	assertDatabaseConstraint(t, ctx, pool, "55000", "wager_transactions_terminal_immutable",
		`UPDATE wager_transactions SET updated_at = updated_at WHERE id = $1`, bet.Transaction.ID())

	if _, err := pool.Exec(ctx, `
		INSERT INTO inbox_messages (
			consumer_name, message_id, payload_hash, transaction_id, received_at, completed_at
		) VALUES ('hardening-consumer', 'hardening-message', decode(repeat('02', 32), 'hex'), $1,
			CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bet.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	assertDatabaseConstraint(t, ctx, pool, "55000", "inbox_messages_snapshot_immutable", `
		UPDATE inbox_messages SET payload_hash = decode(repeat('03', 32), 'hex')
		WHERE consumer_name = 'hardening-consumer' AND message_id = 'hardening-message'`)

	var eventID string
	if err := pool.QueryRow(ctx, `SELECT event_id::text FROM outbox_events ORDER BY created_at, event_id LIMIT 1`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	assertDatabaseConstraint(t, ctx, pool, "55000", "outbox_events_snapshot_immutable",
		`UPDATE outbox_events SET envelope = '{"tampered":true}'::jsonb WHERE event_id = $1`, eventID)
}

func TestFinancialInvariantsMigrationRuntimeRoleRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resource, err := suite.NewRoleDatabase(ctx, "financial_invariants_round_trip")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Close(context.Background()) })
	directory := testsupport.ProjectRoot() + "/migrations"
	if err := bootstrap.RunMigrationsFromDirectory(ctx, resource.OwnerURL, "up", directory); err != nil {
		t.Fatal(err)
	}

	ownerPool := openPool(t, ctx, resource.OwnerURL)
	defer ownerPool.Close()
	runtimePool := openPool(t, ctx, resource.RuntimeURL)
	defer runtimePool.Close()
	walletID := uuid.NewString()
	if _, err := ownerPool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor)
		VALUES ($1, 'round-trip-player', 'BRL', 10000)`, walletID); err != nil {
		t.Fatal(err)
	}
	pendingBeforeDown := insertPendingBet(t, ctx, ownerPool, walletID, "round-trip-player", "before-down")
	if _, err := ownerPool.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash)
		VALUES ('round-trip-consumer', 'round-trip-message', decode(repeat('05', 32), 'hex'))`); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	if _, err := ownerPool.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, aggregate_id, correlation_id, version, envelope
		) VALUES ($1, 'WagerTransactionProcessed', $2, 'round-trip-correlation', 1, '{}'::jsonb)`,
		eventID, walletID); err != nil {
		t.Fatal(err)
	}
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE wallets SET player_id = player_id WHERE id = '`+walletID+`'`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE wager_transactions SET amount_minor = amount_minor WHERE id = '`+pendingBeforeDown+`'`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE inbox_messages SET payload_hash = payload_hash WHERE consumer_name = 'round-trip-consumer' AND message_id = 'round-trip-message'`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE outbox_events SET envelope = envelope WHERE event_id = '`+eventID+`'`)

	if err := bootstrap.RunMigrationsFromDirectory(ctx, resource.OwnerURL, "down", directory); err != nil {
		t.Fatalf("migration 3 down: %v", err)
	}
	assertLatestMigrationVersion(t, ctx, runtimePool, 2)
	if _, err := runtimePool.Exec(ctx, `UPDATE wallets SET player_id = player_id WHERE id = $1`, walletID); err != nil {
		t.Fatalf("migration 2 UPDATE grant was not restored: %v", err)
	}
	for _, statement := range []string{
		`UPDATE wager_transactions SET amount_minor = amount_minor WHERE id = '` + pendingBeforeDown + `'`,
		`UPDATE inbox_messages SET payload_hash = payload_hash WHERE consumer_name = 'round-trip-consumer' AND message_id = 'round-trip-message'`,
		`UPDATE outbox_events SET envelope = envelope WHERE event_id = '` + eventID + `'`,
	} {
		if _, err := runtimePool.Exec(ctx, statement); err != nil {
			t.Fatalf("migration 2 UPDATE grant was not restored for %q: %v", statement, err)
		}
	}
	if _, err := runtimePool.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor
		) VALUES ($1, $2, $3, 'DEBIT', 100, 'BRL', 10000, 9900)`,
		uuid.NewString(), walletID, pendingBeforeDown); err != nil {
		t.Fatalf("migration 2 ledger behavior was not restored: %v", err)
	}

	if err := bootstrap.RunMigrationsFromDirectory(ctx, resource.OwnerURL, "up", directory); err != nil {
		t.Fatalf("migration 3 reapplied: %v", err)
	}
	assertLatestMigrationVersion(t, ctx, runtimePool, 3)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE wallets SET player_id = player_id WHERE id = '`+walletID+`'`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE wager_transactions SET amount_minor = amount_minor WHERE id = '`+pendingBeforeDown+`'`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE inbox_messages SET payload_hash = payload_hash WHERE consumer_name = 'round-trip-consumer' AND message_id = 'round-trip-message'`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE outbox_events SET envelope = envelope WHERE event_id = '`+eventID+`'`)
	pendingAfterUp := insertPendingBet(t, ctx, ownerPool, walletID, "round-trip-player", "after-up")
	assertLedgerInsertConstraint(t, ctx, runtimePool, walletID, pendingAfterUp, "DEBIT", 100, 10000, 9900)
}

func insertPendingBet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, walletID, playerID, suffix string) string {
	t.Helper()
	transactionID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, wallet_id, player_id, provider_id, external_transaction_id,
			idempotency_key, request_hash, round_id, game_id, kind, amount_minor, amount_currency
		) VALUES ($1, $2, $3, 'provider-hardening', $4, $4,
			decode(repeat('04', 32), 'hex'), 'round-hardening', 'game-hardening', 'BET', 100, 'BRL')`,
		transactionID, walletID, playerID, suffix); err != nil {
		t.Fatal(err)
	}
	return transactionID
}

func assertLedgerInsertConstraint(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	walletID, transactionID, direction string,
	amount, before, after int64,
) {
	t.Helper()
	assertDatabaseConstraint(t, ctx, pool, "23514", "wallet_ledger_entries_transaction_consistent", `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor
		) VALUES ($1, $2, $3, $4, $5, 'BRL', $6, $7)`,
		uuid.NewString(), walletID, transactionID, direction, amount, before, after)
}

func assertDatabaseConstraint(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	code, constraint, statement string,
	args ...any,
) {
	t.Helper()
	_, err := pool.Exec(ctx, statement, args...)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code || postgresError.ConstraintName != constraint {
		t.Fatalf("statement error = %v; want SQLSTATE %s constraint %s", err, code, constraint)
	}
}

func assertLatestMigrationVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("latest migration version = %d, want %d", got, want)
	}
}
