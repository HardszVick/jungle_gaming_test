//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/platform"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

const (
	financialFailureHelperModeEnv = "WAGER_FINANCIAL_FAILURE_HELPER"
	helperDatabaseURLEnv          = "WAGER_HELPER_DATABASE_URL"
	helperReadyPathEnv            = "WAGER_HELPER_READY_PATH"
	helperPlayerIDEnv             = "WAGER_HELPER_PLAYER_ID"
	helperWalletIDEnv             = "WAGER_HELPER_WALLET_ID"
	helperExternalIDEnv           = "WAGER_HELPER_EXTERNAL_ID"
	helperIdempotencyKeyEnv       = "WAGER_HELPER_IDEMPOTENCY_KEY"
)

func TestProcessSIGKILLBeforeCommitRollsBackAndRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "precommit_sigkill")
	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	playerID := uuid.NewString()
	wallet := openWallet(t, ctx, service, playerID, "100.00")
	externalID := "precommit-sigkill-" + uuid.NewString()
	idempotencyKey := "precommit-sigkill-key-" + uuid.NewString()
	applicationName := "wager-precommit-sigkill-" + uuid.NewString()
	helperURL := databaseURLWithApplicationName(t, resource.URL, applicationName)
	readyPath := t.TempDir() + "/transaction-open"

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable)
	command.Env = append(os.Environ(),
		financialFailureHelperModeEnv+"=1",
		helperDatabaseURLEnv+"="+helperURL,
		helperReadyPathEnv+"="+readyPath,
		helperPlayerIDEnv+"="+playerID,
		helperWalletIDEnv+"="+wallet.ID(),
		helperExternalIDEnv+"="+externalID,
		helperIdempotencyKeyEnv+"="+idempotencyKey,
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited && command.Process != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()

	waitFor(t, 15*time.Second, func() bool {
		_, statErr := os.Stat(readyPath)
		return statErr == nil
	}, "subprocess to reach the pre-commit boundary")
	assertOpenFinancialTransaction(t, ctx, database, applicationName)
	assertWalletBalance(t, ctx, service, wallet.ID(), "100.00")
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1`, 0, externalID)

	if err := command.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitErr := command.Wait()
	waited = true
	var exitError *exec.ExitError
	if !errors.As(waitErr, &exitError) {
		t.Fatalf("SIGKILL subprocess result = %v, output=%s", waitErr, output.String())
	}
	waitStatus, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("subprocess exit = %v, output=%s", waitErr, output.String())
	}
	waitFor(t, 10*time.Second, func() bool {
		return activeTransactions(t, ctx, database, applicationName) == 0
	}, "PostgreSQL to discard the killed process transaction")

	assertNoFailedFinancialState(t, ctx, service, database, wallet.ID(), externalID)
	process(t, ctx, service, commandInput("provider-a", externalID, playerID, wallet.ID(), "BET", "10.00", ""), idempotencyKey)
	assertRecoveredFinancialState(t, ctx, service, database, wallet.ID(), externalID)
}

func TestPostgresOutageDuringOpenFinancialTransactionRollsBackAndRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	environment, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	postgresRunning := true
	resource, err := environment.NewDatabase(ctx, "transaction_outage")
	if err != nil {
		_ = environment.Close(context.Background())
		t.Fatal(err)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cleanupCancel()
		if !postgresRunning {
			_ = environment.StartPostgresContainer(cleanupContext)
		}
		_ = resource.Close(cleanupContext)
		_ = environment.Close(cleanupContext)
	}()
	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}

	setupService, setupDatabase := newService(t, ctx, resource.URL, defaultServiceOptions())
	playerID := uuid.NewString()
	wallet := openWallet(t, ctx, setupService, playerID, "100.00")
	setupDatabase.Close()
	externalID := "postgres-outage-" + uuid.NewString()
	idempotencyKey := "postgres-outage-key-" + uuid.NewString()
	applicationName := "wager-postgres-outage-" + uuid.NewString()
	databaseURL := databaseURLWithApplicationName(t, resource.URL, applicationName)
	database := openDatabase(t, ctx, databaseURL)
	defer database.Close()
	clock := platform.NewClock()
	reached := make(chan struct{})
	release := make(chan struct{})
	ids := &pausingIDGenerator{
		delegate: platform.NewUUIDv7Generator(clock),
		reached:  reached,
		release:  release,
	}
	service, err := application.NewService(database, clock, ids, defaultServiceOptions())
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	operationContext, operationCancel := context.WithTimeout(ctx, 20*time.Second)
	defer operationCancel()
	go func() {
		_, processErr := processResult(operationContext, service,
			commandInput("provider-a", externalID, playerID, wallet.ID(), "BET", "10.00", ""), idempotencyKey)
		result <- processErr
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("financial transaction did not reach the pre-commit boundary")
	}
	assertOpenFinancialTransaction(t, ctx, database, applicationName)

	if err := environment.StopPostgres(ctx); err != nil {
		t.Fatal(err)
	}
	postgresRunning = false
	close(release)
	select {
	case processErr := <-result:
		if processErr == nil {
			t.Fatal("financial transaction succeeded while PostgreSQL was stopped")
		}
		if class := application.ClassifyProcessingError(processErr); class != application.ProcessingErrorTransient {
			t.Fatalf("PostgreSQL outage error class = %v, want transient: %v", class, processErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("financial transaction did not fail after PostgreSQL stopped")
	}
	if err := environment.StartPostgresContainer(ctx); err != nil {
		t.Fatal(err)
	}
	postgresRunning = true
	database.Close()
	restartedURL, err := url.Parse(environment.PostgresURL)
	if err != nil {
		t.Fatal(err)
	}
	originalURL, err := url.Parse(resource.URL)
	if err != nil {
		t.Fatal(err)
	}
	restartedURL.RawQuery = originalURL.RawQuery
	recoveryService, recoveryDatabase := newService(t, ctx, restartedURL.String(), defaultServiceOptions())
	defer recoveryDatabase.Close()
	assertNoFailedFinancialState(t, ctx, recoveryService, recoveryDatabase, wallet.ID(), externalID)
	process(t, ctx, recoveryService, commandInput("provider-a", externalID, playerID, wallet.ID(), "BET", "10.00", ""), idempotencyKey)
	assertRecoveredFinancialState(t, ctx, recoveryService, recoveryDatabase, wallet.ID(), externalID)
}

type pausingIDGenerator struct {
	delegate application.IDGenerator
	reached  chan struct{}
	release  <-chan struct{}
	calls    int
}

func (generator *pausingIDGenerator) New() (uuid.UUID, error) {
	id, err := generator.delegate.New()
	if err != nil {
		return uuid.Nil, err
	}
	generator.calls++
	if generator.calls == 2 {
		close(generator.reached)
		<-generator.release
	}
	return id, nil
}

type helperIDGenerator struct {
	delegate  application.IDGenerator
	readyPath string
	calls     int
}

func (generator *helperIDGenerator) New() (uuid.UUID, error) {
	id, err := generator.delegate.New()
	if err != nil {
		return uuid.Nil, err
	}
	generator.calls++
	if generator.calls == 2 {
		if err := os.WriteFile(generator.readyPath, []byte("transaction-open\n"), 0o600); err != nil {
			return uuid.Nil, err
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	return id, nil
}

func runFinancialFailureHelper() int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	database, err := postgres.Open(ctx, os.Getenv(helperDatabaseURLEnv))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer database.Close()
	if err := database.Health(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	clock := platform.NewClock()
	service, err := application.NewService(database, clock, &helperIDGenerator{
		delegate:  platform.NewUUIDv7Generator(clock),
		readyPath: os.Getenv(helperReadyPathEnv),
	}, defaultServiceOptions())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	_, err = processResult(ctx, service, commandInput(
		"provider-a", os.Getenv(helperExternalIDEnv), os.Getenv(helperPlayerIDEnv),
		os.Getenv(helperWalletIDEnv), "BET", "10.00", "",
	), os.Getenv(helperIdempotencyKeyEnv))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Fprintln(os.Stderr, "financial failure helper unexpectedly committed")
	return 3
}

func databaseURLWithApplicationName(t *testing.T, rawURL, applicationName string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("application_name", applicationName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func assertOpenFinancialTransaction(t *testing.T, ctx context.Context, database *postgres.Database, applicationName string) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		return activeTransactions(t, ctx, database, applicationName) == 1
	}, "PostgreSQL to report the open financial transaction")
}

func activeTransactions(t *testing.T, ctx context.Context, database *postgres.Database, applicationName string) int {
	t.Helper()
	return queryIntArgs(t, ctx, database.Pool(), `
		SELECT count(*) FROM pg_stat_activity
		WHERE application_name=$1 AND state='idle in transaction'`, applicationName)
}

func assertNoFailedFinancialState(
	t *testing.T,
	ctx context.Context,
	service *application.Service,
	database *postgres.Database,
	walletID, externalID string,
) {
	t.Helper()
	assertWalletBalance(t, ctx, service, walletID, "100.00")
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1`, 0, externalID)
	assertCount(t, ctx, database.Pool(), `
		SELECT count(*) FROM wallet_ledger_entries entries
		JOIN wager_transactions transactions ON transactions.id=entries.transaction_id
		WHERE transactions.external_transaction_id=$1`, 0, externalID)
	assertCount(t, ctx, database.Pool(), `
		SELECT count(*) FROM outbox_events
		WHERE envelope->'data'->>'externalTransactionId'=$1`, 0, externalID)
	assertReconciled(t, ctx, service, walletID)
}

func assertRecoveredFinancialState(
	t *testing.T,
	ctx context.Context,
	service *application.Service,
	database *postgres.Database,
	walletID, externalID string,
) {
	t.Helper()
	assertWalletBalance(t, ctx, service, walletID, "90.00")
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1 AND state='PROCESSED'`, 1, externalID)
	assertCount(t, ctx, database.Pool(), `
		SELECT count(*) FROM wallet_ledger_entries entries
		JOIN wager_transactions transactions ON transactions.id=entries.transaction_id
		WHERE transactions.external_transaction_id=$1`, 1, externalID)
	assertCount(t, ctx, database.Pool(), `
		SELECT count(*) FROM outbox_events
		WHERE envelope->'data'->>'externalTransactionId'=$1`, 1, externalID)
	assertCount(t, ctx, database.Pool(), `
		SELECT count(*) FROM outbox_events
		WHERE event_type='WalletBalanceChanged'
		  AND envelope->'data'->>'transactionId'=(
			SELECT id::text FROM wager_transactions WHERE external_transaction_id=$1
		  )`, 1, externalID)
	assertReconciled(t, ctx, service, walletID)
}
