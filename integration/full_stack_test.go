//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/bootstrap"
	"github.com/junglegaming/backend-challenge-go/internal/config"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/httpapi"
	"github.com/junglegaming/backend-challenge-go/internal/messaging"
	"github.com/junglegaming/backend-challenge-go/internal/platform"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
	"go.uber.org/fx"
)

var suite *testsupport.Environment

func TestMain(testingMain *testing.M) {
	if os.Getenv(financialFailureHelperModeEnv) != "" {
		os.Exit(runFinancialFailureHelper())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	var err error
	suite, err = testsupport.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration infrastructure:", err)
		os.Exit(1)
	}
	code := testingMain.Run()
	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cleanupCancel()
	if err := suite.Close(cleanupContext); err != nil {
		fmt.Fprintln(os.Stderr, "integration teardown:", err)
		if code == 0 {
			code = 1
		}
	}
	if serviceBinary.directory != "" {
		_ = os.RemoveAll(serviceBinary.directory)
	}
	os.Exit(code)
}

func TestMigrationsConstraintsLedgerImmutabilityAndAtomicRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := newDatabase(t, ctx, "schema")

	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatalf("first migration up: %v", err)
	}
	pool := openPool(t, ctx, resource.URL)
	defer pool.Close()
	assertTable(t, ctx, pool, "wallets", true)

	if _, err := pool.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance_minor) VALUES ($1, 'negative', 'BRL', -1)`, uuid.NewString()); err == nil {
		t.Fatal("negative wallet balance bypassed the database constraint")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO wager_transactions (id, wallet_id, player_id, kind, amount_minor, amount_currency) VALUES ($1, $2, 'player', 'BET', 100, 'BRL')`, uuid.NewString(), uuid.NewString()); err == nil {
		t.Fatal("external transaction without origin metadata bypassed constraints")
	}

	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	wallet := openWallet(t, ctx, service, "immutable-player", "100.00")
	var ledgerID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM wallet_ledger_entries WHERE wallet_id=$1`, wallet.ID()).Scan(&ledgerID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor=1 WHERE id=$1`, ledgerID); err == nil {
		t.Fatal("ledger UPDATE succeeded")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE id=$1`, ledgerID); err == nil {
		t.Fatal("ledger DELETE succeeded")
	}
	assertReconciled(t, ctx, service, wallet.ID())

	partialID := uuid.NewString()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance_minor) VALUES ($1, 'atomic-player', 'BRL', 0)`, partialID)
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance_minor) VALUES ($1, 'invalid-part', 'BRL', -1)`, uuid.NewString())
	}
	if err == nil {
		t.Fatal("expected the pre-commit failure")
	}
	_ = tx.Rollback(ctx)
	assertCount(t, ctx, pool, `SELECT count(*) FROM wallets WHERE id=$1`, 0, partialID)

	database.Close()
	pool.Close()
	if err := resource.MigrateDown(ctx); err != nil {
		t.Fatalf("migration down: %v", err)
	}
	pool = openPool(t, ctx, resource.URL)
	assertTable(t, ctx, pool, "wallets", false)
	pool.Close()
	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatalf("second migration up: %v", err)
	}
	pool = openPool(t, ctx, resource.URL)
	defer pool.Close()
	assertTable(t, ctx, pool, "wallet_ledger_entries", true)
}

func TestStartupRequiresCurrentMigrationVersionAndEssentialObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := newDatabase(t, ctx, "schema_version")
	database := openDatabase(t, ctx, resource.URL)
	defer database.Close()
	directory := testsupport.ProjectRoot() + "/migrations"
	catalog, err := bootstrap.LoadMigrationCatalog(directory)
	if err != nil {
		t.Fatal(err)
	}

	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err == nil || !strings.Contains(err.Error(), "schema is absent") {
		t.Fatalf("absent schema verification = %v", err)
	}
	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool().Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, catalog.LatestVersion()); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err == nil || !strings.Contains(err.Error(), "schema is behind") {
		t.Fatalf("behind schema verification = %v", err)
	}
	if _, err := database.Pool().Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, 'restored')`, catalog.LatestVersion()); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err == nil || !strings.Contains(err.Error(), "migration history does not match catalog") {
		t.Fatalf("incorrect migration name verification = %v", err)
	}
	migrations := catalog.Migrations()
	latest := migrations[len(migrations)-1]
	if _, err := database.Pool().Exec(ctx, `UPDATE schema_migrations SET name=$1 WHERE version=$2`, filepath.Base(latest.UpPath), latest.Version); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err != nil {
		t.Fatalf("current schema verification: %v", err)
	}
	first := migrations[0]
	if _, err := database.Pool().Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, first.Version); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err == nil || !strings.Contains(err.Error(), "migration history does not match catalog") {
		t.Fatalf("incomplete migration history verification = %v", err)
	}
	if _, err := database.Pool().Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, first.Version, filepath.Base(first.UpPath)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool().Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, 'future')`, catalog.LatestVersion()+1); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err == nil || !strings.Contains(err.Error(), "schema is ahead") {
		t.Fatalf("ahead schema verification = %v", err)
	}
	if _, err := database.Pool().Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, catalog.LatestVersion()+1); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool().Exec(ctx, `DROP TABLE outbox_events`); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.VerifySchemaFromDirectory(ctx, database, directory); err == nil || !strings.Contains(err.Error(), "missing essential objects") {
		t.Fatalf("incomplete current schema verification = %v", err)
	}
}

func TestMigrationOwnerAndRuntimePermissions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resource, err := suite.NewRoleDatabase(ctx, "permissions")
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
	var ownerUser, runtimeUser, tableOwner string
	if err := ownerPool.QueryRow(ctx, `SELECT current_user`).Scan(&ownerUser); err != nil {
		t.Fatal(err)
	}
	if err := runtimePool.QueryRow(ctx, `SELECT current_user`).Scan(&runtimeUser); err != nil {
		t.Fatal(err)
	}
	if ownerUser == runtimeUser || ownerUser != resource.Owner || runtimeUser != resource.Runtime {
		t.Fatalf("database roles owner=%q runtime=%q", ownerUser, runtimeUser)
	}
	var configuredRuntimeRole string
	if err := ownerPool.QueryRow(ctx, `SELECT current_setting('app.application_role')`).Scan(&configuredRuntimeRole); err != nil {
		t.Fatal(err)
	}
	if configuredRuntimeRole != resource.Runtime {
		t.Fatalf("migration connection application role = %q, want %q", configuredRuntimeRole, resource.Runtime)
	}
	if err := ownerPool.QueryRow(ctx, `SELECT tableowner FROM pg_tables WHERE schemaname=current_schema() AND tablename='wallets'`).Scan(&tableOwner); err != nil {
		t.Fatal(err)
	}
	if tableOwner != resource.Owner {
		t.Fatalf("wallets owner = %q, want %q", tableOwner, resource.Owner)
	}

	service, database := newService(t, ctx, resource.RuntimeURL, defaultServiceOptions())
	defer database.Close()
	wallet := openWallet(t, ctx, service, "least-privilege-player", "100.00")
	process(t, ctx, service, commandInput("provider-a", "permissions-bet", "least-privilege-player", wallet.ID(), "BET", "20.00", ""), "permissions-bet")
	process(t, ctx, service, commandInput("provider-a", "permissions-win", "least-privilege-player", wallet.ID(), "WIN", "5.00", "permissions-bet"), "permissions-win")
	lossCommand, err := application.NormalizeTransactionCommand(commandInput("provider-a", "permissions-loss", "least-privilege-player", wallet.ID(), "LOSS", "0.00", ""))
	if err != nil {
		t.Fatal(err)
	}
	inbox := &application.InboxMessage{ConsumerName: "permissions-consumer", MessageID: "permissions-message"}
	inbox.PayloadHash[0] = 1
	if _, err := service.ProcessTransaction(ctx, application.ProcessTransactionRequest{Command: lossCommand, IdempotencyKey: "permissions-loss", Inbox: inbox}); err != nil {
		t.Fatal(err)
	}
	process(t, ctx, service, commandInput("provider-a", "permissions-refund", "least-privilege-player", wallet.ID(), "REFUND", "20.00", "permissions-bet"), "permissions-refund")
	process(t, ctx, service, commandInput("provider-a", "permissions-rollback", "least-privilege-player", wallet.ID(), "ROLLBACK", "20.00", "permissions-refund"), "permissions-rollback")
	pending := process(t, ctx, service, commandInput("provider-a", "permissions-pending", "least-privilege-player", wallet.ID(), "WIN", "1.00", "missing-bet"), "permissions-pending")
	time.Sleep(75 * time.Millisecond)
	claims, err := service.ClaimPendingReferences(ctx, "permissions-worker", 10, time.Second)
	if err != nil || len(claims) != 1 || claims[0].Record.Transaction.ID() != pending.Transaction.ID() {
		t.Fatalf("runtime pending claims = %d, %v", len(claims), err)
	}
	outboxClaims, err := database.Store().ClaimOutbox(ctx, "permissions-publisher", 100, time.Second)
	if err != nil || len(outboxClaims) == 0 {
		t.Fatalf("runtime outbox claims = %d, %v", len(outboxClaims), err)
	}
	if _, err := service.GetTransactionByExternalID(ctx, "provider-a", "permissions-win"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListLedger(ctx, wallet.ID(), nil, 100); err != nil {
		t.Fatal(err)
	}
	assertReconciled(t, ctx, service, wallet.ID())
	assertCount(t, ctx, runtimePool, `SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL`, 1)
	assertCount(t, ctx, runtimePool, `SELECT count(*) FROM schema_migrations`, len(catalogMigrations(t, directory)))

	assertPermissionDenied(t, ctx, runtimePool, `CREATE TABLE runtime_forbidden (id integer)`)
	assertPermissionDenied(t, ctx, runtimePool, `ALTER TABLE wallets ADD COLUMN runtime_forbidden integer`)
	assertPermissionDenied(t, ctx, runtimePool, `UPDATE wallet_ledger_entries SET amount_minor=amount_minor`)
	assertPermissionDenied(t, ctx, runtimePool, `DELETE FROM wallet_ledger_entries`)
	assertPermissionDenied(t, ctx, runtimePool, `TRUNCATE wallet_ledger_entries`)
	assertPermissionDenied(t, ctx, runtimePool, `INSERT INTO schema_migrations (version, name) VALUES (999999, 'forbidden')`)
}

func catalogMigrations(t *testing.T, directory string) []bootstrap.Migration {
	t.Helper()
	catalog, err := bootstrap.LoadMigrationCatalog(directory)
	if err != nil {
		t.Fatal(err)
	}
	return catalog.Migrations()
}

func assertPermissionDenied(t *testing.T, ctx context.Context, pool *pgxpool.Pool, statement string) {
	t.Helper()
	_, err := pool.Exec(ctx, statement)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("statement %q error = %v; want SQLSTATE 42501", statement, err)
	}
}

func TestRealKeycloakAuthenticationAndAuthorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "auth")
	queues := newQueues(t, ctx, "auth", 2*time.Second)
	client := newSQSClient(t, ctx, queues)
	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	authenticator, err := auth.New(ctx, suite.KeycloakURL+"/realms/wager", "backend-api")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewHandler(service, authenticator, httpapi.ReadinessChecks{PostgreSQL: database, SQS: client}, httpapi.Options{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	providerA := token(t, ctx, "provider-a", testsupport.ProviderASecret)
	providerB := token(t, ctx, "provider-b", testsupport.ProviderBSecret)
	internal := token(t, ctx, "internal-service", testsupport.InternalServiceSecret)
	walletBody := `{"playerId":"auth-player","initialBalance":{"amount":"100.00","currency":"BRL"}}`

	assertHTTPStatus(t, request(t, http.MethodPost, server.URL+"/wallets", walletBody, ""), http.StatusUnauthorized)
	assertHTTPStatus(t, request(t, http.MethodPost, server.URL+"/wallets", walletBody, "not-a-jwt"), http.StatusUnauthorized)
	assertHTTPStatus(t, request(t, http.MethodPost, server.URL+"/wallets", walletBody, providerA), http.StatusForbidden)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wallets`, 0)

	response := request(t, http.MethodPost, server.URL+"/wallets", walletBody, internal)
	if response.StatusCode != http.StatusCreated {
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("open wallet status = %d: %s", response.StatusCode, payload)
	}
	var wallet struct {
		ID string `json:"id"`
	}
	decodeResponse(t, response, &wallet)

	transactionBody := fmt.Sprintf(`{"providerId":"provider-b","externalTransactionId":"provider-isolation","playerId":"auth-player","walletId":%q,"roundId":"round","gameId":"game","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}`, wallet.ID)
	requestValue := requestWithHeaders(t, http.MethodPost, server.URL+"/wagering/transactions", transactionBody, providerA, map[string]string{"Idempotency-Key": "provider-isolation"})
	assertHTTPStatus(t, requestValue, http.StatusForbidden)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id='provider-isolation'`, 0)

	providerBody := strings.Replace(transactionBody, `"provider-b"`, `"provider-a"`, 1)
	processed := requestWithHeaders(t, http.MethodPost, server.URL+"/wagering/transactions", providerBody, providerA, map[string]string{"Idempotency-Key": "provider-a-bet"})
	if processed.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(processed.Body)
		processed.Body.Close()
		t.Fatalf("provider transaction status = %d: %s", processed.StatusCode, payload)
	}
	var transaction struct {
		ExternalTransactionID string `json:"externalTransactionId"`
	}
	decodeResponse(t, processed, &transaction)
	assertHTTPStatus(t, request(t, http.MethodGet, server.URL+"/providers/provider-a/wagering/transactions/provider-isolation", "", providerB), http.StatusForbidden)
	assertHTTPStatus(t, request(t, http.MethodGet, server.URL+"/wallets/"+wallet.ID, "", providerB), http.StatusForbidden)
	assertHTTPStatus(t, request(t, http.MethodGet, server.URL+"/wallets/"+wallet.ID, "", internal), http.StatusOK)
	authorizationEffects := func() string {
		return fmt.Sprintf("%d/%d/%d/%d/%d/%d",
			queryIntArgs(t, ctx, database.Pool(), `SELECT count(*) FROM wallets WHERE id=$1`, wallet.ID),
			queryIntArgs(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE wallet_id=$1`, wallet.ID),
			queryIntArgs(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, wallet.ID),
			queryIntArgs(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'walletId'=$1`, wallet.ID),
			queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM inbox_messages`),
			queryIntArgs(t, ctx, database.Pool(), `SELECT balance_minor FROM wallets WHERE id=$1`, wallet.ID),
		)
	}
	beforeDeniedReads := authorizationEffects()
	if beforeDeniedReads != "1/2/2/4/0/9000" {
		t.Fatalf("pre-authorization wallet/transaction/ledger/outbox/inbox/balance effects = %s", beforeDeniedReads)
	}
	assertHTTPError(t, request(t, http.MethodGet, server.URL+"/wallets/"+wallet.ID+"/ledger", "", providerB), http.StatusForbidden, "FORBIDDEN", "access is forbidden")
	if afterLedger := authorizationEffects(); afterLedger != beforeDeniedReads {
		t.Fatalf("denied ledger read changed effects from %s to %s", beforeDeniedReads, afterLedger)
	}
	assertHTTPError(t, request(t, http.MethodPost, server.URL+"/wallets/"+wallet.ID+"/reconciliation", "", providerB), http.StatusForbidden, "FORBIDDEN", "access is forbidden")
	if afterReconciliation := authorizationEffects(); afterReconciliation != beforeDeniedReads {
		t.Fatalf("denied reconciliation changed effects from %s to %s", beforeDeniedReads, afterReconciliation)
	}

	expired := token(t, ctx, "provider-a", testsupport.ProviderASecret)
	waitForTokenExpiration(t, expired)
	assertHTTPStatus(t, request(t, http.MethodGet, server.URL+"/providers/provider-a/wagering/transactions/provider-isolation", "", expired), http.StatusUnauthorized)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, 2, wallet.ID)
	assertReconciled(t, ctx, service, wallet.ID)
}

func TestFiftyParallelIdenticalDeliveriesAcrossHTTPAndSQS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "duplicates")
	queues := newQueues(t, ctx, "duplicates", time.Second)
	client := newSQSClient(t, ctx, queues)
	services, databases := newServicesAt(t, ctx, resource.URL, 3, defaultServiceOptions(), time.Now().UTC().Add(time.Hour))
	defer closeDatabases(databases)
	playerID := uuid.NewString()
	wallet := openWallet(t, ctx, services[0], playerID, "100.00")
	authenticator, err := auth.New(ctx, suite.KeycloakURL+"/realms/wager", "backend-api")
	if err != nil {
		t.Fatal(err)
	}
	servers := make([]*httptest.Server, 0, 3)
	consumers := make([]*messaging.Consumer, 0, 3)
	for index := range services {
		handler, handlerErr := httpapi.NewHandler(services[index], authenticator, httpapi.ReadinessChecks{PostgreSQL: databases[index], SQS: client}, httpapi.Options{})
		if handlerErr != nil {
			t.Fatal(handlerErr)
		}
		servers = append(servers, httptest.NewServer(handler))
		consumer := newConsumer(t, client, services[index], queues.TransactionsURL, queues.MaxReceiveCount, nil, nil)
		if err := consumer.Start(ctx); err != nil {
			t.Fatal(err)
		}
		consumers = append(consumers, consumer)
	}
	defer func() {
		shutdown, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		for _, consumer := range consumers {
			_ = consumer.Shutdown(shutdown)
		}
		for _, server := range servers {
			server.Close()
		}
	}()

	providerToken := token(t, ctx, "provider-a", testsupport.ProviderASecret)
	body := transactionJSON("provider-a", "duplicate-bet", playerID, wallet.ID(), "BET", "25.00", "")
	start := make(chan struct{})
	errorsChannel := make(chan error, 50)
	var successfulHTTP atomic.Int64
	var wait sync.WaitGroup
	for index := 0; index < 25; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			response, err := doRequest(http.MethodPost, servers[index%len(servers)].URL+"/wagering/transactions", body, providerToken, map[string]string{"Idempotency-Key": "duplicate-key"})
			if err != nil {
				errorsChannel <- err
				return
			}
			defer response.Body.Close()
			switch response.StatusCode {
			case http.StatusOK:
				successfulHTTP.Add(1)
			default:
				payload, _ := io.ReadAll(response.Body)
				errorsChannel <- fmt.Errorf("HTTP duplicate status %d: %s", response.StatusCode, payload)
			}
		}(index)
	}
	for index := 0; index < 25; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			messageID := fmt.Sprintf("duplicate-message-%02d", index)
			envelope := sqsEnvelope(messageID, "duplicate-key", "provider-a", "duplicate-bet", playerID, wallet.ID(), "BET", "25.00", "")
			if err := client.Publish(ctx, queues.TransactionsURL, []byte(envelope), wallet.ID(), messageID); err != nil {
				errorsChannel <- err
			}
		}(index)
	}
	close(start)
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if successfulHTTP.Load() != 25 {
		t.Fatalf("HTTP duplicate outcomes: success=%d; want 25 equivalent replays", successfulHTTP.Load())
	}
	waitFor(t, 20*time.Second, func() bool {
		return queryInt(t, ctx, databases[0].Pool(), `SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL`) == 25
	}, "all SQS inbox deliveries to complete")
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id='duplicate-bet'`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries entries JOIN wager_transactions transactions ON transactions.id=entries.transaction_id WHERE transactions.external_transaction_id='duplicate-bet'`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events`, 4)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE event_type='WagerTransactionProcessed'`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE event_type='WalletBalanceChanged'`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'transactionId'=(SELECT id::text FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id='duplicate-bet')`, 2)
	assertWalletBalance(t, ctx, services[0], wallet.ID(), "75.00")

	process(t, ctx, services[1], commandInput("provider-a", "after-duplicate-win", playerID, wallet.ID(), "WIN", "10.00", ""), "after-duplicate-win")
	replay := requestWithHeaders(t, http.MethodPost, servers[0].URL+"/wagering/transactions", body, providerToken, map[string]string{"Idempotency-Key": "duplicate-key"})
	if replay.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(replay.Body)
		replay.Body.Close()
		t.Fatalf("replay status = %d: %s", replay.StatusCode, payload)
	}
	var replayBody struct {
		IdempotentReplay bool `json:"idempotentReplay"`
		Balance          struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	decodeResponse(t, replay, &replayBody)
	if !replayBody.IdempotentReplay || replayBody.Balance.Amount != "75.00" {
		t.Fatalf("historical replay = %+v", replayBody)
	}
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL`, 25)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions`, 3)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 3)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events`, 6)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'transactionId'=(SELECT id::text FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id='duplicate-bet')`, 2)
	assertReconciled(t, ctx, services[2], wallet.ID())
}

func TestDistributedWalletConcurrencyAndIndependentLocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "concurrency")
	services, databases := newServicesAt(t, ctx, resource.URL, 3, defaultServiceOptions(), time.Now().UTC().Add(time.Hour))
	defer closeDatabases(databases)
	wallet := openWallet(t, ctx, services[0], "hot-player", "100.00")

	start := make(chan struct{})
	results := make(chan application.TransactionResult, 2)
	errorsChannel := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func(index int) {
			<-start
			command, err := application.NormalizeTransactionCommand(commandInput("provider-a", fmt.Sprintf("hot-bet-%d", index), "hot-player", wallet.ID(), "BET", "80.00", ""))
			if err != nil {
				errorsChannel <- err
				return
			}
			result, err := services[index].ProcessTransaction(ctx, application.ProcessTransactionRequest{Command: command, IdempotencyKey: fmt.Sprintf("hot-key-%d", index)})
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- result
		}(index)
	}
	close(start)
	var got []application.TransactionResult
	for len(got) < 2 {
		select {
		case err := <-errorsChannel:
			t.Fatal(err)
		case result := <-results:
			got = append(got, result)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	states := []string{
		string(got[0].Transaction.State()) + "/" + string(got[0].Transaction.FailureCode()),
		string(got[1].Transaction.State()) + "/" + string(got[1].Transaction.FailureCode()),
	}
	slices.Sort(states)
	if !slices.Equal(states, []string{"PROCESSED/", "REJECTED/INSUFFICIENT_FUNDS"}) {
		t.Fatalf("concurrent results = %v", states)
	}
	assertWalletBalance(t, ctx, services[2], wallet.ID(), "20.00")
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='DEBIT'`, 1, wallet.ID())
	assertReconciled(t, ctx, services[0], wallet.ID())

	walletA := openWallet(t, ctx, services[0], "parallel-a", "100.00")
	walletB := openWallet(t, ctx, services[0], "parallel-b", "100.00")
	lockTx, err := databases[0].Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT id FROM wallets WHERE id=$1 FOR UPDATE`, walletA.ID()); err != nil {
		t.Fatal(err)
	}
	blockedDone := make(chan error, 1)
	go func() {
		_, err := processResult(ctx, services[1], commandInput("provider-a", "parallel-a-bet", "parallel-a", walletA.ID(), "BET", "10.00", ""), "parallel-a-bet")
		blockedDone <- err
	}()
	time.Sleep(100 * time.Millisecond)
	independentDone := make(chan error, 1)
	startedAt := time.Now()
	go func() {
		_, err := processResult(ctx, services[2], commandInput("provider-a", "parallel-b-bet", "parallel-b", walletB.ID(), "BET", "10.00", ""), "parallel-b-bet")
		independentDone <- err
	}()
	select {
	case err := <-independentDone:
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(startedAt) > 2*time.Second {
			t.Fatalf("independent wallet was unexpectedly delayed: %v", time.Since(startedAt))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("operation on wallet B waited for wallet A row lock; possible global lock")
	}
	select {
	case err := <-blockedDone:
		t.Fatalf("wallet A operation was not blocked by its row lock: %v", err)
	default:
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-blockedDone; err != nil {
		t.Fatal(err)
	}
	assertWalletBalance(t, ctx, services[0], walletA.ID(), "90.00")
	assertWalletBalance(t, ctx, services[0], walletB.ID(), "90.00")
	assertReconciled(t, ctx, services[0], walletA.ID())
	assertReconciled(t, ctx, services[0], walletB.ID())
}

func TestFinancialFlowRollsBackAfterIntermediateIDFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "financial_atomicity")
	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	wallet := openWallet(t, ctx, service, "atomic-financial-player", "100.00")

	injected := errors.New("injected ledger ID failure")
	clock := platform.NewClock()
	ids := &failAtIDGenerator{delegate: platform.NewUUIDv7Generator(clock), failAt: 2, err: injected}
	failingService, err := application.NewService(database, clock, ids, defaultServiceOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := processResult(ctx, failingService, commandInput("provider-a", "atomic-failed-bet", "atomic-financial-player", wallet.ID(), "BET", "10.00", ""), "atomic-failed-key")
	if !errors.Is(err, injected) || result.Transaction != nil || ids.calls != 2 {
		t.Fatalf("intermediate failure result = %+v, err = %v, ID calls = %d", result, err, ids.calls)
	}

	assertWalletBalance(t, ctx, service, wallet.ID(), "100.00")
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions`, 1)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id='atomic-failed-bet'`, 0)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 1)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events`, 2)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'externalTransactionId'='atomic-failed-bet'`, 0)
	assertReconciled(t, ctx, service, wallet.ID())

	process(t, ctx, service, commandInput("provider-a", "atomic-failed-bet", "atomic-financial-player", wallet.ID(), "BET", "10.00", ""), "atomic-failed-key")
	assertWalletBalance(t, ctx, service, wallet.ID(), "90.00")
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions`, 2)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 2)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events`, 4)
	assertReconciled(t, ctx, service, wallet.ID())
}

func TestConsumerCrashAfterCommitBeforeDeleteRedeliversWithoutDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	databaseURL := migratedPublicDatabase(t, ctx)
	queues := newQueues(t, ctx, "consumer-crash", time.Second)
	client := newSQSClient(t, ctx, queues)
	services, databases := newServices(t, ctx, databaseURL, 2, defaultServiceOptions())
	defer closeDatabases(databases)
	playerID := uuid.NewString()
	wallet := openWallet(t, ctx, services[0], playerID, "100.00")

	reached := make(chan struct{})
	var once sync.Once
	first := newConsumer(t, client, services[0], queues.TransactionsURL, queues.MaxReceiveCount, nil, func(ctx context.Context) error {
		once.Do(func() { close(reached) })
		<-ctx.Done()
		return ctx.Err()
	})
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	messageID := "crash-message-" + uuid.NewString()
	body := sqsEnvelope(messageID, "crash-key", "provider-a", "crash-bet", playerID, wallet.ID(), "BET", "25.00", "")
	if err := client.Publish(ctx, queues.TransactionsURL, []byte(body), wallet.ID(), messageID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not reach post-commit boundary")
	}
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM inbox_messages WHERE message_id=$1 AND completed_at IS NOT NULL`, 1, messageID)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events`, 4)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE event_type='WagerTransactionProcessed' AND envelope->'data'->>'externalTransactionId'='crash-bet'`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE event_type='WalletBalanceChanged' AND envelope->'data'->>'transactionId'=(SELECT id::text FROM wager_transactions WHERE external_transaction_id='crash-bet')`, 1)
	assertWalletBalance(t, ctx, services[0], wallet.ID(), "75.00")
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shutdownCancel()
	if err := first.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("interrupted consumer shutdown = %v", err)
	}

	var redelivery messaging.Message
	waitFor(t, 5*time.Second, func() bool {
		messages, receiveErr := client.Receive(ctx, queues.TransactionsURL, 1, 100*time.Millisecond, time.Second)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		if len(messages) == 0 {
			return false
		}
		redelivery = messages[0]
		return redelivery.ReceiveCount >= 2
	}, "SQS redelivery after committed interruption")
	if err := client.ChangeVisibility(ctx, queues.TransactionsURL, redelivery.ReceiptHandle, 0); err != nil {
		t.Fatal(err)
	}
	binary := buildServiceBinary(t)
	processes := startServiceProcesses(t, ctx, binary, databaseURL, queues, 1)
	assertDistinctProcessIDs(t, processes, 1)
	waitFor(t, 10*time.Second, func() bool { return queueMessageCount(t, ctx, queues.TransactionsURL) == 0 }, "redelivered message deletion")
	stopServiceProcesses(t, processes)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM inbox_messages WHERE message_id=$1 AND completed_at IS NOT NULL`, 1, messageID)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id='crash-bet'`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries entries JOIN wager_transactions transactions ON transactions.id=entries.transaction_id WHERE transactions.external_transaction_id='crash-bet'`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 2)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events`, 4)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE event_type='WagerTransactionProcessed' AND envelope->'data'->>'externalTransactionId'='crash-bet'`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE event_type='WalletBalanceChanged' AND envelope->'data'->>'transactionId'=(SELECT id::text FROM wager_transactions WHERE external_transaction_id='crash-bet')`, 1)
	assertWalletBalance(t, ctx, services[1], wallet.ID(), "75.00")
	assertReconciled(t, ctx, services[1], wallet.ID())
}

func TestOutboxCompetingPublishersCrashBoundariesAndUnavailableSQS(t *testing.T) {
	t.Run("two active publishers serialize one claim and recover it", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resource := migratedDatabase(t, ctx, "outbox_competing")
		queues := newQueues(t, ctx, "outbox-competing", time.Second)
		database := openDatabase(t, ctx, resource.URL)
		defer database.Close()
		eventID := insertOutbox(t, ctx, database.Pool(), "competing")
		repository, _ := messaging.NewPostgresOutbox(database)
		race := &outboxClaimRace{
			ready:         make(chan string, 2),
			release:       make(chan struct{}),
			allowRecovery: make(chan struct{}),
			results:       make(chan outboxClaimResult, 2),
		}
		broker := newBlockingFirstPublishBroker(newSQSClient(t, ctx, queues))
		publisherA := newPublisher(t, &racingOutboxRepository{OutboxRepository: repository, race: race}, broker, "publisher-a", queues.EventsURL, messaging.OutboxOptions{})
		publisherB := newPublisher(t, &racingOutboxRepository{OutboxRepository: repository, race: race}, broker, "publisher-b", queues.EventsURL, messaging.OutboxOptions{})
		if err := publisherA.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := publisherB.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer func() {
			shutdown, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
			defer shutdownCancel()
			_ = publisherA.Shutdown(shutdown)
			_ = publisherB.Shutdown(shutdown)
		}()
		publishers := map[string]*messaging.OutboxPublisher{"publisher-a": publisherA, "publisher-b": publisherB}
		for range 2 {
			select {
			case <-race.ready:
			case <-ctx.Done():
				t.Fatal("both publishers did not reach the coordinated claim barrier")
			}
		}
		close(race.release)

		winnerID := ""
		for range 2 {
			select {
			case result := <-race.results:
				if result.err != nil {
					t.Fatal(result.err)
				}
				switch result.claims {
				case 0:
				case 1:
					if winnerID != "" {
						t.Fatalf("both publishers claimed the same event: %s and %s", winnerID, result.publisherID)
					}
					winnerID = result.publisherID
				default:
					t.Fatalf("publisher %s claimed %d events, want zero or one", result.publisherID, result.claims)
				}
			case <-ctx.Done():
				t.Fatal("coordinated claims did not complete")
			}
		}
		if winnerID == "" {
			t.Fatal("neither active publisher claimed the event")
		}
		select {
		case <-broker.firstEntered:
		case <-ctx.Done():
			t.Fatal("claim winner did not enter publication")
		}
		calls, active, maximum := broker.snapshot()
		if calls != 1 || active != 1 || maximum != 1 {
			t.Fatalf("publication concurrency before crash = calls %d/active %d/max %d", calls, active, maximum)
		}
		var claimedBy *string
		var published bool
		if err := database.Pool().QueryRow(ctx, `SELECT claimed_by, published_at IS NOT NULL FROM outbox_events WHERE event_id=$1`, eventID).Scan(&claimedBy, &published); err != nil {
			t.Fatal(err)
		}
		if claimedBy == nil || *claimedBy != winnerID || published {
			t.Fatalf("active outbox claim = claimed_by %v/published %t, want %s/false", claimedBy, published, winnerID)
		}

		shutdownWithDeadline(t, publishers[winnerID])
		select {
		case <-broker.firstExited:
		case <-ctx.Done():
			t.Fatal("crashed publication did not release")
		}
		close(race.allowRecovery)
		waitFor(t, 10*time.Second, func() bool { return outboxPublished(t, ctx, database.Pool(), eventID) }, "competing publisher lease recovery")
		loserID := "publisher-a"
		if winnerID == loserID {
			loserID = "publisher-b"
		}
		stopPublisher(t, publishers[loserID])
		calls, active, maximum = broker.snapshot()
		if calls != 2 || active != 0 || maximum != 1 {
			t.Fatalf("publication concurrency after recovery = calls %d/active %d/max %d", calls, active, maximum)
		}
		assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE event_id=$1 AND published_at IS NOT NULL AND claimed_by IS NULL AND claim_until IS NULL`, 1, eventID)
		assertEventID(t, receiveOne(t, ctx, queues.EventsURL).Body, eventID)
	})

	t.Run("claim lease recovers before publish", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resource := migratedDatabase(t, ctx, "outbox_before")
		queues := newQueues(t, ctx, "outbox-before", time.Second)
		database := openDatabase(t, ctx, resource.URL)
		defer database.Close()
		eventID := insertOutbox(t, ctx, database.Pool(), "before")
		repository, _ := messaging.NewPostgresOutbox(database)
		reached := make(chan struct{})
		publisherA := newPublisher(t, repository, newSQSClient(t, ctx, queues), "publisher-a", queues.EventsURL, messaging.OutboxOptions{
			BeforePublish: func(ctx context.Context, _ postgres.OutboxClaim) error {
				close(reached)
				<-ctx.Done()
				return ctx.Err()
			},
		})
		publisherB := newPublisher(t, repository, newSQSClient(t, ctx, queues), "publisher-b", queues.EventsURL, messaging.OutboxOptions{})
		if err := publisherA.Start(ctx); err != nil {
			t.Fatal(err)
		}
		<-reached
		shutdownWithDeadline(t, publisherA)
		if err := publisherB.Start(ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 10*time.Second, func() bool { return outboxPublished(t, ctx, database.Pool(), eventID) }, "lease recovery publication")
		stopPublisher(t, publisherB)
		message := receiveOne(t, ctx, queues.EventsURL)
		assertEventID(t, message.Body, eventID)
	})

	t.Run("published event is retried with stable identity", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resource := migratedDatabase(t, ctx, "outbox_after")
		queues := newQueues(t, ctx, "outbox-after", time.Second)
		database := openDatabase(t, ctx, resource.URL)
		defer database.Close()
		eventID := insertOutbox(t, ctx, database.Pool(), "after")
		repository, _ := messaging.NewPostgresOutbox(database)
		transport := &duplicateObservingBroker{Broker: newSQSClient(t, ctx, queues)}
		reached := make(chan struct{})
		publisherA := newPublisher(t, repository, transport, "publisher-a", queues.EventsURL, messaging.OutboxOptions{
			AfterPublishBeforeMark: func(ctx context.Context, _ postgres.OutboxClaim) error {
				close(reached)
				<-ctx.Done()
				return ctx.Err()
			},
		})
		publisherB := newPublisher(t, repository, transport, "publisher-b", queues.EventsURL, messaging.OutboxOptions{})
		if err := publisherA.Start(ctx); err != nil {
			t.Fatal(err)
		}
		<-reached
		shutdownWithDeadline(t, publisherA)
		if err := publisherB.Start(ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 10*time.Second, func() bool { return outboxPublished(t, ctx, database.Pool(), eventID) }, "post-publish lease recovery")
		stopPublisher(t, publisherB)
		transport.mu.Lock()
		defer transport.mu.Unlock()
		if len(transport.deduplicationIDs) != 2 || transport.deduplicationIDs[0] != eventID || transport.deduplicationIDs[1] != eventID {
			t.Fatalf("outbox publication identities = %v", transport.deduplicationIDs)
		}
		if !bytes.Equal(transport.bodies[0], transport.bodies[1]) {
			t.Fatal("outbox envelope changed across ambiguous retry")
		}
	})

	t.Run("SQS outage reschedules and recovers without loss", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		resource := migratedDatabase(t, ctx, "outbox_outage")
		queues := newQueues(t, ctx, "outbox-outage", time.Second)
		database := openDatabase(t, ctx, resource.URL)
		defer database.Close()
		eventID := insertOutbox(t, ctx, database.Pool(), "outage")
		repository, _ := messaging.NewPostgresOutbox(database)
		client := newSQSClient(t, ctx, queues)
		if err := suite.StopLocalStack(ctx); err != nil {
			t.Fatal(err)
		}
		publisher := newPublisher(t, repository, client, "publisher-outage", queues.EventsURL, messaging.OutboxOptions{})
		if err := publisher.Start(ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 10*time.Second, func() bool {
			return queryIntArgs(t, ctx, database.Pool(), `SELECT attempt_count FROM outbox_events WHERE event_id=$1`, eventID) >= 1
		}, "outbox retry persistence while SQS is unavailable")
		stopPublisher(t, publisher)
		if err := suite.StartLocalStackContainer(ctx); err != nil {
			t.Fatal(err)
		}
		queues, err := suite.EnsureQueues(ctx, queues, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		client = newSQSClient(t, ctx, queues)
		eventsURL := client.Queues().Events
		publisher = newPublisher(t, repository, client, "publisher-recovery", eventsURL, messaging.OutboxOptions{})
		if err := publisher.Start(ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 15*time.Second, func() bool { return outboxPublished(t, ctx, database.Pool(), eventID) }, "outbox publication after SQS recovery")
		stopPublisher(t, publisher)
		assertEventID(t, receiveOne(t, ctx, eventsURL).Body, eventID)
	})
}

func TestPendingRefundRollbackResolutionTTLAndFinalReconciliation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "references")
	options := defaultServiceOptions()
	options.Reference = application.ReferencePolicy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond, TTL: 300 * time.Millisecond}
	options.Jitter = func(delay time.Duration) time.Duration { return delay }
	service, database := newService(t, ctx, resource.URL, options)
	defer database.Close()
	wallet := openWallet(t, ctx, service, "reference-player", "100.00")

	refund := process(t, ctx, service, commandInput("provider-a", "early-refund", "reference-player", wallet.ID(), "REFUND", "20.00", "late-bet"), "early-refund")
	if refund.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("early refund state = %s", refund.Transaction.State())
	}
	process(t, ctx, service, commandInput("provider-a", "late-bet", "reference-player", wallet.ID(), "BET", "20.00", ""), "late-bet")
	resumePending(t, ctx, service, refund.Transaction.ID(), "refund-worker")
	assertTransaction(t, ctx, service, refund.Transaction.ID(), domain.TransactionStateProcessed, "")

	rollback := process(t, ctx, service, commandInput("provider-a", "early-rollback", "reference-player", wallet.ID(), "ROLLBACK", "10.00", "late-win"), "early-rollback")
	if rollback.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("early rollback state = %s", rollback.Transaction.State())
	}
	process(t, ctx, service, commandInput("provider-a", "late-win", "reference-player", wallet.ID(), "WIN", "10.00", ""), "late-win")
	resumePending(t, ctx, service, rollback.Transaction.ID(), "rollback-worker")
	assertTransaction(t, ctx, service, rollback.Transaction.ID(), domain.TransactionStateProcessed, "")

	expiring := process(t, ctx, service, commandInput("provider-a", "expiring-refund", "reference-player", wallet.ID(), "REFUND", "1.00", "never-arrives"), "expiring-refund")
	time.Sleep(350 * time.Millisecond)
	resumePending(t, ctx, service, expiring.Transaction.ID(), "expiration-worker")
	assertTransaction(t, ctx, service, expiring.Transaction.ID(), domain.TransactionStateRejected, domain.FailureReferenceNotFound)

	assertWalletBalance(t, ctx, service, wallet.ID(), "100.00")
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionPendingReference'`, 1, refund.Transaction.ID())
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionProcessed'`, 1, refund.Transaction.ID())
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionPendingReference'`, 1, rollback.Transaction.ID())
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionRejected'`, 1, expiring.Transaction.ID())
	assertReconciled(t, ctx, service, wallet.ID())
}

func TestPendingReferenceDistinguishesNonterminalAndIneligibleReferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "reference-states")
	options := defaultServiceOptions()
	options.Reference = application.ReferencePolicy{MaxAttempts: 3, BaseDelay: 25 * time.Millisecond, MaxDelay: 50 * time.Millisecond, TTL: 5 * time.Second}
	options.Jitter = func(delay time.Duration) time.Duration { return delay }
	service, database := newService(t, ctx, resource.URL, options)
	defer database.Close()
	wallet := openWallet(t, ctx, service, "reference-state-player", "100.00")

	insertReferenceFixture(t, ctx, database, wallet.ID(), "reference-state-player", "pending-bet", domain.TransactionStatePending, "")
	pendingTarget := process(t, ctx, service, commandInput("provider-a", "refund-pending-bet", "reference-state-player", wallet.ID(), "REFUND", "10.00", "pending-bet"), "refund-pending-bet")
	assertPendingReferenceRescheduled(t, ctx, service, database, pendingTarget.Transaction.ID(), "pending-target-worker")

	pendingReferenceTarget := process(t, ctx, service, commandInput("provider-a", "pending-reference-target", "reference-state-player", wallet.ID(), "REFUND", "10.00", "absent-bet"), "pending-reference-target")
	if pendingReferenceTarget.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("reference fixture state = %s", pendingReferenceTarget.Transaction.State())
	}
	pendingReference := process(t, ctx, service, commandInput("provider-a", "rollback-pending-reference", "reference-state-player", wallet.ID(), "ROLLBACK", "10.00", "pending-reference-target"), "rollback-pending-reference")
	assertPendingReferenceRescheduled(t, ctx, service, database, pendingReference.Transaction.ID(), "pending-reference-target-worker")

	rejectedTarget := process(t, ctx, service, commandInput("provider-a", "rejected-bet", "reference-state-player", wallet.ID(), "BET", "1000.00", ""), "rejected-bet")
	assertTransaction(t, ctx, service, rejectedTarget.Transaction.ID(), domain.TransactionStateRejected, domain.FailureInsufficientFunds)
	rejectedReference := process(t, ctx, service, commandInput("provider-a", "refund-rejected-bet", "reference-state-player", wallet.ID(), "REFUND", "1000.00", "rejected-bet"), "refund-rejected-bet")
	assertTransaction(t, ctx, service, rejectedReference.Transaction.ID(), domain.TransactionStateRejected, domain.FailureReferenceNotEligible)

	insertReferenceFixture(t, ctx, database, wallet.ID(), "reference-state-player", "failed-bet", domain.TransactionStateFailed, domain.FailurePermanentProcessing)
	failedReference := process(t, ctx, service, commandInput("provider-a", "refund-failed-bet", "reference-state-player", wallet.ID(), "REFUND", "10.00", "failed-bet"), "refund-failed-bet")
	assertTransaction(t, ctx, service, failedReference.Transaction.ID(), domain.TransactionStateRejected, domain.FailureReferenceNotEligible)

	assertWalletBalance(t, ctx, service, wallet.ID(), "100.00")
	assertReconciled(t, ctx, service, wallet.ID())
}

func TestFxRealLifecycleStartsStopsAndReleasesResources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	databaseURL := migratedPublicDatabase(t, ctx)
	queues := newQueues(t, ctx, "fx", time.Second)
	value := loadConfig(t, databaseURL, queues, freeAddress(t), freeAddress(t), "http,sqs-consumer,outbox,pending-reference")
	var database *postgres.Database
	app := bootstrap.NewApp(value, fx.Populate(&database))
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Fx start with real resources: %v", err)
	}
	if database == nil || database.Health(ctx) != nil {
		t.Fatal("Fx did not start a healthy PostgreSQL resource")
	}
	stopContext, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	if err := app.Stop(stopContext); err != nil {
		t.Fatalf("Fx stop with real resources: %v", err)
	}
	if err := database.Health(context.Background()); err == nil {
		t.Fatal("Fx stop did not close the PostgreSQL pool")
	}
}

type duplicateObservingBroker struct {
	messaging.Broker
	mu               sync.Mutex
	deduplicationIDs []string
	bodies           [][]byte
}

func (broker *duplicateObservingBroker) Publish(ctx context.Context, queue string, body []byte, group, deduplicationID string) error {
	broker.mu.Lock()
	broker.deduplicationIDs = append(broker.deduplicationIDs, deduplicationID)
	broker.bodies = append(broker.bodies, append([]byte(nil), body...))
	broker.mu.Unlock()
	// Preserve the eventId in the envelope while bypassing FIFO's five-minute
	// optimization so both ambiguous physical sends are observable in this test.
	return broker.Broker.Publish(ctx, queue, body, group, uuid.NewString())
}

type outboxClaimResult struct {
	publisherID string
	claims      int
	err         error
}

type outboxClaimRace struct {
	ready         chan string
	release       chan struct{}
	allowRecovery chan struct{}
	results       chan outboxClaimResult
}

type racingOutboxRepository struct {
	messaging.OutboxRepository
	race  *outboxClaimRace
	first sync.Once
}

func (repository *racingOutboxRepository) ClaimOutbox(ctx context.Context, claimedBy string, limit int, lease time.Duration) ([]postgres.OutboxClaim, error) {
	first := false
	repository.first.Do(func() { first = true })
	if first {
		select {
		case repository.race.ready <- claimedBy:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-repository.race.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		select {
		case <-repository.race.allowRecovery:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	claims, err := repository.OutboxRepository.ClaimOutbox(ctx, claimedBy, limit, lease)
	if first {
		select {
		case repository.race.results <- outboxClaimResult{publisherID: claimedBy, claims: len(claims), err: err}:
		case <-ctx.Done():
		}
	}
	return claims, err
}

type blockingFirstPublishBroker struct {
	messaging.Broker
	firstEntered chan struct{}
	firstExited  chan struct{}

	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
}

func newBlockingFirstPublishBroker(broker messaging.Broker) *blockingFirstPublishBroker {
	return &blockingFirstPublishBroker{Broker: broker, firstEntered: make(chan struct{}), firstExited: make(chan struct{})}
}

func (broker *blockingFirstPublishBroker) Publish(ctx context.Context, queue string, body []byte, group, deduplicationID string) error {
	broker.mu.Lock()
	call := broker.calls
	broker.calls++
	broker.active++
	if broker.active > broker.maxActive {
		broker.maxActive = broker.active
	}
	broker.mu.Unlock()

	first := call == 0
	var err error
	if first {
		close(broker.firstEntered)
		<-ctx.Done()
		err = ctx.Err()
	} else {
		err = broker.Broker.Publish(ctx, queue, body, group, deduplicationID)
	}
	broker.mu.Lock()
	broker.active--
	broker.mu.Unlock()
	if first {
		close(broker.firstExited)
	}
	return err
}

func (broker *blockingFirstPublishBroker) snapshot() (calls, active, maximum int) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return broker.calls, broker.active, broker.maxActive
}

type servicesFixture struct {
	services  []*application.Service
	databases []*postgres.Database
}

func newServices(t *testing.T, ctx context.Context, databaseURL string, count int, options application.ServiceOptions) ([]*application.Service, []*postgres.Database) {
	t.Helper()
	fixture := servicesFixture{}
	for range count {
		service, database := newService(t, ctx, databaseURL, options)
		fixture.services = append(fixture.services, service)
		fixture.databases = append(fixture.databases, database)
	}
	return fixture.services, fixture.databases
}

type integrationClock struct{ now time.Time }

func (clock integrationClock) Now() time.Time { return clock.now }

type failAtIDGenerator struct {
	delegate application.IDGenerator
	failAt   int
	calls    int
	err      error
}

func (generator *failAtIDGenerator) New() (uuid.UUID, error) {
	generator.calls++
	if generator.calls == generator.failAt {
		return uuid.Nil, generator.err
	}
	return generator.delegate.New()
}

func newServicesAt(t *testing.T, ctx context.Context, databaseURL string, count int, options application.ServiceOptions, now time.Time) ([]*application.Service, []*postgres.Database) {
	t.Helper()
	services := make([]*application.Service, 0, count)
	databases := make([]*postgres.Database, 0, count)
	for range count {
		database := openDatabase(t, ctx, databaseURL)
		clock := integrationClock{now: now}
		service, err := application.NewService(database, clock, platform.NewUUIDv7Generator(clock), options)
		if err != nil {
			database.Close()
			closeDatabases(databases)
			t.Fatal(err)
		}
		services = append(services, service)
		databases = append(databases, database)
	}
	return services, databases
}

func newService(t *testing.T, ctx context.Context, databaseURL string, options application.ServiceOptions) (*application.Service, *postgres.Database) {
	t.Helper()
	database := openDatabase(t, ctx, databaseURL)
	clock := platform.NewClock()
	service, err := application.NewService(database, clock, platform.NewUUIDv7Generator(clock), options)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	return service, database
}

func defaultServiceOptions() application.ServiceOptions {
	return application.ServiceOptions{
		Retry:     application.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond},
		Reference: application.ReferencePolicy{MaxAttempts: 5, BaseDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond, TTL: time.Minute},
		Jitter:    func(delay time.Duration) time.Duration { return delay },
	}
}

func newDatabase(t *testing.T, ctx context.Context, prefix string) *testsupport.Database {
	t.Helper()
	resource, err := suite.NewDatabase(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Close(context.Background()) })
	return resource
}

func migratedDatabase(t *testing.T, ctx context.Context, prefix string) *testsupport.Database {
	t.Helper()
	resource := newDatabase(t, ctx, prefix)
	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}
	return resource
}

func migratedPublicDatabase(t *testing.T, ctx context.Context) string {
	t.Helper()
	directory := testsupport.ProjectRoot() + "/migrations"
	catalog, err := bootstrap.LoadMigrationCatalog(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.RunMigrationsFromDirectory(ctx, suite.PostgresURL, "up", directory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for range catalog.Migrations() {
			_ = bootstrap.RunMigrationsFromDirectory(context.Background(), suite.PostgresURL, "down", directory)
		}
	})
	return suite.PostgresURL
}

func openPool(t *testing.T, ctx context.Context, databaseURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func openDatabase(t *testing.T, ctx context.Context, databaseURL string) *postgres.Database {
	t.Helper()
	database, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Health(ctx); err != nil {
		database.Close()
		t.Fatal(err)
	}
	return database
}

func closeDatabases(databases []*postgres.Database) {
	for _, database := range databases {
		database.Close()
	}
}

func newQueues(t *testing.T, ctx context.Context, prefix string, visibility time.Duration) testsupport.QueueSet {
	t.Helper()
	queues, err := suite.NewQueues(ctx, prefix, visibility)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = suite.DeleteQueues(context.Background(), queues) })
	return queues
}

func loadConfig(t *testing.T, databaseURL string, queues testsupport.QueueSet, publicAddress, internalAddress, components string) config.Config {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("MIGRATIONS_DIR", testsupport.ProjectRoot()+"/migrations")
	values := map[string]string{
		"DATABASE_URL":                     databaseURL,
		"OIDC_ISSUER_URL":                  suite.KeycloakURL + "/realms/wager",
		"OIDC_AUDIENCE":                    "backend-api",
		"AWS_ENDPOINT_URL":                 suite.LocalStackURL,
		"AWS_REGION":                       testsupport.Region,
		"SQS_TRANSACTIONS_QUEUE":           queues.TransactionsName,
		"SQS_TRANSACTIONS_DLQ":             queues.DeadLetterName,
		"SQS_EVENTS_QUEUE":                 queues.EventsName,
		"SQS_REDRIVE_MAX_RECEIVE_COUNT":    strconv.Itoa(queueMaxReceiveCount(queues)),
		"HTTP_ADDR":                        publicAddress,
		"INTERNAL_HTTP_ADDR":               internalAddress,
		"COMPONENTS":                       components,
		"SQS_CONSUMER_POLL_INTERVAL":       "100ms",
		"SQS_CONSUMER_LEASE_DURATION":      "2s",
		"OUTBOX_POLL_INTERVAL":             "50ms",
		"OUTBOX_LEASE_DURATION":            "500ms",
		"PENDING_REFERENCE_POLL_INTERVAL":  "50ms",
		"PENDING_REFERENCE_LEASE_DURATION": "500ms",
		"REFERENCE_RETRY_BASE_DELAY":       "50ms",
		"REFERENCE_RETRY_MAX_DELAY":        "100ms",
		"REFERENCE_TTL":                    "2s",
		"SHUTDOWN_TIMEOUT":                 "10s",
	}
	value, err := config.LoadFrom(func(name string) (string, bool) {
		result, ok := values[name]
		return result, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func newSQSClient(t *testing.T, ctx context.Context, queues testsupport.QueueSet) *messaging.SQSClient {
	t.Helper()
	value := loadConfig(t, "postgres://unused:unused@localhost/unused", queues, "127.0.0.1:1", "127.0.0.1:2", "sqs-consumer")
	client, err := messaging.NewSQSClient(ctx, value.AWS, value.Queues)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newConsumer(t *testing.T, broker messaging.Broker, service *application.Service, queueURL string, maxReceiveCount int, metrics messaging.ConsumerMetrics, hook func(context.Context) error) *messaging.Consumer {
	t.Helper()
	consumer, err := messaging.NewConsumer(broker, service, metrics, slog.New(slog.DiscardHandler), messaging.ConsumerOptions{
		QueueURL: queueURL, ConsumerName: "wager-transactions-v1", BatchSize: 10,
		WaitTime: 100 * time.Millisecond, Visibility: time.Second, ErrorDelay: 10 * time.Millisecond,
		BackoffBase: 50 * time.Millisecond, BackoffMaximum: 200 * time.Millisecond,
		MaxReceiveCount: maxReceiveCount, AfterCommitBeforeDelete: hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func queueMaxReceiveCount(queues testsupport.QueueSet) int {
	if queues.MaxReceiveCount > 0 {
		return queues.MaxReceiveCount
	}
	return 5
}

func newPublisher(t *testing.T, repository messaging.OutboxRepository, broker messaging.Broker, id, queueURL string, hooks messaging.OutboxOptions) *messaging.OutboxPublisher {
	t.Helper()
	options := messaging.OutboxOptions{
		PublisherID: id, QueueURL: queueURL, BatchSize: 1,
		PollInterval: 20 * time.Millisecond, Lease: 300 * time.Millisecond,
		BackoffBase: 100 * time.Millisecond, BackoffMaximum: 200 * time.Millisecond,
		BeforePublish: hooks.BeforePublish, AfterPublishBeforeMark: hooks.AfterPublishBeforeMark,
	}
	publisher, err := messaging.NewOutboxPublisher(repository, broker, platform.NewClock(), nil, slog.New(slog.DiscardHandler), options)
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func openWallet(t *testing.T, ctx context.Context, service *application.Service, playerID, amount string) *domain.Wallet {
	t.Helper()
	money, err := domain.ParseMoneyValue(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	wallet, err := service.OpenWallet(ctx, application.OpenWalletRequest{PlayerID: playerID, InitialBalance: money})
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

func process(t *testing.T, ctx context.Context, service *application.Service, input application.TransactionCommandInput, key string) application.TransactionResult {
	t.Helper()
	result, err := processResult(ctx, service, input, key)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func processResult(ctx context.Context, service *application.Service, input application.TransactionCommandInput, key string) (application.TransactionResult, error) {
	command, err := application.NormalizeTransactionCommand(input)
	if err != nil {
		return application.TransactionResult{}, err
	}
	return service.ProcessTransaction(ctx, application.ProcessTransactionRequest{Command: command, IdempotencyKey: key})
}

func commandInput(providerID, externalID, playerID, walletID, kind, amount, reference string) application.TransactionCommandInput {
	return application.TransactionCommandInput{
		ProviderID: providerID, ExternalTransactionID: externalID, PlayerID: playerID,
		WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: kind,
		Amount: amount, Currency: "BRL", ReferenceExternalTransactionID: reference,
	}
}

func transactionJSON(providerID, externalID, playerID, walletID, kind, amount, reference string) string {
	referenceJSON := ""
	if reference != "" {
		referenceJSON = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, reference)
	}
	return fmt.Sprintf(`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}`,
		providerID, externalID, playerID, walletID, kind, amount, referenceJSON)
}

func sqsEnvelope(messageID, key, providerID, externalID, playerID, walletID, kind, amount, reference string) string {
	referenceJSON := ""
	if reference != "" {
		referenceJSON = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, reference)
	}
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":%q,"data":{"idempotencyKey":%q,"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}}`,
		messageID, time.Now().UTC().Format(time.RFC3339Nano), key, providerID, externalID, playerID, walletID, kind, amount, referenceJSON)
}

func token(t *testing.T, ctx context.Context, clientID, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, suite.KeycloakURL+"/realms/wager/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("Keycloak token %s: status %d: %s", clientID, response.StatusCode, payload)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil || payload.AccessToken == "" {
		t.Fatalf("decode Keycloak token: %v", err)
	}
	return payload.AccessToken
}

func waitForTokenExpiration(t *testing.T, raw string) {
	t.Helper()
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		t.Fatal("Keycloak returned a malformed JWT")
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		t.Fatal(err)
	}
	delay := time.Until(time.Unix(claims.Exp, 0).Add(time.Second))
	if delay > 0 {
		time.Sleep(delay)
	}
}

func request(t *testing.T, method, target, body, bearer string) *http.Response {
	t.Helper()
	return requestWithHeaders(t, method, target, body, bearer, nil)
}

func requestWithHeaders(t *testing.T, method, target, body, bearer string, headers map[string]string) *http.Response {
	t.Helper()
	response, err := doRequest(method, target, body, bearer, headers)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func doRequest(method, target, body, bearer string, headers map[string]string) (*http.Response, error) {
	request, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return http.DefaultClient.Do(request)
}

func assertHTTPStatus(t *testing.T, response *http.Response, wanted int) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != wanted {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("HTTP status = %d, want %d: %s", response.StatusCode, wanted, payload)
	}
}

func assertHTTPError(t *testing.T, response *http.Response, wantedStatus int, wantedCode, wantedMessage string) {
	t.Helper()
	if response.StatusCode != wantedStatus {
		defer response.Body.Close()
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("HTTP status = %d, want %d: %s", response.StatusCode, wantedStatus, payload)
	}
	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	decodeResponse(t, response, &payload)
	if payload.Code != wantedCode || payload.Message != wantedMessage {
		t.Fatalf("HTTP error = %+v, want code %q/message %q", payload, wantedCode, wantedMessage)
	}
}

func decodeResponse(t *testing.T, response *http.Response, destination any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatal(err)
	}
}

func assertCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, wanted int, arguments ...any) {
	t.Helper()
	if got := queryIntArgs(t, ctx, pool, query, arguments...); got != wanted {
		t.Fatalf("query count = %d, want %d; query: %s", got, wanted, query)
	}
}

func queryInt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) int {
	t.Helper()
	return queryIntArgs(t, ctx, pool, query)
}

func queryIntArgs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, arguments ...any) int {
	t.Helper()
	var result int
	if err := pool.QueryRow(ctx, query, arguments...).Scan(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, exists bool) {
	t.Helper()
	var found bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&found); err != nil {
		t.Fatal(err)
	}
	if found != exists {
		t.Fatalf("table %s existence = %t, want %t", table, found, exists)
	}
}

func assertWalletBalance(t *testing.T, ctx context.Context, service *application.Service, walletID, wanted string) {
	t.Helper()
	wallet, err := service.GetWallet(ctx, walletID)
	if err != nil {
		t.Fatal(err)
	}
	if wallet.Balance().String() != wanted {
		t.Fatalf("wallet %s balance = %s, want %s", walletID, wallet.Balance(), wanted)
	}
}

func assertReconciled(t *testing.T, ctx context.Context, service *application.Service, walletID string) {
	t.Helper()
	reconciliation, err := service.Reconcile(ctx, walletID)
	if err != nil {
		t.Fatal(err)
	}
	if !reconciliation.Consistent || !reconciliation.Difference.IsZero() || reconciliation.WalletBalance.String() != reconciliation.LedgerBalance.String() {
		t.Fatalf("wallet %s reconciliation = %+v", walletID, reconciliation)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func queueMessageCount(t *testing.T, ctx context.Context, queueURL string) int {
	t.Helper()
	output, err := suite.SQS().GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible},
	})
	if err != nil {
		t.Fatal(err)
	}
	var visible, hidden int
	fmt.Sscan(output.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], &visible)
	fmt.Sscan(output.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)], &hidden)
	return visible + hidden
}

func stopWorker(t *testing.T, worker *messaging.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := worker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func stopPublisher(t *testing.T, publisher *messaging.OutboxPublisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := publisher.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func shutdownWithDeadline(t *testing.T, publisher *messaging.OutboxPublisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := publisher.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publisher interruption = %v", err)
	}
}

func insertOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool, suffix string) string {
	t.Helper()
	eventID := uuid.NewString()
	aggregateID := uuid.NewString()
	envelope := fmt.Sprintf(`{"eventId":%q,"eventType":"WagerTransactionProcessed","aggregateId":%q,"correlationId":%q,"occurredAt":%q,"version":1,"data":{"probe":%q}}`, eventID, aggregateID, eventID, time.Now().UTC().Format(time.RFC3339Nano), suffix)
	_, err := pool.Exec(ctx, `INSERT INTO outbox_events (event_id,event_type,aggregate_id,correlation_id,version,envelope,next_attempt_at,created_at) VALUES ($1,'WagerTransactionProcessed',$2,$3,1,$4,now(),now())`, eventID, aggregateID, eventID, envelope)
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}

func outboxPublished(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) bool {
	t.Helper()
	var published bool
	if err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id=$1`, eventID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	return published
}

func receiveOne(t *testing.T, ctx context.Context, queueURL string) messaging.Message {
	t.Helper()
	client := newSQSClient(t, ctx, testsupport.QueueSet{TransactionsName: queueName(t, ctx, queueURL), DeadLetterName: queueName(t, ctx, queueURL), EventsName: queueName(t, ctx, queueURL)})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		messages, err := client.Receive(ctx, queueURL, 1, time.Second, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) != 0 {
			return messages[0]
		}
	}
	t.Fatal("timed out receiving SQS event")
	return messaging.Message{}
}

func queueName(t *testing.T, ctx context.Context, queueURL string) string {
	t.Helper()
	output, err := suite.SQS().ListQueues(ctx, &awssqs.ListQueuesInput{})
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range output.QueueUrls {
		if candidate == queueURL {
			parts := strings.Split(candidate, "/")
			return parts[len(parts)-1]
		}
	}
	t.Fatalf("queue URL not found: %s", queueURL)
	return ""
}

func assertEventID(t *testing.T, body, wanted string) {
	t.Helper()
	var envelope struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.EventID != wanted {
		t.Fatalf("eventId = %s, want %s", envelope.EventID, wanted)
	}
}

func resumePending(t *testing.T, ctx context.Context, service *application.Service, transactionID, workerID string) {
	t.Helper()
	time.Sleep(125 * time.Millisecond)
	claims, err := service.ClaimPendingReferences(ctx, workerID, 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, claim := range claims {
		if claim.Record.Transaction.ID() == transactionID {
			if _, err := service.ResumePendingReference(ctx, transactionID, workerID); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("pending transaction %s was not claimed", transactionID)
}

func insertReferenceFixture(t *testing.T, ctx context.Context, database *postgres.Database, walletID, playerID, externalID string, state domain.WagerTransactionState, failure domain.FailureCode) {
	t.Helper()
	now := time.Now().UTC()
	var failureValue, completedAt any
	if failure != "" {
		failureValue = string(failure)
		completedAt = now
	}
	if _, err := database.Pool().Exec(ctx, `
		INSERT INTO wager_transactions (
			id, wallet_id, player_id, provider_id, external_transaction_id,
			idempotency_key, request_hash, round_id, game_id, kind, state,
			amount_minor, amount_currency, failure_code, completed_at, created_at, updated_at
		) VALUES ($1,$2,$3,'provider-a',$4,$4,$5,'round-1','game-1','BET',$6,1000,'BRL',$7,$8,$9,$9)`,
		uuid.NewString(), walletID, playerID, externalID, bytes.Repeat([]byte{1}, 32), string(state), failureValue, completedAt, now); err != nil {
		t.Fatal(err)
	}
}

func assertPendingReferenceRescheduled(t *testing.T, ctx context.Context, service *application.Service, database *postgres.Database, transactionID, workerID string) {
	t.Helper()
	var initialNext time.Time
	if err := database.Pool().QueryRow(ctx, `SELECT reference_next_attempt_at FROM wager_transactions WHERE id=$1`, transactionID).Scan(&initialNext); err != nil {
		t.Fatal(err)
	}
	time.Sleep(75 * time.Millisecond)
	claims, err := service.ClaimPendingReferences(ctx, workerID, 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	claimed := false
	for _, claim := range claims {
		if claim.Record.Transaction.ID() == transactionID {
			claimed = true
			break
		}
	}
	if !claimed {
		t.Fatalf("pending transaction %s was not claimed", transactionID)
	}
	if _, err := service.ResumePendingReference(ctx, transactionID, workerID); err != nil {
		t.Fatal(err)
	}
	assertTransaction(t, ctx, service, transactionID, domain.TransactionStatePendingReference, "")
	var attempts int
	var next time.Time
	var claimedBy *string
	if err := database.Pool().QueryRow(ctx, `SELECT reference_attempts, reference_next_attempt_at, reference_claimed_by FROM wager_transactions WHERE id=$1`, transactionID).Scan(&attempts, &next, &claimedBy); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || !next.After(initialNext) || claimedBy != nil {
		t.Fatalf("pending transaction %s was not rescheduled: attempts=%d initial=%s next=%s claimed_by=%v", transactionID, attempts, initialNext, next, claimedBy)
	}
}

func assertTransaction(t *testing.T, ctx context.Context, service *application.Service, transactionID string, state domain.WagerTransactionState, code domain.FailureCode) {
	t.Helper()
	record, err := service.GetTransaction(ctx, transactionID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Transaction.State() != state || record.Transaction.FailureCode() != code {
		t.Fatalf("transaction %s = %s/%s, want %s/%s", transactionID, record.Transaction.State(), record.Transaction.FailureCode(), state, code)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
