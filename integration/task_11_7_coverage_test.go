//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/httpapi"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

func TestTask117WalletOpeningPositiveZeroAndConcurrentDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	resource := migratedDatabase(t, ctx, "task_11_7_wallets")
	services, databases := newServices(t, ctx, resource.URL, 3, defaultServiceOptions())
	defer closeDatabases(databases)

	positive := openWallet(t, ctx, services[0], "opening-positive-player", "125.00")
	if positive.Balance().String() != "125.00" || positive.Version() != 1 {
		t.Fatalf("positive wallet = balance %s/version %d", positive.Balance(), positive.Version())
	}
	var openingID string
	if err := databases[0].Pool().QueryRow(ctx, `
		SELECT id::text
		FROM wager_transactions
		WHERE wallet_id=$1 AND kind='OPENING' AND state='PROCESSED'
		  AND amount_minor=12500 AND amount_currency='BRL'
		  AND result_balance_minor=12500 AND result_balance_currency='BRL'`, positive.ID()).Scan(&openingID); err != nil {
		t.Fatalf("positive OPENING transaction: %v", err)
	}
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND transaction_id=$2 AND direction='CREDIT' AND amount_minor=12500 AND balance_before_minor=0 AND balance_after_minor=12500`, 1, positive.ID(), openingID)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionProcessed'`, 1, openingID)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WalletBalanceChanged'`, 1, positive.ID())
	assertReconciled(t, ctx, services[1], positive.ID())

	zero := openWallet(t, ctx, services[1], "opening-zero-player", "0.00")
	if zero.Balance().String() != "0.00" || zero.Version() != 1 {
		t.Fatalf("zero wallet = balance %s/version %d", zero.Balance(), zero.Version())
	}
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions WHERE wallet_id=$1`, 0, zero.ID())
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, 0, zero.ID())
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'walletId'=$1`, 0, zero.ID())
	assertReconciled(t, ctx, services[2], zero.ID())

	initial, err := domain.ParseMoneyValue("50.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	type openingOutcome struct {
		walletID string
		err      error
	}
	const attempts = 12
	start := make(chan struct{})
	outcomes := make(chan openingOutcome, attempts)
	var wait sync.WaitGroup
	for index := range attempts {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			wallet, openErr := services[index%len(services)].OpenWallet(ctx, application.OpenWalletRequest{
				PlayerID:       "opening-concurrent-player",
				InitialBalance: initial,
			})
			outcome := openingOutcome{err: openErr}
			if wallet != nil {
				outcome.walletID = wallet.ID()
			}
			outcomes <- outcome
		}(index)
	}
	close(start)
	wait.Wait()
	close(outcomes)

	var concurrentWalletID string
	var created, conflicts int
	for outcome := range outcomes {
		switch {
		case outcome.err == nil:
			created++
			concurrentWalletID = outcome.walletID
		case errors.Is(outcome.err, application.ErrWalletConflict):
			conflicts++
		default:
			t.Errorf("concurrent opening error = %v", outcome.err)
		}
	}
	if created != 1 || conflicts != attempts-1 {
		t.Fatalf("concurrent openings = %d created/%d conflicts; want 1/%d", created, conflicts, attempts-1)
	}
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallets WHERE player_id='opening-concurrent-player' AND currency='BRL' AND balance_minor=5000`, 1)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND kind='OPENING' AND state='PROCESSED'`, 1, concurrentWalletID)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='CREDIT' AND amount_minor=5000`, 1, concurrentWalletID)
	assertCount(t, ctx, databases[0].Pool(), `SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'walletId'=$1`, 2, concurrentWalletID)
	assertReconciled(t, ctx, services[0], concurrentWalletID)
}

func TestTask117PendingReferenceContinuesAfterApplicationRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	resource := migratedDatabase(t, ctx, "task_11_7_restart")
	firstService, firstDatabase := newService(t, ctx, resource.URL, defaultServiceOptions())
	wallet := openWallet(t, ctx, firstService, "task-11-7-restart-player", "100.00")
	pending := process(t, ctx, firstService, commandInput("provider-a", "restart-refund", "task-11-7-restart-player", wallet.ID(), "REFUND", "20.00", "restart-bet"), "restart-refund-key")
	if pending.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("pre-restart transaction state = %s", pending.Transaction.State())
	}
	firstDatabase.Close()

	restartedService, restartedDatabase := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer restartedDatabase.Close()
	process(t, ctx, restartedService, commandInput("provider-a", "restart-bet", "task-11-7-restart-player", wallet.ID(), "BET", "20.00", ""), "restart-bet-key")
	resumePending(t, ctx, restartedService, pending.Transaction.ID(), "post-restart-worker")
	assertTransaction(t, ctx, restartedService, pending.Transaction.ID(), domain.TransactionStateProcessed, "")
	assertCount(t, ctx, restartedDatabase.Pool(), `SELECT count(*) FROM wallet_ledger_entries entries JOIN wager_transactions transactions ON transactions.id=entries.transaction_id WHERE transactions.external_transaction_id IN ('restart-bet','restart-refund')`, 2)
	assertCount(t, ctx, restartedDatabase.Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionPendingReference'`, 1, pending.Transaction.ID())
	assertCount(t, ctx, restartedDatabase.Pool(), `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WagerTransactionProcessed'`, 1, pending.Transaction.ID())
	assertWalletBalance(t, ctx, restartedService, wallet.ID(), "100.00")
	assertReconciled(t, ctx, restartedService, wallet.ID())
}

func TestTask117RealHTTPIdempotencyConflictsAndProviderScopedReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	resource := migratedDatabase(t, ctx, "task_11_7_http")
	queues := newQueues(t, ctx, "task-11-7-http", time.Second)
	client := newSQSClient(t, ctx, queues)
	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	wallet := openWallet(t, ctx, service, "task-11-7-http-player", "100.00")

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

	type transactionResponse struct {
		TransactionID    string `json:"transactionId"`
		IdempotentReplay bool   `json:"idempotentReplay"`
		Balance          struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	post := func(tokenValue, key, body string, wantedStatus int) transactionResponse {
		t.Helper()
		response := requestWithHeaders(t, http.MethodPost, server.URL+"/wagering/transactions", body, tokenValue, map[string]string{"Idempotency-Key": key})
		if response.StatusCode != wantedStatus {
			defer response.Body.Close()
			t.Fatalf("POST transaction status = %d, want %d", response.StatusCode, wantedStatus)
		}
		var payload transactionResponse
		decodeResponse(t, response, &payload)
		return payload
	}
	postConflict := func(key, body, wantedCode string) {
		t.Helper()
		response := requestWithHeaders(t, http.MethodPost, server.URL+"/wagering/transactions", body, providerA, map[string]string{"Idempotency-Key": key})
		if response.StatusCode != http.StatusConflict {
			defer response.Body.Close()
			t.Fatalf("POST conflict status = %d, want 409", response.StatusCode)
		}
		var payload struct {
			Code string `json:"code"`
		}
		decodeResponse(t, response, &payload)
		if payload.Code != wantedCode {
			t.Fatalf("conflict code = %q, want %q", payload.Code, wantedCode)
		}
	}

	const externalID = "task-11-7-shared-external"
	const idempotencyKey = "task-11-7-shared-key"
	bodyA := transactionJSON("provider-a", externalID, "task-11-7-http-player", wallet.ID(), "BET", "10.00", "")
	bodyB := transactionJSON("provider-b", externalID, "task-11-7-http-player", wallet.ID(), "BET", "10.00", "")
	firstA := post(providerA, idempotencyKey, bodyA, http.StatusOK)
	firstB := post(providerB, idempotencyKey, bodyB, http.StatusOK)
	if firstA.TransactionID == "" || firstB.TransactionID == "" || firstA.TransactionID == firstB.TransactionID {
		t.Fatalf("provider-scoped transaction IDs = %q/%q", firstA.TransactionID, firstB.TransactionID)
	}
	if firstA.IdempotentReplay || firstA.Balance.Amount != "90.00" || firstB.IdempotentReplay || firstB.Balance.Amount != "80.00" {
		t.Fatalf("initial provider results = A %+v, B %+v", firstA, firstB)
	}

	replayA := post(providerA, idempotencyKey, bodyA, http.StatusOK)
	replayB := post(providerB, idempotencyKey, bodyB, http.StatusOK)
	if !replayA.IdempotentReplay || replayA.TransactionID != firstA.TransactionID || replayA.Balance.Amount != "90.00" {
		t.Fatalf("provider A historical replay = %+v", replayA)
	}
	if !replayB.IdempotentReplay || replayB.TransactionID != firstB.TransactionID || replayB.Balance.Amount != "80.00" {
		t.Fatalf("provider B historical replay = %+v", replayB)
	}

	financialRows := func() string {
		return fmt.Sprintf("%d/%d/%d", queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions`), queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries`), queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events`))
	}
	beforeConflicts := financialRows()
	changedPayload := transactionJSON("provider-a", externalID, "task-11-7-http-player", wallet.ID(), "BET", "11.00", "")
	postConflict(idempotencyKey, changedPayload, "IDEMPOTENCY_CONFLICT")
	postConflict("task-11-7-another-key", bodyA, "EXTERNAL_TRANSACTION_CONFLICT")
	if afterConflicts := financialRows(); afterConflicts != beforeConflicts {
		t.Fatalf("conflicts changed transaction/ledger/outbox counts from %s to %s", beforeConflicts, afterConflicts)
	}

	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1 AND idempotency_key=$2`, 2, externalID, idempotencyKey)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id=$1`, 1, externalID)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE provider_id='provider-b' AND external_transaction_id=$1`, 1, externalID)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries entries JOIN wager_transactions transactions ON transactions.id=entries.transaction_id WHERE transactions.external_transaction_id=$1`, 2, externalID)
	assertWalletBalance(t, ctx, service, wallet.ID(), "80.00")
	assertReconciled(t, ctx, service, wallet.ID())
}
