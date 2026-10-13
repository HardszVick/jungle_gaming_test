//go:build integration

package application

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

func TestPermanentPendingFailureLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	environment, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer environment.Close(context.Background())
	resource, err := environment.NewDatabase(ctx, "permanent_failure")
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close(context.Background())
	database, err := postgres.Open(ctx, resource.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := resource.MigrateUp(ctx); err != nil {
		t.Fatal(err)
	}

	clock := &integrationClock{now: time.Now().UTC()}
	service := newPermanentFailureService(t, database, clock)
	brl, err := domain.ParseCurrency("BRL")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := domain.NewMoney(10000, brl)
	if err != nil {
		t.Fatal(err)
	}
	wallet, err := service.OpenWallet(ctx, OpenWalletRequest{PlayerID: "player-permanent", InitialBalance: initial})
	if err != nil {
		t.Fatal(err)
	}

	overflowCommand := permanentFailureCommand(t, wallet.ID(), "overflow-win", "WIN", "92233720368547758.07", "overflow-bet")
	pending := processPermanentFailureCommand(t, ctx, service, overflowCommand, "overflow-win-key", nil)
	if pending.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("overflow transaction state = %s", pending.Transaction.State())
	}
	betCommand := permanentFailureCommand(t, wallet.ID(), "overflow-bet", "BET", "1.00", "")
	processPermanentFailureCommand(t, ctx, service, betCommand, "overflow-bet-key", nil)
	clock.now = clock.now.Add(2 * time.Second)
	makePendingReferenceDue(t, ctx, database, pending.Transaction.ID())
	claims, err := service.ClaimPendingReferences(ctx, "overflow-worker", 10, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Record.Transaction.ID() != pending.Transaction.ID() {
		t.Fatalf("overflow claims = %#v, %v", claims, err)
	}
	walletBefore := permanentFailureWallet(t, ctx, service, wallet.ID())
	ledgerBefore, outboxBefore := permanentFailureCounts(t, ctx, database)

	failed, err := service.ResumePendingReference(ctx, pending.Transaction.ID(), "overflow-worker")
	if err != nil {
		t.Fatal(err)
	}
	assertPermanentFailure(t, failed.Transaction)
	persisted, err := service.GetTransaction(ctx, pending.Transaction.ID())
	if err != nil {
		t.Fatal(err)
	}
	assertPermanentFailure(t, persisted.Transaction)
	if persisted.ReferenceNextAttemptAt != nil || persisted.ReferenceExpiresAt != nil ||
		persisted.ReferenceClaimedBy != "" || persisted.ReferenceClaimUntil != nil {
		t.Fatalf("FAILED transaction retained reference scheduling: %+v", persisted)
	}
	walletAfter := permanentFailureWallet(t, ctx, service, wallet.ID())
	ledgerAfter, outboxAfter := permanentFailureCounts(t, ctx, database)
	if walletAfter.Balance() != walletBefore.Balance() || walletAfter.Version() != walletBefore.Version() ||
		ledgerAfter != ledgerBefore || outboxAfter != outboxBefore {
		t.Fatalf("terminalization changed financial state: wallet %s/%d -> %s/%d, ledger %d -> %d, outbox %d -> %d",
			walletBefore.Balance(), walletBefore.Version(), walletAfter.Balance(), walletAfter.Version(),
			ledgerBefore, ledgerAfter, outboxBefore, outboxAfter)
	}

	restarted := newPermanentFailureService(t, database, clock)
	afterRestart, err := restarted.GetTransaction(ctx, pending.Transaction.ID())
	if err != nil {
		t.Fatal(err)
	}
	assertPermanentFailure(t, afterRestart.Transaction)
	replay := processPermanentFailureCommand(t, ctx, restarted, overflowCommand, "overflow-win-key", nil)
	if !replay.IdempotentReplay {
		t.Fatal("FAILED transaction was not returned as an idempotent replay")
	}
	assertPermanentFailure(t, replay.Transaction)

	inbox := &InboxMessage{ConsumerName: "permanent-consumer", MessageID: "failed-message"}
	inbox.PayloadHash[0] = 7
	inboxReplay := processPermanentFailureCommand(t, ctx, restarted, overflowCommand, "overflow-win-key", inbox)
	if !inboxReplay.IdempotentReplay {
		t.Fatal("FAILED inbox delivery was not returned as a replay")
	}
	inboxResult, err := database.Store().BeginInbox(ctx, inbox.ConsumerName, inbox.MessageID, inbox.PayloadHash, clock.Now())
	if err != nil || inboxResult.Status != postgres.InboxCompleted || inboxResult.TransactionID != pending.Transaction.ID() {
		t.Fatalf("completed FAILED inbox = %+v, %v", inboxResult, err)
	}
	ledgerAfterReplay, outboxAfterReplay := permanentFailureCounts(t, ctx, database)
	if ledgerAfterReplay != ledgerBefore || outboxAfterReplay != outboxBefore {
		t.Fatalf("FAILED replay created financial output: ledger=%d outbox=%d", ledgerAfterReplay, outboxAfterReplay)
	}

	t.Run("rollback remains recoverable", func(t *testing.T) {
		command := permanentFailureCommand(t, wallet.ID(), "rollback-pending", "REFUND", "1.00", "missing-for-rollback")
		pendingRollback := processPermanentFailureCommand(t, ctx, service, command, "rollback-pending-key", nil)
		clock.now = clock.now.Add(2 * time.Second)
		makePendingReferenceDue(t, ctx, database, pendingRollback.Transaction.ID())
		claims, claimErr := service.ClaimPendingReferences(ctx, "rollback-worker", 10, time.Minute)
		if claimErr != nil || len(claims) != 1 || claims[0].Record.Transaction.ID() != pendingRollback.Transaction.ID() {
			t.Fatalf("rollback claims = %#v, %v", claims, claimErr)
		}
		blocker, beginErr := database.Pool().Begin(ctx)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		defer blocker.Rollback(context.Background())
		var lockedID string
		if lockErr := blocker.QueryRow(ctx, `SELECT id::text FROM wager_transactions WHERE id = $1 FOR UPDATE`, pendingRollback.Transaction.ID()).Scan(&lockedID); lockErr != nil {
			t.Fatal(lockErr)
		}
		blockedContext, blockedCancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer blockedCancel()
		if _, failErr := service.terminalizePermanentProcessing(blockedContext, lockedID, "rollback-worker"); !errors.Is(failErr, context.DeadlineExceeded) {
			t.Fatalf("blocked terminalization error = %v", failErr)
		}
		if rollbackErr := blocker.Rollback(ctx); rollbackErr != nil {
			t.Fatal(rollbackErr)
		}
		stillPending, loadErr := service.GetTransaction(ctx, lockedID)
		if loadErr != nil || stillPending.Transaction.State() != domain.TransactionStatePendingReference || stillPending.ReferenceClaimedBy != "rollback-worker" {
			t.Fatalf("rolled-back terminalization = %+v, %v", stillPending, loadErr)
		}
		recovered, recoverErr := service.terminalizePermanentProcessing(ctx, lockedID, "rollback-worker")
		if recoverErr != nil {
			t.Fatal(recoverErr)
		}
		assertPermanentFailure(t, recovered.Transaction)
	})

	t.Run("two instances converge under row lock", func(t *testing.T) {
		command := permanentFailureCommand(t, wallet.ID(), "race-pending", "REFUND", "1.00", "missing-for-race")
		pendingRace := processPermanentFailureCommand(t, ctx, service, command, "race-pending-key", nil)
		clock.now = clock.now.Add(2 * time.Second)
		makePendingReferenceDue(t, ctx, database, pendingRace.Transaction.ID())
		claims, claimErr := service.ClaimPendingReferences(ctx, "race-worker", 10, time.Minute)
		if claimErr != nil || len(claims) != 1 || claims[0].Record.Transaction.ID() != pendingRace.Transaction.ID() {
			t.Fatalf("race claims = %#v, %v", claims, claimErr)
		}
		services := []*Service{service, restarted}
		results := make(chan TransactionResult, len(services))
		errorsChannel := make(chan error, len(services))
		var waitGroup sync.WaitGroup
		for _, candidate := range services {
			waitGroup.Add(1)
			go func(candidate *Service) {
				defer waitGroup.Done()
				result, terminalErr := candidate.terminalizePermanentProcessing(ctx, pendingRace.Transaction.ID(), "race-worker")
				results <- result
				errorsChannel <- terminalErr
			}(candidate)
		}
		waitGroup.Wait()
		close(results)
		close(errorsChannel)
		for terminalErr := range errorsChannel {
			if terminalErr != nil {
				t.Fatal(terminalErr)
			}
		}
		for result := range results {
			assertPermanentFailure(t, result.Transaction)
		}
	})

	t.Run("persisted PENDING can be terminalized", func(t *testing.T) {
		command := permanentFailureCommand(t, wallet.ID(), "plain-pending", "WIN", "1.00", "missing-for-plain-pending")
		hash, hashErr := command.SHA256()
		if hashErr != nil {
			t.Fatal(hashErr)
		}
		transaction, createErr := domain.NewWagerTransaction(uuid.NewString(), command.DomainDetails(), clock.Now())
		if createErr != nil {
			t.Fatal(createErr)
		}
		if insertErr := database.ReadCommitted(ctx, func(store *postgres.Store) error {
			_, inserted, insertErr := store.InsertExternalTransaction(ctx, postgres.TransactionInsert{
				Transaction: transaction, IdempotencyKey: "plain-pending-key", RequestHash: hash[:], CreatedAt: clock.Now(),
			})
			if insertErr == nil && !inserted {
				t.Fatal("plain PENDING transaction was not inserted")
			}
			return insertErr
		}); insertErr != nil {
			t.Fatal(insertErr)
		}
		result, terminalErr := service.terminalizePermanentProcessing(ctx, transaction.ID(), "")
		if terminalErr != nil {
			t.Fatal(terminalErr)
		}
		assertPermanentFailure(t, result.Transaction)
	})
}

func makePendingReferenceDue(t *testing.T, ctx context.Context, database *postgres.Database, transactionID string) {
	t.Helper()
	if _, err := database.Pool().Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1`, transactionID); err != nil {
		t.Fatal(err)
	}
}

func newPermanentFailureService(t *testing.T, database *postgres.Database, clock Clock) *Service {
	t.Helper()
	service, err := NewService(database, clock, integrationIDs{}, ServiceOptions{
		Retry:     RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Reference: ReferencePolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute, TTL: time.Hour},
		Jitter:    func(delay time.Duration) time.Duration { return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func permanentFailureCommand(t *testing.T, walletID, externalID, kind, amount, reference string) TransactionCommand {
	t.Helper()
	command, err := NormalizeTransactionCommand(TransactionCommandInput{
		ProviderID: "permanent-provider", ExternalTransactionID: externalID,
		PlayerID: "player-permanent", WalletID: walletID, RoundID: "permanent-round",
		GameID: "permanent-game", Kind: kind, Amount: amount, Currency: "BRL",
		ReferenceExternalTransactionID: reference,
	})
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func processPermanentFailureCommand(t *testing.T, ctx context.Context, service *Service, command TransactionCommand, key string, inbox *InboxMessage) TransactionResult {
	t.Helper()
	result, err := service.ProcessTransaction(ctx, ProcessTransactionRequest{
		Command: command, IdempotencyKey: key, Inbox: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func permanentFailureWallet(t *testing.T, ctx context.Context, service *Service, walletID string) *domain.Wallet {
	t.Helper()
	wallet, err := service.GetWallet(ctx, walletID)
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

func permanentFailureCounts(t *testing.T, ctx context.Context, database *postgres.Database) (ledger, outbox int64) {
	t.Helper()
	if err := database.Pool().QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries`).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if err := database.Pool().QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	return ledger, outbox
}

func assertPermanentFailure(t *testing.T, transaction *domain.WagerTransaction) {
	t.Helper()
	if transaction == nil || transaction.State() != domain.TransactionStateFailed ||
		transaction.FailureCode() != domain.FailurePermanentProcessing {
		t.Fatalf("permanent failure = %#v", transaction)
	}
	if _, ok := transaction.ResultBalance(); ok {
		t.Fatal("FAILED transaction has a result balance")
	}
}
