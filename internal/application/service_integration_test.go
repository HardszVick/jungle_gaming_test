//go:build integration

package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

type integrationClock struct{ now time.Time }

func (clock *integrationClock) Now() time.Time { return clock.now }

type integrationIDs struct{}

func (integrationIDs) New() (uuid.UUID, error) { return uuid.New(), nil }

func TestServiceFinancialWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	environment, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer environment.Close(context.Background())
	resource, err := environment.NewDatabase(ctx, "application")
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
	service, err := NewService(database, clock, integrationIDs{}, ServiceOptions{
		Retry:     RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Reference: ReferencePolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute, TTL: time.Hour},
		Jitter:    func(delay time.Duration) time.Duration { return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	brl, _ := domain.ParseCurrency("BRL")
	initial, _ := domain.NewMoney(10000, brl)
	wallet, err := service.OpenWallet(ctx, OpenWalletRequest{PlayerID: "player-1", InitialBalance: initial})
	if err != nil {
		t.Fatal(err)
	}

	processInput := func(input TransactionCommandInput, key string, inbox *InboxMessage) TransactionResult {
		t.Helper()
		command, normalizeErr := NormalizeTransactionCommand(input)
		if normalizeErr != nil {
			t.Fatal(normalizeErr)
		}
		result, processErr := service.ProcessTransaction(ctx, ProcessTransactionRequest{
			Command: command, IdempotencyKey: key, Inbox: inbox,
		})
		if processErr != nil {
			t.Fatal(processErr)
		}
		return result
	}
	process := func(externalID, key, kind, amount, reference string, inbox *InboxMessage) TransactionResult {
		t.Helper()
		return processInput(TransactionCommandInput{
			ProviderID: "provider-1", ExternalTransactionID: externalID,
			PlayerID: "player-1", WalletID: wallet.ID(), RoundID: "round-1",
			GameID: "game-1", Kind: kind, Amount: amount, Currency: "BRL",
			ReferenceExternalTransactionID: reference,
		}, key, inbox)
	}

	bet := process("bet-1", "key-bet-1", "BET", "25.00", "", nil)
	assertResultBalance(t, bet, "75.00")
	win := process("win-1", "key-win-1", "WIN", "10.00", "bet-1", nil)
	persistedWin, err := service.GetTransactionByExternalID(ctx, "provider-1", "win-1")
	if err != nil || persistedWin.ReferenceTransactionID != bet.Transaction.ID() {
		t.Fatalf("persisted WIN reference = %q, %v", persistedWin.ReferenceTransactionID, err)
	}
	assertResultBalance(t, win, "85.00")
	inbox := &InboxMessage{ConsumerName: "integration-consumer", MessageID: "message-1"}
	inbox.PayloadHash[0] = 1
	process("loss-1", "key-loss-1", "LOSS", "0.00", "", inbox)
	inboxReplay := process("loss-1", "key-loss-1", "LOSS", "0.00", "", inbox)
	if !inboxReplay.IdempotentReplay {
		t.Fatal("completed inbox delivery was not replayed")
	}
	process("refund-1", "key-refund-1", "REFUND", "25.00", "bet-1", nil)
	process("rollback-refund-1", "key-rollback-refund-1", "ROLLBACK", "25.00", "refund-1", nil)
	duplicate := process("refund-duplicate", "key-refund-duplicate", "REFUND", "25.00", "bet-1", nil)
	if duplicate.Transaction.State() != domain.TransactionStateRejected || duplicate.Transaction.FailureCode() != domain.FailureAlreadyReversed {
		t.Fatalf("duplicate reversal = %s/%s", duplicate.Transaction.State(), duplicate.Transaction.FailureCode())
	}

	pending := process("win-late", "key-win-late", "WIN", "2.00", "late-bet", nil)
	if pending.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("pending state = %s", pending.Transaction.State())
	}
	process("late-bet", "key-late-bet", "BET", "5.00", "", nil)
	if _, err := database.Pool().Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1`, pending.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	claims, err := service.ClaimPendingReferences(ctx, "worker-1", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("pending claims = %d, %v", len(claims), err)
	}
	competingClaims, err := service.ClaimPendingReferences(ctx, "worker-2", 10, time.Minute)
	if err != nil || len(competingClaims) != 0 {
		t.Fatalf("competing pending claims = %d, %v", len(competingClaims), err)
	}
	resumed, err := service.ResumePendingReference(ctx, claims[0].Record.Transaction.ID(), "worker-1")
	if err != nil || resumed.Transaction.State() != domain.TransactionStateProcessed {
		t.Fatalf("resumed transaction = %#v, %v", resumed.Transaction, err)
	}
	walletBeforeMismatch, err := service.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	playerMismatchInput := TransactionCommandInput{
		ProviderID: "provider-1", ExternalTransactionID: "player-mismatch",
		PlayerID: "other-player", WalletID: wallet.ID(), RoundID: "round-1",
		GameID: "game-1", Kind: "BET", Amount: "1.00", Currency: "BRL",
	}
	mismatchInbox := &InboxMessage{ConsumerName: "integration-consumer", MessageID: "mismatch-message"}
	mismatchInbox.PayloadHash[0] = 3
	playerMismatch := processInput(playerMismatchInput, "key-player-mismatch", mismatchInbox)
	assertRejected(t, playerMismatch, domain.FailureWalletMismatch)
	persistedPlayerMismatch, err := service.GetTransactionByExternalID(ctx, "provider-1", "player-mismatch")
	if err != nil || persistedPlayerMismatch.Transaction.ID() != playerMismatch.Transaction.ID() {
		t.Fatalf("persisted player mismatch = %#v, %v", persistedPlayerMismatch.Transaction, err)
	}
	if _, ok := persistedPlayerMismatch.Transaction.ResultBalance(); ok {
		t.Fatal("rejected player mismatch persisted a result balance")
	}
	playerMismatchReplay := processInput(playerMismatchInput, "key-player-mismatch", mismatchInbox)
	if !playerMismatchReplay.IdempotentReplay || playerMismatchReplay.Transaction.ID() != playerMismatch.Transaction.ID() {
		t.Fatalf("player mismatch replay = %#v", playerMismatchReplay)
	}
	currencyMismatchInput := TransactionCommandInput{
		ProviderID: "provider-1", ExternalTransactionID: "currency-mismatch",
		PlayerID: "player-1", WalletID: wallet.ID(), RoundID: "round-1",
		GameID: "game-1", Kind: "BET", Amount: "1.00", Currency: "USD",
	}
	currencyMismatch := processInput(currencyMismatchInput, "key-currency-mismatch", nil)
	assertRejected(t, currencyMismatch, domain.FailureCurrencyMismatch)
	walletAfterMismatch, err := service.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if walletAfterMismatch.Balance().String() != walletBeforeMismatch.Balance().String() || walletAfterMismatch.Version() != walletBeforeMismatch.Version() {
		t.Fatalf("mismatch changed wallet from %s/%d to %s/%d",
			walletBeforeMismatch.Balance(), walletBeforeMismatch.Version(),
			walletAfterMismatch.Balance(), walletAfterMismatch.Version())
	}

	replay := process("bet-1", "key-bet-1", "BET", "25.00", "", nil)
	if !replay.IdempotentReplay {
		t.Fatal("financial replay was not identified")
	}
	assertResultBalance(t, replay, "75.00")
	reconciliation, err := service.Reconcile(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !reconciliation.Consistent || reconciliation.WalletBalance.String() != "82.00" || reconciliation.EntryCount != 7 {
		t.Fatalf("reconciliation = %+v", reconciliation)
	}
	expiring := process("refund-expiring", "key-refund-expiring", "REFUND", "1.00", "never-arrives", nil)
	clock.now = clock.now.Add(2 * time.Hour)
	if _, err := database.Pool().Exec(ctx, `
		UPDATE wager_transactions
		SET reference_next_attempt_at = CURRENT_TIMESTAMP, reference_expires_at = CURRENT_TIMESTAMP
		WHERE id = $1`, expiring.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	expiredClaims, err := service.ClaimPendingReferences(ctx, "worker-expiration", 10, time.Minute)
	if err != nil || len(expiredClaims) != 1 {
		t.Fatalf("expired claims = %d, %v", len(expiredClaims), err)
	}
	expired, err := service.ResumePendingReference(ctx, expiring.Transaction.ID(), "worker-expiration")
	if err != nil || expired.Transaction.State() != domain.TransactionStateRejected || expired.Transaction.FailureCode() != domain.FailureReferenceNotFound {
		t.Fatalf("expired transaction = %#v, %v", expired.Transaction, err)
	}
	firstPage, err := service.ListLedger(ctx, wallet.ID(), nil, 2)
	if err != nil || len(firstPage.Entries) != 2 || firstPage.NextCursor == nil {
		t.Fatalf("first ledger page = %+v, %v", firstPage, err)
	}
	secondPage, err := service.ListLedger(ctx, wallet.ID(), firstPage.NextCursor, 2)
	if err != nil || len(secondPage.Entries) != 2 {
		t.Fatalf("second ledger page = %+v, %v", secondPage, err)
	}
	if firstPage.Entries[0].ID() == secondPage.Entries[0].ID() || firstPage.Entries[1].ID() == secondPage.Entries[0].ID() {
		t.Fatal("stable ledger cursor repeated an entry")
	}

	var conflictingHash [32]byte
	conflictingHash[0] = 2
	err = database.ReadCommitted(ctx, func(store *postgres.Store) error {
		_, beginErr := store.BeginInbox(ctx, inbox.ConsumerName, inbox.MessageID, conflictingHash, clock.Now())
		return beginErr
	})
	if !errors.Is(err, postgres.ErrInboxHashConflict) {
		t.Fatalf("inbox hash conflict = %v", err)
	}
	outboxClaims, err := database.Store().ClaimOutbox(ctx, "publisher-1", 100, time.Minute)
	if err != nil || len(outboxClaims) == 0 {
		t.Fatalf("outbox claims = %d, %v", len(outboxClaims), err)
	}
	var playerMismatchEvent, currencyMismatchEvent bool
	for _, claim := range outboxClaims {
		if claim.EventType != domain.EventTypeWagerTransactionRejected {
			continue
		}
		playerMismatchEvent = playerMismatchEvent || claim.AggregateID == playerMismatch.Transaction.ID()
		currencyMismatchEvent = currencyMismatchEvent || claim.AggregateID == currencyMismatch.Transaction.ID()
	}
	if !playerMismatchEvent || !currencyMismatchEvent {
		t.Fatalf("missing mismatch rejection events: player=%t currency=%t", playerMismatchEvent, currencyMismatchEvent)
	}
	competingOutboxClaims, err := database.Store().ClaimOutbox(ctx, "publisher-2", 100, time.Minute)
	if err != nil || len(competingOutboxClaims) != 0 {
		t.Fatalf("competing outbox claims = %d, %v", len(competingOutboxClaims), err)
	}

	databaseNow, err := database.Store().TransactionTimestamp(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clock.now = databaseNow.Add(-24 * time.Hour)
	skewedPending := process("skewed-pending", "key-skewed-pending", "WIN", "1.00", "still-missing", nil)
	if _, err := database.Pool().Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1`, skewedPending.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	skewedClaims, err := service.ClaimPendingReferences(ctx, "skewed-worker-1", 10, time.Minute)
	if err != nil || len(skewedClaims) != 1 {
		t.Fatalf("initial skewed claim = %d, %v", len(skewedClaims), err)
	}
	clock.now = databaseNow.Add(24 * time.Hour)
	prematureClaims, err := service.ClaimPendingReferences(ctx, "skewed-worker-2", 10, time.Minute)
	if err != nil || len(prematureClaims) != 0 {
		t.Fatalf("future-clock replica reclaimed active lease: claims=%d err=%v", len(prematureClaims), err)
	}
	rescheduled, err := service.ResumePendingReference(ctx, skewedPending.Transaction.ID(), "skewed-worker-1")
	if err != nil || rescheduled.Transaction.State() != domain.TransactionStatePendingReference {
		t.Fatalf("future-clock replica expired pending reference: state=%v err=%v", rescheduled.Transaction, err)
	}
	persistedSkewed, err := service.GetTransaction(ctx, skewedPending.Transaction.ID())
	if err != nil || persistedSkewed.ReferenceNextAttemptAt == nil ||
		persistedSkewed.ReferenceNextAttemptAt.Before(databaseNow) || persistedSkewed.ReferenceNextAttemptAt.After(databaseNow.Add(time.Minute)) {
		t.Fatalf("pending reference was not rescheduled from PostgreSQL time: %+v, %v", persistedSkewed, err)
	}

	walletBeforeSkew, err := service.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	clock.now = walletBeforeSkew.UpdatedAt().Add(-time.Hour)
	behindReplica := process("behind-replica-bet", "key-behind-replica-bet", "BET", "1.00", "", nil)
	if behindReplica.Transaction.State() != domain.TransactionStateProcessed {
		t.Fatalf("behind-replica transaction state = %s", behindReplica.Transaction.State())
	}
	walletAfterSkew, err := service.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if walletAfterSkew.UpdatedAt().Before(walletBeforeSkew.UpdatedAt()) {
		t.Fatalf("wallet timestamp regressed from %v to %v", walletBeforeSkew.UpdatedAt(), walletAfterSkew.UpdatedAt())
	}
}

func assertResultBalance(t *testing.T, result TransactionResult, want string) {
	t.Helper()
	balance, ok := result.Transaction.ResultBalance()
	if !ok || balance.String() != want {
		t.Fatalf("result balance = %s, %t; want %s", balance.String(), ok, want)
	}
}

func assertRejected(t *testing.T, result TransactionResult, code domain.FailureCode) {
	t.Helper()
	if result.Transaction.State() != domain.TransactionStateRejected || result.Transaction.FailureCode() != code {
		t.Fatalf("transaction rejection = %s/%s; want REJECTED/%s",
			result.Transaction.State(), result.Transaction.FailureCode(), code)
	}
}
