package domain

import (
	"errors"
	"testing"
	"time"
)

func testTransactionDetails(t *testing.T, kind WagerTransactionKind) WagerTransactionDetails {
	t.Helper()
	brl := testCurrency(t, "BRL")
	amount := "10.00"
	if kind == TransactionKindLoss {
		amount = "0.00"
	}
	details := WagerTransactionDetails{
		ProviderID: "provider-1", ExternalTransactionID: "external-1",
		PlayerID: "player-1", WalletID: "wallet-1", RoundID: "round-1",
		GameID: "game-1", Kind: kind, Money: testMoney(t, amount, brl),
	}
	if kind.IsReversal() {
		details.ReferenceExternalTransactionID = "reference-1"
	}
	return details
}

func TestWagerTransactionValidTransitions(t *testing.T) {
	now := time.Now().UTC()
	balance := testMoney(t, "100.00", testCurrency(t, "BRL"))
	actions := []struct {
		name string
		to   WagerTransactionState
		act  func(*WagerTransaction) error
	}{
		{"pending reference", TransactionStatePendingReference, func(tx *WagerTransaction) error { return tx.MarkPendingReference(now.Add(time.Second)) }},
		{"processed", TransactionStateProcessed, func(tx *WagerTransaction) error { return tx.MarkProcessed(balance, now.Add(time.Second)) }},
		{"rejected", TransactionStateRejected, func(tx *WagerTransaction) error { return tx.Reject(FailureInvalidAmount, now.Add(time.Second)) }},
		{"failed", TransactionStateFailed, func(tx *WagerTransaction) error { return tx.Fail(now.Add(time.Second)) }},
	}

	for _, start := range []WagerTransactionState{TransactionStatePending, TransactionStatePendingReference} {
		for _, action := range actions {
			t.Run(string(start)+"_to_"+action.name, func(t *testing.T) {
				tx, err := RehydrateWagerTransaction(WagerTransactionSnapshot{
					ID: "tx-1", Details: testTransactionDetails(t, TransactionKindRefund),
					State: start, CreatedAt: now, UpdatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				err = action.act(tx)
				wantAllowed := start == TransactionStatePending || action.to.IsTerminal()
				if wantAllowed && err != nil {
					t.Fatalf("transition returned %v", err)
				}
				if !wantAllowed && !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("transition error = %v, want ErrInvalidTransition", err)
				}
			})
		}
	}
}

func TestWagerTransactionTerminalStatesAreImmutable(t *testing.T) {
	now := time.Now().UTC()
	balance := testMoney(t, "100.00", testCurrency(t, "BRL"))
	tests := []WagerTransactionSnapshot{
		{ID: "processed", Details: testTransactionDetails(t, TransactionKindBet), State: TransactionStateProcessed, ResultBalance: &balance, CreatedAt: now, UpdatedAt: now},
		{ID: "rejected", Details: testTransactionDetails(t, TransactionKindBet), State: TransactionStateRejected, FailureCode: FailureInsufficientFunds, CreatedAt: now, UpdatedAt: now},
		{ID: "failed", Details: testTransactionDetails(t, TransactionKindBet), State: TransactionStateFailed, FailureCode: FailurePermanentProcessing, CreatedAt: now, UpdatedAt: now},
	}
	for _, snapshot := range tests {
		t.Run(string(snapshot.State), func(t *testing.T) {
			tx, err := RehydrateWagerTransaction(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			originalState := tx.State()
			if err := tx.Reject(FailureInvalidAmount, now.Add(time.Second)); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("terminal transition error = %v", err)
			}
			if tx.State() != originalState {
				t.Fatal("failed terminal transition mutated state")
			}
		})
	}
}

func TestWagerTransactionTransitionClampsReplicaClockSkew(t *testing.T) {
	updatedAt := time.Now().UTC()
	tx, err := RehydrateWagerTransaction(WagerTransactionSnapshot{
		ID: "tx-clock-skew", Details: testTransactionDetails(t, TransactionKindBet),
		State: TransactionStatePending, CreatedAt: updatedAt.Add(-time.Hour), UpdatedAt: updatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	balance := testMoney(t, "90.00", testCurrency(t, "BRL"))
	if err := tx.MarkProcessed(balance, updatedAt.Add(-30*time.Minute)); err != nil {
		t.Fatalf("transition from a behind-clock replica: %v", err)
	}
	if tx.State() != TransactionStateProcessed || !tx.UpdatedAt().Equal(updatedAt) {
		t.Fatalf("transaction state/time = %s/%s, want PROCESSED/%s", tx.State(), tx.UpdatedAt(), updatedAt)
	}
}

func TestWagerTransactionCompleteTransitionMatrix(t *testing.T) {
	now := time.Now().UTC()
	balance := testMoney(t, "100.00", testCurrency(t, "BRL"))
	states := []WagerTransactionState{
		TransactionStatePending,
		TransactionStatePendingReference,
		TransactionStateProcessed,
		TransactionStateRejected,
		TransactionStateFailed,
	}
	targets := []WagerTransactionState{
		TransactionStatePendingReference,
		TransactionStateProcessed,
		TransactionStateRejected,
		TransactionStateFailed,
	}
	for _, from := range states {
		for _, to := range targets {
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				snapshot := WagerTransactionSnapshot{
					ID: "tx-matrix", Details: testTransactionDetails(t, TransactionKindRefund),
					State: from, CreatedAt: now, UpdatedAt: now,
				}
				switch from {
				case TransactionStateProcessed:
					snapshot.ResultBalance = &balance
				case TransactionStateRejected:
					snapshot.FailureCode = FailureInvalidAmount
				case TransactionStateFailed:
					snapshot.FailureCode = FailurePermanentProcessing
				}
				tx, err := RehydrateWagerTransaction(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				switch to {
				case TransactionStatePendingReference:
					err = tx.MarkPendingReference(now.Add(time.Second))
				case TransactionStateProcessed:
					err = tx.MarkProcessed(balance, now.Add(time.Second))
				case TransactionStateRejected:
					err = tx.Reject(FailureInvalidAmount, now.Add(time.Second))
				case TransactionStateFailed:
					err = tx.Fail(now.Add(time.Second))
				}
				wantValid := from == TransactionStatePending ||
					(from == TransactionStatePendingReference && to.IsTerminal())
				if wantValid && err != nil {
					t.Fatalf("valid transition returned %v", err)
				}
				if !wantValid && !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("invalid transition error = %v", err)
				}
				if !wantValid && tx.State() != from {
					t.Fatalf("invalid transition changed state to %s", tx.State())
				}
			})
		}
	}
}

func TestWagerTransactionValidation(t *testing.T) {
	now := time.Now().UTC()
	tx, err := NewWagerTransaction("tx-1", testTransactionDetails(t, TransactionKindBet), now)
	if err != nil || tx.State() != TransactionStatePending {
		t.Fatalf("NewWagerTransaction = %v, %v", tx, err)
	}
	if err := tx.MarkPendingReference(now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("BET pending reference error = %v", err)
	}
	if _, err := ParseExternalWagerTransactionKind("opening"); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("external OPENING error = %v", err)
	}
	if _, err := ParseExternalWagerTransactionKind("bet"); err != nil {
		t.Fatalf("lowercase BET error = %v", err)
	}
}

func assertTransactionConstructorsReject(t *testing.T, details WagerTransactionDetails) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := NewWagerTransaction("tx-invalid", details, now); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("NewWagerTransaction error = %v, want ErrInvalidTransaction", err)
	}
	if _, err := RehydrateWagerTransaction(WagerTransactionSnapshot{
		ID: "tx-invalid", Details: details, State: TransactionStatePending,
		CreatedAt: now, UpdatedAt: now,
	}); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("RehydrateWagerTransaction error = %v, want ErrInvalidTransaction", err)
	}
}

func TestWagerTransactionConstructorsRejectInvalidAmountPolicy(t *testing.T) {
	brl := testCurrency(t, "BRL")
	negative, err := NewMoney(-100, brl)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		kind   WagerTransactionKind
		amount Money
	}{
		{"non-zero loss", TransactionKindLoss, testMoney(t, "10.00", brl)},
		{"zero opening", TransactionKindOpening, testMoney(t, "0.00", brl)},
		{"zero bet", TransactionKindBet, testMoney(t, "0.00", brl)},
		{"zero win", TransactionKindWin, testMoney(t, "0.00", brl)},
		{"zero refund", TransactionKindRefund, testMoney(t, "0.00", brl)},
		{"zero rollback", TransactionKindRollback, testMoney(t, "0.00", brl)},
		{"negative amount", TransactionKindBet, negative},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			details := testTransactionDetails(t, test.kind)
			details.Money = test.amount
			if test.kind == TransactionKindOpening {
				details.ProviderID = ""
				details.ExternalTransactionID = ""
				details.RoundID = ""
				details.GameID = ""
			}
			assertTransactionConstructorsReject(t, details)
		})
	}
}

func TestWagerTransactionConstructorsRejectOpeningExternalMetadata(t *testing.T) {
	base := WagerTransactionDetails{
		PlayerID: "player-1", WalletID: "wallet-1", Kind: TransactionKindOpening,
		Money: testMoney(t, "10.00", testCurrency(t, "BRL")),
	}
	mutations := []struct {
		name   string
		mutate func(*WagerTransactionDetails)
	}{
		{"provider ID", func(details *WagerTransactionDetails) { details.ProviderID = "provider-1" }},
		{"external transaction ID", func(details *WagerTransactionDetails) { details.ExternalTransactionID = "external-1" }},
		{"round ID", func(details *WagerTransactionDetails) { details.RoundID = "round-1" }},
		{"game ID", func(details *WagerTransactionDetails) { details.GameID = "game-1" }},
		{"reference transaction ID", func(details *WagerTransactionDetails) { details.ReferenceExternalTransactionID = "reference-1" }},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			details := base
			mutation.mutate(&details)
			assertTransactionConstructorsReject(t, details)
		})
	}
}

func TestWagerTransactionReferencePolicy(t *testing.T) {
	now := time.Now().UTC()
	winDetails := testTransactionDetails(t, TransactionKindWin)
	winDetails.ReferenceExternalTransactionID = "bet-1"
	win, err := NewWagerTransaction("win-1", winDetails, now)
	if err != nil {
		t.Fatalf("referenced WIN: %v", err)
	}
	if err := win.MarkPendingReference(now.Add(time.Second)); err != nil {
		t.Fatalf("referenced WIN pending transition: %v", err)
	}

	unreferencedWin, err := NewWagerTransaction("win-2", testTransactionDetails(t, TransactionKindWin), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := unreferencedWin.MarkPendingReference(now.Add(time.Second)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unreferenced WIN pending error = %v", err)
	}

	for _, kind := range []WagerTransactionKind{TransactionKindBet, TransactionKindLoss} {
		details := testTransactionDetails(t, kind)
		details.ReferenceExternalTransactionID = "bet-1"
		if _, err := NewWagerTransaction("invalid-reference", details, now); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("%s reference error = %v", kind, err)
		}
	}
}
