package domain

import (
	"errors"
	"testing"
	"time"
)

func TestOperationAmountPolicyAndDirections(t *testing.T) {
	brl := testCurrency(t, "BRL")
	zero := testMoney(t, "0.00", brl)
	positive := testMoney(t, "1.00", brl)
	for _, kind := range []WagerTransactionKind{TransactionKindBet, TransactionKindWin, TransactionKindRefund, TransactionKindRollback, TransactionKindOpening} {
		if err := ValidateOperationAmount(kind, zero, false); codeOf(err) != FailureInvalidAmount {
			t.Errorf("%s zero code = %q, err=%v", kind, codeOf(err), err)
		}
		if err := ValidateOperationAmount(kind, positive, false); err != nil {
			t.Errorf("%s positive error = %v", kind, err)
		}
	}
	if err := ValidateOperationAmount(TransactionKindLoss, zero, true); err != nil {
		t.Errorf("zero LOSS error = %v", err)
	}
	if err := ValidateOperationAmount(TransactionKindLoss, positive, true); codeOf(err) != FailureInvalidAmount {
		t.Errorf("positive LOSS code = %q", codeOf(err))
	}
	if err := ValidateOperationAmount(TransactionKindOpening, positive, true); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("external OPENING error = %v", err)
	}

	directions := []struct {
		kind      WagerTransactionKind
		reference WagerTransactionKind
		direction LedgerDirection
		changes   bool
	}{
		{TransactionKindOpening, "", LedgerDirectionCredit, true},
		{TransactionKindBet, "", LedgerDirectionDebit, true},
		{TransactionKindWin, "", LedgerDirectionCredit, true},
		{TransactionKindLoss, "", "", false},
		{TransactionKindRefund, TransactionKindBet, LedgerDirectionCredit, true},
		{TransactionKindRollback, TransactionKindBet, LedgerDirectionCredit, true},
		{TransactionKindRollback, TransactionKindWin, LedgerDirectionDebit, true},
		{TransactionKindRollback, TransactionKindRefund, LedgerDirectionDebit, true},
	}
	for _, test := range directions {
		direction, changes, err := OperationDirection(test.kind, test.reference)
		if err != nil || direction != test.direction || changes != test.changes {
			t.Errorf("OperationDirection(%s, %s) = (%s, %t, %v)", test.kind, test.reference, direction, changes, err)
		}
	}
}

func TestApplyOperationAndDistinctInsufficientFundsCodes(t *testing.T) {
	brl := testCurrency(t, "BRL")
	now := time.Now().UTC()
	wallet, _ := NewWallet("wallet-1", "player-1", testMoney(t, "10.00", brl), now)
	direction, changed, err := ApplyOperation(wallet, TransactionKindLoss, testMoney(t, "0.00", brl), "", now)
	if err != nil || changed || direction != "" || wallet.Version() != 1 {
		t.Fatalf("LOSS = (%s, %t, %v), version=%d", direction, changed, err, wallet.Version())
	}
	_, _, err = ApplyOperation(wallet, TransactionKindBet, testMoney(t, "10.01", brl), "", now)
	if codeOf(err) != FailureInsufficientFunds {
		t.Fatalf("BET insufficient code = %q, err=%v", codeOf(err), err)
	}
	_, _, err = ApplyOperation(wallet, TransactionKindRollback, testMoney(t, "10.01", brl), TransactionKindWin, now)
	if codeOf(err) != FailureReversalInsufficientFunds {
		t.Fatalf("ROLLBACK insufficient code = %q, err=%v", codeOf(err), err)
	}
	if wallet.Version() != 1 || wallet.Balance().String() != "10.00" {
		t.Fatal("rejected debit mutated wallet")
	}
}

func TestValidateReversalEligibilityAndMatching(t *testing.T) {
	now := time.Now().UTC()
	brl := testCurrency(t, "BRL")
	balance := testMoney(t, "90.00", brl)
	referenceDetails := testTransactionDetails(t, TransactionKindBet)
	referenceDetails.ExternalTransactionID = "reference-1"
	reference, _ := NewWagerTransaction("reference", referenceDetails, now)
	if err := reference.MarkProcessed(balance, now); err != nil {
		t.Fatal(err)
	}
	reversal, _ := NewWagerTransaction("reversal", testTransactionDetails(t, TransactionKindRefund), now)

	direction, err := ValidateReversal(reversal, reference, false)
	if err != nil || direction != LedgerDirectionCredit {
		t.Fatalf("valid refund = %s, %v", direction, err)
	}
	if _, err := ValidateReversal(reversal, reference, true); codeOf(err) != FailureAlreadyReversed {
		t.Fatalf("duplicate code = %q, err=%v", codeOf(err), err)
	}

	pending, _ := NewWagerTransaction("pending", referenceDetails, now)
	if _, err := ValidateReversal(reversal, pending, false); codeOf(err) != FailureReferencePending {
		t.Fatalf("pending code = %q, err=%v", codeOf(err), err)
	}

	mismatchDetails := testTransactionDetails(t, TransactionKindRefund)
	mismatchDetails.WalletID = "other-wallet"
	mismatch, _ := NewWagerTransaction("mismatch", mismatchDetails, now)
	if _, err := ValidateReversal(mismatch, reference, false); codeOf(err) != FailureWalletMismatch {
		t.Fatalf("wallet mismatch code = %q, err=%v", codeOf(err), err)
	}
}

func TestRollbackDirectionsForEveryEligibleReference(t *testing.T) {
	now := time.Now().UTC()
	brl := testCurrency(t, "BRL")
	balance := testMoney(t, "90.00", brl)
	tests := []struct {
		referenceKind WagerTransactionKind
		wantDirection LedgerDirection
	}{
		{TransactionKindBet, LedgerDirectionCredit},
		{TransactionKindWin, LedgerDirectionDebit},
		{TransactionKindRefund, LedgerDirectionDebit},
	}
	for _, test := range tests {
		t.Run(string(test.referenceKind), func(t *testing.T) {
			referenceDetails := testTransactionDetails(t, test.referenceKind)
			referenceDetails.ExternalTransactionID = "reference-1"
			reference, err := NewWagerTransaction("reference", referenceDetails, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := reference.MarkProcessed(balance, now); err != nil {
				t.Fatal(err)
			}
			rollback, err := NewWagerTransaction("rollback", testTransactionDetails(t, TransactionKindRollback), now)
			if err != nil {
				t.Fatal(err)
			}
			direction, err := ValidateReversal(rollback, reference, false)
			if err != nil || direction != test.wantDirection {
				t.Fatalf("ValidateReversal = %s, %v", direction, err)
			}
		})
	}
}

func TestValidateWinReference(t *testing.T) {
	now := time.Now().UTC()
	brl := testCurrency(t, "BRL")
	referenceDetails := testTransactionDetails(t, TransactionKindBet)
	referenceDetails.ExternalTransactionID = "bet-1"
	referenceDetails.Money = testMoney(t, "25.00", brl)
	reference, err := NewWagerTransaction("bet", referenceDetails, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := reference.MarkProcessed(testMoney(t, "75.00", brl), now); err != nil {
		t.Fatal(err)
	}

	winDetails := testTransactionDetails(t, TransactionKindWin)
	winDetails.ReferenceExternalTransactionID = "bet-1"
	winDetails.Money = testMoney(t, "10.00", brl)
	win, err := NewWagerTransaction("win", winDetails, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateWinReference(win, reference); err != nil {
		t.Fatalf("different WIN and BET amounts should be valid: %v", err)
	}

	mismatches := []struct {
		name   string
		code   FailureCode
		mutate func(*WagerTransactionDetails)
	}{
		{"external ID", FailureReferenceMismatch, func(details *WagerTransactionDetails) { details.ExternalTransactionID = "other-bet" }},
		{"provider", FailureReferenceMismatch, func(details *WagerTransactionDetails) { details.ProviderID = "other-provider" }},
		{"player", FailureReferenceMismatch, func(details *WagerTransactionDetails) { details.PlayerID = "other-player" }},
		{"round", FailureReferenceMismatch, func(details *WagerTransactionDetails) { details.RoundID = "other-round" }},
		{"wallet", FailureWalletMismatch, func(details *WagerTransactionDetails) { details.WalletID = "other-wallet" }},
		{"currency", FailureCurrencyMismatch, func(details *WagerTransactionDetails) {
			details.Money = testMoney(t, "25.00", testCurrency(t, "USD"))
		}},
	}
	for _, test := range mismatches {
		t.Run(test.name, func(t *testing.T) {
			details := referenceDetails
			test.mutate(&details)
			candidate, err := NewWagerTransaction("mismatched-bet", details, now)
			if err != nil {
				t.Fatal(err)
			}
			balance, _ := NewMoney(7500, details.Money.Currency())
			if err := candidate.MarkProcessed(balance, now); err != nil {
				t.Fatal(err)
			}
			if err := ValidateWinReference(win, candidate); codeOf(err) != test.code {
				t.Fatalf("code = %q, err=%v", codeOf(err), err)
			}
		})
	}

	pending, _ := NewWagerTransaction("pending-bet", referenceDetails, now)
	if err := ValidateWinReference(win, pending); codeOf(err) != FailureReferencePending {
		t.Fatalf("pending reference code = %q, err=%v", codeOf(err), err)
	}

	wrongKindDetails := referenceDetails
	wrongKindDetails.Kind = TransactionKindWin
	wrongKindDetails.ReferenceExternalTransactionID = ""
	wrongKind, _ := NewWagerTransaction("other-win", wrongKindDetails, now)
	if err := wrongKind.MarkProcessed(testMoney(t, "75.00", brl), now); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWinReference(win, wrongKind); codeOf(err) != FailureReferenceNotEligible {
		t.Fatalf("wrong reference kind code = %q, err=%v", codeOf(err), err)
	}
}

func codeOf(err error) FailureCode {
	code, _ := FailureCodeOf(err)
	return code
}
