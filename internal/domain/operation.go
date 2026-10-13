package domain

import (
	"errors"
	"fmt"
	"time"
)

type LedgerDirection string

const (
	LedgerDirectionCredit LedgerDirection = "CREDIT"
	LedgerDirectionDebit  LedgerDirection = "DEBIT"
)

func (d LedgerDirection) IsValid() bool {
	return d == LedgerDirectionCredit || d == LedgerDirectionDebit
}

func ValidateOperationAmount(kind WagerTransactionKind, amount Money, external bool) error {
	if !kind.IsValid() || !amount.Currency().IsValid() || amount.IsNegative() {
		return newRuleError(FailureInvalidAmount, "invalid operation amount", ErrInvalidMoney)
	}
	if external && kind == TransactionKindOpening {
		return fmt.Errorf("%w: OPENING is internal only", ErrInvalidTransaction)
	}
	if kind == TransactionKindLoss {
		if !amount.IsZero() {
			return newRuleError(FailureInvalidAmount, "LOSS amount must be zero", ErrInvalidMoney)
		}
		return nil
	}
	if amount.IsZero() {
		return newRuleError(FailureInvalidAmount, "operation amount must be positive", ErrInvalidMoney)
	}
	return nil
}

func OperationDirection(kind WagerTransactionKind, referencedKind WagerTransactionKind) (LedgerDirection, bool, error) {
	switch kind {
	case TransactionKindOpening, TransactionKindWin, TransactionKindRefund:
		return LedgerDirectionCredit, true, nil
	case TransactionKindBet:
		return LedgerDirectionDebit, true, nil
	case TransactionKindLoss:
		return "", false, nil
	case TransactionKindRollback:
		switch referencedKind {
		case TransactionKindBet:
			return LedgerDirectionCredit, true, nil
		case TransactionKindWin, TransactionKindRefund:
			return LedgerDirectionDebit, true, nil
		default:
			return "", false, newRuleError(FailureReferenceNotEligible, "ROLLBACK reference kind is not eligible", nil)
		}
	default:
		return "", false, fmt.Errorf("%w: unknown operation kind", ErrInvalidTransaction)
	}
}

// ValidateReversal checks reference eligibility and returns the compensating
// ledger direction. alreadyReversed is supplied by the persistence layer after
// checking successful reversals under the wallet lock.
func ValidateReversal(reversal, reference *WagerTransaction, alreadyReversed bool) (LedgerDirection, error) {
	if reversal == nil || reference == nil || !reversal.details.Kind.IsReversal() {
		return "", newRuleError(FailureReferenceNotEligible, "invalid reversal or reference", nil)
	}
	if reference.state == TransactionStatePending || reference.state == TransactionStatePendingReference {
		return "", newRuleError(FailureReferencePending, "reference is not terminal", nil)
	}
	if reference.state != TransactionStateProcessed {
		return "", newRuleError(FailureReferenceNotEligible, "reference was not processed", nil)
	}
	if alreadyReversed {
		return "", newRuleError(FailureAlreadyReversed, "reference already has a successful reversal", nil)
	}

	rd, ref := reversal.details, reference.details
	if rd.ReferenceExternalTransactionID != ref.ExternalTransactionID {
		return "", newRuleError(FailureReferenceMismatch, "reference identity differs", nil)
	}
	if rd.ProviderID != ref.ProviderID || rd.PlayerID != ref.PlayerID || rd.RoundID != ref.RoundID {
		return "", newRuleError(FailureReferenceMismatch, "provider, player, or round differs", nil)
	}
	if rd.WalletID != ref.WalletID {
		return "", newRuleError(FailureWalletMismatch, "wallet differs", nil)
	}
	equal, err := rd.Money.Equal(ref.Money)
	if errors.Is(err, ErrCurrencyMismatch) {
		return "", newRuleError(FailureCurrencyMismatch, "currency differs", err)
	}
	if err != nil || !equal {
		return "", newRuleError(FailureReferenceMismatch, "amount differs", err)
	}

	if rd.Kind == TransactionKindRefund && ref.Kind != TransactionKindBet {
		return "", newRuleError(FailureReferenceNotEligible, "REFUND requires a BET", nil)
	}
	returnDirection, _, err := OperationDirection(rd.Kind, ref.Kind)
	return returnDirection, err
}

// ValidateWinReference validates the optional association between a WIN and
// its originating BET. Unlike a reversal, their monetary values may differ.
func ValidateWinReference(win, reference *WagerTransaction) error {
	if win == nil || reference == nil || win.details.Kind != TransactionKindWin ||
		win.details.ReferenceExternalTransactionID == "" {
		return newRuleError(FailureReferenceNotEligible, "invalid WIN or reference", nil)
	}
	if reference.state == TransactionStatePending || reference.state == TransactionStatePendingReference {
		return newRuleError(FailureReferencePending, "reference is not terminal", nil)
	}
	if reference.state != TransactionStateProcessed || reference.details.Kind != TransactionKindBet {
		return newRuleError(FailureReferenceNotEligible, "WIN reference must be a processed BET", nil)
	}

	wd, ref := win.details, reference.details
	if wd.ReferenceExternalTransactionID != ref.ExternalTransactionID {
		return newRuleError(FailureReferenceMismatch, "reference identity differs", nil)
	}
	if wd.ProviderID != ref.ProviderID || wd.PlayerID != ref.PlayerID || wd.RoundID != ref.RoundID {
		return newRuleError(FailureReferenceMismatch, "provider, player, or round differs", nil)
	}
	if wd.WalletID != ref.WalletID {
		return newRuleError(FailureWalletMismatch, "wallet differs", nil)
	}
	if wd.Money.Currency() != ref.Money.Currency() {
		return newRuleError(FailureCurrencyMismatch, "currency differs", ErrCurrencyMismatch)
	}
	return nil
}

// ApplyOperation mutates a wallet only after all operation-level validation.
// Reversal eligibility itself is checked separately by ValidateReversal.
func ApplyOperation(wallet *Wallet, kind WagerTransactionKind, amount Money, referencedKind WagerTransactionKind, at time.Time) (LedgerDirection, bool, error) {
	if wallet == nil {
		return "", false, ErrInvalidWallet
	}
	if err := ValidateOperationAmount(kind, amount, false); err != nil {
		return "", false, err
	}
	if wallet.Currency() != amount.Currency() {
		return "", false, newRuleError(FailureCurrencyMismatch, "operation and wallet currencies differ", ErrCurrencyMismatch)
	}
	direction, changesBalance, err := OperationDirection(kind, referencedKind)
	if err != nil || !changesBalance {
		return direction, changesBalance, err
	}
	if direction == LedgerDirectionCredit {
		if err := wallet.Credit(amount, at); err != nil {
			return "", false, err
		}
		return direction, true, nil
	}
	if err := wallet.Debit(amount, at); err != nil {
		if errors.Is(err, ErrInsufficientFunds) {
			code := FailureInsufficientFunds
			if kind == TransactionKindRollback {
				code = FailureReversalInsufficientFunds
			}
			return "", false, newRuleError(code, "available balance is too low", err)
		}
		return "", false, err
	}
	return direction, true, nil
}
