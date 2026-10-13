package domain

import (
	"fmt"
	"strings"
	"time"
)

type WagerTransactionKind string

const (
	TransactionKindOpening  WagerTransactionKind = "OPENING"
	TransactionKindBet      WagerTransactionKind = "BET"
	TransactionKindWin      WagerTransactionKind = "WIN"
	TransactionKindLoss     WagerTransactionKind = "LOSS"
	TransactionKindRefund   WagerTransactionKind = "REFUND"
	TransactionKindRollback WagerTransactionKind = "ROLLBACK"
)

func ParseWagerTransactionKind(value string) (WagerTransactionKind, error) {
	kind := WagerTransactionKind(strings.ToUpper(value))
	if !kind.IsValid() {
		return "", fmt.Errorf("%w: unknown kind %q", ErrInvalidTransaction, value)
	}
	return kind, nil
}

func ParseExternalWagerTransactionKind(value string) (WagerTransactionKind, error) {
	kind, err := ParseWagerTransactionKind(value)
	if err != nil {
		return "", err
	}
	if kind == TransactionKindOpening {
		return "", fmt.Errorf("%w: OPENING is internal only", ErrInvalidTransaction)
	}
	return kind, nil
}

func (k WagerTransactionKind) IsValid() bool {
	switch k {
	case TransactionKindOpening, TransactionKindBet, TransactionKindWin,
		TransactionKindLoss, TransactionKindRefund, TransactionKindRollback:
		return true
	default:
		return false
	}
}

func (k WagerTransactionKind) IsReversal() bool {
	return k == TransactionKindRefund || k == TransactionKindRollback
}

type WagerTransactionState string

const (
	TransactionStatePending          WagerTransactionState = "PENDING"
	TransactionStatePendingReference WagerTransactionState = "PENDING_REFERENCE"
	TransactionStateProcessed        WagerTransactionState = "PROCESSED"
	TransactionStateRejected         WagerTransactionState = "REJECTED"
	TransactionStateFailed           WagerTransactionState = "FAILED"
)

func (s WagerTransactionState) IsValid() bool {
	switch s {
	case TransactionStatePending, TransactionStatePendingReference,
		TransactionStateProcessed, TransactionStateRejected, TransactionStateFailed:
		return true
	default:
		return false
	}
}

func (s WagerTransactionState) IsTerminal() bool {
	return s == TransactionStateProcessed || s == TransactionStateRejected || s == TransactionStateFailed
}

type FailureCode string

const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	FailureWalletMismatch            FailureCode = "WALLET_MISMATCH"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferencePending          FailureCode = "REFERENCE_PENDING"
	FailureReferenceNotEligible      FailureCode = "REFERENCE_NOT_ELIGIBLE"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	FailureInvalidAmount             FailureCode = "INVALID_AMOUNT"
	FailurePermanentProcessing       FailureCode = "PERMANENT_PROCESSING_FAILURE"
)

func (c FailureCode) IsValid() bool {
	switch c {
	case FailureInsufficientFunds, FailureReversalInsufficientFunds,
		FailureCurrencyMismatch, FailureWalletMismatch, FailureReferenceNotFound,
		FailureReferencePending, FailureReferenceNotEligible, FailureReferenceMismatch,
		FailureAlreadyReversed, FailureInvalidAmount, FailurePermanentProcessing:
		return true
	default:
		return false
	}
}

type WagerTransactionDetails struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           WagerTransactionKind
	Money                          Money
	ReferenceExternalTransactionID string
}

type WagerTransactionSnapshot struct {
	ID            string
	Details       WagerTransactionDetails
	State         WagerTransactionState
	FailureCode   FailureCode
	ResultBalance *Money
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type WagerTransaction struct {
	id            string
	details       WagerTransactionDetails
	state         WagerTransactionState
	failureCode   FailureCode
	resultBalance *Money
	createdAt     time.Time
	updatedAt     time.Time
}

func NewWagerTransaction(id string, details WagerTransactionDetails, now time.Time) (*WagerTransaction, error) {
	return RehydrateWagerTransaction(WagerTransactionSnapshot{
		ID: id, Details: details, State: TransactionStatePending,
		CreatedAt: now, UpdatedAt: now,
	})
}

func RehydrateWagerTransaction(snapshot WagerTransactionSnapshot) (*WagerTransaction, error) {
	if snapshot.ID == "" || snapshot.Details.PlayerID == "" || snapshot.Details.WalletID == "" {
		return nil, fmt.Errorf("%w: required identity is missing", ErrInvalidTransaction)
	}
	if !snapshot.Details.Kind.IsValid() || !snapshot.Details.Money.Currency().IsValid() {
		return nil, fmt.Errorf("%w: invalid kind or money", ErrInvalidTransaction)
	}
	if err := ValidateOperationAmount(snapshot.Details.Kind, snapshot.Details.Money, false); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	if snapshot.Details.Kind != TransactionKindOpening &&
		(snapshot.Details.ProviderID == "" || snapshot.Details.ExternalTransactionID == "" ||
			snapshot.Details.RoundID == "" || snapshot.Details.GameID == "") {
		return nil, fmt.Errorf("%w: external transaction metadata is required", ErrInvalidTransaction)
	}
	if snapshot.Details.Kind == TransactionKindOpening &&
		(snapshot.Details.ProviderID != "" || snapshot.Details.ExternalTransactionID != "" ||
			snapshot.Details.RoundID != "" || snapshot.Details.GameID != "" ||
			snapshot.Details.ReferenceExternalTransactionID != "") {
		return nil, fmt.Errorf("%w: OPENING cannot contain external metadata", ErrInvalidTransaction)
	}
	hasReference := snapshot.Details.ReferenceExternalTransactionID != ""
	if (snapshot.Details.Kind.IsReversal() && !hasReference) ||
		(!snapshot.Details.Kind.IsReversal() && snapshot.Details.Kind != TransactionKindWin && hasReference) {
		return nil, fmt.Errorf("%w: reference presence does not match transaction kind", ErrInvalidTransaction)
	}
	if !snapshot.State.IsValid() || snapshot.CreatedAt.IsZero() || snapshot.UpdatedAt.IsZero() ||
		snapshot.UpdatedAt.Before(snapshot.CreatedAt) {
		return nil, fmt.Errorf("%w: invalid state or timestamps", ErrInvalidTransaction)
	}
	if snapshot.State == TransactionStatePendingReference &&
		(!hasReference || (!snapshot.Details.Kind.IsReversal() && snapshot.Details.Kind != TransactionKindWin)) {
		return nil, fmt.Errorf("%w: transaction cannot await a reference", ErrInvalidTransaction)
	}
	if snapshot.State == TransactionStateRejected || snapshot.State == TransactionStateFailed {
		if !snapshot.FailureCode.IsValid() {
			return nil, fmt.Errorf("%w: terminal failure requires a failure code", ErrInvalidTransaction)
		}
		if snapshot.State == TransactionStateFailed && snapshot.FailureCode != FailurePermanentProcessing {
			return nil, fmt.Errorf("%w: FAILED is reserved for permanent processing failures", ErrInvalidTransaction)
		}
		if snapshot.State == TransactionStateRejected && snapshot.FailureCode == FailurePermanentProcessing {
			return nil, fmt.Errorf("%w: permanent processing failure cannot be a rejection", ErrInvalidTransaction)
		}
	} else if snapshot.FailureCode != "" {
		return nil, fmt.Errorf("%w: failure code on non-failed state", ErrInvalidTransaction)
	}
	if snapshot.State == TransactionStateProcessed && snapshot.ResultBalance == nil {
		return nil, fmt.Errorf("%w: processed transaction requires result balance", ErrInvalidTransaction)
	}
	if snapshot.State != TransactionStateProcessed && snapshot.ResultBalance != nil {
		return nil, fmt.Errorf("%w: result balance on unprocessed transaction", ErrInvalidTransaction)
	}
	if snapshot.ResultBalance != nil {
		if snapshot.ResultBalance.IsNegative() {
			return nil, fmt.Errorf("%w: result balance cannot be negative", ErrInvalidTransaction)
		}
		if _, err := snapshot.Details.Money.Compare(*snapshot.ResultBalance); err != nil {
			return nil, err
		}
	}

	tx := &WagerTransaction{
		id: snapshot.ID, details: snapshot.Details, state: snapshot.State,
		failureCode: snapshot.FailureCode, createdAt: snapshot.CreatedAt.UTC(), updatedAt: snapshot.UpdatedAt.UTC(),
	}
	if snapshot.ResultBalance != nil {
		balance := *snapshot.ResultBalance
		tx.resultBalance = &balance
	}
	return tx, nil
}

func (t *WagerTransaction) ID() string                       { return t.id }
func (t *WagerTransaction) Details() WagerTransactionDetails { return t.details }
func (t *WagerTransaction) State() WagerTransactionState     { return t.state }
func (t *WagerTransaction) FailureCode() FailureCode         { return t.failureCode }
func (t *WagerTransaction) CreatedAt() time.Time             { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time             { return t.updatedAt }

func (t *WagerTransaction) ResultBalance() (Money, bool) {
	if t.resultBalance == nil {
		return Money{}, false
	}
	return *t.resultBalance, true
}

func (t *WagerTransaction) MarkPendingReference(at time.Time) error {
	return t.transition(TransactionStatePendingReference, "", nil, at)
}

func (t *WagerTransaction) MarkProcessed(balance Money, at time.Time) error {
	return t.transition(TransactionStateProcessed, "", &balance, at)
}

func (t *WagerTransaction) Reject(code FailureCode, at time.Time) error {
	if !code.IsValid() || code == FailurePermanentProcessing {
		return fmt.Errorf("%w: invalid rejection code", ErrInvalidTransition)
	}
	return t.transition(TransactionStateRejected, code, nil, at)
}

func (t *WagerTransaction) Fail(at time.Time) error {
	return t.transition(TransactionStateFailed, FailurePermanentProcessing, nil, at)
}

func (t *WagerTransaction) transition(to WagerTransactionState, code FailureCode, balance *Money, at time.Time) error {
	if t == nil || at.IsZero() {
		return fmt.Errorf("%w: invalid timestamp", ErrInvalidTransition)
	}
	allowed := t.state == TransactionStatePending &&
		(to == TransactionStatePendingReference || to.IsTerminal())
	allowed = allowed || t.state == TransactionStatePendingReference && to.IsTerminal()
	if !allowed {
		return fmt.Errorf("%w: %s to %s", ErrInvalidTransition, t.state, to)
	}
	if to == TransactionStatePendingReference &&
		(t.details.ReferenceExternalTransactionID == "" || (!t.details.Kind.IsReversal() && t.details.Kind != TransactionKindWin)) {
		return fmt.Errorf("%w: transaction cannot await a reference", ErrInvalidTransition)
	}
	if balance != nil {
		if balance.IsNegative() {
			return fmt.Errorf("%w: negative result balance", ErrInvalidTransition)
		}
		if _, err := t.details.Money.Compare(*balance); err != nil {
			return err
		}
	}
	if at.Before(t.updatedAt) {
		at = t.updatedAt
	}
	t.state = to
	t.failureCode = code
	t.resultBalance = balance
	t.updatedAt = at.UTC()
	return nil
}
