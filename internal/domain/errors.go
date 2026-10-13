package domain

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidCurrency    = errors.New("invalid currency")
	ErrInvalidMoney       = errors.New("invalid money")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrMoneyOverflow      = errors.New("money overflow")
	ErrInvalidWallet      = errors.New("invalid wallet")
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrInvalidTransaction = errors.New("invalid wager transaction")
	ErrInvalidTransition  = errors.New("invalid transaction state transition")
	ErrInvalidLedgerEntry = errors.New("invalid wallet ledger entry")
	ErrInvalidEvent       = errors.New("invalid domain event")
)

// RuleError identifies a definitive business-rule rejection suitable for
// persistence and transport without coupling the domain to either layer.
type RuleError struct {
	Code    FailureCode
	Message string
	Cause   error
}

func (e *RuleError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return string(e.Code)
}

func (e *RuleError) Unwrap() error { return e.Cause }

func newRuleError(code FailureCode, message string, cause error) error {
	return &RuleError{Code: code, Message: message, Cause: cause}
}

func FailureCodeOf(err error) (FailureCode, bool) {
	var ruleErr *RuleError
	if !errors.As(err, &ruleErr) {
		return "", false
	}
	return ruleErr.Code, true
}
