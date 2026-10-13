package application

import (
	"context"
	"errors"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type ProcessingErrorClass uint8

const (
	ProcessingErrorNone ProcessingErrorClass = iota
	ProcessingErrorDomain
	ProcessingErrorTransient
	ProcessingErrorPermanent
)

// ClassifyProcessingError is deliberately conservative: only deterministic
// invariant and constraint failures are terminal. Unknown errors remain safe
// to retry.
func ClassifyProcessingError(err error) ProcessingErrorClass {
	if err == nil {
		return ProcessingErrorNone
	}
	if _, ok := domain.FailureCodeOf(err); ok {
		return ProcessingErrorDomain
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ProcessingErrorTransient
	}
	for _, permanent := range []error{
		domain.ErrInvalidCurrency,
		domain.ErrInvalidMoney,
		domain.ErrCurrencyMismatch,
		domain.ErrMoneyOverflow,
		domain.ErrInvalidWallet,
		domain.ErrInsufficientFunds,
		domain.ErrInvalidTransaction,
		domain.ErrInvalidTransition,
		domain.ErrInvalidLedgerEntry,
		domain.ErrInvalidEvent,
		postgres.ErrConstraintViolation,
	} {
		if errors.Is(err, permanent) {
			return ProcessingErrorPermanent
		}
	}
	return ProcessingErrorTransient
}
