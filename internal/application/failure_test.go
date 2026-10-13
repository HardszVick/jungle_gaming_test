package application

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

func TestClassifyProcessingError(t *testing.T) {
	ruleError := &domain.RuleError{
		Code:  domain.FailureInvalidAmount,
		Cause: domain.ErrInvalidMoney,
	}
	tests := []struct {
		name string
		err  error
		want ProcessingErrorClass
	}{
		{name: "nil", want: ProcessingErrorNone},
		{name: "domain rule takes precedence over wrapped invariant", err: ruleError, want: ProcessingErrorDomain},
		{name: "wrapped domain rule", err: fmt.Errorf("operation: %w", ruleError), want: ProcessingErrorDomain},
		{name: "canceled context", err: context.Canceled, want: ProcessingErrorTransient},
		{name: "deadline", err: fmt.Errorf("query: %w", context.DeadlineExceeded), want: ProcessingErrorTransient},
		{name: "serialization", err: postgres.ErrSerializationFailure, want: ProcessingErrorTransient},
		{name: "deadlock", err: postgres.ErrDeadlock, want: ProcessingErrorTransient},
		{name: "claim lost", err: postgres.ErrClaimLost, want: ProcessingErrorTransient},
		{name: "money overflow", err: fmt.Errorf("credit: %w", domain.ErrMoneyOverflow), want: ProcessingErrorPermanent},
		{name: "invalid persisted transaction", err: domain.ErrInvalidTransaction, want: ProcessingErrorPermanent},
		{name: "database constraint", err: postgres.ErrConstraintViolation, want: ProcessingErrorPermanent},
		{name: "unknown fallback", err: errors.New("unexpected dependency failure"), want: ProcessingErrorTransient},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyProcessingError(test.err); got != test.want {
				t.Fatalf("ClassifyProcessingError(%v) = %d, want %d", test.err, got, test.want)
			}
		})
	}
}
