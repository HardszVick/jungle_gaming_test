package application

import (
	"context"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// TransactionRetryRecorder observes the bounded reasons for SQL transaction
// retries without coupling application logic to a metrics implementation.
type TransactionRetryRecorder interface {
	RecordTransactionRetry(reason string)
}

func (p RetryPolicy) validate() error {
	if p.MaxAttempts <= 0 || p.BaseDelay <= 0 || p.MaxDelay <= 0 || p.BaseDelay > p.MaxDelay {
		return errors.New("application: invalid retry policy")
	}
	return nil
}

// RetryTransaction retries only PostgreSQL serialization failures and
// deadlocks. Domain and validation errors are returned immediately.
func RetryTransaction(ctx context.Context, policy RetryPolicy, operation func() error) error {
	return retryTransaction(ctx, policy, nil, operation)
}

func retryTransaction(ctx context.Context, policy RetryPolicy, recorder TransactionRetryRecorder, operation func() error) error {
	if err := policy.validate(); err != nil {
		return err
	}
	if operation == nil {
		return errors.New("application: retry operation is nil")
	}
	delay := policy.BaseDelay
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil || !postgres.IsRetryable(err) || attempt >= policy.MaxAttempts {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if recorder != nil {
			recorder.RecordTransactionRetry(transactionRetryReason(err))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		if delay < policy.MaxDelay {
			delay *= 2
			if delay > policy.MaxDelay {
				delay = policy.MaxDelay
			}
		}
	}
}

func transactionRetryReason(err error) string {
	if errors.Is(err, postgres.ErrDeadlock) {
		return "deadlock"
	}
	return "serialization_failure"
}
