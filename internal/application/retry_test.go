package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

func testRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond}
}

func TestRetryTransactionRetriesOnlyDatabaseConcurrencyFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "serialization", err: postgres.ErrSerializationFailure},
		{name: "deadlock", err: postgres.ErrDeadlock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			err := RetryTransaction(context.Background(), testRetryPolicy(), func() error {
				attempts++
				if attempts == 1 {
					return test.err
				}
				return nil
			})
			if err != nil || attempts != 2 {
				t.Fatalf("RetryTransaction() = %v after %d attempts", err, attempts)
			}
		})
	}
}

type retryReasonRecorder struct{ reasons []string }

func (recorder *retryReasonRecorder) RecordTransactionRetry(reason string) {
	recorder.reasons = append(recorder.reasons, reason)
}

func TestRetryTransactionRecordsBoundedConcurrencyReason(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{name: "serialization", err: postgres.ErrSerializationFailure, wantReason: "serialization_failure"},
		{name: "deadlock", err: postgres.ErrDeadlock, wantReason: "deadlock"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &retryReasonRecorder{}
			attempts := 0
			err := retryTransaction(context.Background(), testRetryPolicy(), recorder, func() error {
				attempts++
				if attempts == 1 {
					return test.err
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(recorder.reasons) != 1 || recorder.reasons[0] != test.wantReason {
				t.Fatalf("retry reasons = %v, want [%s]", recorder.reasons, test.wantReason)
			}
		})
	}
}

func TestRetryTransactionDoesNotRetryDomainErrors(t *testing.T) {
	want := &domain.RuleError{Code: domain.FailureInsufficientFunds, Message: "no funds"}
	attempts := 0
	err := RetryTransaction(context.Background(), testRetryPolicy(), func() error {
		attempts++
		return want
	})
	if !errors.Is(err, want) || attempts != 1 {
		t.Fatalf("RetryTransaction() = %v after %d attempts", err, attempts)
	}
}

func TestRetryTransactionRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := RetryTransaction(ctx, testRetryPolicy(), func() error {
		attempts++
		return postgres.ErrDeadlock
	})
	if !errors.Is(err, context.Canceled) || attempts != 0 {
		t.Fatalf("RetryTransaction() = %v after %d attempts", err, attempts)
	}
}
