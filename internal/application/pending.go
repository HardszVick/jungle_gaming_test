package application

import (
	"context"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

func (s *Service) ClaimPendingReferences(ctx context.Context, claimedBy string, limit int, lease time.Duration) ([]postgres.PendingReferenceClaim, error) {
	return s.database.Store().ClaimPendingReferences(ctx, claimedBy, limit, s.reference.MaxAttempts, lease)
}

// ResumePendingReference resolves one previously claimed transaction. The claim
// transaction is deliberately separate, so no database lock is held while a
// worker waits between attempts.
func (s *Service) ResumePendingReference(ctx context.Context, transactionID, claimedBy string) (TransactionResult, error) {
	var result TransactionResult
	err := s.retryTransaction(ctx, func() error {
		result = TransactionResult{}
		return s.database.ReadCommitted(ctx, func(store *postgres.Store) error {
			now := s.clock.Now().UTC()
			transactionTimestamp, err := store.TransactionTimestamp(ctx)
			if err != nil {
				return err
			}
			record, err := store.LockTransaction(ctx, transactionID)
			if err != nil {
				return err
			}
			if record.Transaction.State() != domain.TransactionStatePendingReference {
				result.Transaction = record.Transaction
				result.IdempotentReplay = true
				return nil
			}
			if record.ReferenceClaimedBy != claimedBy || record.ReferenceClaimUntil == nil || !record.ReferenceClaimUntil.After(transactionTimestamp) {
				return postgres.ErrClaimLost
			}
			correlationID := record.Transaction.ID()
			if record.ReferenceExpiresAt == nil || !record.ReferenceExpiresAt.After(transactionTimestamp) {
				reason := &domain.RuleError{Code: domain.FailureReferenceNotFound, Message: "reference lookup expired"}
				if err := s.reject(ctx, store, &record, reason, correlationID, "", now); err != nil {
					return err
				}
				result.Transaction = record.Transaction
				return nil
			}

			wallet, err := store.LockWallet(ctx, record.Transaction.Details().WalletID)
			if err != nil {
				return err
			}
			details := record.Transaction.Details()
			reference, referenceErr := store.GetTransactionByExternalID(ctx, details.ProviderID, details.ReferenceExternalTransactionID)
			if errors.Is(referenceErr, postgres.ErrNotFound) ||
				(referenceErr == nil && (reference.Transaction.State() == domain.TransactionStatePending || reference.Transaction.State() == domain.TransactionStatePendingReference)) {
				if record.ReferenceAttempts >= s.reference.MaxAttempts {
					reason := &domain.RuleError{Code: domain.FailureReferenceNotFound, Message: "reference attempts exhausted"}
					if err := s.reject(ctx, store, &record, reason, correlationID, "", now); err != nil {
						return err
					}
					result.Transaction = record.Transaction
					return nil
				}
				next := transactionTimestamp.Add(s.referenceDelay(record.ReferenceAttempts + 1))
				if next.After(*record.ReferenceExpiresAt) {
					next = *record.ReferenceExpiresAt
				}
				if err := store.ReschedulePendingReference(ctx, transactionID, claimedBy, next); err != nil {
					return err
				}
				result.Transaction = record.Transaction
				return nil
			}
			if referenceErr != nil {
				return referenceErr
			}
			if err := s.processLocked(ctx, store, wallet, &record, correlationID, "", now); err != nil {
				return err
			}
			result.Transaction = record.Transaction
			return nil
		})
	})
	if err == nil || ClassifyProcessingError(err) != ProcessingErrorPermanent {
		return result, err
	}
	return s.terminalizePermanentProcessing(ctx, transactionID, claimedBy)
}

// terminalizePermanentProcessing runs only after the processing transaction
// has rolled back, so committing FAILED cannot retain a partial wallet change.
func (s *Service) terminalizePermanentProcessing(ctx context.Context, transactionID, claimedBy string) (TransactionResult, error) {
	var result TransactionResult
	err := s.retryTransaction(ctx, func() error {
		result = TransactionResult{}
		return s.database.ReadCommitted(ctx, func(store *postgres.Store) error {
			now := s.clock.Now().UTC()
			transactionTimestamp, err := store.TransactionTimestamp(ctx)
			if err != nil {
				return err
			}
			record, err := store.LockTransaction(ctx, transactionID)
			if err != nil {
				return err
			}
			if record.Transaction.State().IsTerminal() {
				result = TransactionResult{Transaction: record.Transaction, IdempotentReplay: true}
				return nil
			}
			if record.Transaction.State() == domain.TransactionStatePendingReference &&
				(record.ReferenceClaimedBy != claimedBy || record.ReferenceClaimUntil == nil || !record.ReferenceClaimUntil.After(transactionTimestamp)) {
				return postgres.ErrClaimLost
			}
			if err := record.Transaction.Fail(now); err != nil {
				return err
			}
			record.ReferenceNextAttemptAt = nil
			record.ReferenceExpiresAt = nil
			record.ReferenceClaimedBy = ""
			record.ReferenceClaimUntil = nil
			if err := store.SaveTransaction(ctx, record); err != nil {
				return err
			}
			result.Transaction = record.Transaction
			return nil
		})
	})
	if err != nil {
		return TransactionResult{}, err
	}
	return result, nil
}
