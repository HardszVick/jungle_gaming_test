package application

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

var (
	ErrInvalidRequest = errors.New("application: invalid request")
	ErrWalletConflict = errors.New("application: wallet already exists")
)

type Clock interface{ Now() time.Time }

type IDGenerator interface{ New() (uuid.UUID, error) }

type ReferencePolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	TTL         time.Duration
}

type ServiceOptions struct {
	Retry              RetryPolicy
	Reference          ReferencePolicy
	Jitter             func(time.Duration) time.Duration
	TransactionRetries TransactionRetryRecorder
}

type Service struct {
	database  *postgres.Database
	clock     Clock
	ids       IDGenerator
	retry     RetryPolicy
	reference ReferencePolicy
	jitter    func(time.Duration) time.Duration
	retries   TransactionRetryRecorder
}

func NewService(database *postgres.Database, clock Clock, ids IDGenerator, options ServiceOptions) (*Service, error) {
	if database == nil || clock == nil || ids == nil {
		return nil, errors.New("application: service dependencies are required")
	}
	if err := options.Retry.validate(); err != nil {
		return nil, err
	}
	if options.Reference.MaxAttempts <= 0 || options.Reference.BaseDelay <= 0 ||
		options.Reference.MaxDelay <= 0 || options.Reference.TTL <= 0 ||
		options.Reference.BaseDelay > options.Reference.MaxDelay || options.Reference.MaxDelay > options.Reference.TTL {
		return nil, errors.New("application: invalid reference policy")
	}
	if options.Jitter == nil {
		options.Jitter = defaultJitter
	}
	return &Service{
		database: database, clock: clock, ids: ids,
		retry: options.Retry, reference: options.Reference, jitter: options.Jitter, retries: options.TransactionRetries,
	}, nil
}

type OpenWalletRequest struct {
	PlayerID       string
	InitialBalance domain.Money
	CorrelationID  string
	CausationID    string
}

func (s *Service) OpenWallet(ctx context.Context, request OpenWalletRequest) (*domain.Wallet, error) {
	if request.PlayerID == "" || strings.TrimSpace(request.PlayerID) != request.PlayerID ||
		!request.InitialBalance.Currency().IsValid() || request.InitialBalance.IsNegative() {
		return nil, ErrInvalidRequest
	}
	now := s.clock.Now().UTC()
	walletID, err := s.newID()
	if err != nil {
		return nil, err
	}
	wallet, err := domain.NewWallet(walletID, request.PlayerID, request.InitialBalance, now)
	if err != nil {
		return nil, err
	}
	correlationID := request.CorrelationID
	if correlationID == "" {
		correlationID = walletID
	}

	err = s.retryTransaction(ctx, func() error {
		return s.database.ReadCommitted(ctx, func(store *postgres.Store) error {
			if err := store.InsertWallet(ctx, wallet); err != nil {
				if errors.Is(err, postgres.ErrConflict) {
					return ErrWalletConflict
				}
				return err
			}
			if request.InitialBalance.IsZero() {
				return nil
			}
			transactionID, err := s.newID()
			if err != nil {
				return err
			}
			tx, err := domain.NewWagerTransaction(transactionID, domain.WagerTransactionDetails{
				PlayerID: request.PlayerID, WalletID: walletID,
				Kind: domain.TransactionKindOpening, Money: request.InitialBalance,
			}, now)
			if err != nil {
				return err
			}
			if err := tx.MarkProcessed(request.InitialBalance, now); err != nil {
				return err
			}
			if err := store.InsertTransaction(ctx, postgres.TransactionInsert{Transaction: tx, CreatedAt: now}); err != nil {
				return err
			}
			zero, _ := domain.ZeroMoney(request.InitialBalance.Currency())
			entry, err := s.newLedgerEntry(walletID, transactionID, domain.LedgerDirectionCredit, request.InitialBalance, zero, request.InitialBalance, now)
			if err != nil {
				return err
			}
			if err := store.InsertLedgerEntry(ctx, entry); err != nil {
				return err
			}
			return s.insertProcessedEvents(ctx, store, tx, &entry, wallet.Version(), correlationID, request.CausationID, now)
		})
	})
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

type InboxMessage struct {
	ConsumerName string
	MessageID    string
	PayloadHash  [32]byte
}

type ProcessTransactionRequest struct {
	Command        TransactionCommand
	IdempotencyKey string
	CorrelationID  string
	CausationID    string
	Inbox          *InboxMessage
}

type TransactionResult struct {
	Transaction      *domain.WagerTransaction
	IdempotentReplay bool
}

func (s *Service) ProcessTransaction(ctx context.Context, request ProcessTransactionRequest) (TransactionResult, error) {
	if request.IdempotencyKey == "" || strings.TrimSpace(request.IdempotencyKey) != request.IdempotencyKey {
		return TransactionResult{}, ErrInvalidRequest
	}
	hash, err := request.Command.SHA256()
	if err != nil {
		return TransactionResult{}, err
	}
	transactionID, err := s.newID()
	if err != nil {
		return TransactionResult{}, err
	}
	var result TransactionResult
	err = s.retryTransaction(ctx, func() error {
		result = TransactionResult{}
		return s.database.ReadCommitted(ctx, func(store *postgres.Store) error {
			now := s.clock.Now().UTC()
			if request.Inbox != nil {
				inbox, inboxErr := store.BeginInbox(ctx, request.Inbox.ConsumerName, request.Inbox.MessageID, request.Inbox.PayloadHash, now)
				if inboxErr != nil {
					return inboxErr
				}
				if inbox.Status == postgres.InboxCompleted {
					record, loadErr := store.GetTransaction(ctx, inbox.TransactionID)
					if loadErr != nil {
						return loadErr
					}
					result = TransactionResult{Transaction: record.Transaction, IdempotentReplay: true}
					return nil
				}
			}
			existing, lookupErr := store.GetTransactionByProviderKey(ctx, request.Command.ProviderID(), request.IdempotencyKey)
			if lookupErr == nil {
				if !existing.HasRequestHash || existing.RequestHash != hash {
					return postgres.ErrIdempotencyConflict
				}
				if existing.Transaction.State() != domain.TransactionStatePending {
					result = TransactionResult{Transaction: existing.Transaction, IdempotentReplay: true}
					if request.Inbox != nil {
						return store.CompleteInbox(ctx, request.Inbox.ConsumerName, request.Inbox.MessageID, existing.Transaction.ID(), now)
					}
					return nil
				}
				transactionID = existing.Transaction.ID()
			} else if !errors.Is(lookupErr, postgres.ErrNotFound) {
				return lookupErr
			} else {
				external, externalErr := store.GetTransactionByExternalID(ctx, request.Command.ProviderID(), request.Command.ExternalTransactionID())
				if externalErr == nil {
					if external.IdempotencyKey != request.IdempotencyKey {
						return postgres.ErrExternalIDConflict
					}
					if !external.HasRequestHash || external.RequestHash != hash {
						return postgres.ErrIdempotencyConflict
					}
					if external.Transaction.State() != domain.TransactionStatePending {
						result = TransactionResult{Transaction: external.Transaction, IdempotentReplay: true}
						if request.Inbox != nil {
							return store.CompleteInbox(ctx, request.Inbox.ConsumerName, request.Inbox.MessageID, external.Transaction.ID(), now)
						}
						return nil
					}
					transactionID = external.Transaction.ID()
				}
				if !errors.Is(externalErr, postgres.ErrNotFound) {
					if externalErr != nil {
						return externalErr
					}
				}
			}

			tx, createErr := domain.NewWagerTransaction(transactionID, request.Command.DomainDetails(), now)
			if createErr != nil {
				return createErr
			}
			record, inserted, insertErr := store.InsertExternalTransaction(ctx, postgres.TransactionInsert{
				Transaction: tx, IdempotencyKey: request.IdempotencyKey, RequestHash: hash[:], CreatedAt: now,
			})
			if insertErr != nil {
				return insertErr
			}
			if !inserted && record.Transaction.State() != domain.TransactionStatePending {
				result = TransactionResult{Transaction: record.Transaction, IdempotentReplay: true}
				if request.Inbox != nil {
					return store.CompleteInbox(ctx, request.Inbox.ConsumerName, request.Inbox.MessageID, record.Transaction.ID(), now)
				}
				return nil
			}
			if !inserted {
				tx = record.Transaction
			}
			correlationID := request.CorrelationID
			if correlationID == "" {
				correlationID = tx.ID()
			}
			record.Transaction = tx
			wallet, lockErr := store.LockWallet(ctx, request.Command.WalletID())
			if lockErr != nil {
				return lockErr
			}
			var identityErr error
			if wallet.PlayerID() != request.Command.PlayerID() {
				identityErr = &domain.RuleError{Code: domain.FailureWalletMismatch, Message: "wallet player differs"}
			} else if wallet.Currency() != request.Command.Money().Currency() {
				identityErr = &domain.RuleError{Code: domain.FailureCurrencyMismatch, Message: "wallet currency differs", Cause: domain.ErrCurrencyMismatch}
			}
			if identityErr != nil {
				if err := s.reject(ctx, store, &record, identityErr, correlationID, request.CausationID, now); err != nil {
					return err
				}
				result = TransactionResult{Transaction: record.Transaction}
				if request.Inbox != nil {
					return store.CompleteInbox(ctx, request.Inbox.ConsumerName, request.Inbox.MessageID, tx.ID(), now)
				}
				return nil
			}
			if err := s.processLocked(ctx, store, wallet, &record, correlationID, request.CausationID, now); err != nil {
				return err
			}
			result = TransactionResult{Transaction: record.Transaction}
			if request.Inbox != nil {
				return store.CompleteInbox(ctx, request.Inbox.ConsumerName, request.Inbox.MessageID, tx.ID(), now)
			}
			return nil
		})
	})
	return result, err
}

func (s *Service) processLocked(ctx context.Context, store *postgres.Store, wallet *domain.Wallet, record *postgres.TransactionRecord, correlationID, causationID string, now time.Time) error {
	tx := record.Transaction
	details := tx.Details()
	var referencedKind domain.WagerTransactionKind
	if details.ReferenceExternalTransactionID != "" {
		reference, err := store.GetTransactionByExternalID(ctx, details.ProviderID, details.ReferenceExternalTransactionID)
		if errors.Is(err, postgres.ErrNotFound) {
			return s.markPendingReference(ctx, store, record, correlationID, causationID, now)
		}
		if err != nil {
			return err
		}
		if reference.Transaction.State() == domain.TransactionStatePending || reference.Transaction.State() == domain.TransactionStatePendingReference {
			return s.markPendingReference(ctx, store, record, correlationID, causationID, now)
		}
		var validationErr error
		if details.Kind.IsReversal() {
			alreadyReversed, err := store.HasProcessedDirectReversal(ctx, reference.Transaction.ID(), tx.ID())
			if err != nil {
				return err
			}
			_, validationErr = domain.ValidateReversal(tx, reference.Transaction, alreadyReversed)
		} else {
			validationErr = domain.ValidateWinReference(tx, reference.Transaction)
		}
		if validationErr != nil {
			return s.reject(ctx, store, record, validationErr, correlationID, causationID, now)
		}
		record.ReferenceTransactionID = reference.Transaction.ID()
		referencedKind = reference.Transaction.Details().Kind
	}

	before := wallet.Balance()
	direction, changed, err := domain.ApplyOperation(wallet, details.Kind, details.Money, referencedKind, now)
	if err != nil {
		if _, classified := domain.FailureCodeOf(err); classified {
			return s.reject(ctx, store, record, err, correlationID, causationID, now)
		}
		return err
	}
	if err := tx.MarkProcessed(wallet.Balance(), now); err != nil {
		return err
	}
	record.ReferenceNextAttemptAt = nil
	record.ReferenceExpiresAt = nil
	record.ReferenceClaimedBy = ""
	record.ReferenceClaimUntil = nil
	if changed {
		if err := store.UpdateWallet(ctx, wallet, wallet.Version()-1); err != nil {
			return err
		}
		entry, err := s.newLedgerEntry(wallet.ID(), tx.ID(), direction, details.Money, before, wallet.Balance(), now)
		if err != nil {
			return err
		}
		if err := store.SaveTransaction(ctx, *record); err != nil {
			return err
		}
		if err := store.InsertLedgerEntry(ctx, entry); err != nil {
			return err
		}
		return s.insertProcessedEvents(ctx, store, tx, &entry, wallet.Version(), correlationID, causationID, now)
	}
	if err := store.SaveTransaction(ctx, *record); err != nil {
		return err
	}
	return s.insertProcessedEvents(ctx, store, tx, nil, wallet.Version(), correlationID, causationID, now)
}

func (s *Service) markPendingReference(ctx context.Context, store *postgres.Store, record *postgres.TransactionRecord, correlationID, causationID string, now time.Time) error {
	if record.Transaction.State() == domain.TransactionStatePending {
		if err := record.Transaction.MarkPendingReference(now); err != nil {
			return err
		}
	}
	transactionTimestamp, err := store.TransactionTimestamp(ctx)
	if err != nil {
		return err
	}
	next := transactionTimestamp.Add(s.referenceDelay(max(1, record.ReferenceAttempts+1)))
	expires := transactionTimestamp.Add(s.reference.TTL)
	if next.After(expires) {
		next = expires
	}
	record.ReferenceNextAttemptAt = &next
	record.ReferenceExpiresAt = &expires
	record.ReferenceClaimedBy = ""
	record.ReferenceClaimUntil = nil
	if err := store.SaveTransaction(ctx, *record); err != nil {
		return err
	}
	data, err := domain.NewWagerTransactionPendingReferenceData(record.Transaction)
	if err != nil {
		return err
	}
	eventID, err := s.newID()
	if err != nil {
		return err
	}
	event, err := domain.NewWagerTransactionPendingReferenceEvent(eventID, correlationID, causationID, now, data)
	if err != nil {
		return err
	}
	return store.InsertOutboxEvent(ctx, event)
}

func (s *Service) reject(ctx context.Context, store *postgres.Store, record *postgres.TransactionRecord, cause error, correlationID, causationID string, now time.Time) error {
	code, ok := domain.FailureCodeOf(cause)
	if !ok {
		return cause
	}
	if err := record.Transaction.Reject(code, now); err != nil {
		return err
	}
	record.ReferenceNextAttemptAt = nil
	record.ReferenceExpiresAt = nil
	record.ReferenceClaimedBy = ""
	record.ReferenceClaimUntil = nil
	if err := store.SaveTransaction(ctx, *record); err != nil {
		return err
	}
	data, err := domain.NewWagerTransactionRejectedData(record.Transaction)
	if err != nil {
		return err
	}
	eventID, err := s.newID()
	if err != nil {
		return err
	}
	event, err := domain.NewWagerTransactionRejectedEvent(eventID, correlationID, causationID, now, data)
	if err != nil {
		return err
	}
	return store.InsertOutboxEvent(ctx, event)
}

func (s *Service) insertProcessedEvents(ctx context.Context, store *postgres.Store, tx *domain.WagerTransaction, entry *domain.WalletLedgerEntry, walletVersion int64, correlationID, causationID string, now time.Time) error {
	balance, _ := tx.ResultBalance()
	data, err := domain.NewWagerTransactionProcessedData(tx, balance)
	if err != nil {
		return err
	}
	eventID, err := s.newID()
	if err != nil {
		return err
	}
	event, err := domain.NewWagerTransactionProcessedEvent(eventID, correlationID, causationID, now, data)
	if err != nil {
		return err
	}
	if err := store.InsertOutboxEvent(ctx, event); err != nil {
		return err
	}
	if entry == nil {
		return nil
	}
	balanceData, err := domain.NewWalletBalanceChangedData(*entry, walletVersion)
	if err != nil {
		return err
	}
	balanceEventID, err := s.newID()
	if err != nil {
		return err
	}
	balanceEvent, err := domain.NewWalletBalanceChangedEvent(balanceEventID, correlationID, causationID, now, balanceData)
	if err != nil {
		return err
	}
	return store.InsertOutboxEvent(ctx, balanceEvent)
}

func (s *Service) newLedgerEntry(walletID, transactionID string, direction domain.LedgerDirection, amount, before, after domain.Money, now time.Time) (domain.WalletLedgerEntry, error) {
	id, err := s.newID()
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	return domain.NewWalletLedgerEntry(id, walletID, transactionID, direction, amount, before, after, now)
}

func (s *Service) newID() (string, error) {
	id, err := s.ids.New()
	if err != nil {
		return "", err
	}
	if id == uuid.Nil {
		return "", errors.New("application: generated nil UUID")
	}
	return id.String(), nil
}

func (s *Service) retryTransaction(ctx context.Context, operation func() error) error {
	return retryTransaction(ctx, s.retry, s.retries, operation)
}

func (s *Service) referenceDelay(attempt int) time.Duration {
	delay := s.reference.BaseDelay
	for current := 1; current < attempt && delay < s.reference.MaxDelay; current++ {
		delay *= 2
		if delay > s.reference.MaxDelay {
			delay = s.reference.MaxDelay
		}
	}
	return s.jitter(delay)
}

func defaultJitter(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	half := delay / 2
	value, err := rand.Int(rand.Reader, big.NewInt(int64(delay-half)+1))
	if err != nil {
		return delay
	}
	return half + time.Duration(value.Int64())
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Service) GetWallet(ctx context.Context, walletID string) (*domain.Wallet, error) {
	return s.database.Store().GetWallet(ctx, walletID)
}

func (s *Service) GetTransaction(ctx context.Context, transactionID string) (postgres.TransactionRecord, error) {
	return s.database.Store().GetTransaction(ctx, transactionID)
}

func (s *Service) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (postgres.TransactionRecord, error) {
	return s.database.Store().GetTransactionByExternalID(ctx, providerID, externalID)
}

func (s *Service) ListLedger(ctx context.Context, walletID string, cursor *postgres.LedgerCursor, limit int) (postgres.LedgerPage, error) {
	return s.database.Store().ListLedger(ctx, walletID, cursor, limit)
}

func (s *Service) Reconcile(ctx context.Context, walletID string) (result postgres.Reconciliation, err error) {
	err = s.database.RepeatableRead(ctx, func(store *postgres.Store) error {
		var reconcileErr error
		result, reconcileErr = store.ReconcileWallet(ctx, walletID)
		return reconcileErr
	})
	return result, err
}
