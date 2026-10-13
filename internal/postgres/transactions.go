package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

const transactionColumns = `
	id::text, wallet_id::text, player_id, provider_id, external_transaction_id,
	idempotency_key, request_hash, round_id, game_id, kind, state,
	amount_minor, amount_currency::text, reference_external_transaction_id,
	reference_transaction_id::text, reference_attempts, reference_next_attempt_at,
	reference_expires_at, reference_claimed_by, reference_claim_until,
	result_balance_minor, result_balance_currency::text, failure_code,
	completed_at, created_at, updated_at`

const claimedTransactionColumns = `
	transactions.id::text, transactions.wallet_id::text, transactions.player_id,
	transactions.provider_id, transactions.external_transaction_id,
	transactions.idempotency_key, transactions.request_hash, transactions.round_id,
	transactions.game_id, transactions.kind, transactions.state,
	transactions.amount_minor, transactions.amount_currency::text,
	transactions.reference_external_transaction_id,
	transactions.reference_transaction_id::text, transactions.reference_attempts,
	transactions.reference_next_attempt_at, transactions.reference_expires_at,
	transactions.reference_claimed_by, transactions.reference_claim_until,
	transactions.result_balance_minor, transactions.result_balance_currency::text,
	transactions.failure_code, transactions.completed_at, transactions.created_at,
	transactions.updated_at`

func (s *Store) InsertTransaction(ctx context.Context, input TransactionInsert) error {
	if input.Transaction == nil {
		return errors.New("postgres: transaction is nil")
	}
	tx := input.Transaction
	details := tx.Details()
	var providerID, externalID, idempotencyKey, requestHash, roundID, gameID, referenceExternal any
	if details.Kind != domain.TransactionKindOpening {
		providerID, externalID = details.ProviderID, details.ExternalTransactionID
		idempotencyKey, requestHash = input.IdempotencyKey, input.RequestHash
		roundID, gameID = details.RoundID, details.GameID
		if details.ReferenceExternalTransactionID != "" {
			referenceExternal = details.ReferenceExternalTransactionID
		}
	}
	var resultMinor, resultCurrency, failureCode, completedAt any
	if balance, ok := tx.ResultBalance(); ok {
		resultMinor, resultCurrency = balance.MinorUnits(), balance.Currency().String()
	}
	if tx.FailureCode() != "" {
		failureCode = string(tx.FailureCode())
	}
	if tx.State().IsTerminal() {
		completedAt = tx.UpdatedAt()
	}
	createdAt := input.CreatedAt
	if createdAt.IsZero() {
		createdAt = tx.CreatedAt()
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, wallet_id, player_id, provider_id, external_transaction_id,
			idempotency_key, request_hash, round_id, game_id, kind, state,
			amount_minor, amount_currency, reference_external_transaction_id,
			result_balance_minor, result_balance_currency, failure_code,
			completed_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20
		)`,
		tx.ID(), details.WalletID, details.PlayerID, providerID, externalID,
		idempotencyKey, requestHash, roundID, gameID, string(details.Kind), string(tx.State()),
		details.Money.MinorUnits(), details.Money.Currency().String(), referenceExternal,
		resultMinor, resultCurrency, failureCode, completedAt, createdAt, tx.UpdatedAt())
	return TranslateError(err)
}

// InsertExternalTransaction inserts PENDING or returns the persisted identity
// result after a uniqueness race.
func (s *Store) InsertExternalTransaction(ctx context.Context, input TransactionInsert) (record TransactionRecord, inserted bool, err error) {
	if input.Transaction == nil {
		return TransactionRecord{}, false, errors.New("postgres: transaction is nil")
	}
	details := input.Transaction.Details()
	var insertedID string
	err = s.db.QueryRow(ctx, `
		INSERT INTO wager_transactions (
			id, wallet_id, player_id, provider_id, external_transaction_id,
			idempotency_key, request_hash, round_id, game_id, kind, state,
			amount_minor, amount_currency, reference_external_transaction_id,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'PENDING',
			$11, $12, $13, $14, $14
		)
		ON CONFLICT DO NOTHING
		RETURNING id::text`,
		input.Transaction.ID(), details.WalletID, details.PlayerID, details.ProviderID,
		details.ExternalTransactionID, input.IdempotencyKey, input.RequestHash,
		details.RoundID, details.GameID, string(details.Kind), details.Money.MinorUnits(),
		details.Money.Currency().String(), nullString(details.ReferenceExternalTransactionID), input.CreatedAt,
	).Scan(&insertedID)
	if err == nil {
		record, err = s.GetTransaction(ctx, input.Transaction.ID())
		return record, true, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return TransactionRecord{}, false, TranslateError(err)
	}

	existing, lookupErr := s.GetTransactionByProviderKey(ctx, details.ProviderID, input.IdempotencyKey)
	if lookupErr == nil {
		if !existing.HasRequestHash || !bytes.Equal(existing.RequestHash[:], input.RequestHash) {
			return TransactionRecord{}, false, ErrIdempotencyConflict
		}
		return existing, false, nil
	}
	if !errors.Is(lookupErr, ErrNotFound) {
		return TransactionRecord{}, false, lookupErr
	}
	if existing, externalErr := s.GetTransactionByExternalID(ctx, details.ProviderID, details.ExternalTransactionID); externalErr == nil {
		if existing.IdempotencyKey != input.IdempotencyKey {
			return TransactionRecord{}, false, ErrExternalIDConflict
		}
		if !existing.HasRequestHash || !bytes.Equal(existing.RequestHash[:], input.RequestHash) {
			return TransactionRecord{}, false, ErrIdempotencyConflict
		}
		return existing, false, nil
	} else {
		lookupErr = externalErr
	}
	if !errors.Is(lookupErr, ErrNotFound) {
		return TransactionRecord{}, false, lookupErr
	}
	return TransactionRecord{}, false, ErrConflict
}

func (s *Store) GetTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	return scanTransaction(s.db.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (s *Store) LockTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	return scanTransaction(s.db.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1 FOR UPDATE`, id))
}

func (s *Store) GetTransactionByProviderKey(ctx context.Context, providerID, key string) (TransactionRecord, error) {
	return scanTransaction(s.db.QueryRow(ctx, `SELECT `+transactionColumns+`
		FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (s *Store) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (TransactionRecord, error) {
	return scanTransaction(s.db.QueryRow(ctx, `SELECT `+transactionColumns+`
		FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (s *Store) HasProcessedDirectReversal(ctx context.Context, referenceTransactionID, excludingID string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE reference_transaction_id = $1
			  AND state = 'PROCESSED'
			  AND kind IN ('REFUND', 'ROLLBACK')
			  AND id <> $2
		)`, referenceTransactionID, excludingID).Scan(&exists)
	return exists, TranslateError(err)
}

func (s *Store) SaveTransaction(ctx context.Context, record TransactionRecord) error {
	if record.Transaction == nil {
		return errors.New("postgres: transaction is nil")
	}
	tx := record.Transaction
	var resultMinor, resultCurrency, failureCode, completedAt, referenceID any
	balance, hasResultBalance := tx.ResultBalance()
	if tx.State() == domain.TransactionStateProcessed && !hasResultBalance {
		return errors.New("postgres: processed transaction result balance is missing")
	}
	if tx.State() != domain.TransactionStateProcessed && hasResultBalance {
		return errors.New("postgres: result balance is only valid for a processed transaction")
	}
	if hasResultBalance {
		resultMinor, resultCurrency = balance.MinorUnits(), balance.Currency().String()
	}
	if tx.FailureCode() != "" {
		failureCode = string(tx.FailureCode())
	}
	if tx.State().IsTerminal() {
		completedAt = tx.UpdatedAt()
	}
	if record.ReferenceTransactionID != "" {
		referenceID = record.ReferenceTransactionID
	}
	tag, err := s.db.Exec(ctx, `
		UPDATE wager_transactions SET
			state = $1,
			reference_transaction_id = $2,
			reference_attempts = $3,
			reference_next_attempt_at = $4,
			reference_expires_at = $5,
			reference_claimed_by = $6,
			reference_claim_until = $7,
			result_balance_minor = $8,
			result_balance_currency = $9,
			failure_code = $10,
			completed_at = $11,
			updated_at = $12
		WHERE id = $13 AND state IN ('PENDING', 'PENDING_REFERENCE')`,
		string(tx.State()), referenceID, record.ReferenceAttempts,
		record.ReferenceNextAttemptAt, record.ReferenceExpiresAt,
		nullString(record.ReferenceClaimedBy), record.ReferenceClaimUntil,
		resultMinor, resultCurrency, failureCode, completedAt, tx.UpdatedAt(), tx.ID())
	if err != nil {
		return TranslateError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConcurrentUpdate
	}
	return nil
}

func scanTransaction(row pgx.Row) (TransactionRecord, error) {
	var id, walletID, playerID, kindValue, stateValue, currencyCode string
	var providerID, externalID, idempotencyKey, roundID, gameID, referenceExternal *string
	var referenceID, referenceClaimedBy, resultCurrency, failureCode *string
	var requestHash []byte
	var amountMinor int64
	var referenceAttempts int
	var nextAttemptAt, expiresAt, claimUntil, completedAt *time.Time
	var resultMinor *int64
	var createdAt, updatedAt time.Time
	err := row.Scan(
		&id, &walletID, &playerID, &providerID, &externalID,
		&idempotencyKey, &requestHash, &roundID, &gameID, &kindValue, &stateValue,
		&amountMinor, &currencyCode, &referenceExternal, &referenceID, &referenceAttempts,
		&nextAttemptAt, &expiresAt, &referenceClaimedBy, &claimUntil,
		&resultMinor, &resultCurrency, &failureCode, &completedAt, &createdAt, &updatedAt,
	)
	if err != nil {
		return TransactionRecord{}, TranslateError(err)
	}
	currency, err := domain.ParseCurrency(currencyCode)
	if err != nil {
		return TransactionRecord{}, err
	}
	money, err := domain.NewMoney(amountMinor, currency)
	if err != nil {
		return TransactionRecord{}, err
	}
	kind, err := domain.ParseWagerTransactionKind(kindValue)
	if err != nil {
		return TransactionRecord{}, err
	}
	state := domain.WagerTransactionState(stateValue)
	var balance *domain.Money
	if resultMinor != nil {
		if resultCurrency == nil {
			return TransactionRecord{}, fmt.Errorf("postgres: result currency missing for transaction %s", id)
		}
		resultCurrencyValue, parseErr := domain.ParseCurrency(*resultCurrency)
		if parseErr != nil {
			return TransactionRecord{}, parseErr
		}
		value, moneyErr := domain.NewMoney(*resultMinor, resultCurrencyValue)
		if moneyErr != nil {
			return TransactionRecord{}, moneyErr
		}
		balance = &value
	}
	snapshot := domain.WagerTransactionSnapshot{
		ID: id,
		Details: domain.WagerTransactionDetails{
			ProviderID: value(providerID), ExternalTransactionID: value(externalID),
			PlayerID: playerID, WalletID: walletID, RoundID: value(roundID), GameID: value(gameID),
			Kind: kind, Money: money, ReferenceExternalTransactionID: value(referenceExternal),
		},
		State: state, FailureCode: domain.FailureCode(value(failureCode)), ResultBalance: balance,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	tx, err := domain.RehydrateWagerTransaction(snapshot)
	if err != nil {
		return TransactionRecord{}, err
	}
	record := TransactionRecord{
		Transaction: tx, IdempotencyKey: value(idempotencyKey),
		ReferenceTransactionID: value(referenceID), ReferenceAttempts: referenceAttempts,
		ReferenceNextAttemptAt: nextAttemptAt, ReferenceExpiresAt: expiresAt,
		ReferenceClaimedBy: value(referenceClaimedBy), ReferenceClaimUntil: claimUntil,
		CompletedAt: completedAt,
	}
	if len(requestHash) != 0 {
		if len(requestHash) != len(record.RequestHash) {
			return TransactionRecord{}, fmt.Errorf("postgres: invalid request hash length for transaction %s", id)
		}
		copy(record.RequestHash[:], requestHash)
		record.HasRequestHash = true
	}
	return record, nil
}

func value(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
