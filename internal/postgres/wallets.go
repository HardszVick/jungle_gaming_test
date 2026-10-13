package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func (s *Store) InsertWallet(ctx context.Context, wallet *domain.Wallet) error {
	if s == nil || s.db == nil || wallet == nil {
		return errors.New("postgres: wallet repository is not initialized")
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		wallet.ID(), wallet.PlayerID(), wallet.Currency().String(), wallet.Balance().MinorUnits(),
		wallet.Version(), wallet.CreatedAt(), wallet.UpdatedAt())
	return TranslateError(err)
}

func (s *Store) GetWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	return s.getWallet(ctx, id, false)
}

func (s *Store) LockWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	return s.getWallet(ctx, id, true)
}

func (s *Store) getWallet(ctx context.Context, id string, lock bool) (*domain.Wallet, error) {
	query := `
		SELECT id::text, player_id, currency::text, balance_minor, version, created_at, updated_at
		FROM wallets WHERE id = $1`
	if lock {
		query += ` FOR UPDATE`
	}
	var walletID, playerID, currencyCode string
	var balanceMinor, version int64
	var createdAt, updatedAt time.Time
	err := s.db.QueryRow(ctx, query, id).Scan(
		&walletID, &playerID, &currencyCode, &balanceMinor, &version, &createdAt, &updatedAt,
	)
	if err != nil {
		return nil, TranslateError(err)
	}
	currency, err := domain.ParseCurrency(currencyCode)
	if err != nil {
		return nil, err
	}
	balance, err := domain.NewMoney(balanceMinor, currency)
	if err != nil {
		return nil, err
	}
	wallet, err := domain.RehydrateWallet(walletID, playerID, balance, version, createdAt, updatedAt)
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (s *Store) UpdateWallet(ctx context.Context, wallet *domain.Wallet, expectedVersion int64) error {
	if wallet == nil {
		return errors.New("postgres: wallet is nil")
	}
	tag, err := s.db.Exec(ctx, `
		UPDATE wallets
		SET balance_minor = $1, version = $2, updated_at = $3
		WHERE id = $4 AND version = $5`,
		wallet.Balance().MinorUnits(), wallet.Version(), wallet.UpdatedAt(), wallet.ID(), expectedVersion)
	if err != nil {
		return TranslateError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConcurrentUpdate
	}
	return nil
}
