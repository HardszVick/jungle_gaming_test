package postgres

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func (s *Store) InsertLedgerEntry(ctx context.Context, entry domain.WalletLedgerEntry) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		entry.ID(), entry.WalletID(), entry.TransactionID(), string(entry.Direction()),
		entry.Money().MinorUnits(), entry.Money().Currency().String(),
		entry.BalanceBefore().MinorUnits(), entry.BalanceAfter().MinorUnits(), entry.CreatedAt())
	return TranslateError(err)
}

func (s *Store) ListLedger(ctx context.Context, walletID string, cursor *LedgerCursor, limit int) (LedgerPage, error) {
	if limit <= 0 || limit > 1000 {
		return LedgerPage{}, errors.New("postgres: ledger limit must be between 1 and 1000")
	}
	query := `
		SELECT id::text, wallet_id::text, transaction_id::text, direction,
		       amount_minor, currency::text, balance_before_minor, balance_after_minor, created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1`
	args := []any{walletID}
	if cursor != nil {
		query += ` AND (created_at, id) < ($2, $3)`
		args = append(args, cursor.CreatedAt, cursor.ID)
	}
	args = append(args, limit+1)
	query += ` ORDER BY created_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return LedgerPage{}, TranslateError(err)
	}
	defer rows.Close()
	entries := make([]domain.WalletLedgerEntry, 0, limit+1)
	for rows.Next() {
		entry, scanErr := scanLedgerEntry(rows)
		if scanErr != nil {
			return LedgerPage{}, scanErr
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return LedgerPage{}, TranslateError(err)
	}
	page := LedgerPage{Entries: entries}
	if len(entries) > limit {
		last := entries[limit-1]
		page.Entries = entries[:limit]
		page.NextCursor = &LedgerCursor{CreatedAt: last.CreatedAt(), ID: last.ID()}
	}
	return page, nil
}

type scanner interface{ Scan(...any) error }

func scanLedgerEntry(row scanner) (domain.WalletLedgerEntry, error) {
	var id, walletID, transactionID, directionValue, currencyCode string
	var amountMinor, beforeMinor, afterMinor int64
	var createdAt time.Time
	if err := row.Scan(&id, &walletID, &transactionID, &directionValue, &amountMinor, &currencyCode, &beforeMinor, &afterMinor, &createdAt); err != nil {
		return domain.WalletLedgerEntry{}, TranslateError(err)
	}
	currency, err := domain.ParseCurrency(currencyCode)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	money, err := domain.NewMoney(amountMinor, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	before, err := domain.NewMoney(beforeMinor, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	after, err := domain.NewMoney(afterMinor, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	return domain.RehydrateWalletLedgerEntry(id, walletID, transactionID, domain.LedgerDirection(directionValue), money, before, after, createdAt)
}
