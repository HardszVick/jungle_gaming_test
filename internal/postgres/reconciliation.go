package postgres

import (
	"context"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// ReconcileWallet must be called in a REPEATABLE READ transaction when the
// caller needs the wallet and ledger to come from one stable snapshot.
func (s *Store) ReconcileWallet(ctx context.Context, walletID string) (Reconciliation, error) {
	wallet, err := s.GetWallet(ctx, walletID)
	if err != nil {
		return Reconciliation{}, err
	}
	var ledgerMinor, count int64
	err = s.db.QueryRow(ctx, `
		SELECT COALESCE(SUM(
			CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END
		), 0)::bigint, COUNT(*)::bigint
		FROM wallet_ledger_entries
		WHERE wallet_id = $1`, walletID).Scan(&ledgerMinor, &count)
	if err != nil {
		return Reconciliation{}, TranslateError(err)
	}
	ledgerBalance, err := domain.NewMoney(ledgerMinor, wallet.Currency())
	if err != nil {
		return Reconciliation{}, err
	}
	difference, err := wallet.Balance().Subtract(ledgerBalance)
	if err != nil {
		return Reconciliation{}, err
	}
	return Reconciliation{
		WalletBalance: wallet.Balance(), LedgerBalance: ledgerBalance,
		Difference: difference, EntryCount: count, Consistent: difference.IsZero(),
	}, nil
}
