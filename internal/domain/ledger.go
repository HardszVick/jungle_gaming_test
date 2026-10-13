package domain

import (
	"fmt"
	"time"
)

// WalletLedgerEntry is immutable after construction. Database constraints and
// permissions provide the corresponding persisted append-only guarantee.
type WalletLedgerEntry struct {
	id            string
	walletID      string
	transactionID string
	direction     LedgerDirection
	money         Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

func NewWalletLedgerEntry(
	id, walletID, transactionID string,
	direction LedgerDirection,
	money, balanceBefore, balanceAfter Money,
	createdAt time.Time,
) (WalletLedgerEntry, error) {
	if id == "" || walletID == "" || transactionID == "" || createdAt.IsZero() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: identity and timestamp are required", ErrInvalidLedgerEntry)
	}
	if !direction.IsValid() || money.IsNegative() || money.IsZero() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: direction and positive money are required", ErrInvalidLedgerEntry)
	}
	if balanceBefore.IsNegative() || balanceAfter.IsNegative() {
		return WalletLedgerEntry{}, fmt.Errorf("%w: balances cannot be negative", ErrInvalidLedgerEntry)
	}
	var expected Money
	var err error
	if direction == LedgerDirectionCredit {
		expected, err = balanceBefore.Add(money)
	} else {
		expected, err = balanceBefore.Subtract(money)
	}
	if err != nil {
		return WalletLedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
	}
	equal, err := expected.Equal(balanceAfter)
	if err != nil || !equal {
		return WalletLedgerEntry{}, fmt.Errorf("%w: balance equation is inconsistent", ErrInvalidLedgerEntry)
	}
	return WalletLedgerEntry{
		id: id, walletID: walletID, transactionID: transactionID,
		direction: direction, money: money, balanceBefore: balanceBefore,
		balanceAfter: balanceAfter, createdAt: createdAt.UTC(),
	}, nil
}

func RehydrateWalletLedgerEntry(
	id, walletID, transactionID string,
	direction LedgerDirection,
	money, balanceBefore, balanceAfter Money,
	createdAt time.Time,
) (WalletLedgerEntry, error) {
	return NewWalletLedgerEntry(id, walletID, transactionID, direction, money, balanceBefore, balanceAfter, createdAt)
}

func (e WalletLedgerEntry) ID() string                 { return e.id }
func (e WalletLedgerEntry) WalletID() string           { return e.walletID }
func (e WalletLedgerEntry) TransactionID() string      { return e.transactionID }
func (e WalletLedgerEntry) Direction() LedgerDirection { return e.direction }
func (e WalletLedgerEntry) Money() Money               { return e.money }
func (e WalletLedgerEntry) BalanceBefore() Money       { return e.balanceBefore }
func (e WalletLedgerEntry) BalanceAfter() Money        { return e.balanceAfter }
func (e WalletLedgerEntry) CreatedAt() time.Time       { return e.createdAt }
