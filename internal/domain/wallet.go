package domain

import (
	"errors"
	"fmt"
	"math"
	"time"
)

type Wallet struct {
	id        string
	playerID  string
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(id, playerID string, initialBalance Money, now time.Time) (*Wallet, error) {
	return RehydrateWallet(id, playerID, initialBalance, 1, now, now)
}

func RehydrateWallet(
	id, playerID string,
	balance Money,
	version int64,
	createdAt, updatedAt time.Time,
) (*Wallet, error) {
	if id == "" || playerID == "" {
		return nil, fmt.Errorf("%w: id and player id are required", ErrInvalidWallet)
	}
	if !balance.Currency().IsValid() || balance.IsNegative() {
		return nil, fmt.Errorf("%w: balance must be valid and nonnegative", ErrInvalidWallet)
	}
	if version < 1 {
		return nil, fmt.Errorf("%w: version must be positive", ErrInvalidWallet)
	}
	if createdAt.IsZero() || updatedAt.IsZero() || updatedAt.Before(createdAt) {
		return nil, fmt.Errorf("%w: invalid timestamps", ErrInvalidWallet)
	}
	return &Wallet{
		id:        id,
		playerID:  playerID,
		balance:   balance,
		version:   version,
		createdAt: createdAt.UTC(),
		updatedAt: updatedAt.UTC(),
	}, nil
}

func (w *Wallet) ID() string           { return w.id }
func (w *Wallet) PlayerID() string     { return w.playerID }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Currency() Currency   { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

func (w *Wallet) Credit(amount Money, at time.Time) error {
	if err := w.validateChange(amount, at); err != nil {
		return err
	}
	if amount.IsNegative() {
		return fmt.Errorf("%w: credit cannot be negative", ErrInvalidMoney)
	}
	if amount.IsZero() {
		return nil
	}
	if w.version == math.MaxInt64 {
		return fmt.Errorf("%w: version overflow", ErrInvalidWallet)
	}
	next, err := w.balance.Add(amount)
	if err != nil {
		return err
	}
	return w.applyBalance(next, at)
}

func (w *Wallet) Debit(amount Money, at time.Time) error {
	if err := w.validateChange(amount, at); err != nil {
		return err
	}
	if amount.IsNegative() {
		return fmt.Errorf("%w: debit cannot be negative", ErrInvalidMoney)
	}
	if amount.IsZero() {
		return nil
	}
	if w.version == math.MaxInt64 {
		return fmt.Errorf("%w: version overflow", ErrInvalidWallet)
	}
	comparison, err := w.balance.Compare(amount)
	if err != nil {
		return err
	}
	if comparison < 0 {
		return ErrInsufficientFunds
	}
	next, err := w.balance.Subtract(amount)
	if err != nil {
		return err
	}
	return w.applyBalance(next, at)
}

func (w *Wallet) validateChange(amount Money, at time.Time) error {
	if w == nil {
		return ErrInvalidWallet
	}
	if at.IsZero() {
		return fmt.Errorf("%w: change timestamp is required", ErrInvalidWallet)
	}
	if _, err := w.balance.Compare(amount); err != nil {
		return err
	}
	return nil
}

func (w *Wallet) applyBalance(balance Money, at time.Time) error {
	if balance.IsNegative() {
		return ErrInsufficientFunds
	}
	w.balance = balance
	w.version++
	if at.Before(w.updatedAt) {
		at = w.updatedAt
	}
	w.updatedAt = at.UTC()
	return nil
}

func IsInsufficientFunds(err error) bool { return errors.Is(err, ErrInsufficientFunds) }
