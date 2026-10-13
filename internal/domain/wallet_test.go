package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestWalletCreateRehydrateAndChanges(t *testing.T) {
	brl := testCurrency(t, "BRL")
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("BRT", -3*60*60))
	wallet, err := NewWallet("wallet-1", "player-1", testMoney(t, "100.00", brl), now)
	if err != nil {
		t.Fatal(err)
	}
	if wallet.Version() != 1 || wallet.Balance().String() != "100.00" || wallet.CreatedAt().Location() != time.UTC {
		t.Fatalf("unexpected new wallet: version=%d balance=%s created=%v", wallet.Version(), wallet.Balance(), wallet.CreatedAt())
	}

	zero := testMoney(t, "0.00", brl)
	if err := wallet.Credit(zero, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if wallet.Version() != 1 || !wallet.UpdatedAt().Equal(now) {
		t.Fatal("zero credit changed wallet")
	}
	if err := wallet.Debit(testMoney(t, "20.00", brl), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if wallet.Version() != 2 || wallet.Balance().String() != "80.00" {
		t.Fatalf("after debit: version=%d balance=%s", wallet.Version(), wallet.Balance())
	}
	if err := wallet.Credit(testMoney(t, "5.00", brl), now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if wallet.Version() != 3 || wallet.Balance().String() != "85.00" {
		t.Fatalf("after credit: version=%d balance=%s", wallet.Version(), wallet.Balance())
	}

	rehydrated, err := RehydrateWallet("wallet-1", "player-1", wallet.Balance(), 10, now, now.Add(time.Hour))
	if err != nil || rehydrated.Version() != 10 {
		t.Fatalf("RehydrateWallet = %v, %v", rehydrated, err)
	}
}

func TestWalletRejectsInvalidChangesWithoutMutation(t *testing.T) {
	brl := testCurrency(t, "BRL")
	usd := testCurrency(t, "USD")
	now := time.Now().UTC()
	wallet, _ := NewWallet("wallet-1", "player-1", testMoney(t, "10.00", brl), now)

	if err := wallet.Debit(testMoney(t, "10.01", brl), now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("insufficient debit error = %v", err)
	}
	if err := wallet.Credit(testMoney(t, "1.00", usd), now); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("cross-currency credit error = %v", err)
	}
	negative, _ := NewMoney(-1, brl)
	if err := wallet.Debit(negative, now); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("negative debit error = %v", err)
	}
	if wallet.Version() != 1 || wallet.Balance().String() != "10.00" {
		t.Fatal("failed operation mutated wallet")
	}
}

func TestWalletRejectsInvalidRehydrationAndVersionOverflow(t *testing.T) {
	brl := testCurrency(t, "BRL")
	now := time.Now().UTC()
	negative, _ := NewMoney(-1, brl)
	if _, err := RehydrateWallet("wallet", "player", negative, 1, now, now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("negative balance error = %v", err)
	}
	wallet, err := RehydrateWallet("wallet", "player", testMoney(t, "1.00", brl), math.MaxInt64, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := wallet.Credit(testMoney(t, "0.01", brl), now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("version overflow error = %v", err)
	}
	if err := wallet.Credit(testMoney(t, "0.00", brl), now); err != nil {
		t.Fatalf("zero change at max version returned %v", err)
	}
}

func TestWalletClampsChangeTimestampFromBehindReplica(t *testing.T) {
	brl := testCurrency(t, "BRL")
	updatedAt := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	wallet, err := RehydrateWallet(
		"wallet-1", "player-1", testMoney(t, "10.00", brl), 4,
		updatedAt.Add(-time.Hour), updatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := wallet.Credit(testMoney(t, "2.00", brl), updatedAt.Add(-30*time.Minute)); err != nil {
		t.Fatalf("credit from behind replica: %v", err)
	}
	if wallet.Balance().String() != "12.00" || wallet.Version() != 5 {
		t.Fatalf("wallet after credit = %s/%d", wallet.Balance(), wallet.Version())
	}
	if !wallet.UpdatedAt().Equal(updatedAt) {
		t.Fatalf("updated at regressed to %v, want %v", wallet.UpdatedAt(), updatedAt)
	}
}
