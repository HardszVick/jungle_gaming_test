package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func testCurrency(t *testing.T, code string) Currency {
	t.Helper()
	currency, err := ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q): %v", code, err)
	}
	return currency
}

func testMoney(t *testing.T, amount string, currency Currency) Money {
	t.Helper()
	money, err := ParseMoney(amount, currency)
	if err != nil {
		t.Fatalf("ParseMoney(%q): %v", amount, err)
	}
	return money
}

func TestCurrencyValidation(t *testing.T) {
	if iso4217CurrencyListVersion == "" {
		t.Fatal("ISO 4217 currency list version is empty")
	}
	for _, valid := range []string{"AUD", "BRL", "CAD", "CHF", "CNY", "EUR", "GBP", "MXN", "NZD", "USD"} {
		if _, err := ParseCurrency(valid); err != nil {
			t.Errorf("ParseCurrency(%q) returned %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "BR", "USDD", "brl", "B1L", "BRÇ", "JPY", "ZZZ", "RUR"} {
		if _, err := ParseCurrency(invalid); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) error = %v, want ErrInvalidCurrency", invalid, err)
		}
	}
}

func TestParseMoneyAndSerialization(t *testing.T) {
	brl := testCurrency(t, "BRL")
	tests := []struct {
		input string
		minor int64
		text  string
	}{
		{"0", 0, "0.00"},
		{"0.00", 0, "0.00"},
		{"25.00", 2500, "25.00"},
		{"00.01", 1, "0.01"},
		{"92233720368547758.07", math.MaxInt64, "92233720368547758.07"},
	}
	for _, test := range tests {
		money, err := ParseMoney(test.input, brl)
		if err != nil {
			t.Fatalf("ParseMoney(%q): %v", test.input, err)
		}
		if money.MinorUnits() != test.minor || money.String() != test.text {
			t.Errorf("ParseMoney(%q) = (%d, %q), want (%d, %q)", test.input, money.MinorUnits(), money.String(), test.minor, test.text)
		}
	}

	money := testMoney(t, "25.00", brl)
	encoded, err := json.Marshal(money)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"amount":"25.00","currency":"BRL"}`; got != want {
		t.Fatalf("json = %s, want %s", got, want)
	}
}

func TestParseMoneyRejectsInvalidExternalValues(t *testing.T) {
	brl := testCurrency(t, "BRL")
	invalid := []string{
		"", "-1.00", "+1.00", "NaN", "Infinity", "1e2", "1", "1.0",
		"1.000", ".00", "1,00", " 1.00", "1.00 ", "92233720368547758.08",
	}
	for _, input := range invalid {
		if _, err := ParseMoney(input, brl); err == nil {
			t.Errorf("ParseMoney(%q) succeeded", input)
		}
	}
}

func TestMoneyArithmeticAndComparison(t *testing.T) {
	brl := testCurrency(t, "BRL")
	usd := testCurrency(t, "USD")
	a := testMoney(t, "10.00", brl)
	b := testMoney(t, "2.50", brl)

	added, err := a.Add(b)
	if err != nil || added.String() != "12.50" {
		t.Fatalf("Add = %v, %v", added, err)
	}
	subtracted, err := b.Subtract(a)
	if err != nil || subtracted.String() != "-7.50" || !subtracted.IsNegative() {
		t.Fatalf("Subtract = %v, %v", subtracted, err)
	}
	negated, err := subtracted.Negate()
	if err != nil || negated.String() != "7.50" {
		t.Fatalf("Negate = %v, %v", negated, err)
	}
	if comparison, err := a.Compare(b); err != nil || comparison != 1 {
		t.Fatalf("Compare = %d, %v", comparison, err)
	}

	other, _ := NewMoney(1000, usd)
	if _, err := a.Add(other); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("cross-currency Add error = %v", err)
	}
	if _, err := a.Compare(other); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("cross-currency Compare error = %v", err)
	}
}

func TestMoneyArithmeticOverflow(t *testing.T) {
	brl := testCurrency(t, "BRL")
	max, _ := NewMoney(math.MaxInt64, brl)
	min, _ := NewMoney(math.MinInt64, brl)
	one, _ := NewMoney(1, brl)
	minusOne, _ := NewMoney(-1, brl)

	tests := []struct {
		name string
		fn   func() error
	}{
		{"add positive", func() error { _, err := max.Add(one); return err }},
		{"add negative", func() error { _, err := min.Add(minusOne); return err }},
		{"subtract positive", func() error { _, err := min.Subtract(one); return err }},
		{"subtract negative", func() error { _, err := max.Subtract(minusOne); return err }},
		{"negate", func() error { _, err := min.Negate(); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.fn(); !errors.Is(err, ErrMoneyOverflow) {
				t.Fatalf("error = %v, want ErrMoneyOverflow", err)
			}
		})
	}
	if min.String() != "-92233720368547758.08" {
		t.Fatalf("MinInt64 format = %s", min.String())
	}
}
