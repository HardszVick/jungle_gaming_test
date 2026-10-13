package domain

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// iso4217CurrencyListVersion identifies the closed ISO 4217 snapshot used by
// this package. Monetary scale remains fixed at two decimal places regardless
// of the ISO minor-unit recommendation for an individual currency.
const iso4217CurrencyListVersion = "v1"

var iso4217CurrencyCodes = map[string]struct{}{
	"AUD": {}, "BRL": {}, "CAD": {}, "CHF": {}, "CNY": {},
	"EUR": {}, "GBP": {}, "MXN": {}, "NZD": {}, "USD": {},
}

// Currency is a validated three-letter uppercase ISO 4217 code.
type Currency struct {
	code string
}

func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return Currency{}, fmt.Errorf("%w: must contain exactly three letters", ErrInvalidCurrency)
	}
	for i := range code {
		if code[i] < 'A' || code[i] > 'Z' {
			return Currency{}, fmt.Errorf("%w: must contain only uppercase ASCII letters", ErrInvalidCurrency)
		}
	}
	if _, supported := iso4217CurrencyCodes[code]; !supported {
		return Currency{}, fmt.Errorf("%w: unsupported ISO 4217 code", ErrInvalidCurrency)
	}
	return Currency{code: code}, nil
}

func (c Currency) String() string { return c.code }

func (c Currency) IsValid() bool {
	_, err := ParseCurrency(c.code)
	return err == nil
}

func (c Currency) MarshalText() ([]byte, error) {
	if !c.IsValid() {
		return nil, ErrInvalidCurrency
	}
	return []byte(c.code), nil
}

func (c Currency) MarshalJSON() ([]byte, error) {
	if !c.IsValid() {
		return nil, ErrInvalidCurrency
	}
	return json.Marshal(c.code)
}

// Money stores exact minor units at a fixed scale of two decimal places.
// NewMoney permits negative values for internal differences. ParseMoney is the
// constructor for external input and deliberately rejects a minus sign.
type Money struct {
	minor    int64
	currency Currency
}

func NewMoney(minor int64, currency Currency) (Money, error) {
	if !currency.IsValid() {
		return Money{}, ErrInvalidCurrency
	}
	return Money{minor: minor, currency: currency}, nil
}

func ParseMoney(amount string, currency Currency) (Money, error) {
	if !currency.IsValid() {
		return Money{}, ErrInvalidCurrency
	}
	minor, err := parseExternalMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: currency}, nil
}

func parseExternalMinor(amount string) (int64, error) {
	if amount == "0" {
		return 0, nil
	}
	if len(amount) < 4 || amount[len(amount)-3] != '.' {
		return 0, fmt.Errorf("%w: amount must be 0 or use exactly two decimal places", ErrInvalidMoney)
	}

	wholeEnd := len(amount) - 3
	if wholeEnd == 0 {
		return 0, fmt.Errorf("%w: missing whole-number part", ErrInvalidMoney)
	}

	var minor int64
	for i := 0; i < len(amount); i++ {
		if i == wholeEnd {
			continue
		}
		ch := amount[i]
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("%w: amount contains a non-decimal character", ErrInvalidMoney)
		}
		digit := int64(ch - '0')
		if minor > (math.MaxInt64-digit)/10 {
			return 0, ErrMoneyOverflow
		}
		minor = minor*10 + digit
	}
	return minor, nil
}

func ZeroMoney(currency Currency) (Money, error) { return NewMoney(0, currency) }

func (m Money) MinorUnits() int64  { return m.minor }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsZero() bool       { return m.minor == 0 }
func (m Money) IsNegative() bool   { return m.minor < 0 }

func (m Money) String() string {
	if m.minor >= 0 {
		return fmt.Sprintf("%d.%02d", m.minor/100, m.minor%100)
	}
	// -(MinInt64) overflows, so format its digits as an unsigned magnitude.
	magnitude := uint64(-(m.minor + 1)) + 1
	return fmt.Sprintf("-%d.%02d", magnitude/100, magnitude%100)
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.currency.IsValid() {
		return nil, ErrInvalidCurrency
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: m.String(), Currency: m.currency.String()})
}

func (m Money) Add(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor > math.MaxInt64-other.minor) ||
		(other.minor < 0 && m.minor < math.MinInt64-other.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: m.minor + other.minor, currency: m.currency}, nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor < math.MinInt64+other.minor) ||
		(other.minor < 0 && m.minor > math.MaxInt64+other.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: m.minor - other.minor, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	if m.minor == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Compare(other Money) (int, error) {
	if err := m.sameCurrency(other); err != nil {
		return 0, err
	}
	if m.minor < other.minor {
		return -1, nil
	}
	if m.minor > other.minor {
		return 1, nil
	}
	return 0, nil
}

func (m Money) Equal(other Money) (bool, error) {
	comparison, err := m.Compare(other)
	return comparison == 0, err
}

func (m Money) sameCurrency(other Money) error {
	if !m.currency.IsValid() || !other.currency.IsValid() {
		return ErrInvalidCurrency
	}
	if m.currency != other.currency {
		return fmt.Errorf("%w: %s and %s", ErrCurrencyMismatch, m.currency, other.currency)
	}
	return nil
}

// ParseMoneyValue normalizes a currency code to uppercase before validating
// external input. It is useful at transport boundaries where JSON is textual.
func ParseMoneyValue(amount, currency string) (Money, error) {
	c, err := ParseCurrency(strings.ToUpper(currency))
	if err != nil {
		return Money{}, err
	}
	return ParseMoney(amount, c)
}
