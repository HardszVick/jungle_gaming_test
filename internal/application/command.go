package application

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

var ErrInvalidTransactionCommand = errors.New("invalid transaction command")

// TransactionCommandInput is the transport-neutral textual representation
// accepted from HTTP and SQS adapters.
type TransactionCommandInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// TransactionCommand is normalized once at the application boundary. Its
// fields are private so all hashes necessarily use validated canonical data.
type TransactionCommand struct {
	providerID                     string
	externalTransactionID          string
	playerID                       string
	walletID                       string
	roundID                        string
	gameID                         string
	kind                           domain.WagerTransactionKind
	money                          domain.Money
	referenceExternalTransactionID string
}

func NormalizeTransactionCommand(input TransactionCommandInput) (TransactionCommand, error) {
	identifiers := []struct {
		name  string
		value string
	}{
		{"providerId", input.ProviderID},
		{"externalTransactionId", input.ExternalTransactionID},
		{"playerId", input.PlayerID},
		{"walletId", input.WalletID},
		{"roundId", input.RoundID},
		{"gameId", input.GameID},
	}
	for _, identifier := range identifiers {
		if identifier.value == "" || strings.TrimSpace(identifier.value) != identifier.value {
			return TransactionCommand{}, fmt.Errorf("%w: %s is empty or has surrounding whitespace", ErrInvalidTransactionCommand, identifier.name)
		}
	}
	if input.ReferenceExternalTransactionID != "" && strings.TrimSpace(input.ReferenceExternalTransactionID) != input.ReferenceExternalTransactionID {
		return TransactionCommand{}, fmt.Errorf("%w: referenceExternalTransactionId has surrounding whitespace", ErrInvalidTransactionCommand)
	}

	kind, err := domain.ParseExternalWagerTransactionKind(strings.ToUpper(input.Kind))
	if err != nil {
		return TransactionCommand{}, fmt.Errorf("%w: %v", ErrInvalidTransactionCommand, err)
	}
	money, err := domain.ParseMoneyValue(input.Amount, strings.ToUpper(input.Currency))
	if err != nil {
		return TransactionCommand{}, fmt.Errorf("%w: %v", ErrInvalidTransactionCommand, err)
	}
	if err := domain.ValidateOperationAmount(kind, money, true); err != nil {
		return TransactionCommand{}, fmt.Errorf("%w: %v", ErrInvalidTransactionCommand, err)
	}
	hasReference := input.ReferenceExternalTransactionID != ""
	if (kind.IsReversal() && !hasReference) || (!kind.IsReversal() && kind != domain.TransactionKindWin && hasReference) {
		return TransactionCommand{}, fmt.Errorf("%w: reference presence does not match kind", ErrInvalidTransactionCommand)
	}

	return TransactionCommand{
		providerID: input.ProviderID, externalTransactionID: input.ExternalTransactionID,
		playerID: input.PlayerID, walletID: input.WalletID, roundID: input.RoundID,
		gameID: input.GameID, kind: kind, money: money,
		referenceExternalTransactionID: input.ReferenceExternalTransactionID,
	}, nil
}

func (c TransactionCommand) ProviderID() string                { return c.providerID }
func (c TransactionCommand) ExternalTransactionID() string     { return c.externalTransactionID }
func (c TransactionCommand) PlayerID() string                  { return c.playerID }
func (c TransactionCommand) WalletID() string                  { return c.walletID }
func (c TransactionCommand) RoundID() string                   { return c.roundID }
func (c TransactionCommand) GameID() string                    { return c.gameID }
func (c TransactionCommand) Kind() domain.WagerTransactionKind { return c.kind }
func (c TransactionCommand) Money() domain.Money               { return c.money }
func (c TransactionCommand) ReferenceExternalTransactionID() (string, bool) {
	return c.referenceExternalTransactionID, c.referenceExternalTransactionID != ""
}

func (c TransactionCommand) DomainDetails() domain.WagerTransactionDetails {
	return domain.WagerTransactionDetails{
		ProviderID: c.providerID, ExternalTransactionID: c.externalTransactionID,
		PlayerID: c.playerID, WalletID: c.walletID, RoundID: c.roundID,
		GameID: c.gameID, Kind: c.kind, Money: c.money,
		ReferenceExternalTransactionID: c.referenceExternalTransactionID,
	}
}

func (c TransactionCommand) CanonicalJSON() ([]byte, error) {
	if c.providerID == "" || !c.kind.IsValid() || !c.money.Currency().IsValid() {
		return nil, ErrInvalidTransactionCommand
	}
	type canonicalMoney struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	type canonicalCommand struct {
		ExternalTransactionID          string         `json:"externalTransactionId"`
		GameID                         string         `json:"gameId"`
		Kind                           string         `json:"kind"`
		Money                          canonicalMoney `json:"money"`
		PlayerID                       string         `json:"playerId"`
		ProviderID                     string         `json:"providerId"`
		ReferenceExternalTransactionID string         `json:"referenceExternalTransactionId,omitempty"`
		RoundID                        string         `json:"roundId"`
		WalletID                       string         `json:"walletId"`
	}
	return json.Marshal(canonicalCommand{
		ProviderID: c.providerID, ExternalTransactionID: c.externalTransactionID,
		PlayerID: c.playerID, WalletID: c.walletID, RoundID: c.roundID, GameID: c.gameID,
		Kind: string(c.kind), Money: canonicalMoney{Amount: c.money.String(), Currency: c.money.Currency().String()},
		ReferenceExternalTransactionID: c.referenceExternalTransactionID,
	})
}

func (c TransactionCommand) SHA256() ([sha256.Size]byte, error) {
	canonical, err := c.CanonicalJSON()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func (c TransactionCommand) SHA256Hex() (string, error) {
	digest, err := c.SHA256()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

func CanonicalTransactionCommandHash(input TransactionCommandInput) (string, error) {
	command, err := NormalizeTransactionCommand(input)
	if err != nil {
		return "", err
	}
	return command.SHA256Hex()
}
