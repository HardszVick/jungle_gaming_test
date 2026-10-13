package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

const InternalRole = "wallet:internal"

var (
	ErrInvalidToken  = errors.New("auth: invalid token")
	ErrInvalidClaims = errors.New("auth: invalid token claims")
	ErrUnauthorized  = errors.New("auth: unauthorized")
	ErrForbidden     = errors.New("auth: forbidden")
)

// VerifiedToken is the minimal verified JWT data needed to construct a Principal.
// Implementations must validate the token before returning it.
type VerifiedToken struct {
	Subject string
	Claims  json.RawMessage
}

// Verifier makes token verification replaceable in unit tests without weakening
// the production OIDC verifier.
type Verifier interface {
	Verify(context.Context, string) (VerifiedToken, error)
}

type oidcVerifier struct {
	verifier *oidc.IDTokenVerifier
	client   *http.Client
}

func (v *oidcVerifier) Verify(ctx context.Context, rawToken string) (VerifiedToken, error) {
	if v.client != nil {
		ctx = oidc.ClientContext(ctx, v.client)
	}

	token, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return VerifiedToken{}, ErrInvalidToken
	}

	var claims json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return VerifiedToken{}, ErrInvalidToken
	}

	return VerifiedToken{Subject: token.Subject, Claims: claims}, nil
}

// Authenticator verifies bearer tokens and stores their Principal in the request context.
type Authenticator struct {
	verifier Verifier
}

// New performs OIDC discovery and creates a verifier for the configured audience.
// The provider verifier owns the remote JWKS cache and refreshes it on key rotation.
func New(ctx context.Context, issuerURL, audience string) (*Authenticator, error) {
	return NewWithHTTPClient(ctx, issuerURL, audience, nil)
}

// NewWithHTTPClient is New with an optional HTTP client used for discovery and JWKS requests.
func NewWithHTTPClient(ctx context.Context, issuerURL, audience string, client *http.Client) (*Authenticator, error) {
	issuerURL = strings.TrimSpace(issuerURL)
	audience = strings.TrimSpace(audience)
	if issuerURL == "" {
		return nil, errors.New("auth: issuer URL is required")
	}
	if audience == "" {
		return nil, errors.New("auth: audience is required")
	}
	if ctx == nil {
		return nil, errors.New("auth: context is required")
	}

	discoveryContext := ctx
	if client != nil {
		discoveryContext = oidc.ClientContext(ctx, client)
	}
	provider, err := oidc.NewProvider(discoveryContext, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider: %w", err)
	}

	return NewAuthenticator(&oidcVerifier{
		verifier: provider.Verifier(&oidc.Config{ClientID: audience}),
		client:   client,
	})
}

// NewAuthenticator constructs an Authenticator around an already configured verifier.
func NewAuthenticator(verifier Verifier) (*Authenticator, error) {
	if verifier == nil {
		return nil, errors.New("auth: verifier is required")
	}
	return &Authenticator{verifier: verifier}, nil
}

// Principal is the authenticated identity and its Keycloak authorization claims.
type Principal struct {
	Subject    string
	ProviderID string
	Scopes     []string
	RealmRoles []string
}

func (p Principal) HasScope(scope string) bool {
	return contains(p.Scopes, scope)
}

func (p Principal) HasRealmRole(role string) bool {
	return contains(p.RealmRoles, role)
}

// HasPermission accepts either an OAuth scope or a Keycloak realm role.
func (p Principal) HasPermission(permission string) bool {
	return p.HasScope(permission) || p.HasRealmRole(permission)
}

func contains(values []string, wanted string) bool {
	if wanted == "" {
		return false
	}
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

type tokenClaims struct {
	ProviderID  string `json:"provider_id"`
	Scope       string `json:"scope"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// PrincipalFromToken parses authorization claims only after a Verifier has accepted the token.
func PrincipalFromToken(token VerifiedToken) (Principal, error) {
	if strings.TrimSpace(token.Subject) == "" || token.Subject != strings.TrimSpace(token.Subject) {
		return Principal{}, ErrInvalidClaims
	}

	var claims tokenClaims
	if len(token.Claims) == 0 || json.Unmarshal(token.Claims, &claims) != nil {
		return Principal{}, ErrInvalidClaims
	}
	if claims.ProviderID != strings.TrimSpace(claims.ProviderID) {
		return Principal{}, ErrInvalidClaims
	}

	principal := Principal{
		Subject:    token.Subject,
		ProviderID: claims.ProviderID,
		Scopes:     unique(strings.Fields(claims.Scope)),
		RealmRoles: unique(claims.RealmAccess.Roles),
	}
	return principal, nil
}

func unique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

type principalContextKey struct{}

func withPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext returns the identity installed by Authenticator.Middleware.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	if ctx == nil {
		return Principal{}, false
	}
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

// AuthorizeProvider enforces exact provider isolation for an authenticated context.
func AuthorizeProvider(ctx context.Context, providerID string) error {
	principal, ok := PrincipalFromContext(ctx)
	if !ok {
		return ErrUnauthorized
	}
	if providerID == "" || principal.ProviderID == "" || principal.ProviderID != providerID {
		return ErrForbidden
	}
	return nil
}

// AuthorizeInternal requires the Keycloak internal realm role.
func AuthorizeInternal(ctx context.Context) error {
	principal, ok := PrincipalFromContext(ctx)
	if !ok {
		return ErrUnauthorized
	}
	if !principal.HasRealmRole(InternalRole) {
		return ErrForbidden
	}
	return nil
}
