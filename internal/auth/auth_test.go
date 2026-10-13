package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type verifierFunc func(context.Context, string) (VerifiedToken, error)

func (verify verifierFunc) Verify(ctx context.Context, token string) (VerifiedToken, error) {
	return verify(ctx, token)
}

func TestPrincipalFromToken(t *testing.T) {
	token := VerifiedToken{
		Subject: "service-account-provider-a",
		Claims: json.RawMessage(`{
			"provider_id":"provider-a",
			"scope":"openid wager:read wager:write wager:read",
			"realm_access":{"roles":["wager:read","wallet:internal","wallet:internal"]}
		}`),
	}

	principal, err := PrincipalFromToken(token)
	if err != nil {
		t.Fatalf("PrincipalFromToken() error = %v", err)
	}
	if principal.Subject != token.Subject || principal.ProviderID != "provider-a" {
		t.Fatalf("PrincipalFromToken() = %+v", principal)
	}
	if got, want := strings.Join(principal.Scopes, ","), "openid,wager:read,wager:write"; got != want {
		t.Fatalf("scopes = %q, want %q", got, want)
	}
	if got, want := strings.Join(principal.RealmRoles, ","), "wager:read,wallet:internal"; got != want {
		t.Fatalf("realm roles = %q, want %q", got, want)
	}
	if !principal.HasScope("wager:write") || !principal.HasRealmRole(InternalRole) || !principal.HasPermission(InternalRole) {
		t.Fatal("principal permissions were not extracted")
	}
}

func TestPrincipalFromTokenRejectsMalformedClaims(t *testing.T) {
	tests := []VerifiedToken{
		{Subject: "", Claims: json.RawMessage(`{}`)},
		{Subject: " subject", Claims: json.RawMessage(`{}`)},
		{Subject: "subject", Claims: nil},
		{Subject: "subject", Claims: json.RawMessage(`{`)},
		{Subject: "subject", Claims: json.RawMessage(`{"scope":["wager:read"]}`)},
		{Subject: "subject", Claims: json.RawMessage(`{"provider_id":" provider-a"}`)},
		{Subject: "subject", Claims: json.RawMessage(`{"realm_access":{"roles":"wallet:internal"}}`)},
	}

	for _, token := range tests {
		if _, err := PrincipalFromToken(token); !errors.Is(err, ErrInvalidClaims) {
			t.Fatalf("PrincipalFromToken(%q, %s) error = %v, want ErrInvalidClaims", token.Subject, token.Claims, err)
		}
	}
}

func TestAuthenticationMiddleware(t *testing.T) {
	type contextKey string
	const requestMarker contextKey = "request-marker"

	var verifiedRaw string
	verifier := verifierFunc(func(ctx context.Context, raw string) (VerifiedToken, error) {
		if ctx.Value(requestMarker) != "present" {
			t.Fatal("verifier did not receive the request context")
		}
		verifiedRaw = raw
		return VerifiedToken{
			Subject: "provider-subject",
			Claims:  json.RawMessage(`{"provider_id":"provider-a","scope":"wager:read"}`),
		}, nil
	})
	authenticator, err := NewAuthenticator(verifier)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}

	nextCalled := false
	handler := authenticator.Middleware("wager:read")(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		nextCalled = true
		principal, ok := PrincipalFromContext(request.Context())
		if !ok || principal.ProviderID != "provider-a" {
			t.Fatalf("PrincipalFromContext() = %+v, %v", principal, ok)
		}
		response.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "bearer opaque-secret-token")
	request = request.WithContext(context.WithValue(request.Context(), requestMarker, "present"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent || !nextCalled {
		t.Fatalf("status = %d, next called = %v", response.Code, nextCalled)
	}
	if verifiedRaw != "opaque-secret-token" {
		t.Fatalf("verifier token = %q", verifiedRaw)
	}
}

func TestAuthenticationMiddlewareRejectsMissingAndMalformedCredentials(t *testing.T) {
	secret := "do-not-expose-this-token"
	tests := []struct {
		name    string
		headers []string
	}{
		{name: "absent"},
		{name: "basic", headers: []string{"Basic credentials"}},
		{name: "missing token", headers: []string{"Bearer"}},
		{name: "extra field", headers: []string{"Bearer " + secret + " extra"}},
		{name: "duplicate", headers: []string{"Bearer " + secret, "Bearer another"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			authenticator, err := NewAuthenticator(verifierFunc(func(context.Context, string) (VerifiedToken, error) {
				called = true
				return VerifiedToken{}, errors.New("must not be called")
			}))
			if err != nil {
				t.Fatal(err)
			}
			handler := authenticator.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("next handler was called")
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			for _, header := range test.headers {
				request.Header.Add("Authorization", header)
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", response.Code)
			}
			if called {
				t.Fatal("verifier was called for a malformed authorization header")
			}
			if strings.Contains(response.Body.String(), secret) {
				t.Fatal("response exposed bearer token")
			}
		})
	}
}

func TestAuthenticationMiddlewareDoesNotExposeVerifierFailure(t *testing.T) {
	secret := "secret.jwt.value"
	verifierMessage := "signature failure for " + secret
	authenticator, err := NewAuthenticator(verifierFunc(func(context.Context, string) (VerifiedToken, error) {
		return VerifiedToken{}, errors.New(verifierMessage)
	}))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+secret)
	response := httptest.NewRecorder()

	authenticator.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next handler was called")
	})).ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if strings.Contains(response.Body.String(), secret) || strings.Contains(response.Body.String(), verifierMessage) {
		t.Fatalf("response exposed verifier detail: %q", response.Body.String())
	}
}

func TestAuthenticationMiddlewareRequiresPermissionFromScopeOrRealmRole(t *testing.T) {
	tests := []struct {
		name   string
		claims string
		status int
	}{
		{name: "scope", claims: `{"scope":"wager:write"}`, status: http.StatusNoContent},
		{name: "realm role", claims: `{"realm_access":{"roles":["wager:write"]}}`, status: http.StatusNoContent},
		{name: "missing", claims: `{"scope":"wager:read"}`, status: http.StatusForbidden},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator, err := NewAuthenticator(verifierFunc(func(context.Context, string) (VerifiedToken, error) {
				return VerifiedToken{Subject: "subject", Claims: json.RawMessage(test.claims)}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			handler := authenticator.Middleware("wager:write")(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Authorization", "Bearer token")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestRequireProvider(t *testing.T) {
	tests := []struct {
		name       string
		principal  Principal
		withAuth   bool
		providerID string
		status     int
	}{
		{name: "matching provider", principal: Principal{ProviderID: "provider-a"}, withAuth: true, providerID: "provider-a", status: http.StatusNoContent},
		{name: "different provider", principal: Principal{ProviderID: "provider-b"}, withAuth: true, providerID: "provider-a", status: http.StatusForbidden},
		{name: "internal identity", principal: Principal{RealmRoles: []string{InternalRole}}, withAuth: true, providerID: "provider-a", status: http.StatusForbidden},
		{name: "missing principal", providerID: "provider-a", status: http.StatusUnauthorized},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := RequireProvider(test.providerID)(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			if test.withAuth {
				request = request.WithContext(withPrincipal(request.Context(), test.principal))
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestRequireInternalRequiresRealmRole(t *testing.T) {
	tests := []struct {
		name      string
		principal Principal
		status    int
	}{
		{name: "internal role", principal: Principal{RealmRoles: []string{InternalRole}}, status: http.StatusNoContent},
		{name: "provider", principal: Principal{ProviderID: "provider-a", RealmRoles: []string{"wager:read"}}, status: http.StatusForbidden},
		{name: "scope is not internal role", principal: Principal{Scopes: []string{InternalRole}}, status: http.StatusForbidden},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := RequireInternal(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/wallets", nil)
			request = request.WithContext(withPrincipal(request.Context(), test.principal))
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestOIDCVerifierValidatesJWTAndCachesJWKS(t *testing.T) {
	key := mustRSAKey(t)
	unknownKey := mustRSAKey(t)
	const keyID = "current-key"
	var issuer string
	var jwksRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/.well-known/openid-configuration":
			writeJSON(t, response, map[string]any{
				"issuer":                                issuer,
				"authorization_endpoint":                issuer + "/authorize",
				"token_endpoint":                        issuer + "/token",
				"jwks_uri":                              issuer + "/keys",
				"response_types_supported":              []string{"token"},
				"subject_types_supported":               []string{"public"},
				"id_token_signing_alg_values_supported": []string{"RS256"},
			})
		case "/keys":
			jwksRequests.Add(1)
			writeJSON(t, response, map[string]any{"keys": []any{jwk(&key.PublicKey, keyID)}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	issuer = server.URL

	authenticator, err := NewWithHTTPClient(context.Background(), issuer, "backend-api", server.Client())
	if err != nil {
		t.Fatalf("NewWithHTTPClient() error = %v", err)
	}
	now := time.Now()
	claims := map[string]any{
		"iss":         issuer,
		"aud":         "backend-api",
		"sub":         "service-account-provider-a",
		"iat":         now.Add(-time.Minute).Unix(),
		"exp":         now.Add(time.Hour).Unix(),
		"provider_id": "provider-a",
		"scope":       "wager:read wager:write",
		"realm_access": map[string]any{
			"roles": []string{"wager:read", "wager:write"},
		},
	}

	valid := signJWT(t, key, keyID, claims)
	assertAuthenticationStatus(t, authenticator, valid, http.StatusNoContent)
	assertAuthenticationStatus(t, authenticator, valid, http.StatusNoContent)
	if got := jwksRequests.Load(); got != 1 {
		t.Fatalf("JWKS requests after repeated valid token = %d, want 1", got)
	}

	tests := []struct {
		name  string
		token string
	}{
		{name: "malformed", token: "not-a-jwt"},
		{name: "expired", token: signJWT(t, key, keyID, cloneClaims(claims, "exp", now.Add(-time.Minute).Unix()))},
		{name: "wrong audience", token: signJWT(t, key, keyID, cloneClaims(claims, "aud", "another-api"))},
		{name: "wrong issuer", token: signJWT(t, key, keyID, cloneClaims(claims, "iss", issuer+"/other"))},
		{name: "invalid signature", token: signJWT(t, unknownKey, keyID, claims)},
		{name: "unknown key", token: signJWT(t, unknownKey, "unknown-key", claims)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertAuthenticationStatus(t, authenticator, test.token, http.StatusUnauthorized)
		})
	}
	if got := jwksRequests.Load(); got < 2 {
		t.Fatalf("JWKS requests = %d, unknown key did not trigger refresh", got)
	}
}

func assertAuthenticationStatus(t *testing.T, authenticator *Authenticator, token string, wanted int) {
	t.Helper()
	handler := authenticator.Middleware()(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != wanted {
		t.Fatalf("status = %d, want %d; body = %q", response.Code, wanted, response.Body.String())
	}
	if wanted != http.StatusNoContent && strings.Contains(response.Body.String(), token) {
		t.Fatal("authentication response exposed token")
	}
}

func mustRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}
	return key
}

func signJWT(t *testing.T, key *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	unsigned := encodedHeader + "." + encodedPayload
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func jwk(key *rsa.PublicKey, keyID string) map[string]string {
	exponent := big.NewInt(int64(key.E)).Bytes()
	return map[string]string{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": keyID,
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(exponent),
	}
}

func cloneClaims(claims map[string]any, key string, value any) map[string]any {
	clone := make(map[string]any, len(claims))
	for claim, existing := range claims {
		clone[claim] = existing
	}
	clone[key] = value
	return clone
}

func writeJSON(t *testing.T, response http.ResponseWriter, value any) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(value); err != nil {
		t.Errorf("encode HTTP response: %v", err)
	}
}

func TestNewValidatesConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		issuer   string
		audience string
	}{
		{name: "nil context", issuer: "https://issuer.example", audience: "api"},
		{name: "missing issuer", ctx: context.Background(), audience: "api"},
		{name: "missing audience", ctx: context.Background(), issuer: "https://issuer.example"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.ctx, test.issuer, test.audience); err == nil {
				t.Fatal("New() error = nil")
			}
		})
	}
}

func TestNewAuthenticatorRejectsNilVerifier(t *testing.T) {
	if _, err := NewAuthenticator(nil); err == nil {
		t.Fatal("NewAuthenticator(nil) error = nil")
	}
}

func ExampleRequireProviderFrom() {
	middleware := RequireProviderFrom(func(request *http.Request) string {
		return request.PathValue("providerId")
	})
	fmt.Printf("%T\n", middleware)
	// Output: auth.Middleware
}
