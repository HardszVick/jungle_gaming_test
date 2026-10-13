package auth

import (
	"net/http"
	"strings"
)

type Middleware func(http.Handler) http.Handler

// Middleware authenticates requests and optionally requires every listed permission.
// Permissions may be represented by either scope or realm_access.roles in Keycloak.
func (a *Authenticator) Middleware(requiredPermissions ...string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			rawToken, ok := bearerToken(request.Header.Values("Authorization"))
			if !ok || a == nil || a.verifier == nil {
				writeUnauthorized(response)
				return
			}

			verified, err := a.verifier.Verify(request.Context(), rawToken)
			if err != nil {
				writeUnauthorized(response)
				return
			}
			principal, err := PrincipalFromToken(verified)
			if err != nil {
				writeUnauthorized(response)
				return
			}
			for _, permission := range requiredPermissions {
				if !principal.HasPermission(permission) {
					writeForbidden(response)
					return
				}
			}

			next.ServeHTTP(response, request.WithContext(withPrincipal(request.Context(), principal)))
		})
	}
}

func bearerToken(headers []string) (string, bool) {
	if len(headers) != 1 {
		return "", false
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

// RequireProvider allows only a principal whose provider_id exactly matches providerID.
func RequireProvider(providerID string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if err := AuthorizeProvider(request.Context(), providerID); err != nil {
				writeAuthorizationError(response, err)
				return
			}
			next.ServeHTTP(response, request)
		})
	}
}

// RequireProviderFrom obtains the expected provider from the request, such as a path value.
func RequireProviderFrom(providerID func(*http.Request) string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if providerID == nil {
				writeForbidden(response)
				return
			}
			if err := AuthorizeProvider(request.Context(), providerID(request)); err != nil {
				writeAuthorizationError(response, err)
				return
			}
			next.ServeHTTP(response, request)
		})
	}
}

// RequireInternal allows only principals with the wallet:internal realm role.
func RequireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if err := AuthorizeInternal(request.Context()); err != nil {
			writeAuthorizationError(response, err)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func writeAuthorizationError(response http.ResponseWriter, err error) {
	if err == ErrUnauthorized {
		writeUnauthorized(response)
		return
	}
	writeForbidden(response)
}

func writeUnauthorized(response http.ResponseWriter) {
	response.Header().Set("WWW-Authenticate", "Bearer")
	http.Error(response, "unauthorized", http.StatusUnauthorized)
}

func writeForbidden(response http.ResponseWriter) {
	http.Error(response, "forbidden", http.StatusForbidden)
}
