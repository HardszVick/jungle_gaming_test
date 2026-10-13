package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
)

const correlationHeader = "X-Correlation-ID"

type errorEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func defaultCorrelationID() string { return uuid.NewString() }

func correlationMiddleware(newID func() string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			correlationID := request.Header.Get(correlationHeader)
			if len(request.Header.Values(correlationHeader)) != 1 || !validCorrelationID(correlationID) {
				correlationID = newID()
			}
			if !validCorrelationID(correlationID) {
				correlationID = uuid.NewString()
			}
			response.Header().Set(correlationHeader, correlationID)
			ctx := observability.WithIdentifiers(request.Context(), observability.Identifiers{CorrelationID: correlationID})
			next.ServeHTTP(response, request.WithContext(ctx))
		})
	}
}

func validCorrelationID(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func correlationID(ctx context.Context) string {
	return observability.IdentifiersFromContext(ctx).CorrelationID
}

func requestLoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if logger == nil {
			return next
		}
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			next.ServeHTTP(response, request)
			observability.Log(request.Context(), logger, slog.LevelInfo, "HTTP request completed")
		})
	}
}

func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recover() != nil {
				writeError(response, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error", nil)
			}
		}()
		next.ServeHTTP(response, request)
	})
}

// stableErrorsMiddleware buffers public responses so errors produced by
// net/http and auth middleware use the same JSON envelope as handler errors.
// It also lets panic recovery discard a partially written response.
func stableErrorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		buffer := &bufferedResponse{header: make(http.Header), status: http.StatusOK}
		next.ServeHTTP(buffer, request)

		if buffer.status >= 400 && !strings.HasPrefix(buffer.header.Get("Content-Type"), "application/json") {
			switch buffer.status {
			case http.StatusUnauthorized:
				buffer.replaceError("UNAUTHORIZED", "authentication is required")
			case http.StatusForbidden:
				buffer.replaceError("FORBIDDEN", "access is forbidden")
			case http.StatusNotFound:
				buffer.replaceError("NOT_FOUND", "resource not found")
			case http.StatusMethodNotAllowed:
				buffer.replaceError("METHOD_NOT_ALLOWED", "method not allowed")
			}
		}
		copyHeader(response.Header(), buffer.header)
		response.WriteHeader(buffer.status)
		_, _ = response.Write(buffer.body.Bytes())
	})
}

type bufferedResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
	wrote  bool
}

func (response *bufferedResponse) Header() http.Header { return response.header }

func (response *bufferedResponse) WriteHeader(status int) {
	if response.wrote {
		return
	}
	response.status = status
	response.wrote = true
}

func (response *bufferedResponse) Write(value []byte) (int, error) {
	if !response.wrote {
		response.WriteHeader(http.StatusOK)
	}
	return response.body.Write(value)
}

func (response *bufferedResponse) replaceError(code, message string) {
	response.body.Reset()
	response.header.Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(&response.body).Encode(errorEnvelope{Code: code, Message: message})
}

func copyHeader(destination, source http.Header) {
	for key, values := range source {
		destination.Del(key)
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func decodeJSON(response http.ResponseWriter, request *http.Request, limit int64, destination any) bool {
	request.Body = http.MaxBytesReader(response, request.Body, limit)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeJSONDecodeError(response, err)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			writeError(response, http.StatusBadRequest, "INVALID_JSON", "request body must contain one JSON value", nil)
		} else {
			writeJSONDecodeError(response, err)
		}
		return false
	}
	return true
}

func writeJSONDecodeError(response http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(response, http.StatusRequestEntityTooLarge, "REQUEST_BODY_TOO_LARGE", "request body is too large", nil)
		return
	}
	writeError(response, http.StatusBadRequest, "INVALID_JSON", "request body contains invalid JSON", nil)
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, code, message string, details any) {
	writeJSON(response, status, errorEnvelope{Code: code, Message: message, Details: details})
}
