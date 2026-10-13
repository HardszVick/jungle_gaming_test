package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/junglegaming/backend-challenge-go/internal/observability"
)

func TestMetricsAreOnlyOnInternalHandler(t *testing.T) {
	metrics := observability.NewMetrics()
	internal, err := NewInternalHandler(metrics.Registry())
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	internal.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "wager_") {
		t.Fatalf("metrics response = %d %s", response.Code, response.Body)
	}

	public := testHandler(t, &fakeService{}, Options{}, nil, nil)
	publicResponse := request(public, http.MethodGet, "/metrics", "", "", nil)
	if publicResponse.Code != http.StatusNotFound {
		t.Fatalf("public metrics status = %d", publicResponse.Code)
	}
}
