package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLivenessDoesNotCallDependencies(t *testing.T) {
	called := false
	check := healthFunc(func(context.Context) error {
		called = true
		return errors.New("down")
	})
	handler := testHandler(t, &fakeService{}, Options{}, check, check)
	response := request(handler, http.MethodGet, "/health/live", "", "", nil)
	if response.Code != http.StatusOK || called {
		t.Fatalf("liveness status/called = %d/%v", response.Code, called)
	}
}

func TestReadinessRunsChecksConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	check := func(name string) HealthChecker {
		return healthFunc(func(ctx context.Context) error {
			started <- name
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	handler := testHandler(t, &fakeService{}, Options{ReadinessTimeout: time.Second}, check("postgresql"), check("sqs"))
	responseChannel := make(chan int, 1)
	go func() {
		responseChannel <- request(handler, http.MethodGet, "/health/ready", "", "", nil).Code
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("readiness checks did not start concurrently")
		}
	}
	close(release)
	if status := <-responseChannel; status != http.StatusOK {
		t.Fatalf("readiness status = %d", status)
	}
}

func TestReadinessReportsEachDegradedDependencyAndTimeout(t *testing.T) {
	tests := []struct {
		name      string
		postgres  HealthChecker
		sqs       HealthChecker
		component string
	}{
		{name: "postgresql", postgres: healthFunc(func(context.Context) error { return errors.New("database URL secret") }), sqs: healthFunc(func(context.Context) error { return nil }), component: "postgresql"},
		{name: "sqs", postgres: healthFunc(func(context.Context) error { return nil }), sqs: healthFunc(func(context.Context) error { return errors.New("AWS secret") }), component: "sqs"},
		{name: "timeout", postgres: healthFunc(func(context.Context) error { select {} }), sqs: healthFunc(func(context.Context) error { return nil }), component: "postgresql"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := testHandler(t, &fakeService{}, Options{ReadinessTimeout: 10 * time.Millisecond}, test.postgres, test.sqs)
			response := request(handler, http.MethodGet, "/health/ready", "", "", nil)
			if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" {
				t.Fatalf("readiness response = %d %s", response.Code, response.Body)
			}
			body := response.Body.String()
			if !strings.Contains(body, `"`+test.component+`":"down"`) || strings.Contains(body, "secret") {
				t.Fatalf("readiness body = %s", body)
			}
		})
	}
}
