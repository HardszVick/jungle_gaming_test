package httpapi

import (
	"context"
	"net/http"
	"time"
)

type healthResponse struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components,omitempty"`
}

func (server *api) live(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, healthResponse{Status: "live"})
}

func (server *api) ready(response http.ResponseWriter, request *http.Request) {
	type result struct {
		name string
		err  error
	}
	results := make(chan result, 2)
	checks := []struct {
		name    string
		checker HealthChecker
	}{
		{name: "postgresql", checker: server.checks.PostgreSQL},
		{name: "sqs", checker: server.checks.SQS},
	}
	for _, check := range checks {
		go func() {
			results <- result{name: check.name, err: runHealthCheck(request.Context(), check.checker, server.options.ReadinessTimeout)}
		}()
	}

	body := healthResponse{Status: "ready", Components: map[string]string{}}
	for range checks {
		result := <-results
		if result.err != nil {
			body.Status = "not_ready"
			body.Components[result.name] = "down"
		} else {
			body.Components[result.name] = "up"
		}
	}
	if body.Status != "ready" {
		response.Header().Set("Retry-After", retryAfterValue(server.options.RetryAfter))
		writeJSON(response, http.StatusServiceUnavailable, body)
		return
	}
	writeJSON(response, http.StatusOK, body)
}

func runHealthCheck(parent context.Context, checker HealthChecker, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- checker.Health(ctx) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
