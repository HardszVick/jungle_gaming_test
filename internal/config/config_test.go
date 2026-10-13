package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLoadFromDefaults(t *testing.T) {
	config, err := LoadFrom(testEnvironment(nil))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	if config.HTTP.Address != ":8080" || config.HTTP.MetricsAddress != ":9090" {
		t.Fatalf("unexpected HTTP defaults: %+v", config.HTTP)
	}
	if config.Database.URL.Value() != "postgres://app:password@postgres:5432/wager?sslmode=disable" {
		t.Fatal("DATABASE_URL was not loaded")
	}
	if config.AWS != (AWSConfig{Region: "us-east-1", EndpointURL: "http://localstack:4566"}) {
		t.Fatalf("unexpected AWS defaults: %+v", config.AWS)
	}
	if config.Queues != (QueueConfig{
		Transactions:           "wager-transactions.fifo",
		DeadLetter:             "wager-transactions-dlq.fifo",
		Events:                 "wager-events.fifo",
		RedriveMaxReceiveCount: 5,
	}) {
		t.Fatalf("unexpected queue defaults: %+v", config.Queues)
	}
	if config.Workers.SQSConsumer != (WorkerConfig{10, 20 * time.Second, time.Minute}) {
		t.Fatalf("unexpected consumer defaults: %+v", config.Workers.SQSConsumer)
	}
	if config.Workers.Outbox != (WorkerConfig{100, 500 * time.Millisecond, 30 * time.Second}) {
		t.Fatalf("unexpected outbox defaults: %+v", config.Workers.Outbox)
	}
	if config.Workers.PendingReference != (WorkerConfig{100, time.Second, 30 * time.Second}) {
		t.Fatalf("unexpected reference worker defaults: %+v", config.Workers.PendingReference)
	}
	if config.Retry != (RetryConfig{3, 50 * time.Millisecond, time.Second}) {
		t.Fatalf("unexpected retry defaults: %+v", config.Retry)
	}
	if config.Reference != (ReferenceConfig{12, time.Second, 5 * time.Minute, 24 * time.Hour}) {
		t.Fatalf("unexpected reference defaults: %+v", config.Reference)
	}
	if config.Shutdown.Timeout != 30*time.Second {
		t.Fatalf("unexpected shutdown timeout: %v", config.Shutdown.Timeout)
	}
	for _, component := range []Component{ComponentHTTP, ComponentSQSConsumer, ComponentOutbox, ComponentPendingReference} {
		if !config.Components.Enabled(component) {
			t.Errorf("default components do not enable %q", component)
		}
	}
}

func TestLoadFromOverridesDefaults(t *testing.T) {
	config, err := LoadFrom(testEnvironment(map[string]string{
		"HTTP_ADDR":                        "127.0.0.1:18080",
		"INTERNAL_HTTP_ADDR":               "127.0.0.1:19090",
		"AWS_REGION":                       "sa-east-1",
		"AWS_ENDPOINT_URL":                 "https://sqs.example.test",
		"AWS_PROFILE":                      "runtime",
		"AWS_SHARED_CREDENTIALS_FILE":      "/run/secrets/aws-credentials",
		"SQS_TRANSACTIONS_QUEUE":           "input.fifo",
		"SQS_TRANSACTIONS_DLQ":             "input-dlq.fifo",
		"SQS_EVENTS_QUEUE":                 "output.fifo",
		"SQS_REDRIVE_MAX_RECEIVE_COUNT":    "2",
		"SQS_CONSUMER_BATCH_SIZE":          "5",
		"SQS_CONSUMER_POLL_INTERVAL":       "10s",
		"SQS_CONSUMER_LEASE_DURATION":      "45s",
		"OUTBOX_BATCH_SIZE":                "25",
		"OUTBOX_POLL_INTERVAL":             "2s",
		"OUTBOX_LEASE_DURATION":            "15s",
		"PENDING_REFERENCE_BATCH_SIZE":     "30",
		"PENDING_REFERENCE_POLL_INTERVAL":  "3s",
		"PENDING_REFERENCE_LEASE_DURATION": "20s",
		"TRANSACTION_RETRY_MAX_ATTEMPTS":   "4",
		"TRANSACTION_RETRY_BASE_DELAY":     "100ms",
		"TRANSACTION_RETRY_MAX_DELAY":      "2s",
		"REFERENCE_MAX_ATTEMPTS":           "8",
		"REFERENCE_RETRY_BASE_DELAY":       "2s",
		"REFERENCE_RETRY_MAX_DELAY":        "1m",
		"REFERENCE_TTL":                    "12h",
		"SHUTDOWN_TIMEOUT":                 "45s",
	}))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	if config.HTTP != (HTTPConfig{"127.0.0.1:18080", "127.0.0.1:19090"}) {
		t.Fatalf("HTTP config = %+v", config.HTTP)
	}
	if config.AWS != (AWSConfig{Region: "sa-east-1", EndpointURL: "https://sqs.example.test", Profile: "runtime", SharedCredentialsFile: "/run/secrets/aws-credentials"}) {
		t.Fatalf("AWS config = %+v", config.AWS)
	}
	if config.Queues != (QueueConfig{"input.fifo", "input-dlq.fifo", "output.fifo", 2}) {
		t.Fatalf("queue config = %+v", config.Queues)
	}
	if config.Workers.SQSConsumer != (WorkerConfig{5, 10 * time.Second, 45 * time.Second}) ||
		config.Workers.Outbox != (WorkerConfig{25, 2 * time.Second, 15 * time.Second}) ||
		config.Workers.PendingReference != (WorkerConfig{30, 3 * time.Second, 20 * time.Second}) {
		t.Fatalf("worker config = %+v", config.Workers)
	}
	if config.Retry != (RetryConfig{4, 100 * time.Millisecond, 2 * time.Second}) {
		t.Fatalf("retry config = %+v", config.Retry)
	}
	if config.Reference != (ReferenceConfig{8, 2 * time.Second, time.Minute, 12 * time.Hour}) {
		t.Fatalf("reference config = %+v", config.Reference)
	}
	if config.Shutdown.Timeout != 45*time.Second {
		t.Fatalf("shutdown timeout = %v", config.Shutdown.Timeout)
	}
}

func TestLoadFromRejectsNilLookup(t *testing.T) {
	if _, err := LoadFrom(nil); err == nil {
		t.Fatal("LoadFrom(nil) error = nil")
	}
}

func TestLoadFromRequiresCriticalConfiguration(t *testing.T) {
	for _, name := range []string{"DATABASE_URL", "OIDC_ISSUER_URL", "OIDC_AUDIENCE"} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadFrom(testEnvironment(map[string]string{name: ""}))
			if err == nil || !strings.Contains(err.Error(), name+" is required") {
				t.Fatalf("LoadFrom() error = %v, want required-field error", err)
			}
		})
	}
}

func TestLoadFromRejectsInvalidDurations(t *testing.T) {
	names := []string{
		"SQS_CONSUMER_POLL_INTERVAL", "SQS_CONSUMER_LEASE_DURATION",
		"OUTBOX_POLL_INTERVAL", "OUTBOX_LEASE_DURATION",
		"PENDING_REFERENCE_POLL_INTERVAL", "PENDING_REFERENCE_LEASE_DURATION",
		"TRANSACTION_RETRY_BASE_DELAY", "TRANSACTION_RETRY_MAX_DELAY",
		"REFERENCE_RETRY_BASE_DELAY", "REFERENCE_RETRY_MAX_DELAY", "REFERENCE_TTL",
		"SHUTDOWN_TIMEOUT",
	}
	for _, name := range names {
		t.Run(name+"/syntax", func(t *testing.T) {
			_, err := LoadFrom(testEnvironment(map[string]string{name: "later"}))
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("LoadFrom() error = %v, want %s error", err, name)
			}
		})
		t.Run(name+"/non-positive", func(t *testing.T) {
			_, err := LoadFrom(testEnvironment(map[string]string{name: "0s"}))
			if err == nil || !strings.Contains(err.Error(), name+" must be positive") {
				t.Fatalf("LoadFrom() error = %v, want positive-duration error", err)
			}
		})
	}
}

func TestLoadFromRejectsInvalidPositiveLimits(t *testing.T) {
	for _, name := range []string{
		"SQS_CONSUMER_BATCH_SIZE", "OUTBOX_BATCH_SIZE", "PENDING_REFERENCE_BATCH_SIZE",
		"TRANSACTION_RETRY_MAX_ATTEMPTS", "REFERENCE_MAX_ATTEMPTS", "SQS_REDRIVE_MAX_RECEIVE_COUNT",
	} {
		for _, value := range []string{"0", "-1", "many"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				_, err := LoadFrom(testEnvironment(map[string]string{name: value}))
				if err == nil || !strings.Contains(err.Error(), name) {
					t.Fatalf("LoadFrom() error = %v, want %s error", err, name)
				}
			})
		}
	}
}

func TestLoadFromRejectsSQSBatchAboveServiceLimit(t *testing.T) {
	_, err := LoadFrom(testEnvironment(map[string]string{"SQS_CONSUMER_BATCH_SIZE": "11"}))
	if err == nil || !strings.Contains(err.Error(), "must not exceed 10") {
		t.Fatalf("LoadFrom() error = %v, want SQS batch limit error", err)
	}
}

func TestLoadFromValidatesDelayRanges(t *testing.T) {
	tests := []map[string]string{
		{"TRANSACTION_RETRY_BASE_DELAY": "2s", "TRANSACTION_RETRY_MAX_DELAY": "1s"},
		{"REFERENCE_RETRY_BASE_DELAY": "2m", "REFERENCE_RETRY_MAX_DELAY": "1m"},
		{"REFERENCE_RETRY_MAX_DELAY": "25h", "REFERENCE_TTL": "24h"},
	}
	for index, values := range tests {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			if _, err := LoadFrom(testEnvironment(values)); err == nil {
				t.Fatal("LoadFrom() error = nil, want invalid delay range")
			}
		})
	}
}

func TestLoadFromSelectsComponents(t *testing.T) {
	config, err := LoadFrom(testEnvironment(map[string]string{"COMPONENTS": "http, outbox"}))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}
	if len(config.Components) != 2 || !config.Components.Enabled(ComponentHTTP) || !config.Components.Enabled(ComponentOutbox) {
		t.Fatalf("components = %v, want HTTP and outbox", config.Components)
	}
	if config.Components.Enabled(ComponentSQSConsumer) || config.Components.Enabled(ComponentPendingReference) {
		t.Fatalf("components unexpectedly enabled: %v", config.Components)
	}
}

func TestLoadFromRejectsInvalidComponents(t *testing.T) {
	for _, value := range []string{"", "http,unknown", "http,http", "http,"} {
		t.Run(value, func(t *testing.T) {
			if _, err := LoadFrom(testEnvironment(map[string]string{"COMPONENTS": value})); err == nil {
				t.Fatalf("LoadFrom() accepted COMPONENTS=%q", value)
			}
		})
	}
}

func TestLoadFromRejectsMalformedNetworkConfiguration(t *testing.T) {
	tests := []map[string]string{
		{"HTTP_ADDR": "8080"},
		{"INTERNAL_HTTP_ADDR": ":0"},
		{"DATABASE_URL": "http://postgres/database"},
		{"OIDC_ISSUER_URL": "keycloak"},
		{"AWS_ENDPOINT_URL": "localstack:4566"},
	}
	for index, values := range tests {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			if _, err := LoadFrom(testEnvironment(values)); err == nil {
				t.Fatalf("LoadFrom() accepted invalid values: %v", values)
			}
		})
	}
}

func TestSecretsAreRedacted(t *testing.T) {
	const (
		database = "postgres://secret-user:secret-password@postgres:5432/wager"
	)
	config, err := LoadFrom(testEnvironment(map[string]string{
		"DATABASE_URL": database,
	}))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	formatted := fmt.Sprintf("%v %+v %#v", config, config, config)
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, secret := range []string{database} {
		if strings.Contains(formatted, secret) || strings.Contains(string(encoded), secret) {
			t.Fatalf("configuration output exposed secret %q", secret)
		}
	}
	if !strings.Contains(formatted, redacted) || !strings.Contains(string(encoded), redacted) {
		t.Fatal("configuration output does not make redaction explicit")
	}
}

func testEnvironment(overrides map[string]string) LookupEnv {
	values := map[string]string{
		"DATABASE_URL":    "postgres://app:password@postgres:5432/wager?sslmode=disable",
		"OIDC_ISSUER_URL": "http://keycloak:8080/realms/wager",
		"OIDC_AUDIENCE":   "wager-api",
	}
	for name, value := range overrides {
		values[name] = value
	}
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
