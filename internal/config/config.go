package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const redacted = "[REDACTED]"

// Secret keeps credentials usable without exposing them through common formatting
// and serialization paths.
type Secret struct {
	value string
}

func newSecret(value string) Secret {
	return Secret{value: value}
}

func (s Secret) Value() string {
	return s.value
}

func (Secret) String() string {
	return redacted
}

func (Secret) GoString() string {
	return redacted
}

func (Secret) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, redacted)
}

func (Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(redacted)
}

type HTTPConfig struct {
	Address        string
	MetricsAddress string
}

type DatabaseConfig struct {
	URL Secret
}

type OIDCConfig struct {
	IssuerURL string
	Audience  string
}

type AWSConfig struct {
	Region                string
	EndpointURL           string
	Profile               string
	SharedCredentialsFile string
}

type QueueConfig struct {
	Transactions           string
	DeadLetter             string
	Events                 string
	RedriveMaxReceiveCount int
}

type WorkerConfig struct {
	BatchSize     int
	PollInterval  time.Duration
	LeaseDuration time.Duration
}

type WorkersConfig struct {
	SQSConsumer      WorkerConfig
	Outbox           WorkerConfig
	PendingReference WorkerConfig
}

type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type ReferenceConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	TTL         time.Duration
}

type ShutdownConfig struct {
	Timeout time.Duration
}

type Component string

const (
	ComponentHTTP             Component = "http"
	ComponentSQSConsumer      Component = "sqs-consumer"
	ComponentOutbox           Component = "outbox"
	ComponentPendingReference Component = "pending-reference"
)

type Components []Component

func (components Components) Enabled(wanted Component) bool {
	for _, component := range components {
		if component == wanted {
			return true
		}
	}
	return false
}

type Config struct {
	HTTP       HTTPConfig
	Database   DatabaseConfig
	OIDC       OIDCConfig
	AWS        AWSConfig
	Queues     QueueConfig
	Workers    WorkersConfig
	Retry      RetryConfig
	Reference  ReferenceConfig
	Shutdown   ShutdownConfig
	Components Components
}

type LookupEnv func(string) (string, bool)

// Load reads and validates all service configuration before any component starts.
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom permits deterministic configuration tests without changing process state.
func LoadFrom(lookup LookupEnv) (Config, error) {
	if lookup == nil {
		return Config{}, errors.New("config: environment lookup is nil")
	}

	get := func(name, fallback string) string {
		if value, ok := lookup(name); ok {
			return strings.TrimSpace(value)
		}
		return fallback
	}
	required := func(name string) (string, error) {
		value := get(name, "")
		if value == "" {
			return "", fmt.Errorf("config: %s is required", name)
		}
		return value, nil
	}
	duration := func(name, fallback string) (time.Duration, error) {
		value := get(name, fallback)
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("config: %s must be a valid duration", name)
		}
		if parsed <= 0 {
			return 0, fmt.Errorf("config: %s must be positive", name)
		}
		return parsed, nil
	}
	positiveInt := func(name, fallback string) (int, error) {
		value := get(name, fallback)
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("config: %s must be an integer", name)
		}
		if parsed <= 0 {
			return 0, fmt.Errorf("config: %s must be positive", name)
		}
		return parsed, nil
	}

	databaseURL, err := required("DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	issuerURL, err := required("OIDC_ISSUER_URL")
	if err != nil {
		return Config{}, err
	}
	audience, err := required("OIDC_AUDIENCE")
	if err != nil {
		return Config{}, err
	}
	components, err := parseComponents(get("COMPONENTS", "http,sqs-consumer,outbox,pending-reference"))
	if err != nil {
		return Config{}, err
	}

	config := Config{
		HTTP: HTTPConfig{
			Address:        get("HTTP_ADDR", ":8080"),
			MetricsAddress: get("INTERNAL_HTTP_ADDR", ":9090"),
		},
		Database: DatabaseConfig{URL: newSecret(databaseURL)},
		OIDC: OIDCConfig{
			IssuerURL: issuerURL,
			Audience:  audience,
		},
		AWS: AWSConfig{
			Region:                get("AWS_REGION", "us-east-1"),
			EndpointURL:           get("AWS_ENDPOINT_URL", "http://localstack:4566"),
			Profile:               get("AWS_PROFILE", ""),
			SharedCredentialsFile: get("AWS_SHARED_CREDENTIALS_FILE", ""),
		},
		Queues: QueueConfig{
			Transactions: get("SQS_TRANSACTIONS_QUEUE", "wager-transactions.fifo"),
			DeadLetter:   get("SQS_TRANSACTIONS_DLQ", "wager-transactions-dlq.fifo"),
			Events:       get("SQS_EVENTS_QUEUE", "wager-events.fifo"),
		},
		Components: components,
	}
	config.Queues.RedriveMaxReceiveCount, err = positiveInt("SQS_REDRIVE_MAX_RECEIVE_COUNT", "5")
	if err != nil {
		return Config{}, err
	}

	config.Workers.SQSConsumer, err = loadWorker(positiveInt, duration, "SQS_CONSUMER", "10", "20s", "60s")
	if err != nil {
		return Config{}, err
	}
	config.Workers.Outbox, err = loadWorker(positiveInt, duration, "OUTBOX", "100", "500ms", "30s")
	if err != nil {
		return Config{}, err
	}
	config.Workers.PendingReference, err = loadWorker(positiveInt, duration, "PENDING_REFERENCE", "100", "1s", "30s")
	if err != nil {
		return Config{}, err
	}

	config.Retry.MaxAttempts, err = positiveInt("TRANSACTION_RETRY_MAX_ATTEMPTS", "3")
	if err != nil {
		return Config{}, err
	}
	config.Retry.BaseDelay, err = duration("TRANSACTION_RETRY_BASE_DELAY", "50ms")
	if err != nil {
		return Config{}, err
	}
	config.Retry.MaxDelay, err = duration("TRANSACTION_RETRY_MAX_DELAY", "1s")
	if err != nil {
		return Config{}, err
	}
	config.Reference.MaxAttempts, err = positiveInt("REFERENCE_MAX_ATTEMPTS", "12")
	if err != nil {
		return Config{}, err
	}
	config.Reference.BaseDelay, err = duration("REFERENCE_RETRY_BASE_DELAY", "1s")
	if err != nil {
		return Config{}, err
	}
	config.Reference.MaxDelay, err = duration("REFERENCE_RETRY_MAX_DELAY", "5m")
	if err != nil {
		return Config{}, err
	}
	config.Reference.TTL, err = duration("REFERENCE_TTL", "24h")
	if err != nil {
		return Config{}, err
	}
	config.Shutdown.Timeout, err = duration("SHUTDOWN_TIMEOUT", "30s")
	if err != nil {
		return Config{}, err
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

type intParser func(string, string) (int, error)
type durationParser func(string, string) (time.Duration, error)

func loadWorker(positiveInt intParser, duration durationParser, prefix, batchDefault, pollDefault, leaseDefault string) (WorkerConfig, error) {
	batchSize, err := positiveInt(prefix+"_BATCH_SIZE", batchDefault)
	if err != nil {
		return WorkerConfig{}, err
	}
	pollInterval, err := duration(prefix+"_POLL_INTERVAL", pollDefault)
	if err != nil {
		return WorkerConfig{}, err
	}
	leaseDuration, err := duration(prefix+"_LEASE_DURATION", leaseDefault)
	if err != nil {
		return WorkerConfig{}, err
	}
	return WorkerConfig{BatchSize: batchSize, PollInterval: pollInterval, LeaseDuration: leaseDuration}, nil
}

func parseComponents(value string) (Components, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("config: COMPONENTS must contain at least one component")
	}

	allowed := map[Component]struct{}{
		ComponentHTTP:             {},
		ComponentSQSConsumer:      {},
		ComponentOutbox:           {},
		ComponentPendingReference: {},
	}
	seen := make(map[Component]struct{}, len(allowed))
	parts := strings.Split(value, ",")
	components := make(Components, 0, len(parts))
	for _, part := range parts {
		component := Component(strings.TrimSpace(part))
		if _, ok := allowed[component]; !ok {
			return nil, fmt.Errorf("config: COMPONENTS contains unknown component %q", component)
		}
		if _, duplicate := seen[component]; duplicate {
			return nil, fmt.Errorf("config: COMPONENTS contains duplicate component %q", component)
		}
		seen[component] = struct{}{}
		components = append(components, component)
	}
	return components, nil
}

func (config Config) Validate() error {
	if err := validateAddress("HTTP_ADDR", config.HTTP.Address); err != nil {
		return err
	}
	if err := validateAddress("INTERNAL_HTTP_ADDR", config.HTTP.MetricsAddress); err != nil {
		return err
	}
	if config.Database.URL.Value() == "" {
		return errors.New("config: DATABASE_URL is required")
	}
	if err := validateURL("DATABASE_URL", config.Database.URL.Value(), "postgres", "postgresql"); err != nil {
		return err
	}
	if config.OIDC.IssuerURL == "" {
		return errors.New("config: OIDC_ISSUER_URL is required")
	}
	if err := validateURL("OIDC_ISSUER_URL", config.OIDC.IssuerURL, "http", "https"); err != nil {
		return err
	}
	if config.OIDC.Audience == "" {
		return errors.New("config: OIDC_AUDIENCE is required")
	}
	if config.AWS.Region == "" {
		return errors.New("config: AWS_REGION is required")
	}
	if config.AWS.EndpointURL != "" {
		if err := validateURL("AWS_ENDPOINT_URL", config.AWS.EndpointURL, "http", "https"); err != nil {
			return err
		}
	}
	if config.Queues.Transactions == "" {
		return errors.New("config: SQS_TRANSACTIONS_QUEUE is required")
	}
	if config.Queues.DeadLetter == "" {
		return errors.New("config: SQS_TRANSACTIONS_DLQ is required")
	}
	if config.Queues.Events == "" {
		return errors.New("config: SQS_EVENTS_QUEUE is required")
	}
	if config.Queues.RedriveMaxReceiveCount <= 0 {
		return errors.New("config: SQS_REDRIVE_MAX_RECEIVE_COUNT must be positive")
	}
	workers := []struct {
		name   string
		config WorkerConfig
	}{
		{"SQS_CONSUMER", config.Workers.SQSConsumer},
		{"OUTBOX", config.Workers.Outbox},
		{"PENDING_REFERENCE", config.Workers.PendingReference},
	}
	for _, worker := range workers {
		if worker.config.BatchSize <= 0 {
			return fmt.Errorf("config: %s_BATCH_SIZE must be positive", worker.name)
		}
		if worker.config.PollInterval <= 0 {
			return fmt.Errorf("config: %s_POLL_INTERVAL must be positive", worker.name)
		}
		if worker.config.LeaseDuration <= 0 {
			return fmt.Errorf("config: %s_LEASE_DURATION must be positive", worker.name)
		}
	}
	if config.Workers.SQSConsumer.BatchSize > 10 {
		return errors.New("config: SQS_CONSUMER_BATCH_SIZE must not exceed 10")
	}
	if config.Retry.MaxAttempts <= 0 {
		return errors.New("config: TRANSACTION_RETRY_MAX_ATTEMPTS must be positive")
	}
	if config.Retry.BaseDelay <= 0 || config.Retry.MaxDelay <= 0 {
		return errors.New("config: transaction retry delays must be positive")
	}
	if config.Retry.BaseDelay > config.Retry.MaxDelay {
		return errors.New("config: TRANSACTION_RETRY_BASE_DELAY must not exceed TRANSACTION_RETRY_MAX_DELAY")
	}
	if config.Reference.MaxAttempts <= 0 {
		return errors.New("config: REFERENCE_MAX_ATTEMPTS must be positive")
	}
	if config.Reference.BaseDelay <= 0 || config.Reference.MaxDelay <= 0 || config.Reference.TTL <= 0 {
		return errors.New("config: reference retry delays and TTL must be positive")
	}
	if config.Reference.BaseDelay > config.Reference.MaxDelay {
		return errors.New("config: REFERENCE_RETRY_BASE_DELAY must not exceed REFERENCE_RETRY_MAX_DELAY")
	}
	if config.Reference.MaxDelay > config.Reference.TTL {
		return errors.New("config: REFERENCE_RETRY_MAX_DELAY must not exceed REFERENCE_TTL")
	}
	if config.Shutdown.Timeout <= 0 {
		return errors.New("config: SHUTDOWN_TIMEOUT must be positive")
	}
	if len(config.Components) == 0 {
		return errors.New("config: COMPONENTS must contain at least one component")
	}
	seenComponents := make(map[Component]struct{}, len(config.Components))
	for _, component := range config.Components {
		switch component {
		case ComponentHTTP, ComponentSQSConsumer, ComponentOutbox, ComponentPendingReference:
		default:
			return fmt.Errorf("config: COMPONENTS contains unknown component %q", component)
		}
		if _, duplicate := seenComponents[component]; duplicate {
			return fmt.Errorf("config: COMPONENTS contains duplicate component %q", component)
		}
		seenComponents[component] = struct{}{}
	}
	return nil
}

func validateAddress(name, value string) error {
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("config: %s must be a host:port address", name)
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return fmt.Errorf("config: %s must contain a port from 1 to 65535", name)
	}
	return nil
}

func validateURL(name, value string, allowedSchemes ...string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("config: %s must be an absolute URL", name)
	}
	for _, scheme := range allowedSchemes {
		if parsed.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("config: %s uses an unsupported URL scheme", name)
}
