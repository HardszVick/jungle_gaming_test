package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/config"
	"github.com/junglegaming/backend-challenge-go/internal/httpapi"
	"github.com/junglegaming/backend-challenge-go/internal/messaging"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/platform"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const (
	consumerName = "wager-transactions-v1"
)

type instanceID string

func newInstanceID() instanceID {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return instanceID(fmt.Sprintf("%s-%d", hostname, os.Getpid()))
}

func ConfigModule(value config.Config) fx.Option {
	return fx.Module("config", fx.Supply(value))
}

var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func() *slog.Logger { return observability.NewJSONLogger(os.Stdout, slog.LevelInfo) },
		observability.NewMetrics,
	),
)

var PlatformModule = fx.Module("platform",
	fx.Provide(
		platform.NewClock,
		platform.NewUUIDv7Generator,
		newInstanceID,
	),
)

var PersistenceModule = fx.Module("persistence", fx.Provide(newDatabase))

var ApplicationModule = fx.Module("application", fx.Provide(newApplicationService))

var AuthModule = fx.Module("auth", fx.Provide(newAuthenticator))

var brokerModule = fx.Module("sqs", fx.Provide(newSQSClient))

var HTTPModule = fx.Module("http",
	fx.Provide(
		newHTTPHandlers,
		fx.Annotate(newHTTPComponent, fx.ResultTags(`group:"runtime-components"`)),
	),
)

func MessagingModule(components config.Components) fx.Option {
	options := []fx.Option{}
	if components.Enabled(config.ComponentSQSConsumer) {
		options = append(options, fx.Provide(
			fx.Annotate(newConsumerComponent, fx.ResultTags(`group:"runtime-components"`)),
		))
	}
	if components.Enabled(config.ComponentOutbox) {
		options = append(options, fx.Provide(
			messaging.NewPostgresOutbox,
			fx.Annotate(newOutboxComponent, fx.ResultTags(`group:"runtime-components"`)),
		))
	}
	if components.Enabled(config.ComponentPendingReference) {
		options = append(options, fx.Provide(
			fx.Annotate(newPendingComponent, fx.ResultTags(`group:"runtime-components"`)),
		))
	}
	return fx.Module("messaging", options...)
}

// AppOptions builds only the constructors needed by the selected components.
// In particular, a pending-reference-only process never constructs OIDC or SQS.
func AppOptions(value config.Config) []fx.Option {
	options := []fx.Option{
		ConfigModule(value),
		ObservabilityModule,
		PlatformModule,
		PersistenceModule,
		ApplicationModule,
		MessagingModule(value.Components),
		fx.Invoke(registerRuntimeComponents),
		fx.WithLogger(func(logger *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: logger}
		}),
		fx.StartTimeout(value.Shutdown.Timeout),
		fx.StopTimeout(value.Shutdown.Timeout),
	}

	needsSQS := value.Components.Enabled(config.ComponentHTTP) ||
		value.Components.Enabled(config.ComponentSQSConsumer) ||
		value.Components.Enabled(config.ComponentOutbox)
	if needsSQS {
		options = append(options, brokerModule)
	}
	if value.Components.Enabled(config.ComponentHTTP) {
		options = append(options, AuthModule, HTTPModule)
	}
	return options
}

func NewApp(value config.Config, extra ...fx.Option) *fx.App {
	options := AppOptions(value)
	options = append(options, extra...)
	return fx.New(options...)
}

func newDatabase(lifecycle fx.Lifecycle, value config.Config) (*postgres.Database, error) {
	database, err := postgres.Open(context.Background(), value.Database.URL.Value())
	if err != nil {
		return nil, err
	}
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := database.Health(ctx); err != nil {
				return fmt.Errorf("bootstrap: PostgreSQL startup check: %w", err)
			}
			return verifySchema(ctx, database)
		},
		OnStop: func(context.Context) error {
			database.Close()
			return nil
		},
	})
	return database, nil
}

func verifySchema(ctx context.Context, database *postgres.Database) error {
	catalog, err := DefaultMigrationCatalog()
	if err != nil {
		return fmt.Errorf("bootstrap: load migration catalog: %w", err)
	}
	return verifySchemaCatalog(ctx, database, catalog)
}

func VerifySchemaFromDirectory(ctx context.Context, database *postgres.Database, directory string) error {
	catalog, err := LoadMigrationCatalog(directory)
	if err != nil {
		return fmt.Errorf("bootstrap: load migration catalog: %w", err)
	}
	return verifySchemaCatalog(ctx, database, catalog)
}

func verifySchemaCatalog(ctx context.Context, database *postgres.Database, catalog MigrationCatalog) error {
	var versionTableExists bool
	if err := database.Pool().QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&versionTableExists); err != nil {
		return fmt.Errorf("bootstrap: verify database schema version table: %w", err)
	}
	if !versionTableExists {
		return fmt.Errorf("bootstrap: database schema is absent; expected migration version %d", catalog.LatestVersion())
	}
	type appliedMigration struct {
		version int64
		name    string
	}
	rows, err := database.Pool().Query(ctx, `SELECT version, name FROM schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("bootstrap: read database schema migrations: %w", err)
	}
	applied := make([]appliedMigration, 0)
	for rows.Next() {
		var item appliedMigration
		if err := rows.Scan(&item.version, &item.name); err != nil {
			rows.Close()
			return fmt.Errorf("bootstrap: read database schema migrations: %w", err)
		}
		applied = append(applied, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("bootstrap: read database schema migrations: %w", err)
	}
	rows.Close()

	appliedVersion := int64(0)
	if len(applied) > 0 {
		appliedVersion = applied[len(applied)-1].version
	}
	if appliedVersion < catalog.LatestVersion() {
		return fmt.Errorf("bootstrap: database schema is behind: applied version %d, expected %d", appliedVersion, catalog.LatestVersion())
	}
	if appliedVersion > catalog.LatestVersion() {
		return fmt.Errorf("bootstrap: database schema is ahead: applied version %d, expected %d", appliedVersion, catalog.LatestVersion())
	}
	expected := catalog.Migrations()
	if len(applied) != len(expected) {
		return fmt.Errorf("bootstrap: database schema migration history does not match catalog: applied %d migrations, expected %d", len(applied), len(expected))
	}
	for index, item := range expected {
		expectedName := filepath.Base(item.UpPath)
		if applied[index].version != item.Version || applied[index].name != expectedName {
			return fmt.Errorf(
				"bootstrap: database schema migration history does not match catalog at position %d: applied (%d, %q), expected (%d, %q)",
				index+1, applied[index].version, applied[index].name, item.Version, expectedName,
			)
		}
	}

	var ready bool
	err = database.Pool().QueryRow(ctx, `
		SELECT to_regclass('wallets') IS NOT NULL
		   AND to_regclass('wager_transactions') IS NOT NULL
		   AND to_regclass('wallet_ledger_entries') IS NOT NULL
		   AND to_regclass('inbox_messages') IS NOT NULL
		   AND to_regclass('outbox_events') IS NOT NULL`).Scan(&ready)
	if err != nil {
		return fmt.Errorf("bootstrap: verify database schema: %w", err)
	}
	if !ready {
		return fmt.Errorf("bootstrap: database schema version %d is missing essential objects", appliedVersion)
	}
	return nil
}

func newApplicationService(database *postgres.Database, clock platform.Clock, ids *platform.UUIDv7Generator, metrics *observability.Metrics, value config.Config) (*application.Service, error) {
	return application.NewService(database, clock, ids, application.ServiceOptions{
		Retry: application.RetryPolicy{
			MaxAttempts: value.Retry.MaxAttempts,
			BaseDelay:   value.Retry.BaseDelay,
			MaxDelay:    value.Retry.MaxDelay,
		},
		Reference: application.ReferencePolicy{
			MaxAttempts: value.Reference.MaxAttempts,
			BaseDelay:   value.Reference.BaseDelay,
			MaxDelay:    value.Reference.MaxDelay,
			TTL:         value.Reference.TTL,
		},
		TransactionRetries: metrics,
	})
}

func newAuthenticator(value config.Config) (*auth.Authenticator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return auth.New(ctx, value.OIDC.IssuerURL, value.OIDC.Audience)
}

func newSQSClient(value config.Config) (*messaging.SQSClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return messaging.NewSQSClient(ctx, value.AWS, value.Queues)
}

type httpHandlers struct {
	fx.Out

	Public   http.Handler `name:"public-http"`
	Internal http.Handler `name:"internal-http"`
}

func newHTTPHandlers(service *application.Service, authenticator *auth.Authenticator, database *postgres.Database, sqs *messaging.SQSClient, metrics *observability.Metrics, logger *slog.Logger) (httpHandlers, error) {
	public, err := httpapi.NewHandler(service, authenticator, httpapi.ReadinessChecks{
		PostgreSQL: database,
		SQS:        sqs,
	}, httpapi.Options{
		MaxBodyBytes:     httpapi.DefaultMaxBodyBytes,
		ReadinessTimeout: httpapi.DefaultReadinessTimeout,
		RetryAfter:       httpapi.DefaultRetryAfter,
		Metrics:          metrics,
		Logger:           logger,
	})
	if err != nil {
		return httpHandlers{}, err
	}
	internal, err := httpapi.NewInternalHandler(metrics.Registry())
	if err != nil {
		return httpHandlers{}, err
	}
	return httpHandlers{Public: public, Internal: internal}, nil
}

type httpComponentParams struct {
	fx.In

	Config     config.Config
	Logger     *slog.Logger
	Shutdowner fx.Shutdowner
	Public     http.Handler `name:"public-http"`
	Internal   http.Handler `name:"internal-http"`
}

func newHTTPComponent(params httpComponentParams) (runtimeComponent, error) {
	return newHTTPServers(params.Config.HTTP, params.Public, params.Internal, params.Logger, func() {
		if err := params.Shutdowner.Shutdown(fx.ExitCode(1)); err != nil {
			params.Logger.Error("failed to request Fx shutdown", slog.String("error", err.Error()))
		}
	})
}

func newConsumerComponent(client *messaging.SQSClient, service *application.Service, metrics *observability.Metrics, logger *slog.Logger, value config.Config) (runtimeComponent, error) {
	worker := value.Workers.SQSConsumer
	consumer, err := messaging.NewConsumer(client, service, metrics, logger, messaging.ConsumerOptions{
		QueueURL:        client.Queues().Transactions,
		ConsumerName:    consumerName,
		BatchSize:       worker.BatchSize,
		WaitTime:        worker.PollInterval,
		Visibility:      worker.LeaseDuration,
		ErrorDelay:      value.Retry.BaseDelay,
		BackoffBase:     value.Retry.BaseDelay,
		BackoffMaximum:  value.Retry.MaxDelay,
		MaxReceiveCount: value.Queues.RedriveMaxReceiveCount,
	})
	if err != nil {
		return nil, err
	}
	return namedRuntimeComponent{name: string(config.ComponentSQSConsumer), target: consumer}, nil
}

func newOutboxComponent(repository *messaging.PostgresOutbox, client *messaging.SQSClient, clock platform.Clock, metrics *observability.Metrics, logger *slog.Logger, id instanceID, value config.Config) (runtimeComponent, error) {
	worker := value.Workers.Outbox
	publisher, err := messaging.NewOutboxPublisher(repository, client, clock, metrics, logger, messaging.OutboxOptions{
		PublisherID:    string(id) + "/outbox",
		QueueURL:       client.Queues().Events,
		BatchSize:      worker.BatchSize,
		PollInterval:   worker.PollInterval,
		Lease:          worker.LeaseDuration,
		BackoffBase:    value.Retry.BaseDelay,
		BackoffMaximum: value.Retry.MaxDelay,
	})
	if err != nil {
		return nil, err
	}
	return namedRuntimeComponent{name: string(config.ComponentOutbox), target: publisher}, nil
}

func newPendingComponent(service *application.Service, metrics *observability.Metrics, logger *slog.Logger, id instanceID, value config.Config) (runtimeComponent, error) {
	worker := value.Workers.PendingReference
	pending, err := messaging.NewPendingWorker(service, metrics, logger, messaging.PendingWorkerOptions{
		WorkerID:     string(id) + "/pending-reference",
		BatchSize:    worker.BatchSize,
		PollInterval: worker.PollInterval,
		Lease:        worker.LeaseDuration,
	})
	if err != nil {
		return nil, err
	}
	return namedRuntimeComponent{name: string(config.ComponentPendingReference), target: pending}, nil
}
