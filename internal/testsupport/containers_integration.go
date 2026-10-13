//go:build integration

package testsupport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/junglegaming/backend-challenge-go/internal/migrations"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	Region                = "us-east-1"
	ProviderASecret       = "integration-provider-a"
	ProviderBSecret       = "integration-provider-b"
	InternalServiceSecret = "integration-internal"
)

type Environment struct {
	PostgresURL   string
	KeycloakURL   string
	LocalStackURL string

	containers []testcontainers.Container
	postgres   testcontainers.Container
	localStack testcontainers.Container
}

func StartPostgres(ctx context.Context) (*Environment, error) {
	environment := &Environment{}
	container, err := testcontainers.Run(ctx, "postgres:16-alpine",
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     "integration",
			"POSTGRES_PASSWORD": "integration",
			"POSTGRES_DB":       "integration",
		}),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL 16 container: %w", err)
	}
	environment.containers = append(environment.containers, container)
	environment.postgres = container
	host, port, err := endpoint(ctx, container, "5432/tcp")
	if err != nil {
		_ = environment.Close(context.Background())
		return nil, err
	}
	environment.PostgresURL = fmt.Sprintf("postgres://integration:integration@%s:%s/integration?sslmode=disable", host, port)
	return environment, nil
}

func StartLocalStack(ctx context.Context) (*Environment, error) {
	environment := &Environment{}
	container, err := testcontainers.Run(ctx, "localstack/localstack:3.8.1",
		testcontainers.WithEnv(map[string]string{
			"SERVICES":              "sqs",
			"AWS_DEFAULT_REGION":    Region,
			"SQS_ENDPOINT_STRATEGY": "path",
		}),
		testcontainers.WithExposedPorts("4566/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/_localstack/health").WithPort("4566/tcp").WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("start LocalStack container: %w", err)
	}
	environment.containers = append(environment.containers, container)
	environment.localStack = container
	host, port, err := endpoint(ctx, container, "4566/tcp")
	if err != nil {
		_ = environment.Close(context.Background())
		return nil, err
	}
	environment.LocalStackURL = "http://" + host + ":" + port
	return environment, nil
}

func Start(ctx context.Context) (*Environment, error) {
	postgresEnvironment, err := StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	localStackEnvironment, err := StartLocalStack(ctx)
	if err != nil {
		_ = postgresEnvironment.Close(context.Background())
		return nil, err
	}
	environment := &Environment{
		PostgresURL:   postgresEnvironment.PostgresURL,
		LocalStackURL: localStackEnvironment.LocalStackURL,
		containers:    append(postgresEnvironment.containers, localStackEnvironment.containers...),
		postgres:      postgresEnvironment.postgres,
		localStack:    localStackEnvironment.localStack,
	}

	realmFile, err := renderRealm()
	if err != nil {
		_ = environment.Close(context.Background())
		return nil, err
	}
	defer os.Remove(realmFile)
	keycloak, err := testcontainers.Run(ctx, "quay.io/keycloak/keycloak:25.0.6",
		testcontainers.WithEnv(map[string]string{
			"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
			"KC_BOOTSTRAP_ADMIN_PASSWORD": "admin",
			"KC_HEALTH_ENABLED":           "true",
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      realmFile,
			ContainerFilePath: "/opt/keycloak/data/import/wager-realm.json",
			FileMode:          0o644,
		}),
		testcontainers.WithCmd("start-dev", "--import-realm", "--http-port=8080"),
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/realms/wager/.well-known/openid-configuration").WithPort("8080/tcp").WithStartupTimeout(180*time.Second)),
	)
	if err != nil {
		_ = environment.Close(context.Background())
		return nil, fmt.Errorf("start Keycloak container: %w", err)
	}
	environment.containers = append(environment.containers, keycloak)
	host, port, err := endpoint(ctx, keycloak, "8080/tcp")
	if err != nil {
		_ = environment.Close(context.Background())
		return nil, err
	}
	environment.KeycloakURL = "http://" + host + ":" + port
	return environment, nil
}

func (environment *Environment) StopLocalStack(ctx context.Context) error {
	if environment == nil || environment.localStack == nil {
		return errors.New("LocalStack container is not running")
	}
	timeout := 10 * time.Second
	return environment.localStack.Stop(ctx, &timeout)
}

func (environment *Environment) StopPostgres(ctx context.Context) error {
	if environment == nil || environment.postgres == nil {
		return errors.New("PostgreSQL container is not running")
	}
	timeout := 10 * time.Second
	return environment.postgres.Stop(ctx, &timeout)
}

func (environment *Environment) StartPostgresContainer(ctx context.Context) error {
	if environment == nil || environment.postgres == nil {
		return errors.New("PostgreSQL container is not configured")
	}
	if err := environment.postgres.Start(ctx); err != nil {
		return err
	}
	host, port, err := endpoint(ctx, environment.postgres, "5432/tcp")
	if err != nil {
		return err
	}
	environment.PostgresURL = fmt.Sprintf("postgres://integration:integration@%s:%s/integration?sslmode=disable", host, port)
	deadline := time.Now().Add(30 * time.Second)
	for {
		pool, openErr := pgxpool.New(ctx, environment.PostgresURL)
		if openErr == nil {
			pingErr := pool.Ping(ctx)
			pool.Close()
			if pingErr == nil {
				return nil
			}
			err = pingErr
		} else {
			err = openErr
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (environment *Environment) StartLocalStackContainer(ctx context.Context) error {
	if environment == nil || environment.localStack == nil {
		return errors.New("LocalStack container is not configured")
	}
	if err := environment.localStack.Start(ctx); err != nil {
		return err
	}
	host, port, err := endpoint(ctx, environment.localStack, "4566/tcp")
	if err != nil {
		return err
	}
	environment.LocalStackURL = "http://" + host + ":" + port
	deadline := time.Now().Add(30 * time.Second)
	client := environment.SQS()
	for {
		_, err := client.ListQueues(ctx, &awssqs.ListQueuesInput{MaxResults: aws.Int32(1)})
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (environment *Environment) Close(ctx context.Context) error {
	if environment == nil {
		return nil
	}
	var result error
	for index := len(environment.containers) - 1; index >= 0; index-- {
		result = errors.Join(result, testcontainers.TerminateContainer(environment.containers[index]))
	}
	environment.containers = nil
	return result
}

type Database struct {
	URL    string
	Schema string
	admin  *pgxpool.Pool
}

type RoleDatabase struct {
	OwnerURL   string
	RuntimeURL string
	Owner      string
	Runtime    string
	Schema     string
	admin      *pgxpool.Pool
}

func (environment *Environment) NewRoleDatabase(ctx context.Context, prefix string) (*RoleDatabase, error) {
	admin, err := pgxpool.New(ctx, environment.PostgresURL)
	if err != nil {
		return nil, err
	}
	identifier := strings.ReplaceAll(sanitize(prefix), "-", "_") + "_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	owner := identifier + "_owner"
	runtimeRole := identifier + "_runtime"
	ownerPassword := identifier + "_owner_password"
	runtimePassword := identifier + "_runtime_password"
	cleanup := func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{identifier}.Sanitize()+` CASCADE`)
		_, _ = admin.Exec(context.Background(), `REVOKE CONNECT ON DATABASE integration FROM `+pgx.Identifier{owner}.Sanitize()+`, `+pgx.Identifier{runtimeRole}.Sanitize())
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{runtimeRole}.Sanitize())
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{owner}.Sanitize())
		admin.Close()
	}
	for _, role := range []struct {
		name     string
		password string
	}{{owner, ownerPassword}, {runtimeRole, runtimePassword}} {
		var statement string
		if err := admin.QueryRow(ctx, `SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', $1::text, $2::text)`, role.name, role.password).Scan(&statement); err != nil {
			cleanup()
			return nil, err
		}
		if _, err := admin.Exec(ctx, statement); err != nil {
			cleanup()
			return nil, err
		}
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{identifier}.Sanitize()+` AUTHORIZATION `+pgx.Identifier{owner}.Sanitize()); err != nil {
		cleanup()
		return nil, err
	}
	if _, err := admin.Exec(ctx, `GRANT CONNECT ON DATABASE integration TO `+pgx.Identifier{owner}.Sanitize()+`, `+pgx.Identifier{runtimeRole}.Sanitize()); err != nil {
		cleanup()
		return nil, err
	}
	buildURL := func(user, password string, migration bool) (string, error) {
		parsed, err := url.Parse(environment.PostgresURL)
		if err != nil {
			return "", err
		}
		parsed.User = url.UserPassword(user, password)
		query := parsed.Query()
		query.Set("search_path", identifier)
		if migration {
			query.Set("options", "-c app.application_role="+runtimeRole)
		}
		parsed.RawQuery = query.Encode()
		return parsed.String(), nil
	}
	ownerURL, err := buildURL(owner, ownerPassword, true)
	if err != nil {
		cleanup()
		return nil, err
	}
	runtimeURL, err := buildURL(runtimeRole, runtimePassword, false)
	if err != nil {
		cleanup()
		return nil, err
	}
	return &RoleDatabase{
		OwnerURL: ownerURL, RuntimeURL: runtimeURL, Owner: owner,
		Runtime: runtimeRole, Schema: identifier, admin: admin,
	}, nil
}

func (database *RoleDatabase) Close(ctx context.Context) error {
	if database == nil || database.admin == nil {
		return nil
	}
	_, schemaErr := database.admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{database.Schema}.Sanitize()+` CASCADE`)
	_, revokeErr := database.admin.Exec(ctx, `REVOKE CONNECT ON DATABASE integration FROM `+pgx.Identifier{database.Owner}.Sanitize()+`, `+pgx.Identifier{database.Runtime}.Sanitize())
	_, runtimeErr := database.admin.Exec(ctx, `DROP ROLE IF EXISTS `+pgx.Identifier{database.Runtime}.Sanitize())
	_, ownerErr := database.admin.Exec(ctx, `DROP ROLE IF EXISTS `+pgx.Identifier{database.Owner}.Sanitize())
	database.admin.Close()
	database.admin = nil
	return errors.Join(schemaErr, revokeErr, runtimeErr, ownerErr)
}

func (environment *Environment) NewDatabase(ctx context.Context, prefix string) (*Database, error) {
	admin, err := pgxpool.New(ctx, environment.PostgresURL)
	if err != nil {
		return nil, err
	}
	schema := sanitize(prefix) + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		return nil, err
	}
	parsed, err := url.Parse(environment.PostgresURL)
	if err != nil {
		admin.Close()
		return nil, err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return &Database{URL: parsed.String(), Schema: schema, admin: admin}, nil
}

func (database *Database) MigrateUp(ctx context.Context) error {
	return database.executeMigrations(ctx, "up")
}

func (database *Database) MigrateDown(ctx context.Context) error {
	return database.executeMigrations(ctx, "down")
}

func (database *Database) executeMigrations(ctx context.Context, direction string) error {
	directory := filepath.Join(ProjectRoot(), "migrations")
	catalog, err := migrations.LoadCatalog(directory)
	if err != nil {
		return err
	}
	if direction == "up" {
		return migrations.RunFromDirectory(ctx, database.URL, direction, directory)
	}
	for range catalog.Migrations() {
		if err := migrations.RunFromDirectory(ctx, database.URL, direction, directory); err != nil {
			return err
		}
	}
	return nil
}

func (database *Database) Close(ctx context.Context) error {
	if database == nil || database.admin == nil {
		return nil
	}
	_, err := database.admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{database.Schema}.Sanitize()+` CASCADE`)
	database.admin.Close()
	database.admin = nil
	return err
}

type QueueSet struct {
	TransactionsName string
	DeadLetterName   string
	EventsName       string
	TransactionsURL  string
	DeadLetterURL    string
	EventsURL        string
	MaxReceiveCount  int
}

func (environment *Environment) SQS() *awssqs.Client {
	return awssqs.NewFromConfig(aws.Config{
		Region:       Region,
		BaseEndpoint: aws.String(environment.LocalStackURL),
		Credentials: aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test", Source: "integration"}, nil
		})),
	})
}

func (environment *Environment) NewQueues(ctx context.Context, prefix string, visibility time.Duration) (QueueSet, error) {
	return environment.NewQueuesWithRedrive(ctx, prefix, visibility, 5)
}

func (environment *Environment) NewQueuesWithRedrive(ctx context.Context, prefix string, visibility time.Duration, maxReceiveCount int) (QueueSet, error) {
	identifier := sanitize(prefix) + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	queues := QueueSet{
		TransactionsName: identifier + "-transactions.fifo",
		DeadLetterName:   identifier + "-dlq.fifo",
		EventsName:       identifier + "-events.fifo",
	}
	return environment.EnsureQueuesWithRedrive(ctx, queues, visibility, maxReceiveCount)
}

func (environment *Environment) EnsureQueues(ctx context.Context, queues QueueSet, visibility time.Duration) (QueueSet, error) {
	return environment.EnsureQueuesWithRedrive(ctx, queues, visibility, 5)
}

func (environment *Environment) EnsureQueuesWithRedrive(ctx context.Context, queues QueueSet, visibility time.Duration, maxReceiveCount int) (QueueSet, error) {
	if maxReceiveCount < 1 {
		return QueueSet{}, errors.New("max receive count must be positive")
	}
	client := environment.SQS()
	dlq, err := client.CreateQueue(ctx, fifoQueueInput(queues.DeadLetterName, 30*time.Second, ""))
	if err != nil {
		return QueueSet{}, err
	}
	attributes, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       dlq.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return QueueSet{}, err
	}
	redrive, err := json.Marshal(map[string]string{
		"deadLetterTargetArn": attributes.Attributes[string(types.QueueAttributeNameQueueArn)],
		"maxReceiveCount":     strconv.Itoa(maxReceiveCount),
	})
	if err != nil {
		return QueueSet{}, err
	}
	input, err := client.CreateQueue(ctx, fifoQueueInput(queues.TransactionsName, visibility, string(redrive)))
	if err != nil {
		return QueueSet{}, err
	}
	events, err := client.CreateQueue(ctx, fifoQueueInput(queues.EventsName, visibility, ""))
	if err != nil {
		return QueueSet{}, err
	}
	queues.TransactionsURL = aws.ToString(input.QueueUrl)
	queues.DeadLetterURL = aws.ToString(dlq.QueueUrl)
	queues.EventsURL = aws.ToString(events.QueueUrl)
	queues.MaxReceiveCount = maxReceiveCount
	return queues, nil
}

func (environment *Environment) DeleteQueues(ctx context.Context, queues QueueSet) error {
	client := environment.SQS()
	var result error
	for _, queueURL := range []string{queues.TransactionsURL, queues.DeadLetterURL, queues.EventsURL} {
		if queueURL == "" {
			continue
		}
		_, err := client.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
		result = errors.Join(result, err)
	}
	return result
}

func (environment *Environment) RecreateEventsQueue(ctx context.Context, queues QueueSet, visibility time.Duration) (string, error) {
	client := environment.SQS()
	_, _ = client.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(queues.EventsURL)})
	deadline := time.Now().Add(10 * time.Second)
	for {
		created, err := client.CreateQueue(ctx, fifoQueueInput(queues.EventsName, visibility, ""))
		if err == nil {
			return aws.ToString(created.QueueUrl), nil
		}
		if time.Now().After(deadline) {
			return "", err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func ProjectRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func endpoint(ctx context.Context, container testcontainers.Container, port string) (string, string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", "", err
	}
	mapped, err := container.MappedPort(ctx, port)
	if err != nil {
		return "", "", err
	}
	return host, mapped.Port(), nil
}

func renderRealm() (string, error) {
	source, err := os.ReadFile(filepath.Join(ProjectRoot(), "infra/keycloak/realm.json"))
	if err != nil {
		return "", err
	}
	contents := strings.NewReplacer(
		"__PROVIDER_A_CLIENT_SECRET__", ProviderASecret,
		"__PROVIDER_B_CLIENT_SECRET__", ProviderBSecret,
		"__INTERNAL_SERVICE_CLIENT_SECRET__", InternalServiceSecret,
	).Replace(string(source))
	var realm map[string]any
	if err := json.Unmarshal([]byte(contents), &realm); err != nil {
		return "", err
	}
	// A short lifetime makes expiration testable without forging or waiting minutes.
	realm["accessTokenLifespan"] = 3
	encoded, err := json.Marshal(realm)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp("", "wager-realm-*.json")
	if err != nil {
		return "", err
	}
	name := file.Name()
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		os.Remove(name)
		return "", err
	}
	if err := file.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

func fifoQueueInput(name string, visibility time.Duration, redrive string) *awssqs.CreateQueueInput {
	attributes := map[string]string{
		string(types.QueueAttributeNameFifoQueue):                     "true",
		string(types.QueueAttributeNameContentBasedDeduplication):     "false",
		string(types.QueueAttributeNameVisibilityTimeout):             fmt.Sprintf("%d", max(1, int(visibility.Seconds()))),
		string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds): "0",
	}
	if redrive != "" {
		attributes[string(types.QueueAttributeNameRedrivePolicy)] = redrive
	}
	return &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attributes}
}

func sanitize(value string) string {
	value = strings.ToLower(value)
	var result strings.Builder
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' || character == '-' {
			result.WriteRune(character)
		}
	}
	if result.Len() == 0 {
		return "integration"
	}
	return result.String()
}
