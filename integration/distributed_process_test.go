//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/bootstrap"
	"github.com/junglegaming/backend-challenge-go/internal/messaging"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

var serviceBinary struct {
	sync.Once
	directory string
	path      string
	output    []byte
	err       error
}

func TestThreeServiceProcessesSerializeConcurrentBetsAndPersistAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	databaseURL := migratedPublicDatabase(t, ctx)
	queues := newQueues(t, ctx, "processes", time.Second)
	client := newSQSClient(t, ctx, queues)
	service, database := newService(t, ctx, databaseURL, defaultServiceOptions())
	defer database.Close()
	catalog, err := bootstrap.LoadMigrationCatalog(testsupport.ProjectRoot() + "/migrations")
	if err != nil {
		t.Fatal(err)
	}
	var appliedCount int
	var appliedVersion int64
	if err := database.Pool().QueryRow(ctx, `SELECT count(*), max(version) FROM schema_migrations`).Scan(&appliedCount, &appliedVersion); err != nil {
		t.Fatal(err)
	}
	if appliedCount != len(catalog.Migrations()) || appliedVersion != catalog.LatestVersion() {
		t.Fatalf("subprocess schema versions = count %d/latest %d, want %d/%d", appliedCount, appliedVersion, len(catalog.Migrations()), catalog.LatestVersion())
	}
	playerID := uuid.NewString()
	wallet := openWallet(t, ctx, service, playerID, "100.00")
	binary := buildServiceBinary(t)

	processes := startServiceProcesses(t, ctx, binary, databaseURL, queues, 3)
	assertDistinctProcessIDs(t, processes, 3)
	messageA := sqsEnvelope("process-message-a", "process-key-a", "provider-a", "process-bet-a", playerID, wallet.ID(), "BET", "80.00", "")
	messageB := sqsEnvelope("process-message-b", "process-key-b", "provider-a", "process-bet-b", playerID, wallet.ID(), "BET", "80.00", "")
	start := make(chan struct{})
	errorsChannel := make(chan error, 2)
	for index, body := range []string{messageA, messageB} {
		go func(index int, body string) {
			<-start
			messageID := []string{"process-message-a", "process-message-b"}[index]
			errorsChannel <- client.Publish(ctx, queues.TransactionsURL, []byte(body), wallet.ID(), messageID)
		}(index, body)
	}
	close(start)
	for range 2 {
		if err := <-errorsChannel; err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 20*time.Second, func() bool {
		return queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE external_transaction_id IN ('process-bet-a','process-bet-b') AND state IN ('PROCESSED','REJECTED')`) == 2
	}, "two subprocess-consumed bets to become terminal")
	assertProcessBetOutcome(t, ctx, database, wallet.ID())
	stopServiceProcesses(t, processes)

	processes = startServiceProcesses(t, ctx, binary, databaseURL, queues, 3)
	assertDistinctProcessIDs(t, processes, 3)
	replayA := sqsEnvelope("process-replay-a", "process-key-a", "provider-a", "process-bet-a", playerID, wallet.ID(), "BET", "80.00", "")
	replayB := sqsEnvelope("process-replay-b", "process-key-b", "provider-a", "process-bet-b", playerID, wallet.ID(), "BET", "80.00", "")
	if err := client.Publish(ctx, queues.TransactionsURL, []byte(replayA), wallet.ID(), "process-replay-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, queues.TransactionsURL, []byte(replayB), wallet.ID(), "process-replay-b"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM inbox_messages WHERE message_id IN ('process-replay-a','process-replay-b') AND completed_at IS NOT NULL`) == 2
	}, "replayed messages to complete after all worker processes restart")
	assertProcessBetOutcome(t, ctx, database, wallet.ID())
	assertReconciled(t, ctx, service, wallet.ID())
	stopServiceProcesses(t, processes)
}

func TestInvalidMessageIsPhysicallyRedrivenAndReceivedFromDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resource := migratedDatabase(t, ctx, "physical_dlq")
	queues, err := suite.NewQueuesWithRedrive(ctx, "physical-dlq", time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = suite.DeleteQueues(context.Background(), queues) })
	client := newSQSClient(t, ctx, queues)
	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	metrics := observability.NewMetrics()
	consumer := newConsumer(t, client, service, queues.TransactionsURL, queues.MaxReceiveCount, metrics, nil)
	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopWorker(t, consumer) })

	invalidBody := `{"messageId":"physical-dlq-message","type":"NotWagerTransactionRequested","occurredAt":"2026-10-10T12:00:00Z","data":{}}`
	if err := client.Publish(ctx, queues.TransactionsURL, []byte(invalidBody), "invalid-group", "physical-dlq-message"); err != nil {
		t.Fatal(err)
	}
	var deadLetter messaging.Message
	waitFor(t, 20*time.Second, func() bool {
		messages, receiveErr := client.Receive(ctx, queues.DeadLetterURL, 1, 100*time.Millisecond, 5*time.Second)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		if len(messages) == 0 {
			return false
		}
		deadLetter = messages[0]
		return true
	}, "LocalStack to redrive and expose the invalid message on the physical DLQ")
	if deadLetter.Body != invalidBody || deadLetter.ReceiveCount < 1 {
		t.Fatalf("DLQ message = %+v", deadLetter)
	}
	if err := client.Delete(ctx, queues.DeadLetterURL, deadLetter.ReceiptHandle); err != nil {
		t.Fatal(err)
	}
	if value := metricValue(t, metrics, "wager_dlq_messages_total"); value != 1 {
		t.Fatalf("wager_dlq_messages_total = %v, want 1 at maxReceiveCount=%d", value, queues.MaxReceiveCount)
	}
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM inbox_messages`, 0)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions`, 0)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries`, 0)
	assertCount(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events`, 0)
}

func metricValue(t *testing.T, metrics *observability.Metrics, name string) float64 {
	t.Helper()
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var value float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			value += metric.GetCounter().GetValue()
		}
	}
	return value
}

func assertProcessBetOutcome(t *testing.T, ctx context.Context, database *postgres.Database, walletID string) {
	t.Helper()
	pool := database.Pool()
	var processed, rejected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='PROCESSED'), count(*) FILTER (WHERE state='REJECTED' AND failure_code='INSUFFICIENT_FUNDS') FROM wager_transactions WHERE external_transaction_id IN ('process-bet-a','process-bet-b')`).Scan(&processed, &rejected); err != nil {
		t.Fatal(err)
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("subprocess bet states: processed=%d rejected-insufficient=%d", processed, rejected)
	}
	assertCount(t, ctx, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='DEBIT'`, 1, walletID)
	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 2000 {
		t.Fatalf("subprocess wallet balance_minor = %d, want 2000", balance)
	}
}

type serviceProcess struct {
	command *exec.Cmd
	done    chan struct{}
	logFile *os.File
	logPath string

	mu      sync.Mutex
	exitErr error
	stopped bool
}

func buildServiceBinary(t *testing.T) string {
	t.Helper()
	serviceBinary.Do(func() {
		serviceBinary.directory, serviceBinary.err = os.MkdirTemp("", "wager-service-")
		if serviceBinary.err != nil {
			return
		}
		serviceBinary.path = filepath.Join(serviceBinary.directory, "service")
		command := exec.Command("go", "build", "-o", serviceBinary.path, "./cmd/service")
		command.Dir = testsupport.ProjectRoot()
		serviceBinary.output, serviceBinary.err = command.CombinedOutput()
	})
	if serviceBinary.err != nil {
		t.Fatalf("build cmd/service: %v\n%s", serviceBinary.err, serviceBinary.output)
	}
	return serviceBinary.path
}

func startServiceProcesses(t *testing.T, ctx context.Context, binary, databaseURL string, queues testsupport.QueueSet, count int) []*serviceProcess {
	t.Helper()
	processes := make([]*serviceProcess, 0, count)
	for index := range count {
		logFile, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("service-%d-*.log", index))
		if err != nil {
			stopServiceProcesses(t, processes)
			t.Fatal(err)
		}
		command := exec.Command(binary)
		command.Dir = testsupport.ProjectRoot()
		command.Env = serviceEnvironment(databaseURL, queues)
		command.Stdout = logFile
		command.Stderr = logFile
		if err := command.Start(); err != nil {
			logFile.Close()
			stopServiceProcesses(t, processes)
			t.Fatal(err)
		}
		process := &serviceProcess{command: command, done: make(chan struct{}), logFile: logFile, logPath: logFile.Name()}
		go func() {
			err := command.Wait()
			process.mu.Lock()
			process.exitErr = err
			process.mu.Unlock()
			close(process.done)
		}()
		processes = append(processes, process)
	}
	t.Cleanup(func() { stopServiceProcesses(t, processes) })
	for _, process := range processes {
		waitForProcessStartup(t, process)
	}
	return processes
}

func serviceEnvironment(databaseURL string, queues testsupport.QueueSet) []string {
	values := map[string]string{
		"DATABASE_URL":                  databaseURL,
		"COMPONENTS":                    "sqs-consumer",
		"AWS_ENDPOINT_URL":              suite.LocalStackURL,
		"AWS_REGION":                    testsupport.Region,
		"AWS_ACCESS_KEY_ID":             "test",
		"AWS_SECRET_ACCESS_KEY":         "test",
		"SQS_TRANSACTIONS_QUEUE":        queues.TransactionsName,
		"SQS_TRANSACTIONS_DLQ":          queues.DeadLetterName,
		"SQS_EVENTS_QUEUE":              queues.EventsName,
		"SQS_REDRIVE_MAX_RECEIVE_COUNT": fmt.Sprint(queueMaxReceiveCount(queues)),
		"SQS_CONSUMER_BATCH_SIZE":       "1",
		"SQS_CONSUMER_POLL_INTERVAL":    "100ms",
		"SQS_CONSUMER_LEASE_DURATION":   "1s",
		"TRANSACTION_RETRY_BASE_DELAY":  "5ms",
		"TRANSACTION_RETRY_MAX_DELAY":   "50ms",
		"SHUTDOWN_TIMEOUT":              "3s",
	}
	environment := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, replaced := values[name]; !replaced {
			environment = append(environment, entry)
		}
	}
	for name, value := range values {
		environment = append(environment, name+"="+value)
	}
	return environment
}

func waitForProcessStartup(t *testing.T, process *serviceProcess) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			contents, _ := os.ReadFile(process.logPath)
			t.Fatalf("service process %d exited during startup: %v\n%s", process.command.Process.Pid, process.waitError(), contents)
		default:
		}
		contents, _ := os.ReadFile(process.logPath)
		if strings.Contains(string(contents), `"component":"sqs-consumer"`) && strings.Contains(string(contents), `"msg":"component started"`) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	contents, _ := os.ReadFile(process.logPath)
	t.Fatalf("service process %d did not start\n%s", process.command.Process.Pid, contents)
}

func stopServiceProcesses(t *testing.T, processes []*serviceProcess) {
	t.Helper()
	for _, process := range processes {
		process.stop(t)
	}
}

func (process *serviceProcess) stop(t *testing.T) {
	t.Helper()
	process.mu.Lock()
	if process.stopped {
		process.mu.Unlock()
		return
	}
	process.stopped = true
	process.mu.Unlock()
	select {
	case <-process.done:
	default:
		if process.command.Process != nil {
			_ = process.command.Process.Signal(syscall.SIGTERM)
		}
	}
	select {
	case <-process.done:
		err := process.waitError()
		if err != nil {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != 0 {
				contents, _ := os.ReadFile(process.logPath)
				t.Errorf("service process %d shutdown: %v\n%s", process.command.Process.Pid, err, contents)
			}
		}
	case <-time.After(5 * time.Second):
		_ = process.command.Process.Kill()
		<-process.done
		t.Errorf("service process %d required SIGKILL", process.command.Process.Pid)
	}
	_ = process.logFile.Close()
}

func (process *serviceProcess) waitError() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.exitErr
}

func assertDistinctProcessIDs(t *testing.T, processes []*serviceProcess, wanted int) {
	t.Helper()
	ids := make(map[int]struct{}, len(processes))
	for _, process := range processes {
		ids[process.command.Process.Pid] = struct{}{}
	}
	if len(ids) != wanted {
		t.Fatalf("distinct service PIDs = %v, want %d", ids, wanted)
	}
}
