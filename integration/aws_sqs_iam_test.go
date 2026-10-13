//go:build integration

package integration_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/config"
	"github.com/junglegaming/backend-challenge-go/internal/messaging"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

func TestAWSSQSLeastPrivilegeHasNoUnauthorizedFinancialEffects(t *testing.T) {
	if os.Getenv("AWS_IAM_TEST") != "1" {
		t.Skip("run through make verify-aws-sqs-iam")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	region := requiredEnv(t, "AWS_REGION")
	queues := config.QueueConfig{
		Transactions: requiredEnv(t, "AWS_TEST_INPUT_QUEUE"),
		DeadLetter:   requiredEnv(t, "AWS_TEST_DLQ"),
		Events:       requiredEnv(t, "AWS_TEST_OUTPUT_QUEUE"),
	}
	runtimeClient, err := messaging.NewSQSClient(ctx, config.AWSConfig{Region: region}, queues)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimeClient.Health(ctx); err != nil {
		t.Fatalf("runtime health: %v", err)
	}

	producerConfig, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			requiredEnv(t, "AWS_TEST_PRODUCER_ACCESS_KEY_ID"),
			requiredEnv(t, "AWS_TEST_PRODUCER_SECRET_ACCESS_KEY"),
			requiredEnv(t, "AWS_TEST_PRODUCER_SESSION_TOKEN"),
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	producerClient := awssqs.NewFromConfig(producerConfig)
	urls := runtimeClient.Queues()

	resource := migratedDatabase(t, ctx, "aws_iam")
	service, database := newService(t, ctx, resource.URL, defaultServiceOptions())
	defer database.Close()
	playerID := uuid.NewString()
	wallet := openWallet(t, ctx, service, playerID, "100.00")

	consumer := newConsumer(t, runtimeClient, service, urls.Transactions, 3, nil, nil)
	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		shutdown, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = consumer.Shutdown(shutdown)
	}()

	messageID := uuid.NewString()
	externalID := "aws-iam-allowed-" + uuid.NewString()
	envelope := sqsEnvelope(messageID, "aws-iam-allowed", "provider-a", externalID, playerID, wallet.ID(), "BET", "10.00", "")
	if _, err := producerClient.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(urls.Transactions),
		MessageBody:            aws.String(envelope),
		MessageGroupId:         aws.String(wallet.ID()),
		MessageDeduplicationId: aws.String(messageID),
	}); err != nil {
		t.Fatalf("allowed producer send: %v", err)
	}

	waitFor(t, 30*time.Second, func() bool {
		return queryIntArgs(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id=$1`, externalID) == 1
	}, "AWS SQS command processing")
	assertWalletBalance(t, ctx, service, wallet.ID(), "90.00")
	assertReconciled(t, ctx, service, wallet.ID())

	baseline := financialCounts(t, ctx, database)
	expectAWSAccessDenied(t, func() error {
		_, err := producerClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(urls.Transactions), MaxNumberOfMessages: 1,
		})
		return err
	})
	expectAWSAccessDenied(t, func() error {
		_, err := producerClient.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl: aws.String(urls.Events), MessageBody: aws.String("{}"),
			MessageGroupId: aws.String("denied"), MessageDeduplicationId: aws.String(uuid.NewString()),
		})
		return err
	})
	expectAWSAccessDenied(t, func() error {
		_, err := producerClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
			QueueUrl: aws.String(urls.DeadLetter), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		return err
	})
	expectAWSAccessDenied(t, func() error {
		return runtimeClient.Publish(ctx, urls.Transactions, []byte(envelope), wallet.ID(), uuid.NewString())
	})
	expectAWSAccessDenied(t, func() error {
		_, err := runtimeClient.Receive(ctx, urls.Events, 1, time.Second, time.Second)
		return err
	})

	time.Sleep(2 * time.Second)
	if current := financialCounts(t, ctx, database); current != baseline {
		t.Fatalf("unauthorized SQS calls changed financial state: before=%+v after=%+v", baseline, current)
	}
}

type databaseCounts struct {
	Transactions int
	Inbox        int
	Ledger       int
	Outbox       int
}

func financialCounts(t *testing.T, ctx context.Context, database *postgres.Database) databaseCounts {
	t.Helper()
	return databaseCounts{
		Transactions: queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM wager_transactions`),
		Inbox:        queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM inbox_messages`),
		Ledger:       queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM wallet_ledger_entries`),
		Outbox:       queryInt(t, ctx, database.Pool(), `SELECT count(*) FROM outbox_events`),
	}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func expectAWSAccessDenied(t *testing.T, operation func() error) {
	t.Helper()
	err := operation()
	if err == nil {
		t.Fatal("AWS operation unexpectedly allowed")
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) && (apiError.ErrorCode() == "AccessDenied" || apiError.ErrorCode() == "AccessDeniedException") {
		return
	}
	if !strings.Contains(err.Error(), "AccessDenied") && !strings.Contains(err.Error(), "not authorized") {
		t.Fatalf("AWS operation failed without AccessDenied: %v", err)
	}
}
