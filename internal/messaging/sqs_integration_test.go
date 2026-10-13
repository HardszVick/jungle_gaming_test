//go:build integration

package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/config"
	"github.com/junglegaming/backend-challenge-go/internal/testsupport"
)

func TestSQSClientAgainstLocalStack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	environment, err := testsupport.StartLocalStack(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer environment.Close(context.Background())
	loaded, err := config.LoadFrom(func(name string) (string, bool) {
		fallback := map[string]string{
			"DATABASE_URL":     "postgres://localhost/test",
			"OIDC_ISSUER_URL":  "http://issuer.test",
			"OIDC_AUDIENCE":    "audience",
			"AWS_ENDPOINT_URL": environment.LocalStackURL,
		}
		value, ok := fallback[name]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	sdkConfig, err := awsSDKConfig(ctx, loaded.AWS)
	if err != nil {
		t.Fatal(err)
	}
	api := awssqs.NewFromConfig(sdkConfig)
	queueName := "messaging-integration-" + uuid.NewString() + ".fifo"
	created, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			string(types.QueueAttributeNameFifoQueue):                 "true",
			string(types.QueueAttributeNameContentBasedDeduplication): "false",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = api.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: created.QueueUrl})
	}()

	client, err := newSQSClient(ctx, api, config.QueueConfig{Transactions: queueName, DeadLetter: queueName, Events: queueName})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, client.Queues().Events, []byte(`{"probe":true}`), "probe", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	messages, err := client.Receive(ctx, client.Queues().Events, 1, time.Second, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Body != `{"probe":true}` {
		t.Fatalf("unexpected LocalStack message: %+v", messages)
	}
	if err := client.Delete(ctx, client.Queues().Events, messages[0].ReceiptHandle); err != nil {
		t.Fatal(err)
	}
}
