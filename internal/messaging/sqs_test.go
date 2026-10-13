package messaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/junglegaming/backend-challenge-go/internal/config"
)

type fakeSQSAPI struct {
	queueURLs    map[string]string
	healthErr    error
	receiveOut   *awssqs.ReceiveMessageOutput
	receiveIn    *awssqs.ReceiveMessageInput
	sendIn       *awssqs.SendMessageInput
	deleteIn     *awssqs.DeleteMessageInput
	visibilityIn *awssqs.ChangeMessageVisibilityInput
	healthCalls  int
}

func (fake *fakeSQSAPI) GetQueueUrl(_ context.Context, input *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	url, ok := fake.queueURLs[aws.ToString(input.QueueName)]
	if !ok {
		return nil, errors.New("missing queue")
	}
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String(url)}, nil
}

func (fake *fakeSQSAPI) GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	fake.healthCalls++
	return &awssqs.GetQueueAttributesOutput{}, fake.healthErr
}

func (fake *fakeSQSAPI) ReceiveMessage(_ context.Context, input *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	fake.receiveIn = input
	return fake.receiveOut, nil
}

func (fake *fakeSQSAPI) DeleteMessage(_ context.Context, input *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	fake.deleteIn = input
	return &awssqs.DeleteMessageOutput{}, nil
}

func (fake *fakeSQSAPI) ChangeMessageVisibility(_ context.Context, input *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	fake.visibilityIn = input
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (fake *fakeSQSAPI) SendMessage(_ context.Context, input *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	fake.sendIn = input
	return &awssqs.SendMessageOutput{}, nil
}

func TestAWSSDKConfigUsesEnvironmentCredentialChain(t *testing.T) {
	isolateAWSEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "token")
	sdkConfig, err := awsSDKConfig(context.Background(), config.AWSConfig{
		Region: "eu-west-1", EndpointURL: "http://localstack:4566",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sdkConfig.Region != "eu-west-1" || aws.ToString(sdkConfig.BaseEndpoint) != "http://localstack:4566" {
		t.Fatalf("unexpected SDK config")
	}
	credentials, err := sdkConfig.Credentials.Retrieve(context.Background())
	if err != nil || credentials.AccessKeyID != "access" || credentials.SecretAccessKey != "secret" || credentials.SessionToken != "token" {
		t.Fatalf("unexpected credentials: %+v, %v", credentials, err)
	}
}

func TestAWSSDKConfigUsesSharedCredentialsProfile(t *testing.T) {
	isolateAWSEnvironment(t)
	credentialsFile := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(credentialsFile, []byte("[producer]\naws_access_key_id = profile-access\naws_secret_access_key = profile-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sdkConfig, err := awsSDKConfig(context.Background(), config.AWSConfig{
		Region: "us-east-1", Profile: "producer", SharedCredentialsFile: credentialsFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := sdkConfig.Credentials.Retrieve(context.Background())
	if err != nil || credentials.AccessKeyID != "profile-access" || credentials.SecretAccessKey != "profile-secret" {
		t.Fatalf("unexpected profile credentials: %+v, %v", credentials, err)
	}
}

func TestAWSSDKConfigAcceptsAlternativeProvider(t *testing.T) {
	isolateAWSEnvironment(t)
	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "managed-access", SecretAccessKey: "managed-secret", Source: "test-provider"}, nil
	})
	sdkConfig, err := awsSDKConfig(context.Background(), config.AWSConfig{Region: "us-east-1"}, awsconfig.WithCredentialsProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := sdkConfig.Credentials.Retrieve(context.Background())
	if err != nil || credentials.AccessKeyID != "managed-access" || credentials.Source != "test-provider" {
		t.Fatalf("unexpected provider credentials: %+v, %v", credentials, err)
	}
}

func TestAWSSDKConfigReportsMissingCredentials(t *testing.T) {
	isolateAWSEnvironment(t)
	sdkConfig, err := awsSDKConfig(context.Background(), config.AWSConfig{Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdkConfig.Credentials.Retrieve(context.Background()); err == nil {
		t.Fatal("Retrieve() error = nil, want missing credentials")
	}
}

func isolateAWSEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing-credentials"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "missing-config"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestSQSClientResolvesQueuesHealthAndOperations(t *testing.T) {
	fake := &fakeSQSAPI{
		queueURLs: map[string]string{"input": "url-input", "dlq": "url-dlq", "events": "url-events"},
		receiveOut: &awssqs.ReceiveMessageOutput{Messages: []types.Message{{
			Body: aws.String("body"), ReceiptHandle: aws.String("receipt"),
			Attributes: map[string]string{
				string(types.MessageSystemAttributeNameApproximateReceiveCount): "3",
				string(types.MessageSystemAttributeNameMessageGroupId):          "wallet-1",
			},
		}}},
	}
	client, err := newSQSClient(context.Background(), fake, config.QueueConfig{Transactions: "input", DeadLetter: "dlq", Events: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if client.Queues() != (QueueURLs{Transactions: "url-input", DeadLetter: "url-dlq", Events: "url-events"}) {
		t.Fatalf("unexpected queue URLs: %+v", client.Queues())
	}
	if err := client.Health(context.Background()); err != nil || fake.healthCalls != 3 {
		t.Fatalf("health calls=%d err=%v", fake.healthCalls, err)
	}
	messages, err := client.Receive(context.Background(), "url-input", 10, 30*time.Second, 1500*time.Millisecond)
	if err != nil || len(messages) != 1 || messages[0].ReceiveCount != 3 || messages[0].MessageGroupID != "wallet-1" {
		t.Fatalf("unexpected receive: %+v, %v", messages, err)
	}
	if len(fake.receiveIn.MessageSystemAttributeNames) != 2 ||
		fake.receiveIn.MessageSystemAttributeNames[1] != types.MessageSystemAttributeNameMessageGroupId {
		t.Fatalf("message group ID was not requested: %+v", fake.receiveIn.MessageSystemAttributeNames)
	}
	if fake.receiveIn.WaitTimeSeconds != 20 || fake.receiveIn.VisibilityTimeout != 2 {
		t.Fatalf("durations not bounded/rounded: %+v", fake.receiveIn)
	}
	if err := client.Publish(context.Background(), "url-events", []byte("event"), "aggregate", "event-id"); err != nil {
		t.Fatal(err)
	}
	if aws.ToString(fake.sendIn.MessageGroupId) != "aggregate" || aws.ToString(fake.sendIn.MessageDeduplicationId) != "event-id" {
		t.Fatalf("FIFO identities were not forwarded")
	}
	if err := client.Delete(context.Background(), "url-input", "receipt"); err != nil {
		t.Fatal(err)
	}
	if err := client.ChangeVisibility(context.Background(), "url-input", "receipt", 0); err != nil {
		t.Fatal(err)
	}
	if fake.visibilityIn.VisibilityTimeout != 0 || aws.ToString(fake.deleteIn.ReceiptHandle) != "receipt" {
		t.Fatalf("delete or visibility request missing")
	}
}
