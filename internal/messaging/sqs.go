package messaging

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/junglegaming/backend-challenge-go/internal/config"
)

type sqsAPI interface {
	GetQueueUrl(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
	ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

type QueueURLs struct {
	Transactions string
	DeadLetter   string
	Events       string
}

// SQSClient is the AWS SDK v2 adapter used by all messaging components.
type SQSClient struct {
	api    sqsAPI
	queues QueueURLs
}

// NewSQSClient builds an SDK v2 client with the default credential chain and
// an optional custom endpoint, then resolves queue names once at startup.
func NewSQSClient(ctx context.Context, awsConfig config.AWSConfig, queues config.QueueConfig) (*SQSClient, error) {
	sdkConfig, err := awsSDKConfig(ctx, awsConfig)
	if err != nil {
		return nil, err
	}
	return newSQSClient(ctx, awssqs.NewFromConfig(sdkConfig), queues)
}

func awsSDKConfig(ctx context.Context, value config.AWSConfig, options ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
	if value.Region == "" {
		return aws.Config{}, errors.New("messaging: AWS region is required")
	}
	options = append(options, awsconfig.WithRegion(value.Region))
	if value.Profile != "" {
		options = append(options, awsconfig.WithSharedConfigProfile(value.Profile))
	}
	if value.SharedCredentialsFile != "" {
		options = append(options, awsconfig.WithSharedCredentialsFiles([]string{value.SharedCredentialsFile}))
	}
	sdkConfig, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("messaging: load AWS SDK config: %w", err)
	}
	if value.EndpointURL != "" {
		sdkConfig.BaseEndpoint = aws.String(value.EndpointURL)
	}
	return sdkConfig, nil
}

func newSQSClient(ctx context.Context, api sqsAPI, queues config.QueueConfig) (*SQSClient, error) {
	if api == nil {
		return nil, errors.New("messaging: SQS API is required")
	}
	resolved := QueueURLs{}
	var err error
	if resolved.Transactions, err = resolveQueueURL(ctx, api, queues.Transactions); err != nil {
		return nil, fmt.Errorf("messaging: resolve transactions queue: %w", err)
	}
	if resolved.DeadLetter, err = resolveQueueURL(ctx, api, queues.DeadLetter); err != nil {
		return nil, fmt.Errorf("messaging: resolve dead-letter queue: %w", err)
	}
	if resolved.Events, err = resolveQueueURL(ctx, api, queues.Events); err != nil {
		return nil, fmt.Errorf("messaging: resolve events queue: %w", err)
	}
	return &SQSClient{api: api, queues: resolved}, nil
}

func resolveQueueURL(ctx context.Context, api sqsAPI, name string) (string, error) {
	if name == "" {
		return "", errors.New("queue name is required")
	}
	output, err := api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", err
	}
	if output == nil || output.QueueUrl == nil || *output.QueueUrl == "" {
		return "", errors.New("SQS returned an empty queue URL")
	}
	return *output.QueueUrl, nil
}

func (client *SQSClient) Queues() QueueURLs { return client.queues }

// Health verifies lightweight authenticated access to every configured queue.
func (client *SQSClient) Health(ctx context.Context) error {
	if client == nil || client.api == nil {
		return errors.New("messaging: SQS client is unavailable")
	}
	for _, queueURL := range []string{client.queues.Transactions, client.queues.DeadLetter, client.queues.Events} {
		_, err := client.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
			QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		if err != nil {
			return fmt.Errorf("messaging: SQS health check failed: %w", err)
		}
	}
	return nil
}

func (client *SQSClient) Receive(ctx context.Context, queueURL string, maximum int, wait, visibility time.Duration) ([]Message, error) {
	if maximum < 1 || maximum > 10 {
		return nil, errors.New("messaging: SQS receive batch must be from 1 to 10")
	}
	output, err := client.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: int32(maximum),
		WaitTimeSeconds: durationSeconds(wait, 20), VisibilityTimeout: durationSeconds(visibility, 43200),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		return nil, err
	}
	if output == nil {
		return nil, errors.New("messaging: SQS returned an empty receive response")
	}
	messages := make([]Message, 0, len(output.Messages))
	for _, item := range output.Messages {
		if item.Body == nil || item.ReceiptHandle == nil {
			continue
		}
		count, _ := strconv.Atoi(item.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		if count < 1 {
			count = 1
		}
		messages = append(messages, Message{
			Body: *item.Body, ReceiptHandle: *item.ReceiptHandle, ReceiveCount: count,
			MessageGroupID: item.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
		})
	}
	return messages, nil
}

func (client *SQSClient) Delete(ctx context.Context, queueURL, receiptHandle string) error {
	_, err := client.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: aws.String(receiptHandle)})
	return err
}

func (client *SQSClient) ChangeVisibility(ctx context.Context, queueURL, receiptHandle string, visibility time.Duration) error {
	_, err := client.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(queueURL), ReceiptHandle: aws.String(receiptHandle),
		VisibilityTimeout: durationSeconds(visibility, 43200),
	})
	return err
}

func (client *SQSClient) Publish(ctx context.Context, queueURL string, body []byte, groupID, deduplicationID string) error {
	if len(body) == 0 || groupID == "" || deduplicationID == "" {
		return errors.New("messaging: invalid FIFO publication")
	}
	_, err := client.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(string(body)),
		MessageGroupId: aws.String(groupID), MessageDeduplicationId: aws.String(deduplicationID),
	})
	return err
}

func durationSeconds(value time.Duration, maximum int32) int32 {
	if value <= 0 {
		return 0
	}
	seconds := int64(math.Ceil(value.Seconds()))
	if seconds > int64(maximum) {
		return maximum
	}
	return int32(seconds)
}
