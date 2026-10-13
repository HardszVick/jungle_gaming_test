#!/usr/bin/env bash
set -Eeuo pipefail

profile="${AWS_PROFILE:-wager-free-tier}"
region="${AWS_REGION:-us-east-1}"
credentials_dir="${AWS_CREDENTIALS_DIR:-$HOME/.aws}"
aws_image="${AWS_CLI_IMAGE:-amazon/aws-cli:latest}"

for command in docker jq; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'error: required command not found: %s\n' "$command" >&2
    exit 1
  }
done

[[ -d "$credentials_dir" ]] || {
  printf 'error: AWS credentials directory not found: %s\n' "$credentials_dir" >&2
  exit 1
}

parent_aws() {
  docker run --rm --user "$(id -u):$(id -g)" \
    -v "$credentials_dir:/aws:ro" \
    -e AWS_CONFIG_FILE=/aws/config \
    -e AWS_SHARED_CREDENTIALS_FILE=/aws/credentials \
    "$aws_image" --profile "$profile" --region "$region" "$@"
}

runtime_aws() {
  AWS_ACCESS_KEY_ID="$runtime_access_key" \
  AWS_SECRET_ACCESS_KEY="$runtime_secret_key" \
  AWS_SESSION_TOKEN="$runtime_session_token" \
    docker run --rm \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
      "$aws_image" --region "$region" "$@"
}

producer_aws() {
  AWS_ACCESS_KEY_ID="$producer_access_key" \
  AWS_SECRET_ACCESS_KEY="$producer_secret_key" \
  AWS_SESSION_TOKEN="$producer_session_token" \
    docker run --rm \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
      "$aws_image" --region "$region" "$@"
}

expect_denied() {
  local identity="$1"
  shift
  local output
  if output="$("${identity}_aws" "$@" 2>&1)"; then
    printf 'error: %s unexpectedly allowed: aws %s\n' "$identity" "$*" >&2
    exit 1
  fi
  if [[ "$output" != *AccessDenied* && "$output" != *not\ authorized* ]]; then
    printf 'error: %s failed without an authorization denial: %s\n' "$identity" "$output" >&2
    exit 1
  fi
}

account_id="$(parent_aws sts get-caller-identity --query Account --output text)"
suffix="$(date -u +%Y%m%d%H%M%S)-$$"
input_name="wager-test-iam-$suffix-input.fifo"
dlq_name="wager-test-iam-$suffix-dlq.fifo"
output_name="wager-test-iam-$suffix-output.fifo"
input_url=
dlq_url=
output_url=

cleanup() {
  local queue_url
  for queue_url in "$input_url" "$output_url" "$dlq_url"; do
    if [[ -n "$queue_url" ]]; then
      parent_aws sqs delete-queue --queue-url "$queue_url" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

dlq_url="$(parent_aws sqs create-queue \
  --queue-name "$dlq_name" \
  --attributes FifoQueue=true,ContentBasedDeduplication=false,MessageRetentionPeriod=300 \
  --query QueueUrl --output text)"
dlq_arn="$(parent_aws sqs get-queue-attributes \
  --queue-url "$dlq_url" --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)"

input_attributes="$(jq -cn --arg dlq "$dlq_arn" '{
  FifoQueue: "true",
  ContentBasedDeduplication: "false",
  VisibilityTimeout: "30",
  ReceiveMessageWaitTimeSeconds: "1",
  MessageRetentionPeriod: "300",
  RedrivePolicy: ({deadLetterTargetArn: $dlq, maxReceiveCount: "3"} | tojson)
}')"
input_url="$(parent_aws sqs create-queue \
  --queue-name "$input_name" --attributes "$input_attributes" \
  --query QueueUrl --output text)"
output_url="$(parent_aws sqs create-queue \
  --queue-name "$output_name" \
  --attributes FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout=30,ReceiveMessageWaitTimeSeconds=1,MessageRetentionPeriod=300 \
  --query QueueUrl --output text)"

input_arn="arn:aws:sqs:$region:$account_id:$input_name"
output_arn="arn:aws:sqs:$region:$account_id:$output_name"

runtime_policy="$(jq -cn \
  --arg input "$input_arn" --arg dlq "$dlq_arn" --arg output "$output_arn" '{
  Version: "2012-10-17",
  Statement: [
    {
      Effect: "Allow",
      Action: ["sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
      Resource: [$input, $dlq, $output]
    },
    {
      Effect: "Allow",
      Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"],
      Resource: $input
    },
    {
      Effect: "Allow",
      Action: "sqs:SendMessage",
      Resource: $output
    }
  ]
}')"
producer_policy="$(jq -cn --arg input "$input_arn" '{
  Version: "2012-10-17",
  Statement: [{
    Effect: "Allow",
    Action: ["sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:SendMessage"],
    Resource: $input
  }]
}')"

runtime_session="$(parent_aws sts get-federation-token \
  --name "runtime-$suffix" --duration-seconds 900 \
  --policy "$runtime_policy" --output json)"
producer_session="$(parent_aws sts get-federation-token \
  --name "producer-$suffix" --duration-seconds 900 \
  --policy "$producer_policy" --output json)"

runtime_access_key="$(jq -er '.Credentials.AccessKeyId' <<<"$runtime_session")"
runtime_secret_key="$(jq -er '.Credentials.SecretAccessKey' <<<"$runtime_session")"
runtime_session_token="$(jq -er '.Credentials.SessionToken' <<<"$runtime_session")"
producer_access_key="$(jq -er '.Credentials.AccessKeyId' <<<"$producer_session")"
producer_secret_key="$(jq -er '.Credentials.SecretAccessKey' <<<"$producer_session")"
producer_session_token="$(jq -er '.Credentials.SessionToken' <<<"$producer_session")"
unset runtime_session producer_session

probe_id="iam-probe-$suffix"
producer_aws sqs send-message \
  --queue-url "$input_url" --message-body '{"iamProbe":"producer"}' \
  --message-group-id iam-probe --message-deduplication-id "$probe_id-input" \
  >/dev/null

message="$(runtime_aws sqs receive-message \
  --queue-url "$input_url" --max-number-of-messages 1 \
  --wait-time-seconds 5 --attribute-names All --output json)"
receipt="$(jq -er '.Messages[0].ReceiptHandle' <<<"$message")"
runtime_aws sqs change-message-visibility \
  --queue-url "$input_url" --receipt-handle "$receipt" --visibility-timeout 30 \
  >/dev/null
runtime_aws sqs delete-message \
  --queue-url "$input_url" --receipt-handle "$receipt" >/dev/null

runtime_aws sqs send-message \
  --queue-url "$output_url" --message-body '{"iamProbe":"runtime"}' \
  --message-group-id iam-probe --message-deduplication-id "$probe_id-output" \
  >/dev/null
runtime_aws sqs get-queue-attributes \
  --queue-url "$dlq_url" --attribute-names QueueArn >/dev/null

expect_denied producer sqs receive-message \
  --queue-url "$input_url" --max-number-of-messages 1
expect_denied producer sqs send-message \
  --queue-url "$output_url" --message-body '{}' \
  --message-group-id denied --message-deduplication-id "$probe_id-producer-output"
expect_denied producer sqs get-queue-attributes \
  --queue-url "$dlq_url" --attribute-names QueueArn
expect_denied runtime sqs send-message \
  --queue-url "$input_url" --message-body '{}' \
  --message-group-id denied --message-deduplication-id "$probe_id-runtime-input"
expect_denied runtime sqs receive-message \
  --queue-url "$output_url" --max-number-of-messages 1

remaining_input="$(parent_aws sqs receive-message \
  --queue-url "$input_url" --max-number-of-messages 10 \
  --wait-time-seconds 1 --output json || true)"
if jq -e '.Messages | length > 0' <<<"${remaining_input:-{}}" >/dev/null 2>&1; then
  printf 'error: a denied operation left a message in the input queue\n' >&2
  exit 1
fi

output_message="$(parent_aws sqs receive-message \
  --queue-url "$output_url" --max-number-of-messages 10 \
  --wait-time-seconds 5 --output json)"
if ! jq -e '.Messages | length == 1 and .[0].Body == "{\"iamProbe\":\"runtime\"}"' \
  <<<"$output_message" >/dev/null; then
  printf 'error: output queue does not contain exactly the allowed runtime probe\n' >&2
  exit 1
fi

go_test_args=(test -tags=integration ./integration -run '^TestAWSSQSLeastPrivilegeHasNoUnauthorizedFinancialEffects$' -count=1)
if [[ "${AWS_IAM_FULL_SUITE:-0}" == "1" ]]; then
  go_test_args=(test -tags=integration ./... -count=1)
fi
if [[ "${AWS_IAM_RACE:-0}" == "1" ]]; then
  go_test_args=(test -race -tags=integration ./... -count=1)
fi

AWS_ACCESS_KEY_ID="$runtime_access_key" \
AWS_SECRET_ACCESS_KEY="$runtime_secret_key" \
AWS_SESSION_TOKEN="$runtime_session_token" \
AWS_TEST_PRODUCER_ACCESS_KEY_ID="$producer_access_key" \
AWS_TEST_PRODUCER_SECRET_ACCESS_KEY="$producer_secret_key" \
AWS_TEST_PRODUCER_SESSION_TOKEN="$producer_session_token" \
AWS_REGION="$region" \
AWS_TEST_INPUT_QUEUE="$input_name" \
AWS_TEST_DLQ="$dlq_name" \
AWS_TEST_OUTPUT_QUEUE="$output_name" \
  docker run --rm --network host \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    -e AWS_TEST_PRODUCER_ACCESS_KEY_ID \
    -e AWS_TEST_PRODUCER_SECRET_ACCESS_KEY \
    -e AWS_TEST_PRODUCER_SESSION_TOKEN \
    -e AWS_REGION -e AWS_TEST_INPUT_QUEUE -e AWS_TEST_DLQ -e AWS_TEST_OUTPUT_QUEUE \
    -e AWS_IAM_TEST=1 -e AWS_EC2_METADATA_DISABLED=true -e AWS_ENDPOINT_URL= \
    -e TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1 \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$PWD:/workspace" -w /workspace \
    golang:1.25.0 go "${go_test_args[@]}"

printf 'Verified AWS SQS permissions and absence of unauthorized financial effects; queues will be removed.\n'
