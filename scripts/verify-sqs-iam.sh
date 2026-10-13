#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose="$root_dir/scripts/compose.sh"
aws_as() {
  local profile="$1"
  local credentials_file="/var/lib/localstack/${profile}-credentials/credentials"
  shift
  "$compose" exec -T \
    -e "AWS_PROFILE=$profile" \
    -e "AWS_SHARED_CREDENTIALS_FILE=$credentials_file" \
    localstack aws --endpoint-url=http://localhost:4566 "$@"
}

expect_denied() {
  local profile="$1"
  shift
  local output
  if output="$(aws_as "$profile" "$@" 2>&1)"; then
    printf 'error: %s unexpectedly allowed: aws %s\n' "$profile" "$*" >&2
    exit 1
  fi
  if [[ "$output" != *AccessDenied* && "$output" != *not\ authorized* ]]; then
    printf 'error: %s failed without an authorization denial: %s\n' "$profile" "$output" >&2
    exit 1
  fi
}

input_queue="${SQS_INPUT_QUEUE_NAME:-wager-transactions.fifo}"
output_queue="${SQS_OUTPUT_QUEUE_NAME:-wager-events.fifo}"
input_url="$("$compose" exec -T localstack awslocal sqs get-queue-url --queue-name "$input_queue" --query QueueUrl --output text)"
output_url="$("$compose" exec -T localstack awslocal sqs get-queue-url --queue-name "$output_queue" --query QueueUrl --output text)"
probe_id="iam-probe-$(date -u +%Y%m%dT%H%M%SZ)-$$"

aws_as producer sqs send-message \
  --queue-url "$input_url" --message-body '{"iamProbe":true}' \
  --message-group-id iam-probe --message-deduplication-id "$probe_id" >/dev/null
message="$(aws_as runtime sqs receive-message \
  --queue-url "$input_url" --max-number-of-messages 1 --wait-time-seconds 1)"
receipt="$(jq -er '.Messages[0].ReceiptHandle' <<<"$message")"
aws_as runtime sqs delete-message --queue-url "$input_url" --receipt-handle "$receipt" >/dev/null

aws_as runtime sqs send-message \
  --queue-url "$output_url" --message-body '{"iamProbe":true}' \
  --message-group-id iam-probe --message-deduplication-id "$probe_id-runtime" >/dev/null

expect_denied producer sqs receive-message --queue-url "$input_url" --max-number-of-messages 1
expect_denied producer sqs send-message \
  --queue-url "$output_url" --message-body '{}' \
  --message-group-id denied --message-deduplication-id "$probe_id-producer-output"
expect_denied runtime sqs send-message \
  --queue-url "$input_url" --message-body '{}' \
  --message-group-id denied --message-deduplication-id "$probe_id-runtime-input"
expect_denied runtime sqs receive-message --queue-url "$output_url" --max-number-of-messages 1

"$compose" exec -T localstack awslocal sqs purge-queue --queue-url "$output_url" >/dev/null
printf 'Verified positive and negative SQS IAM permissions for runtime and producer profiles.\n'
