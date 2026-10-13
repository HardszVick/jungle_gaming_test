#!/bin/sh
set -eu

region="${AWS_DEFAULT_REGION:-us-east-1}"
input_queue="${SQS_INPUT_QUEUE_NAME:-wager-transactions.fifo}"
dlq="${SQS_DLQ_NAME:-wager-transactions-dlq.fifo}"
output_queue="${SQS_OUTPUT_QUEUE_NAME:-wager-events.fifo}"
account_id="${AWS_ACCOUNT_ID:-000000000000}"
visibility_timeout="${SQS_VISIBILITY_TIMEOUT:-60}"
wait_time="${SQS_WAIT_TIME_SECONDS:-20}"
max_receive_count="${SQS_REDRIVE_MAX_RECEIVE_COUNT:-5}"

case "$visibility_timeout:$wait_time:$max_receive_count" in
  *[!0-9:]*) echo "SQS timeout and redrive settings must be integers" >&2; exit 1 ;;
esac

dlq_url="$(awslocal sqs create-queue \
  --region "$region" \
  --queue-name "$dlq" \
  --attributes \
    FifoQueue=true,ContentBasedDeduplication=false,MessageRetentionPeriod=1209600 \
  --query QueueUrl --output text)"

dlq_arn="$(awslocal sqs get-queue-attributes \
  --region "$region" \
  --queue-url "$dlq_url" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)"

cat > /tmp/input-queue-attributes.json <<EOF
{
  "FifoQueue": "true",
  "ContentBasedDeduplication": "false",
  "VisibilityTimeout": "${visibility_timeout}",
  "ReceiveMessageWaitTimeSeconds": "${wait_time}",
  "MessageRetentionPeriod": "345600",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${dlq_arn}\",\"maxReceiveCount\":\"${max_receive_count}\"}"
}
EOF

awslocal sqs create-queue \
  --region "$region" \
  --queue-name "$input_queue" \
  --attributes file:///tmp/input-queue-attributes.json \
  >/dev/null

awslocal sqs create-queue \
  --region "$region" \
  --queue-name "$output_queue" \
  --attributes \
    FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout="$visibility_timeout",ReceiveMessageWaitTimeSeconds="$wait_time",MessageRetentionPeriod=345600 \
  >/dev/null

input_arn="arn:aws:sqs:${region}:${account_id}:${input_queue}"
dlq_arn="arn:aws:sqs:${region}:${account_id}:${dlq}"
output_arn="arn:aws:sqs:${region}:${account_id}:${output_queue}"

runtime_policy="$(printf '%s' "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"sqs:GetQueueUrl\",\"sqs:GetQueueAttributes\"],\"Resource\":[\"$input_arn\",\"$dlq_arn\",\"$output_arn\"]},{\"Effect\":\"Allow\",\"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:DeleteMessageBatch\",\"sqs:ChangeMessageVisibility\",\"sqs:ChangeMessageVisibilityBatch\"],\"Resource\":\"$input_arn\"},{\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:SendMessageBatch\"],\"Resource\":\"$output_arn\"}]}")"
producer_policy="$(printf '%s' "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"sqs:GetQueueUrl\",\"sqs:GetQueueAttributes\",\"sqs:SendMessage\",\"sqs:SendMessageBatch\"],\"Resource\":\"$input_arn\"}]}")"

for user in wager-runtime wager-producer; do
  awslocal iam get-user --user-name "$user" >/dev/null 2>&1 || awslocal iam create-user --user-name "$user" >/dev/null
done
awslocal iam put-user-policy --user-name wager-runtime --policy-name wager-runtime-sqs --policy-document "$runtime_policy"
awslocal iam put-user-policy --user-name wager-producer --policy-name wager-producer-sqs --policy-document "$producer_policy"

ensure_credentials() {
  profile=$1
  user=$2
  credentials_dir="/var/lib/localstack/${profile}-credentials"
  credentials_file="$credentials_dir/credentials"
  access_key=
  if [ -s "$credentials_file" ]; then
    access_key="$(awk -F ' *= *' '/^aws_access_key_id/ { print $2; exit }' "$credentials_file")"
  fi
  if [ -n "$access_key" ] && awslocal iam list-access-keys --user-name "$user" \
    --query "AccessKeyMetadata[?AccessKeyId=='$access_key'].AccessKeyId | [0]" \
    --output text | grep -qx "$access_key"; then
    return
  fi

  mkdir -p "$credentials_dir"
  key="$(awslocal iam create-access-key --user-name "$user" --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)"
  set -- $key
  umask 077
  chmod 0600 "$credentials_file" 2>/dev/null || true
  printf '[%s]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
    "$profile" "$1" "$2" > "$credentials_file"
  chmod 0444 "$credentials_file"
}

ensure_credentials runtime wager-runtime
ensure_credentials producer wager-producer

echo "Provisioned FIFO queues and least-privilege runtime/producer identities"
