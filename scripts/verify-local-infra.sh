#!/bin/bash
set -euo pipefail

root_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
env_file="${ENV_FILE:-}"
if [[ -z "$env_file" ]]; then
  if [[ -f "$root_dir/.env" ]]; then
    env_file="$root_dir/.env"
  else
    env_file="$root_dir/.env.example"
  fi
elif [[ "$env_file" != /* ]]; then
  env_file="$root_dir/$env_file"
fi

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

compose="$root_dir/scripts/compose.sh"
for queue_name in "$SQS_INPUT_QUEUE_NAME" "$SQS_DLQ_NAME" "$SQS_OUTPUT_QUEUE_NAME"; do
  queue_url="$("$compose" exec -T localstack awslocal sqs get-queue-url \
    --queue-name "$queue_name" --query QueueUrl --output text)"
  attributes="$("$compose" exec -T localstack awslocal sqs get-queue-attributes \
    --queue-url "$queue_url" --attribute-names All)"
  jq -e '.Attributes.FifoQueue == "true" and .Attributes.ContentBasedDeduplication == "false"' \
    <<<"$attributes" >/dev/null
  if [[ "$queue_name" == "$SQS_INPUT_QUEUE_NAME" ]]; then
    jq -e \
      --arg visibility "$SQS_VISIBILITY_TIMEOUT" \
      --arg wait "$SQS_WAIT_TIME_SECONDS" \
      --arg max "$SQS_REDRIVE_MAX_RECEIVE_COUNT" '
        .Attributes.VisibilityTimeout == $visibility and
        .Attributes.ReceiveMessageWaitTimeSeconds == $wait and
        .Attributes.MessageRetentionPeriod == "345600" and
        (.Attributes.RedrivePolicy | fromjson | .maxReceiveCount == $max)
      ' <<<"$attributes" >/dev/null
  elif [[ "$queue_name" == "$SQS_DLQ_NAME" ]]; then
    jq -e '.Attributes.MessageRetentionPeriod == "1209600"' \
      <<<"$attributes" >/dev/null
  else
    jq -e \
      --arg visibility "$SQS_VISIBILITY_TIMEOUT" \
      --arg wait "$SQS_WAIT_TIME_SECONDS" '
        .Attributes.VisibilityTimeout == $visibility and
        .Attributes.ReceiveMessageWaitTimeSeconds == $wait and
        .Attributes.MessageRetentionPeriod == "345600"
      ' <<<"$attributes" >/dev/null
  fi
  echo "Verified queue: $queue_name"
done

token_url="${KEYCLOAK_EXTERNAL_URL:-http://127.0.0.1:${KEYCLOAK_HOST_PORT}}/realms/wager/protocol/openid-connect/token"
clients=(provider-a provider-b internal-service)
secrets=("$PROVIDER_A_CLIENT_SECRET" "$PROVIDER_B_CLIENT_SECRET" "$INTERNAL_SERVICE_CLIENT_SECRET")
for index in "${!clients[@]}"; do
  response="$(curl --fail --silent --show-error \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode "client_id=${clients[$index]}" \
    --data-urlencode "client_secret=${secrets[$index]}" \
    "$token_url")"
  token="$(jq -er '.access_token | select(length > 0)' <<<"$response")"
  payload="${token#*.}"
  payload="${payload%%.*}"
  payload="${payload//-/+}"
  payload="${payload//_/\/}"
  case $((${#payload} % 4)) in
    2) payload+="==" ;;
    3) payload+="=" ;;
  esac
  claims="$(base64 --decode <<<"$payload")"

  client="${clients[$index]}"
  if [[ "$client" == "internal-service" ]]; then
    jq -e --arg client "$client" '
      .azp == $client and
      ((.aud == "backend-api") or (.aud | arrays | index("backend-api") != null)) and
      (.scope | split(" ") | index("wallet:internal") != null) and
      (.realm_access.roles | index("wallet:internal") != null) and
      (has("provider_id") | not)
    ' <<<"$claims" >/dev/null
  else
    jq -e --arg client "$client" '
      .azp == $client and .provider_id == $client and
      ((.aud == "backend-api") or (.aud | arrays | index("backend-api") != null)) and
      (.scope | split(" ") | index("wager:read") != null) and
      (.scope | split(" ") | index("wager:write") != null) and
      (.realm_access.roles | index("wager:read") != null) and
      (.realm_access.roles | index("wager:write") != null)
    ' <<<"$claims" >/dev/null
  fi
  echo "Verified client_credentials token and claims: $client"
done
