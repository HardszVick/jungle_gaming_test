#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose="$root_dir/scripts/compose.sh"
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
[[ -f "$env_file" ]] || { printf 'error: environment file not found: %s\n' "$env_file" >&2; exit 2; }

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

: "${APP_HTTP_PORT:=8080}"
: "${KEYCLOAK_HOST_PORT:=8081}"
: "${SQS_INPUT_QUEUE_NAME:=wager-transactions.fifo}"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

sql() {
  "$compose" exec -T -e PGPASSWORD="$APP_DATABASE_PASSWORD" postgres \
    psql -X -v ON_ERROR_STOP=1 -Atq -U "$APP_DATABASE_USER" -d "$APP_DATABASE_NAME" -c "$1"
}

wait_for_sql() {
  local query="$1" expected="$2" description="$3" deadline actual
  deadline=$((SECONDS + 90))
  while ((SECONDS < deadline)); do
    actual="$(sql "$query")"
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  die "timed out waiting for $description (expected $expected, got ${actual:-<empty>})"
}

assert_sql() {
  local query="$1" expected="$2" description="$3" actual
  actual="$(sql "$query")"
  [[ "$actual" == "$expected" ]] || die "$description (expected $expected, got ${actual:-<empty>})"
}

app_url() {
  local address
  address="$("$compose" port --index "$1" app "$APP_HTTP_PORT")"
  [[ -n "$address" ]] || die "could not resolve HTTP port for app replica $1"
  printf 'http://127.0.0.1:%s\n' "${address##*:}"
}

get_token() {
  local client_id="$1" secret="$2"
  curl --fail --silent --show-error \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode "client_id=$client_id" \
    --data-urlencode "client_secret=$secret" \
    "$token_url" | jq -er '.access_token | select(length > 0)'
}

wait_for_clean_exit() {
  local container="$1" deadline status exit_code
  deadline=$((SECONDS + 45))
  while ((SECONDS < deadline)); do
    status="$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null || true)"
    if [[ "$status" == "exited" ]]; then
      exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container")"
      [[ "$exit_code" == 0 ]] || die "app replica exited with code $exit_code after SIGTERM"
      return
    fi
    sleep 1
  done
  die "app replica did not exit within its graceful shutdown deadline"
}

require_command curl
require_command docker
require_command jq
[[ -x "$compose" ]] || die "compose wrapper is not executable: $compose"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
run_id="smoke-$(date -u +%Y%m%dT%H%M%SZ)-$$"
player_id="$(< /proc/sys/kernel/random/uuid)"

printf 'Starting three application replicas for run %s...\n' "$run_id"
"$compose" up --build -d --wait --scale app=3

mapfile -t containers < <("$compose" ps -q app)
[[ "${#containers[@]}" -eq 3 ]] || die "expected exactly 3 app containers, found ${#containers[@]}"

api_urls=()
for index in 1 2 3; do
  api_urls+=("$(app_url "$index")")
  curl --fail --silent --show-error "${api_urls[$((index - 1))]}/health/live" >/dev/null
  curl --fail --silent --show-error "${api_urls[$((index - 1))]}/health/ready" >/dev/null
done
printf 'Verified liveness and readiness through all three published ports.\n'

token_url="http://127.0.0.1:${KEYCLOAK_HOST_PORT}/realms/wager/protocol/openid-connect/token"
internal_token="$(get_token internal-service "$INTERNAL_SERVICE_CLIENT_SECRET")"
provider_token="$(get_token provider-a "$PROVIDER_A_CLIENT_SECRET")"

wallet_body="$(jq -nc --arg player "$player_id" '{playerId:$player,initialBalance:{amount:"100.00",currency:"BRL"}}')"
wallet_response="$(curl --fail --silent --show-error -X POST "${api_urls[0]}/wallets" \
  -H "Authorization: Bearer $internal_token" -H 'Content-Type: application/json' -d "$wallet_body")"
wallet_id="$(jq -er '.id' <<<"$wallet_response")"
jq -e '.balance.amount == "100.00" and .balance.currency == "BRL" and .version == 1' \
  <<<"$wallet_response" >/dev/null || die "wallet opening response did not match the expected initial state"

submit_bet() {
  local api_url="$1" suffix="$2"
  local body
  body="$(jq -nc --arg run "$run_id" --arg player "$player_id" --arg wallet "$wallet_id" --arg suffix "$suffix" '{
    providerId:"provider-a", externalTransactionId:("ext-bet-"+$run+"-"+$suffix),
    playerId:$player, walletId:$wallet, roundId:("round-"+$run), gameId:"game-smoke",
    kind:"BET", money:{amount:"80.00",currency:"BRL"}
  }')"
  curl --silent --show-error -o "$tmp_dir/bet-$suffix.json" -w '%{http_code}' \
    -X POST "$api_url/wagering/transactions" \
    -H "Authorization: Bearer $provider_token" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: idem-bet-$run_id-$suffix" -d "$body" >"$tmp_dir/bet-$suffix.status"
}

submit_bet "${api_urls[1]}" a &
bet_a_pid=$!
submit_bet "${api_urls[2]}" b &
bet_b_pid=$!
wait "$bet_a_pid"
wait "$bet_b_pid"

mapfile -t bet_statuses < <(printf '%s\n' "$(<"$tmp_dir/bet-a.status")" "$(<"$tmp_dir/bet-b.status")" | sort)
[[ "${bet_statuses[*]}" == "200 409" ]] || die "concurrent bets returned HTTP ${bet_statuses[*]}, expected 200 and 409"
processed_count="$(jq -s '[.[] | select(.status == "PROCESSED" and .balance.amount == "20.00")] | length' "$tmp_dir"/bet-*.json)"
rejected_count="$(jq -s '[.[] | select(.status == "REJECTED" and .failureCode == "INSUFFICIENT_FUNDS")] | length' "$tmp_dir"/bet-*.json)"
[[ "$processed_count" == 1 && "$rejected_count" == 1 ]] || die "concurrent bet results did not preserve the 100/80/80 invariant"

message_id="message-$run_id"
sqs_external_id="ext-sqs-$run_id"
occurred_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
message_body="$(jq -nc \
  --arg message "$message_id" --arg occurred "$occurred_at" --arg run "$run_id" \
  --arg player "$player_id" --arg wallet "$wallet_id" --arg external "$sqs_external_id" '{
    messageId:$message, type:"WagerTransactionRequested", occurredAt:$occurred,
    data:{idempotencyKey:("idem-sqs-"+$run),providerId:"provider-a",externalTransactionId:$external,
      playerId:$player,walletId:$wallet,roundId:("round-sqs-"+$run),gameId:"game-smoke",
      kind:"WIN",money:{amount:"5.00",currency:"BRL"}}
  }')"
input_queue_url="$("$compose" exec -T localstack awslocal sqs get-queue-url \
  --queue-name "$SQS_INPUT_QUEUE_NAME" --query QueueUrl --output text)"
"$compose" exec -T \
  -e AWS_PROFILE=producer \
  -e AWS_SHARED_CREDENTIALS_FILE=/var/lib/localstack/producer-credentials/credentials \
  localstack awslocal sqs send-message \
  --queue-url "$input_queue_url" --message-body "$message_body" \
  --message-group-id "$wallet_id" --message-deduplication-id "$message_id" >/dev/null

wait_for_sql "SELECT count(*) FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id='$sqs_external_id' AND state='PROCESSED';" 1 "SQS transaction commit"
wait_for_sql "SELECT count(*) FROM inbox_messages WHERE message_id='$message_id' AND completed_at IS NOT NULL;" 1 "completed inbox"
wait_for_sql "SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'walletId'='$wallet_id' AND published_at IS NULL;" 0 "outbox publication"

assert_sql "SELECT balance_minor FROM wallets WHERE id='$wallet_id';" 2500 "final wallet balance is not 25.00"
assert_sql "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id='$wallet_id';" 3 "expected opening, bet, and SQS win ledger entries"
assert_sql "SELECT count(*) FROM wager_transactions WHERE wallet_id='$wallet_id';" 4 "expected opening and three external transaction records"
assert_sql "SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'walletId'='$wallet_id';" 7 "expected seven financial events"
assert_sql "SELECT count(*) FROM outbox_events WHERE envelope->'data'->>'walletId'='$wallet_id' AND published_at IS NOT NULL;" 7 "all smoke events were not published"

reconciliation="$(curl --fail --silent --show-error -X POST \
  -H "Authorization: Bearer $internal_token" "${api_urls[0]}/wallets/$wallet_id/reconciliation")"
jq -e --arg wallet "$wallet_id" '.walletId == $wallet and .consistent == true and .storedBalance.amount == "25.00" and .calculatedBalance.amount == "25.00" and .difference.amount == "0.00" and .checkedEntries == 3' \
  <<<"$reconciliation" >/dev/null || die "HTTP reconciliation did not match the ledger"

victim="${containers[0]}"
printf 'Sending SIGTERM to one replica and waiting for a clean exit...\n'
docker kill --signal TERM "$victim" >/dev/null
wait_for_clean_exit "$victim"
"$compose" up -d --wait --scale app=3

printf 'Restarting the complete stack while preserving volumes...\n'
"$compose" stop
"$compose" up -d --wait --scale app=3
api_after_restart="$(app_url 1)"
internal_token="$(get_token internal-service "$INTERNAL_SERVICE_CLIENT_SECRET")"
wallet_after_restart="$(curl --fail --silent --show-error \
  -H "Authorization: Bearer $internal_token" "$api_after_restart/wallets/$wallet_id")"
jq -e --arg wallet "$wallet_id" '.id == $wallet and .balance.amount == "25.00" and .balance.currency == "BRL"' <<<"$wallet_after_restart" >/dev/null \
  || die "wallet state was not preserved across full restart"
reconciliation="$(curl --fail --silent --show-error -X POST \
  -H "Authorization: Bearer $internal_token" "$api_after_restart/wallets/$wallet_id/reconciliation")"
jq -e --arg wallet "$wallet_id" '.walletId == $wallet and .consistent == true and .storedBalance.amount == "25.00" and .calculatedBalance.amount == "25.00" and .checkedEntries == 3' \
  <<<"$reconciliation" >/dev/null || die "reconciliation failed after full restart"

assert_sql "SELECT count(*) FROM wallets WHERE balance_minor < 0;" 0 "a wallet has a negative balance"
assert_sql "SELECT count(*) FROM (SELECT wallet_id,transaction_id FROM wallet_ledger_entries GROUP BY wallet_id,transaction_id HAVING count(*) <> 1) duplicates;" 0 "duplicate ledger identities detected"
assert_sql "SELECT count(*) FROM wallet_ledger_entries WHERE (direction='CREDIT' AND balance_after_minor<>balance_before_minor+amount_minor) OR (direction='DEBIT' AND balance_after_minor<>balance_before_minor-amount_minor);" 0 "invalid ledger equation detected"

printf 'Distributed smoke passed: wallet=%s balance=25.00 ledgerEntries=3 eventsPublished=7\n' "$wallet_id"
