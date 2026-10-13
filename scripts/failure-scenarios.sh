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
[[ -f "$env_file" ]] || {
  printf 'error: environment file not found: %s\n' "$env_file" >&2
  exit 2
}

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

: "${SQS_INPUT_QUEUE_NAME:=wager-transactions.fifo}"
: "${SQS_DLQ_NAME:=wager-transactions-dlq.fifo}"
: "${SQS_OUTPUT_QUEUE_NAME:=wager-events.fifo}"

usage() {
  cat <<'USAGE'
Usage: scripts/failure-scenarios.sh <command> [arguments]

Commands:
  up-three                  Build and start the stack with exactly 3 app replicas
  status                    Show Compose services and application containers
  kill-one                  SIGKILL one app replica (restore it with up-three)
  term-one                  SIGTERM one app replica to exercise graceful shutdown
  outage <service> [secs]   Stop postgres, localstack, or keycloak temporarily
  full-restart              Stop and start the stack, preserving named volumes
  queues                    Show attributes for all three local SQS queues
  deterministic-failpoints  Run deterministic consumer/outbox crash-boundary tests

ENV_FILE is honored by scripts/compose.sh. Commands manipulate infrastructure;
they do not assert financial correctness. See docs/test-plan.md.
USAGE
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

first_app_container() {
  local containers container
  containers="$("$compose" ps -q app)"
  [[ -n "$containers" ]] || die "no app replica is running"
  while IFS= read -r container; do
    if [[ -n "$container" ]]; then
      printf '%s\n' "$container"
      return 0
    fi
  done <<<"$containers"
  die "no app replica is running"
}

show_status() {
  local containers container
  "$compose" ps
  containers="$("$compose" ps -q app)"
  if [[ -z "$containers" ]]; then
    printf '\nNo app containers are running.\n'
    return 0
  fi
  printf '\nApplication containers:\n'
  while IFS= read -r container; do
    [[ -n "$container" ]] || continue
    docker inspect --format '{{.Name}} status={{.State.Status}} restartCount={{.RestartCount}} started={{.State.StartedAt}}' "$container"
  done <<<"$containers"
}

temporary_outage() {
  local service="$1"
  local seconds="${2:-15}"
  case "$service" in
    postgres|localstack|keycloak) ;;
    *) die "outage service must be postgres, localstack, or keycloak" ;;
  esac
  case "$seconds" in
    ''|*[!0-9]*) die "outage duration must be a non-negative integer" ;;
  esac

  printf 'Stopping %s for %s seconds...\n' "$service" "$seconds"
  "$compose" stop "$service"
  sleep "$seconds"
  printf 'Starting %s and waiting for health...\n' "$service"
  "$compose" up -d --wait "$service"
  "$compose" ps "$service"
}

show_queues() {
  local queue_name queue_url
  for queue_name in "$SQS_INPUT_QUEUE_NAME" "$SQS_DLQ_NAME" "$SQS_OUTPUT_QUEUE_NAME"; do
    queue_url="$("$compose" exec -T localstack awslocal sqs get-queue-url \
      --queue-name "$queue_name" --query QueueUrl --output text)"
    printf '\n%s (%s)\n' "$queue_name" "$queue_url"
    "$compose" exec -T localstack awslocal sqs get-queue-attributes \
      --queue-url "$queue_url" --attribute-names All
  done
}

run_deterministic_failpoints() {
  local test_pattern
  test_pattern='^(TestConsumerCrashAfterCommitBeforeDeleteRedeliversWithoutDuplicate|TestOutboxCompetingPublishersCrashBoundariesAndUnavailableSQS)$'
  if command -v go >/dev/null 2>&1; then
    (
      cd "$root_dir"
      go test -tags=integration ./integration -run "$test_pattern" -count=1 -v
    )
    return
  fi

  printf 'Go was not found on the host; running the validated Docker wrapper.\n'
  docker run --rm --network host \
    -e TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1 \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$root_dir:/workspace" \
    -w /workspace \
    golang:1.25.0 \
    go test -tags=integration ./integration -run "$test_pattern" -count=1 -v
}

require_command docker
[[ -x "$compose" ]] || die "compose wrapper is not executable: $compose"

command_name="${1:-}"
case "$command_name" in
  up-three)
    "$compose" up --build -d --wait --scale app=3
    show_status
    ;;
  status)
    show_status
    ;;
  kill-one)
    container="$(first_app_container)"
    printf 'Sending SIGKILL to %s.\n' "$container"
    docker kill --signal KILL "$container" >/dev/null
    printf 'Use up-three to restore the replica after inspecting recovery state.\n'
    ;;
  term-one)
    container="$(first_app_container)"
    printf 'Sending SIGTERM to %s.\n' "$container"
    docker kill --signal TERM "$container" >/dev/null
    printf 'Use status to inspect the clean exit and up-three to restore the replica.\n'
    ;;
  outage)
    [[ $# -ge 2 ]] || die "outage requires a service name"
    temporary_outage "$2" "${3:-15}"
    ;;
  full-restart)
    "$compose" stop
    "$compose" up -d --wait --scale app=3
    show_status
    ;;
  queues)
    show_queues
    ;;
  deterministic-failpoints)
    run_deterministic_failpoints
    ;;
  -h|--help|help)
    usage
    ;;
  '')
    usage >&2
    exit 2
    ;;
  *)
    usage >&2
    die "unknown command: $command_name"
    ;;
esac
