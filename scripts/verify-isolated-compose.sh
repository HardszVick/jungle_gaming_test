#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose="$root_dir/scripts/compose.sh"
source_env="${ENV_FILE:-$root_dir/.env}"
[[ -f "$source_env" ]] || source_env="$root_dir/.env.example"
[[ -f "$source_env" ]] || {
  printf 'error: environment file not found\n' >&2
  exit 2
}

set -a
# shellcheck disable=SC1090
source "$source_env"
set +a

export COMPOSE_PROJECT_NAME="${ISOLATED_COMPOSE_PROJECT_NAME:-wager-gate-$(date -u +%Y%m%d%H%M%S)-$$}"
export POSTGRES_HOST_PORT="${ISOLATED_POSTGRES_HOST_PORT:-55432}"
export KEYCLOAK_HOST_PORT="${ISOLATED_KEYCLOAK_HOST_PORT:-58081}"
export KEYCLOAK_HTTP_PORT="$KEYCLOAK_HOST_PORT"
export LOCALSTACK_HOST_PORT="${ISOLATED_LOCALSTACK_HOST_PORT:-54566}"
export KEYCLOAK_PUBLIC_URL="http://keycloak.localhost:$KEYCLOAK_HOST_PORT"
export OIDC_ISSUER_URL="$KEYCLOAK_PUBLIC_URL/realms/wager"
empty_env="$(mktemp)"
export ENV_FILE="$empty_env"

cleanup() {
  local status=$?
  if ((status != 0)); then
    "$compose" ps >&2 || true
    "$compose" logs --no-color app keycloak >&2 || true
  fi
  "$compose" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -f "$empty_env"
  return "$status"
}
trap cleanup EXIT

expect_runtime_denied() {
  local statement="$1" description="$2" output
  if output="$("$compose" exec -T -e PGPASSWORD="$APP_DATABASE_PASSWORD" postgres \
    psql -X -v ON_ERROR_STOP=1 -U "$APP_DATABASE_USER" -d "$APP_DATABASE_NAME" \
    -c "$statement" 2>&1)"; then
    printf 'error: runtime unexpectedly allowed PostgreSQL %s\n' "$description" >&2
    exit 1
  fi
  [[ "$output" == *"permission denied"* ]] || {
    printf 'error: runtime %s failed without permission denial: %s\n' "$description" "$output" >&2
    exit 1
  }
}

started=$SECONDS
printf 'Starting isolated Compose project %s...\n' "$COMPOSE_PROJECT_NAME"
"$compose" up -d --wait postgres keycloak localstack
"$compose" run --rm --build --no-deps migrations
"$root_dir/scripts/verify-local-infra.sh"
"$root_dir/scripts/verify-sqs-iam.sh"

schema_version="$("$compose" exec -T -e PGPASSWORD="$APP_DATABASE_PASSWORD" postgres \
  psql -X -v ON_ERROR_STOP=1 -Atq -U "$APP_DATABASE_USER" -d "$APP_DATABASE_NAME" \
  -c 'SELECT max(version) FROM schema_migrations;')"
[[ "$schema_version" == "3" ]] || {
  printf 'error: isolated schema version is %s, want 3\n' "${schema_version:-<empty>}" >&2
  exit 1
}

expect_runtime_denied 'CREATE TABLE runtime_ddl_probe(id integer);' 'DDL'
expect_runtime_denied 'UPDATE wallet_ledger_entries SET amount_minor=amount_minor;' 'ledger UPDATE'
expect_runtime_denied 'DELETE FROM wallet_ledger_entries;' 'ledger DELETE'
expect_runtime_denied 'TRUNCATE wallet_ledger_entries;' 'ledger TRUNCATE'

"$compose" up --build -d --wait --scale app=3
"$root_dir/scripts/verify-distributed-scenarios.sh"
printf 'Isolated Compose verification passed in %ss; project and volumes will be removed.\n' "$((SECONDS - started))"
