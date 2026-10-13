#!/bin/sh
set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
replicas="${APP_REPLICAS:-3}"

case "$replicas" in
  ''|*[!0-9]*) echo "APP_REPLICAS must be a positive integer" >&2; exit 2 ;;
esac
if [ "$replicas" -lt 3 ]; then
  echo "Distributed mode requires at least three application replicas" >&2
  exit 2
fi

exec "$script_dir/compose.sh" up --build -d --wait --scale app="$replicas"
