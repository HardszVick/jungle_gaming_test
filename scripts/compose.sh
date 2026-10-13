#!/bin/sh
set -eu

root_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
env_file="${ENV_FILE:-}"

if [ -z "$env_file" ]; then
  if [ -f "$root_dir/.env" ]; then
    env_file="$root_dir/.env"
  else
    env_file="$root_dir/.env.example"
  fi
elif [ "${env_file#/}" = "$env_file" ]; then
  env_file="$root_dir/$env_file"
fi

if [ ! -f "$env_file" ]; then
  echo "Environment file not found: $env_file" >&2
  exit 1
fi

exec docker compose --project-directory "$root_dir" --env-file "$env_file" "$@"
