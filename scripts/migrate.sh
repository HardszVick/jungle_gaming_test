#!/bin/sh
set -eu

direction="${1:-up}"
case "$direction" in
  up|down) ;;
  *) echo "Usage: $0 [up|down]" >&2; exit 2 ;;
esac

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec "$script_dir/compose.sh" run --rm migrations migrate "$direction"
