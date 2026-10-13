#!/bin/bash
set -euo pipefail

for variable in PROVIDER_A_CLIENT_SECRET PROVIDER_B_CLIENT_SECRET INTERNAL_SERVICE_CLIENT_SECRET; do
  value="${!variable:-}"
  if [[ -z "$value" || "$value" == *[^a-zA-Z0-9._-]* ]]; then
    echo "$variable must contain only letters, digits, period, underscore, or hyphen" >&2
    exit 1
  fi
done

mkdir -p /opt/keycloak/data/import
sed \
  -e "s/__PROVIDER_A_CLIENT_SECRET__/${PROVIDER_A_CLIENT_SECRET}/g" \
  -e "s/__PROVIDER_B_CLIENT_SECRET__/${PROVIDER_B_CLIENT_SECRET}/g" \
  -e "s/__INTERNAL_SERVICE_CLIENT_SECRET__/${INTERNAL_SERVICE_CLIENT_SECRET}/g" \
  /opt/keycloak/realm-templates/wager-realm.json \
  > /opt/keycloak/data/import/wager-realm.json

exec /opt/keycloak/bin/kc.sh start-dev --import-realm
