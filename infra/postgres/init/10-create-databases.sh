#!/bin/sh
set -eu

: "${APP_DATABASE_MIGRATION_USER:?APP_DATABASE_MIGRATION_USER is required}"
: "${APP_DATABASE_MIGRATION_PASSWORD:?APP_DATABASE_MIGRATION_PASSWORD is required}"

app_migration_user=$APP_DATABASE_MIGRATION_USER
app_migration_password=$APP_DATABASE_MIGRATION_PASSWORD

if [ "$app_migration_user" = "$APP_DATABASE_USER" ]; then
  printf '%s\n' 'APP_DATABASE_MIGRATION_USER must differ from APP_DATABASE_USER' >&2
  exit 1
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=app_user="$APP_DATABASE_USER" \
  --set=app_password="$APP_DATABASE_PASSWORD" \
  --set=app_migration_user="$app_migration_user" \
  --set=app_migration_password="$app_migration_password" \
  --set=app_database="$APP_DATABASE_NAME" \
  --set=keycloak_user="$KEYCLOAK_DATABASE_USER" \
  --set=keycloak_password="$KEYCLOAK_DATABASE_PASSWORD" \
  --set=keycloak_database="$KEYCLOAK_DATABASE_NAME" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_migration_user', :'app_migration_password') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'app_database', :'app_migration_user') \gexec
SELECT format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM PUBLIC', :'app_database') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'app_database', :'app_user') \gexec
SELECT format('ALTER DATABASE %I SET app.application_role = %L', :'app_database', :'app_user') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'keycloak_user', :'keycloak_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'keycloak_database', :'keycloak_user') \gexec
SQL
