#!/usr/bin/env bash

set -euo pipefail

if [ -z "$DB_CONNECTION_STRING" ]; then
  echo "Error: DB_CONNECTION_STRING is not defined."
  exit 1
fi

if [ -z "$APP_DB_USERNAME" ]; then
  echo "Error: APP_DB_USERNAME is not defined."
  exit 1
fi

if [ -z "$APP_DB_PASSWORD" ]; then
  echo "Error: APP_DB_PASSWORD is not defined."
  exit 1
fi

if [ $# -ne 0 ]; then
  echo "Usage: $0"
  exit 1
fi

psql $DB_CONNECTION_STRING -c "SELECT create_app_user_role_if_not_exists(current_database(), '${APP_DB_USERNAME}', '${APP_DB_PASSWORD}')"
