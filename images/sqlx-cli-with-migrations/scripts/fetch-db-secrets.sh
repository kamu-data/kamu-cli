#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 7 ]; then
    echo "Usage: $0 <master-db-secret-name> <app-db-secret-name> <region> <db-provider> <db-host> <db-port> <db-name>"
    exit 1
fi

MASTER_DB_SECRET_NAME="$1"
APP_DB_SECRET_NAME="$2"
REGION="$3"
DB_PROVIDER="$4"
DB_HOST="$5"
DB_PORT="$6"
DB_NAME="$7"

# Create DB_CONNECTION_STRING using master credentials

MASTER_DB_SECRET_STRING=$(aws secretsmanager get-secret-value --secret-id $MASTER_DB_SECRET_NAME --region $REGION --query SecretString --output text)
MASTER_DB_USERNAME=$(echo $MASTER_DB_SECRET_STRING | jq -r .username)
MASTER_DB_PASSWORD=$(echo $MASTER_DB_SECRET_STRING | jq -r .password)

urlencode() {
    printf %s "$1" | jq -sRr @uri
}
ENCODED_MASTER_DB_USERNAME=$(urlencode "${MASTER_DB_USERNAME}")
ENCODED_MASTER_DB_PASSWORD=$(urlencode "${MASTER_DB_PASSWORD}")

export DB_CONNECTION_STRING="${DB_PROVIDER}://${ENCODED_MASTER_DB_USERNAME}:${ENCODED_MASTER_DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Export App user credentials as well

APP_DB_SECRET_STRING=$(aws secretsmanager get-secret-value --secret-id $APP_DB_SECRET_NAME --region $REGION --query SecretString --output text)
export APP_DB_USERNAME=$(echo $APP_DB_SECRET_STRING | jq -r .username)
export APP_DB_PASSWORD=$(echo $APP_DB_SECRET_STRING | jq -r .password)
