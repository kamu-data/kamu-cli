#!/bin/bash

set -euo pipefail

if [ $# -ne 6 ]; then
    echo "Usage: $0 <secret-name> <region> <db-provider> <db-host> <db-port> <db-name>"
    exit 1
fi

SECRET_NAME="$1"
REGION="$2"
DB_PROVIDER="$3"
DB_HOST="$4"
DB_PORT="$5"
DB_NAME="$6"

SECRET_STRING=$(aws secretsmanager get-secret-value --secret-id $SECRET_NAME --region $REGION --query SecretString --output text)
USERNAME=$(echo $SECRET_STRING | jq -r .username)
PASSWORD=$(echo $SECRET_STRING | jq -r .password)

urlencode() {
    printf %s "$1" | jq -sRr @uri
}
ENCODED_USERNAME=$(urlencode "${USERNAME}")
ENCODED_PASSWORD=$(urlencode "${PASSWORD}")

export DB_CONNECTION_STRING="${DB_PROVIDER}://${ENCODED_USERNAME}:${ENCODED_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
