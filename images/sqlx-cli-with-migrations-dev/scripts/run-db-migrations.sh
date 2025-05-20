#!/usr/bin/env bash

set -euo pipefail

if [ -z "$DB_CONNECTION_STRING" ]; then
  echo "Error: DB_CONNECTION_STRING is not defined."
  exit 1
fi

if [ $# -ne 1 ]; then
  echo "Usage: $0 <migrations source directory>"
  exit 1
fi

SOURCE_DIR="$1"

sqlx migrate run --source "$SOURCE_DIR" --database-url "$DB_CONNECTION_STRING"
