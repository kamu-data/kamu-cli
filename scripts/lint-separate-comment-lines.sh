#!/usr/bin/env bash

set -euo pipefail

if grep -Ernxo -e "/{4,79}" -e "/{81,}" ./src/; then
    echo "⚠️ Found lines other than 80 characters long!"
    exit 1
fi
