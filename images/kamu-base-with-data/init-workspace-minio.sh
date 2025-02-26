#!/usr/bin/env sh

CONTRIB_URL=${CONTRIB_URL:-"s3+http://127.0.0.1:9000/contrib/"}
EXAMPLES_URL=${EXAMPLES_URL:-"s3+http://127.0.0.1:9000/example/"}
EXAMPLES_BUCKET_URL=${EXAMPLES_BUCKET_URL:-"s3://example/"}

set -euo pipefail

# Preparing to work with Minio
export AWS_ACCESS_KEY_ID=kamu
export AWS_SECRET_ACCESS_KEY=password
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_SESSION_TOKEN=

BUCKETS=("example" "contrib")
S3_AWS_DIR=./s3-aws/

# In order for Minio to see the data in our buckets, we have to load it through its API
for i in "${!BUCKETS[@]}"; do
    BUCKET="${BUCKETS[$i]}"

    if ! aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
        aws s3 mb "s3://${BUCKET}"
    fi

    aws s3 sync "${S3_AWS_DIR}${BUCKET}" "s3://${BUCKET}/"
done


# Install kamu if its missing (KAMU_VERSION env var has to be set)
if ! command -v kamu &> /dev/null
then
    echo "Installing kamu-cli"
    curl -s "https://get.kamu.dev" | sh
else
    echo "Using pre-installed kamu-cli"
    kamu --version
fi

# Init workspace
kamu init || true

# Contrib datasets first
kamu pull --no-alias "${CONTRIB_URL}co.alphavantage.tickers.daily.spy"
kamu pull --no-alias "${CONTRIB_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull --no-alias "${CONTRIB_URL}net.rocketpool.reth.tokens-minted"
kamu pull --no-alias "${CONTRIB_URL}net.rocketpool.reth.tokens-burned"

# Example datasets
datasets=`aws s3 ls ${EXAMPLES_BUCKET_URL} | awk '{print $2}' | awk -F '/' '/\// {print $1}'`
for name in $datasets; do
    kamu pull --no-alias "${EXAMPLES_URL}${name}"
done
