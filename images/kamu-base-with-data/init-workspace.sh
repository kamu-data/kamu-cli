#!/bin/sh

# TODO: FIXME: These dual URLs are needed because:
# - AWS CLI can only list the directory using S3 urls
# - Without AWS auth keys `kamu pull` will fail to init AWS SDK, so we switch to plain HTTP instead
EXTERNAL_URL="s3://datasets.kamu.dev/odf/v2/contrib/"
EXTERNAL_URL_HTTP="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/contrib/"
EXAMPLES_URL="s3://datasets.kamu.dev/odf/v2/example/"
EXAMPLES_URL_HTTP="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/example/"

set -e

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

# External datasets first
kamu pull --no-alias "${EXTERNAL_URL_HTTP}co.alphavantage.tickers.daily.spy"
kamu pull --no-alias "${EXTERNAL_URL_HTTP}com.cryptocompare.ohlcv.eth-usd"
kamu pull --no-alias "${EXTERNAL_URL_HTTP}net.rocketpool.reth.mint-burn"

# Example datasets
datasets=`aws --no-sign-request s3 ls ${EXAMPLES_URL} | awk '{print $2}' | awk -F '/' '/\// {print $1}'`
for name in $datasets; do
    kamu pull --no-alias "${EXAMPLES_URL_HTTP}${name}"
done
