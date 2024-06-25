#!/bin/sh

EXTERNAL_URL="s3://datasets.kamu.dev/odf/v2/contrib/"
EXAMPLES_URL="s3://datasets.kamu.dev/odf/v2/example/"

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
kamu pull --no-alias "${EXTERNAL_URL}co.alphavantage.tickers.daily.spy"
kamu pull --no-alias "${EXTERNAL_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull --no-alias "${EXTERNAL_URL}net.rocketpool.reth.tokens-minted"
kamu pull --no-alias "${EXTERNAL_URL}net.rocketpool.reth.tokens-burned"

# Example datasets
datasets=`aws s3 ls ${EXAMPLES_URL} | awk '{print $2}' | awk -F '/' '/\// {print $1}'`
for name in $datasets; do
    kamu pull --no-alias "${EXAMPLES_URL}${name}"
done
