#!/bin/sh

EXTERNAL_URL=s3://datasets.kamu.dev/
EXAMPLES_URL=s3://datasets.kamu.dev/example/

set -e


kamu init || true

# External datasets first
kamu pull --no-alias "${EXTERNAL_URL}co.alphavantage.tickers.daily.spy"
kamu pull --no-alias "${EXTERNAL_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull --no-alias "${EXTERNAL_URL}net.rocketpool.reth.mint-burn"

# Example datasets
datasets=`aws s3 ls ${EXAMPLES_URL} | awk '{print $2}' | awk -F '/' '/\// {print $1}'`
for name in $datasets; do
    kamu pull --no-alias "${EXAMPLES_URL}${name}"
done
