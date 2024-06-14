#!/bin/sh
set -e

S3_CONTRIB_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/contrib/"
S3_EXAMPLE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/example/"

kamu init || true

# Pull from S3 for speed but then alias to IPFS
kamu pull "${S3_CONTRIB_URL}net.rocketpool.reth.tokens-minted" --no-alias
kamu repo alias add --pull net.rocketpool.reth.tokens-minted "ipns://net.rocketpool.reth.tokens-minted.ipns.kamu.dev"

kamu pull "${S3_CONTRIB_URL}net.rocketpool.reth.tokens-burned" --no-alias
kamu repo alias add --pull net.rocketpool.reth.tokens-burned "ipns://net.rocketpool.reth.tokens-burned.ipns.kamu.dev"

kamu pull "${S3_CONTRIB_URL}com.cryptocompare.ohlcv.eth-usd" --no-alias
kamu repo alias add --pull com.cryptocompare.ohlcv.eth-usd "ipns://com.cryptocompare.ohlcv.eth-usd.ipns.kamu.dev"

kamu pull "${S3_CONTRIB_URL}co.alphavantage.tickers.daily.spy" --no-alias
kamu repo alias add --pull co.alphavantage.tickers.daily.spy "ipns://co.alphavantage.tickers.daily.spy.ipns.kamu.dev"

kamu pull "${S3_EXAMPLE_URL}account.transactions" --no-alias
kamu pull "${S3_EXAMPLE_URL}account.tokens.transfers" --no-alias

kamu add -r .
