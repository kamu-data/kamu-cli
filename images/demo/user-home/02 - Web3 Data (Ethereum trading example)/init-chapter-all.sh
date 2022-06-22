#!/bin/sh
set -e

rm -rf .kamu
kamu init

# Pull from S3 for speed but then alias to IPFS
kamu -v pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/net.rocketpool.reth.mint-burn" --as rocketpool.reth.mint-burn --no-alias
kamu repo alias add --pull rocketpool.reth.mint-burn "ipns://net.rocketpool.reth.mint-burn.ipns.kamu.dev"

kamu -v pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/com.cryptocompare.ohlcv.eth-usd" --as cryptocompare.ohlcv.eth-usd --no-alias
kamu repo alias add --pull cryptocompare.ohlcv.eth-usd "ipns://com.cryptocompare.ohlcv.eth-usd.ipns.kamu.dev"

kamu -v pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/co.alphavantage.tickers.daily.spy" --as alphavantage.tickers.daily.spy --no-alias
kamu repo alias add --pull alphavantage.tickers.daily.spy "ipns://co.alphavantage.tickers.daily.spy.ipns.kamu.dev"

kamu add -r datasets/

kamu pull account.transactions account.tokens.transfers
kamu pull --set-watermark `date --iso-8601=s` account.transactions
kamu pull --set-watermark `date --iso-8601=s` account.tokens.transfers

kamu pull --all
