#!/bin/sh
set -e

kamu init || true

# Pull from S3 for speed but then alias to IPFS
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/net.rocketpool.reth.mint-burn" --no-alias
kamu repo alias add --pull net.rocketpool.reth.mint-burn "ipns://net.rocketpool.reth.mint-burn.ipns.kamu.dev"

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/com.cryptocompare.ohlcv.eth-usd" --no-alias
kamu repo alias add --pull com.cryptocompare.ohlcv.eth-usd "ipns://com.cryptocompare.ohlcv.eth-usd.ipns.kamu.dev"

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/co.alphavantage.tickers.daily.spy" --no-alias
kamu repo alias add --pull co.alphavantage.tickers.daily.spy "ipns://co.alphavantage.tickers.daily.spy.ipns.kamu.dev"

kamu add -r .

kamu pull account.transactions account.tokens.transfers
kamu pull --set-watermark `date --iso-8601=s` account.transactions
kamu pull --set-watermark `date --iso-8601=s` account.tokens.transfers

kamu pull --all
