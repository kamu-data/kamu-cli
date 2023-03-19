#!/bin/sh
set -e

rm -rf .kamu
kamu init

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/net.rocketpool.reth.mint-burn"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/com.cryptocompare.ohlcv.eth-usd"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/co.alphavantage.tickers.daily.spy"

kamu add -r datasets/

kamu pull account.transactions account.tokens.transfers
kamu pull --set-watermark `date --iso-8601=s` account.transactions
kamu pull --set-watermark `date --iso-8601=s` account.tokens.transfers

kamu pull --all
