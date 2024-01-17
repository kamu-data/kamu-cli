#!/bin/sh
set -e

S3_BASE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/contrib/"

rm -rf .kamu
kamu init

kamu pull "${S3_BASE_URL}net.rocketpool.reth.mint-burn"
kamu pull "${S3_BASE_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull "${S3_BASE_URL}co.alphavantage.tickers.daily.spy"

kamu add -r datasets/

kamu pull account.transactions account.tokens.transfers
kamu pull --set-watermark `date --iso-8601=s` account.transactions
kamu pull --set-watermark `date --iso-8601=s` account.tokens.transfers

kamu pull --all
