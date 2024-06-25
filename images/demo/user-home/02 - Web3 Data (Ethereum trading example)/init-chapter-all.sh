#!/bin/sh
set -e

REPO_BASE_URL="${KAMU_NODE_URL}kamu/"

rm -rf .kamu
kamu init

kamu pull "${REPO_BASE_URL}net.rocketpool.reth.tokens-minted"
kamu pull "${REPO_BASE_URL}net.rocketpool.reth.tokens-burned"
kamu pull "${REPO_BASE_URL}net.rocketpool.reth.mint-burn"
kamu pull "${REPO_BASE_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull "${REPO_BASE_URL}co.alphavantage.tickers.daily.spy"

kamu add -r datasets/

kamu pull account.transactions account.tokens.transfers
kamu pull --set-watermark "$(date --iso-8601=s)" account.transactions
kamu pull --set-watermark "$(date --iso-8601=s)" account.tokens.transfers

kamu pull --all
