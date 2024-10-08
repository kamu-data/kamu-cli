#!/bin/sh
set -e

NODE_URL="odf+https://node.demo.kamu.dev/kamu/"

kamu init || true

# Root
kamu pull "${NODE_URL}net.rocketpool.reth.tokens-minted"
kamu pull "${NODE_URL}net.rocketpool.reth.tokens-burned"
kamu pull "${NODE_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull "${NODE_URL}co.alphavantage.tickers.daily.spy"

kamu pull "${NODE_URL}account.transactions"
kamu pull "${NODE_URL}account.tokens.transfers"

kamu add -r .
