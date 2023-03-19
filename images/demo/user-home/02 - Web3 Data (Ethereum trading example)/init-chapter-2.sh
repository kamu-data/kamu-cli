#!/bin/sh
set -e

rm -rf .kamu
kamu init

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/net.rocketpool.reth.mint-burn"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/com.cryptocompare.ohlcv.eth-usd"

kamu add \
    datasets/account.tokens.portfolio.yaml \
    datasets/account.tokens.portfolio.market-value.yaml \
    datasets/account.tokens.portfolio.usd.yaml \
    datasets/account.tokens.transfers.yaml \
    datasets/account.transactions.yaml

kamu pull --all
