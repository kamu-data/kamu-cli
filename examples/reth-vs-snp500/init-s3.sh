#!/bin/sh
set -e

S3_BASE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v1/contrib/"

kamu init || true

kamu pull "${S3_BASE_URL}net.rocketpool.reth.mint-burn"
kamu pull "${S3_BASE_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull "${S3_BASE_URL}co.alphavantage.tickers.daily.spy"

kamu pull "${S3_BASE_URL}io.etherscan.account.transactions.0xeadb3840596cabf312f2bc88a4bb0b93a4e1ff5f" --as account.transactions
kamu pull "${S3_BASE_URL}io.etherscan.account.tokens.transfers.0xeadb3840596cabf312f2bc88a4bb0b93a4e1ff5f" --as account.tokens.transfers

kamu add -r .
