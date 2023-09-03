#!/bin/sh
set -e

S3_BASE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v1/contrib/"

kamu init || true

# Pull from S3 for speed but then alias to IPFS
kamu pull "${S3_BASE_URL}net.rocketpool.reth.mint-burn" --no-alias
kamu repo alias add --pull net.rocketpool.reth.mint-burn "ipns://net.rocketpool.reth.mint-burn.ipns.kamu.dev"

kamu pull "${S3_BASE_URL}com.cryptocompare.ohlcv.eth-usd" --no-alias
kamu repo alias add --pull com.cryptocompare.ohlcv.eth-usd "ipns://com.cryptocompare.ohlcv.eth-usd.ipns.kamu.dev"

kamu pull "${S3_BASE_URL}co.alphavantage.tickers.daily.spy" --no-alias
kamu repo alias add --pull co.alphavantage.tickers.daily.spy "ipns://co.alphavantage.tickers.daily.spy.ipns.kamu.dev"

kamu pull "${S3_BASE_URL}io.etherscan.account.transactions.0xeadb3840596cabf312f2bc88a4bb0b93a4e1ff5f" --as account.transactions  --no-alias
kamu pull "${S3_BASE_URL}io.etherscan.account.tokens.transfers.0xeadb3840596cabf312f2bc88a4bb0b93a4e1ff5f" --as account.tokens.transfers  --no-alias

kamu add -r .
