#!/bin/sh
set -e

S3_CONTRIB_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/contrib/"
S3_EXAMPLE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/example/"

kamu init || true

# Root
kamu pull "${S3_CONTRIB_URL}net.rocketpool.reth.mint-burn"
kamu pull "${S3_CONTRIB_URL}com.cryptocompare.ohlcv.eth-usd"
kamu pull "${S3_CONTRIB_URL}co.alphavantage.tickers.daily.spy"

kamu pull "${S3_EXAMPLE_URL}account.transactions"
kamu pull "${S3_EXAMPLE_URL}account.tokens.transfers"

# Deriv
kamu pull "${S3_EXAMPLE_URL}account.tokens.portfolio"
kamu pull "${S3_EXAMPLE_URL}account.tokens.portfolio.market-value"
kamu pull "${S3_EXAMPLE_URL}account.tokens.portfolio.usd"
kamu pull "${S3_EXAMPLE_URL}account.whatif.reth-vs-snp500.market-value"
kamu pull "${S3_EXAMPLE_URL}account.whatif.reth-vs-snp500.portfolio"
