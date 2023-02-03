#!/bin/sh
set -e

kamu init || true

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/net.rocketpool.reth.mint-burn"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/com.cryptocompare.ohlcv.eth-usd"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/co.alphavantage.tickers.daily.spy"

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/io.etherscan.account.transactions.0xeadb3840596cabf312f2bc88a4bb0b93a4e1ff5f" --as account.transactions
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/io.etherscan.account.tokens.transfers.0xeadb3840596cabf312f2bc88a4bb0b93a4e1ff5f" --as account.tokens.transfers
