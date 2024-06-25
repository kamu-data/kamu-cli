#!/bin/sh
# Pulls some of these pipeline's root datasets from IPFS
set -e

kamu init
	
# Improves the speed of IPFS resolution through "proximity" to pins
kamu config set protocol.ipfs.httpGateway "https://kamu.infura-ipfs.io"

kamu pull "ipns://net.rocketpool.reth.tokens-minted.ipns.kamu.dev"
kamu pull "ipns://net.rocketpool.reth.tokens-burned.ipns.kamu.dev"
kamu pull "ipns://co.alphavantage.tickers.daily.spy.ipns.kamu.dev"
kamu pull "ipns://com.cryptocompare.ohlcv.eth-usd.ipns.kamu.dev"

kamu add . -r
