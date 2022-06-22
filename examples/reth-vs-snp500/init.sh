#!/bin/sh
# Pulls some of these pipeline's root datasets from IPFS
set -e

kamu init
	
# Improves the speed of IPFS resolution through "proximity" to pins
kamu config set protocol.ipfs.httpGateway "https://kamu.infura-ipfs.io"

kamu pull "ipns://net.rocketpool.reth.mint-burn.ipns.kamu.dev" --as rocketpool.reth.mint-burn
kamu pull "ipns://co.alphavantage.tickers.daily.spy.ipns.kamu.dev" --as alphavantage.tickers.daily.spy
kamu pull "ipns://com.cryptocompare.ohlcv.eth-usd.ipns.kamu.dev" --as cryptocompare.ohlcv.eth-usd

kamu add . -r
