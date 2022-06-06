
# Pulls some of these pipeline's root datasets from IPFS
.PHONY: init-with-external
init-with-external:
	kamu init
	
	@# Improves the speed of IPFS resolution through "proximity" to pins
	kamu config set protocol.ipfs.httpGateway "https://gateway.pinata.cloud"

	kamu pull "ipns://net.rocketpool.reth.mint-burn.datasets.kamu.dev" --as rocketpool.reth.mint-burn
	kamu pull "ipns://com.coinpaprika.tickers.eth-usd.datasets.kamu.dev" --as coinpaprika.tickers.eth-usd
	kamu pull "ipns://co.alphavantage.tickers.daily.spy.datasets.kamu.dev" --as alphavantage.tickers.daily.spy
	
	kamu add . -r
