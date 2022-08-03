ARG KAMU_VERSION

FROM kamudata/kamu-base:$KAMU_VERSION

RUN kamu init && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/alberta.case-details.hm" --as alberta.case-details.hm && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/alberta.case-details" --as alberta.case-details && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/british-columbia.case-details.hm" --as british-columbia.case-details.hm && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/british-columbia.case-details" --as british-columbia.case-details && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/canada.case-details" --as canada.case-details && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/canada.daily-cases" --as canada.daily-cases && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/co.alphavantage.tickers.daily.spy" --as co.alphavantage.tickers.daily.spy && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/com.cryptocompare.ohlcv.eth-usd" --as com.cryptocompare.ohlcv.eth-usd && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/net.rocketpool.reth.mint-burn" --as net.rocketpool.reth.mint-burn && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/ontario.case-details.hm" --as ontario.case-details.hm && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/ontario.case-details" --as ontario.case-details && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/quebec.case-details.hm" --as quebec.case-details.hm && \
    kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/quebec.case-details" --as quebec.case-details
