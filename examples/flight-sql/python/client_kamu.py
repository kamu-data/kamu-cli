import kamu

# See more examples at: https://github.com/kamu-data/kamu-client-python

# Secure remote connection
con = kamu.connect(
    "grpc+tls://node.demo.kamu.dev:50050",
    # Registered users can provide a bearer token
    # token="<kamu-token>",
)

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --port 50050 --address 0.0.0.0
#
# con = kamu.connect(
#     "grpc://localhost:50050",
#     # Registered users can provide a bearer token
#     # token="<kamu-token>",
# )

with con:
    df = con.query("select 1 as value")
    print(df)

    df = con.query("show tables")
    print(df)

    df = con.query("select * from 'kamu/co.alphavantage.tickers.daily.spy' limit 10")
    print(df)
