import adbc_driver_manager
import adbc_driver_flightsql.dbapi
import pandas

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --flight-sql --port 50050 --address 0.0.0.0
#
con = adbc_driver_flightsql.dbapi.connect(
    "grpc://localhost:50050",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "kamu",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "kamu",
    },
    autocommit=True,
)

# Secure remote connection
# con = adbc_driver_flightsql.dbapi.connect(
#     "grpc+tls://node.demo.kamu.dev:50050",
#     db_kwargs={
#         adbc_driver_manager.DatabaseOptions.USERNAME.value: "kamu",
#         adbc_driver_manager.DatabaseOptions.PASSWORD.value: "kamu",
#     },
#     autocommit=True,
# )

with con:
    df = pandas.read_sql("show tables", con)
    print(df)

    df = pandas.read_sql("select * from 'co.alphavantage.tickers.daily.spy' limit 10", con)
    print(df)
