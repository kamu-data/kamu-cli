import adbc_driver_manager
import adbc_driver_flightsql.dbapi
import pandas

# # Secure remote connection
con = adbc_driver_flightsql.dbapi.connect(
    "grpc+tls://node.demo.kamu.dev:50050",
    db_kwargs={
        # Anonymous users have to authenticate using basic auth so they could be assigned a session token
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "anonymous",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "anonymous",
        # Registered users can provide a bearer token directy
        # adbc_driver_flightsql.DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer <token>",
    },
    autocommit=True,
)

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --port 50050 --address 0.0.0.0
#
# con = adbc_driver_flightsql.dbapi.connect(
#     "grpc://localhost:50050",
#     db_kwargs={
#         # Anonymous users have to authenticate using basic auth so they could be assigned a session token
#         adbc_driver_manager.DatabaseOptions.USERNAME.value: "anonymous",
#         adbc_driver_manager.DatabaseOptions.PASSWORD.value: "anonymous",
#         # Registered users can provide a bearer token directy
#         # adbc_driver_flightsql.DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer <kamu-token>",
#     },
#     autocommit=True,
# )

with con:
    df = pandas.read_sql("select 1", con)
    print(df)

    df = pandas.read_sql("show tables", con)
    print(df)

    df = pandas.read_sql("select * from 'kamu/co.alphavantage.tickers.daily.spy' limit 10", con)
    print(df)
