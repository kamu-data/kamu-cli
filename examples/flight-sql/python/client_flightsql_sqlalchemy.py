import flightsql.sqlalchemy
import sqlalchemy
import pandas as pd

# Secure remote connection
engine = sqlalchemy.create_engine(
    # Anonymous users have to authenticate using basic auth so they could be assigned a session token
    "datafusion+flightsql://anonymous:anonymous@node.demo.kamu.dev:50050"
    # Registered users can provide a bearer token directy
    # "datafusion+flightsql://node.demo.kamu.dev:50050?token=kamu-token"
)

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --port 50050 --address 0.0.0.0
#
# engine = sqlalchemy.create_engine(
#     # Anonymous users have to authenticate using basic auth so they could be assigned a session token
#     "datafusion+flightsql://anonymous:anonymous@localhost:50050?insecure=True"
#     # Registered users can provide a bearer token directy
#     # "datafusion+flightsql://localhost:50050?insecure=True&token=kamu-token"
# )

with engine.connect() as con:
    df = pd.read_sql(sql="select 1 as value", con=con.connection)
    print(df)

    df = pd.read_sql(sql="show tables", con=con.connection)
    print(df)

    df = pd.read_sql(sql="select * from 'kamu/co.alphavantage.tickers.daily.spy' limit 10", con=con.connection)
    print(df)
