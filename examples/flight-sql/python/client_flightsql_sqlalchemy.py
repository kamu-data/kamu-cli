import flightsql.sqlalchemy
import sqlalchemy
import pandas as pd

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --flight-sql --port 50050 --address 0.0.0.0
#
engine = sqlalchemy.create_engine(
    "datafusion+flightsql://kamu:kamu@localhost:50050?insecure=True"
)

# Secure remote connection
# engine = sqlalchemy.create_engine(
#     "datafusion+flightsql://kamu:kamu@node.demo.kamu.dev:50050"
# )

with engine.connect() as con:
    df = pd.read_sql(sql="show tables", con=con.connection)
    print(df)

    df = pd.read_sql(sql="select * from 'co.alphavantage.tickers.daily.spy' limit 10", con=con.connection)
    print(df)
