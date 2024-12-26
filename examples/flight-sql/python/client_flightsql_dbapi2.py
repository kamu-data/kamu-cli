from flightsql import connect, FlightSQLClient

# Secure remote connection
client = FlightSQLClient(
    host="node.demo.kamu.dev",
    port=50050,
    # Anonymous users have to authenticate using basic auth so they could be assigned a session token
    user="anonymous",
    password="anonymous",
    # Registered users can provide a bearer token
    # token="<kamu-token>",
)

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --port 50050 --address 0.0.0.0
#
# client = FlightSQLClient(
#     host="localhost",
#     port=50050,
#     insecure=True,
#     # Anonymous users have to authenticate using basic auth so they could be assigned a session token
#     user="anonymous",
#     password="anonymous",
#     # Registered users can provide a bearer token
#     # token="<kamu-token>",
# )

con = connect(client)
cursor = con.cursor()

cursor.execute("select 1 as value")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])

cursor.execute("show tables")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])

cursor.execute("select * from 'kamu/co.alphavantage.tickers.daily.spy' limit 10")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])


# Feeding query results to Pandas
import pandas

df = pandas.read_sql("show tables", con)
print(df)

df = pandas.read_sql("select * from 'kamu/co.alphavantage.tickers.daily.spy' limit 10", con)
print(df)
