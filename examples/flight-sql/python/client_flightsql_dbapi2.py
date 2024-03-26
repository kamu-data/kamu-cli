from flightsql import connect, FlightSQLClient

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --flight-sql --port 50050 --address 0.0.0.0
#
client = FlightSQLClient(
    host='localhost',
    port=50050,
    user='kamu',
    password='kamu',
    insecure=True,
)

# Secure remote connection
# client = FlightSQLClient(
#     host='node.demo.kamu.dev',
#     port=50050,
#     user='kamu',
#     password='kamu',
# )

con = connect(client)
cursor = con.cursor()

cursor.execute("show tables")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])

cursor.execute("select * from 'co.alphavantage.tickers.daily.spy' limit 10")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])


# Feeding query results to Pandas
import pandas

df = pandas.read_sql("show tables", con)
print(df)

df = pandas.read_sql("select * from 'co.alphavantage.tickers.daily.spy' limit 10", con)
print(df)
