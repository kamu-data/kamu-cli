from flightsql import connect, FlightSQLClient

client = FlightSQLClient(
    host='localhost', 
    port=50050, 
    user='kamu', 
    password='kamu', 
    insecure=True,
)
con = connect(client)
cursor = con.cursor()

cursor.execute("show tables")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])

cursor.execute("select * from 'co.alphavantage.tickers.daily.spy' limit 10")
print("columns:", cursor.description)
print("rows:", [r for r in cursor])



import pandas

df = pandas.read_sql("show tables", con)
print(df)

df = pandas.read_sql("select * from 'co.alphavantage.tickers.daily.spy' limit 10", con)
print(df)
