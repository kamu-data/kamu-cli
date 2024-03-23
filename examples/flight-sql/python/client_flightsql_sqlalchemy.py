import flightsql.sqlalchemy
import sqlalchemy
import pandas as pd


engine = sqlalchemy.create_engine("datafusion+flightsql://kamu:kamu@localhost:50050?insecure=True")

with engine.connect() as con:
    df = pd.read_sql(sql="show tables", con=con.connection)
    print(df)

    df = pd.read_sql(sql="select * from 'co.alphavantage.tickers.daily.spy' limit 10", con=con.connection)
    print(df)
