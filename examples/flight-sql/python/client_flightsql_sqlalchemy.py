import flightsql.sqlalchemy
import sqlalchemy.engine
import pandas as pd


engine = sqlalchemy.engine.create_engine("datafusion+flightsql://kamu:kamu@localhost:50050?insecure=True")

df = pd.read_sql("show tables", engine)
print(df)

df = pd.read_sql("select * from 'co.alphavantage.tickers.daily.spy' limit 10", engine)
print(df)
