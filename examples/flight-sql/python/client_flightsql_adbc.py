import adbc_driver_manager
import adbc_driver_flightsql.dbapi
import pandas

with adbc_driver_flightsql.dbapi.connect(
    "grpc://localhost:50050",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "kamu",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: "kamu",
    },
    autocommit=True,
) as con:
    df = pandas.read_sql("show tables", con)
    print(df)

    df = pandas.read_sql("select * from 'co.alphavantage.tickers.daily.spy' limit 10", con)
    print(df)
