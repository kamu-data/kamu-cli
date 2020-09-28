"""
Fetches historical stock prices
"""
import yfinance


tickers = ["SPY", "IPO"]
start = "2016-01-01"
end = "2020-10-01"

data = yfinance.download(
    tickers=tickers,
    interval="1d",
    start=start,
    end=end)

reshaped = data.unstack().unstack(0).reset_index().rename(
    columns={"level_0": "Symbol"}
).set_index("Date").sort_index()

reshaped.to_csv(f"tickers.csv")
