#!/bin/env/python
# This script fetches data from https://api.coinpaprika.com/
# It is only intended for demonstrative purposes.
# See Coinpaprika's API Terms of Use: https://api.coinpaprika.com/#section/Terms-of-use
import requests
import time
import datetime
import logging
import sys
import os
import json

MIN_START_TIME = int(datetime.datetime(2010, 1, 1).timestamp())
MAX_LIMIT = 5000

logger = logging.getLogger()

# See: https://api.coinpaprika.com/#operation/getTickersHistoricalById
def get_historical_ticks(coin_id, start, end, resolution, limit=MAX_LIMIT):
    resp = requests.get(
        f"https://api.coinpaprika.com/v1/tickers/{coin_id}/historical",
        params=dict(
            start=start,
            end=end,
            interval=resolution,
            limit=limit
        )
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise Exception(f"Unexpected response: {data}")

    # TODO: Coinpaprika returns an empty result instead of an error when limit is reached
    # So we have to guess here whether empty response is justified
    if len(data) == 0 and limit > 1 and end - start > 24 * 60 * 60:
        raise Exception("Empty response, probably hit row limit")
    return data


# Pulls data with highest available resolution under the free tier
# See: https://api.coinpaprika.com/#operation/getTickersHistoricalById
def get_historica_ticks_max_res(coin_id, start):
    now = datetime.datetime.utcnow()
    intervals = []

    # last 24h @ 5m resolution
    intervals.append((
        int((now - datetime.timedelta(hours=23, minutes=59)).timestamp()), 
        int(now.timestamp()), 
        "5m",
    ))

    # last 7d @ 1h resolution
    intervals.append((
        int((now - datetime.timedelta(days=6, hours=23)).timestamp()), 
        intervals[-1][0], 
        "1h",
    ))

    # unlimited @ 24h resolution
    intervals.append((
        MIN_START_TIME, 
        intervals[-1][0], 
        "24h",
    ))

    logger.info(
        "Fetching : start=%s, end=%s, intervals=%s", 
        start,
        int(now.timestamp()), 
        intervals,
    )

    data = []
    for (iv_start, iv_end, iv_res) in intervals:
        iv_start = max(start, iv_start)
        data.extend(
            get_historical_ticks(coin_id, iv_start, iv_end, iv_res)
        )
        if iv_start == start:
            break

    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

    last_modified = os.environ["ODF_LAST_MODIFIED"]
    if last_modified:
        last_modified = int(datetime.datetime.strptime(
            last_modified, "%Y-%m-%dT%H:%M:%S%z",
        ).timestamp())
    else:
        last_modified = 0

    ticks = get_historica_ticks_max_res(
        coin_id="eth-ethereum",
        start=last_modified + 1,
    )

    ticks.sort(key=lambda x: x["timestamp"])
    for t in ticks:
        print(json.dumps(t))

    if ticks:
        new_last_modified = ticks[-1]["timestamp"]
        with open(os.environ["ODF_NEW_LAST_MODIFIED_PATH"], 'w') as f:
            f.write(new_last_modified)
