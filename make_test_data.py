"""Write small, source-schema Parquet files for Phase 1 verification."""
from argparse import ArgumentParser
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA = pa.schema([("Date", pa.date32()), ("Ticker", pa.string())] + [
    (name, pa.float64())
    for name in ("Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits")
])


def bar(day, ticker, open_price, close_price, volume):
    return {
        "Date": day, "Ticker": ticker, "Open": open_price,
        "High": max(open_price, close_price) + 2, "Low": min(open_price, close_price) - 2,
        "Close": close_price, "Volume": volume, "Dividends": 0.0, "Stock_Splits": 0.0,
    }


def make_fixtures(directory):
    directory.mkdir(parents=True, exist_ok=True)
    start = date(2024, 1, 2)
    rows = [bar(start + timedelta(days=n), ticker, base + n, base + n + 1, 100.0 + n)
            for n in range(7) for ticker, base in [("AAA", 100), ("BBB", 200)]]
    rows[8]["Volume"] = None
    broken = [
        {**rows[0], "Open": 0.0},
        {**rows[1], "High": rows[1]["Low"] - 1},
        {**rows[2], "Close": None},
        {**rows[3], "Volume": -10.0},
    ]
    for name, items in (("valid.parquet", rows), ("quality_test.parquet", broken)):
        path = directory / name
        if path.exists():
            raise FileExistsError(path)
        pq.write_table(pa.Table.from_pylist(items, schema=SCHEMA), path)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    make_fixtures(parser.parse_args().directory)
