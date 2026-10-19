"""Benchmark the ranking steps on a large synthetic metrics table.

Fills an EMPTY database (from the usual DB_* variables) with 1,200 stocks,
3 ETFs each and 6 years of random metric rows (5.6M rows), then times a first full
ranking, a typical daily ranking, and a dividend-restatement rewrite.

    docker run -d --rm --name rs-bench -e POSTGRES_PASSWORD=b -p 55440:5432 postgres:17-alpine
    DB_PORT=55440 DB_PASSWORD=b DB_NAME=postgres uv run python scripts/benchmark_ranking.py
"""

from __future__ import annotations

import time
from datetime import date

import numpy as np
import pandas as pd

from rs_pipeline import db
from rs_pipeline.config import DbSettings
from rs_pipeline.rs import METRIC_COLUMNS

STOCKS, ETFS_PER_STOCK = 1200, 3


def timed(label: str, func: object, *args: object) -> None:
    start = time.perf_counter()
    result = func(*args)  # type: ignore[operator]
    print(f"{label:<44} {time.perf_counter() - start:8.2f} s   {result} rows")


def main() -> None:
    pool = db.ConnectionPool(DbSettings())
    with pool.connection() as conn:
        db.apply_schema(conn)
        if db.fetch_holdings(conn) or db.last_stock_metric_dates(conn, "E0"):
            raise SystemExit("refusing to run against a database that holds data")
        with conn.cursor() as cur:
            randoms = ", ".join("random() * 200" for _ in METRIC_COLUMNS)
            cur.execute(
                f"""
                INSERT INTO fact_stock_metrics
                    (date, ticker, etf_ticker, {", ".join(METRIC_COLUMNS)})
                SELECT d::date, 'S' || i, 'E' || (i % 30 + j * 30), {randoms}
                FROM generate_series(date '2020-01-01', date '2025-12-31', interval '1 day') d,
                     generate_series(1, {STOCKS}) i, generate_series(0, {ETFS_PER_STOCK - 1}) j
                WHERE extract(isodow FROM d) < 6
                """  # noqa: S608 - constants only
            )
            conn.commit()
            cur.execute("ANALYZE fact_stock_metrics")
        conn.commit()

        timed("first full rank-indices", db.rank_stocks_within_etf, conn)
        timed("first full rank-all", db.rank_stocks_overall, conn)

        rng = np.random.default_rng(1)
        day = pd.DataFrame(
            [
                (date(2026, 1, 2), f"S{i}", f"E{i % 30 + j * 30}")
                for i in range(1, STOCKS + 1)
                for j in range(ETFS_PER_STOCK)
            ],
            columns=["date", "ticker", "etf_ticker"],
        )
        for col in METRIC_COLUMNS:
            day[col] = rng.uniform(50, 150, len(day))
        db.upsert_stock_metrics(conn, day)
        timed("daily rank-indices (one new day)", db.rank_stocks_within_etf, conn)
        timed("daily rank-all (one new day)", db.rank_stocks_overall, conn)

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT date, ticker, etf_ticker, {', '.join(METRIC_COLUMNS)} "  # noqa: S608
                "FROM fact_stock_metrics WHERE ticker = 'S7'"
            )
            history = pd.DataFrame(
                cur.fetchall(), columns=["date", "ticker", "etf_ticker", *METRIC_COLUMNS]
            )
        conn.commit()
        for col in METRIC_COLUMNS:
            history[col] = history[col].astype(float)
        history["naive_relative_strength"] *= 0.99  # what a dividend restatement changes
        timed("restated stock: upsert its history", db.upsert_stock_metrics, conn, history)
        timed(
            "restated stock: re-rank",
            lambda c: db.rank_stocks_within_etf(c) + db.rank_stocks_overall(c),
            conn,
        )
    pool.close_all()


if __name__ == "__main__":
    main()
