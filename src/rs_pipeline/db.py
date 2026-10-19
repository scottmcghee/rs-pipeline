"""PostgreSQL access: connection pool, schema, and every query the steps use.

Each function takes an open connection and ends its own transaction: writes
commit, and reads commit too, so a connection never sits "idle in
transaction" while a step waits on a slow vendor call.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from importlib.resources import files
from typing import Any

import pandas as pd
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extensions import connection as Connection
from psycopg2.extras import execute_values

from rs_pipeline.config import DbSettings
from rs_pipeline.rs import METRIC_COLUMNS

logger = logging.getLogger(__name__)

OHLC_COLUMNS = ("open", "high", "low", "close", "volume")
STOCK_OHLC_TABLE = "fact_commons_ohlc"
INDEX_OHLC_TABLE = "fact_index_ohlc"
_OHLC_TABLES = frozenset({STOCK_OHLC_TABLE, INDEX_OHLC_TABLE})

Row = tuple[Any, ...]


# Detect a dead peer during long phases instead of hanging on it.
_KEEPALIVES = {"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10,
               "keepalives_count": 5}  # fmt: skip


class ConnectionPool:
    """Thread-safe connection source; ``connection()`` lends one for a ``with`` block.

    No idle connections are kept (``minconn=0``): each block gets a freshly
    opened connection, and it is closed when the block ends. A step that
    spends minutes on vendor calls between blocks therefore never resumes on
    a connection the server or a firewall has silently dropped. Opening a
    connection costs milliseconds; a run opens at most a few hundred.
    """

    def __init__(self, settings: DbSettings, maxconn: int = 10) -> None:
        self._pool = pool.ThreadedConnectionPool(0, maxconn, **settings.as_kwargs(), **_KEEPALIVES)

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)  # closes it (minconn=0), rolling back anything open

    def close_all(self) -> None:
        self._pool.closeall()


@contextmanager
def run_lock(settings: DbSettings) -> Iterator[bool]:
    """Hold a database-wide lock for the duration of a run.

    Yields False if another run holds it. Two overlapping runs could
    otherwise write metrics computed from prices the other run has just
    replaced. The lock is a session-level advisory lock on its own
    connection, released when the connection closes.
    """
    kwargs: dict[str, Any] = {**settings.as_kwargs(), **_KEEPALIVES}
    conn = psycopg2.connect(**kwargs)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext('rs_pipeline'))")
            row = cur.fetchone()
        conn.commit()
        yield bool(row and row[0])
    finally:
        conn.close()


def apply_schema(conn: Connection) -> None:
    """Create any missing tables and indexes."""
    ddl = files("rs_pipeline").joinpath("schema.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _read(conn: Connection, query: str | sql.Composable, params: Any = None) -> list[Row]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    conn.commit()
    return rows


def _query_df(conn: Connection, query: str | sql.Composable, params: Any = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [d.name for d in cur.description or ()]
        df = pd.DataFrame(cur.fetchall(), columns=columns)
    conn.commit()
    return df


def _clean(value: Any) -> float | None:
    """Float for the database, with NaN and infinities stored as NULL."""
    if value is None:
        return None
    f = float(value)
    return None if math.isnan(f) or math.isinf(f) else f


def _ohlc_table(table: str) -> sql.Identifier:
    if table not in _OHLC_TABLES:
        raise ValueError(f"not an OHLC table: {table}")
    return sql.Identifier(table)


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


def ohlc_records(ticker: str, df: pd.DataFrame) -> list[Row]:
    """Rows for ``upsert_ohlc`` from a normalized OHLC frame (missing values as NULL)."""
    return [
        (ts.date(), ticker, _clean(o), _clean(h), _clean(lo), _clean(c), _volume(v))
        for ts, o, h, lo, c, v in zip(
            df.index, df["open"], df["high"], df["low"], df["close"], df["volume"], strict=True
        )
    ]


def _volume(value: Any) -> int | None:
    v = _clean(value)
    return None if v is None else int(v)


def upsert_ohlc(conn: Connection, table: str, records: Sequence[Row]) -> int:
    """Insert or overwrite (date, ticker, open, high, low, close, volume) rows.

    Re-downloaded bars that haven't changed are left alone (not rewritten).
    """
    if not records:
        return 0
    query = sql.SQL(
        """
        INSERT INTO {t} AS o (date, ticker, open, high, low, close, volume) VALUES %s
        ON CONFLICT (date, ticker) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume = EXCLUDED.volume
        WHERE (o.open, o.high, o.low, o.close, o.volume)
              IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low,
                                EXCLUDED.close, EXCLUDED.volume)
        """
    ).format(t=_ohlc_table(table))
    with conn.cursor() as cur:
        execute_values(cur, query, records, page_size=1000)
    conn.commit()
    return len(records)


def fetch_ohlc(
    conn: Connection, table: str, tickers: Sequence[str], start: date, end: date
) -> dict[str, pd.DataFrame]:
    """OHLC per ticker for ``start <= date <= end``, as float frames indexed by date."""
    if not tickers:
        return {}
    query = sql.SQL(
        """
        SELECT date, ticker, open, high, low, close, volume FROM {}
        WHERE ticker = ANY(%s) AND date BETWEEN %s AND %s
        ORDER BY ticker, date
        """
    ).format(_ohlc_table(table))
    df = _query_df(conn, query, (list(tickers), start, end))
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    df[list(OHLC_COLUMNS)] = df[list(OHLC_COLUMNS)].astype(float)
    return {
        str(ticker): group.drop(columns="ticker").set_index("date")
        for ticker, group in df.groupby("ticker")
    }


def latest_ohlc_dates(conn: Connection, table: str) -> dict[str, date]:
    query = sql.SQL("SELECT ticker, MAX(date) FROM {} GROUP BY ticker").format(_ohlc_table(table))
    return dict(_read(conn, query))


def first_ohlc_dates(conn: Connection, table: str, tickers: Sequence[str]) -> dict[str, date]:
    query = sql.SQL(
        "SELECT ticker, MIN(date) FROM {} WHERE ticker = ANY(%s) GROUP BY ticker"
    ).format(_ohlc_table(table))
    return dict(_read(conn, query, (list(tickers),)))


def stored_closes(
    conn: Connection, table: str, since: dict[str, date]
) -> dict[str, dict[date, float]]:
    """Stored closes of each ticker from its given date on: ``{ticker: {date: close}}``."""
    if not since:
        return {}
    query = sql.SQL(
        """
        SELECT o.ticker, o.date, o.close FROM {} o
        JOIN unnest(%s::text[], %s::date[]) AS w(ticker, since)
          ON o.ticker = w.ticker AND o.date >= w.since
        """
    ).format(_ohlc_table(table))
    result: dict[str, dict[date, float]] = {}
    for ticker, day, close in _read(conn, query, (list(since), list(since.values()))):
        if close is not None:
            result.setdefault(ticker, {})[day] = float(close)
    return result


def replace_ohlc(conn: Connection, table: str, ticker: str, df: pd.DataFrame) -> int:
    """Make the stored bars from the first to the last date of *df* exactly *df*.

    Stored bars inside that range that the vendor no longer returns are
    deleted, in the same transaction as the upsert. Bars outside the range
    are untouched.
    """
    if df.empty:
        return 0
    first, last = df.index.min().date(), df.index.max().date()
    keep = [ts.date() for ts in df.index]
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "DELETE FROM {} WHERE ticker = %s AND date BETWEEN %s AND %s "
                "AND NOT (date = ANY(%s::date[]))"
            ).format(_ohlc_table(table)),
            (ticker, first, last, keep),
        )
    return upsert_ohlc(conn, table, ohlc_records(ticker, df))


def queue_rebuild(
    conn: Connection, stocks: Sequence[str], etfs: Sequence[str], benchmark: str | None
) -> int:
    """Queue metric series for a full rebuild after their prices were restated.

    A stock's series against each ETF it currently belongs to; for an ETF,
    every member's series against it and its own series against the
    benchmark; for the benchmark, every ETF's series against it. Returns the
    number of series queued.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metrics_rebuild (kind, ticker, ref)
            SELECT 'stock', ticker, etf_ticker FROM junc_stock_etf
            WHERE ticker = ANY(%(stocks)s) OR etf_ticker = ANY(%(etfs)s)
            UNION
            SELECT 'etf', etf_ticker, ref_ticker FROM fact_etf_metrics
            WHERE etf_ticker = ANY(%(etfs)s) OR ref_ticker = %(benchmark)s
            GROUP BY etf_ticker, ref_ticker
            ON CONFLICT DO NOTHING
            """,
            {"stocks": list(stocks), "etfs": list(etfs), "benchmark": benchmark},
        )
        queued = int(cur.rowcount)
    conn.commit()
    return queued


def pending_rebuilds(conn: Connection, kind: str, ref: str) -> set[str]:
    rows = _read(
        conn, "SELECT ticker FROM metrics_rebuild WHERE kind = %s AND ref = %s", (kind, ref)
    )
    return {row[0] for row in rows}


def finish_rebuilds(conn: Connection, kind: str, ref: str, tickers: Sequence[str]) -> None:
    if not tickers:
        return
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM metrics_rebuild WHERE kind = %s AND ref = %s AND ticker = ANY(%s)",
            (kind, ref, list(tickers)),
        )
    conn.commit()


def stored_window(conn: Connection, table: str) -> str | None:
    """The window a metrics table was built with: ``"252"``, or ``"252?"`` while
    a full recompute to that window is unfinished."""
    rows = _read(conn, "SELECT value FROM pipeline_state WHERE key = %s", (f"window:{table}",))
    return str(rows[0][0]) if rows else None


def save_window(conn: Connection, table: str, value: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_state (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (f"window:{table}", value),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Universe: ETFs, holdings, stock metadata
# ---------------------------------------------------------------------------


def fetch_holdings(conn: Connection, etfs: Sequence[str] | None = None) -> dict[str, list[str]]:
    """``{etf_ticker: [stock tickers]}`` for every ETF, or only for *etfs*."""
    rows = _read(
        conn,
        """
        SELECT etf_ticker, ticker FROM junc_stock_etf
        WHERE %(etfs)s::text[] IS NULL OR etf_ticker = ANY(%(etfs)s)
        ORDER BY etf_ticker, ticker
        """,
        {"etfs": list(etfs) if etfs is not None else None},
    )
    holdings: dict[str, list[str]] = {}
    for etf, ticker in rows:
        holdings.setdefault(etf, []).append(ticker)
    return holdings


def fetch_rankable_etfs(conn: Connection, benchmarks: Sequence[str]) -> list[str]:
    """Every sector, industry and index ETF except the broad-market *benchmarks*."""
    rows = _read(
        conn,
        """
        SELECT etf_ticker FROM (
            SELECT etf_ticker FROM dim_industry
            UNION SELECT etf_ticker FROM dim_sector
            UNION SELECT etf_ticker FROM dim_etf
        ) all_etfs
        WHERE etf_ticker IS NOT NULL AND etf_ticker <> ALL(%s::text[])
        ORDER BY etf_ticker
        """,
        (list(benchmarks),),
    )
    return [row[0] for row in rows]


def tickers_missing_info(conn: Connection, tickers: Sequence[str]) -> list[str]:
    """Those of *tickers* with no ``dim_stock`` row, or only a placeholder row."""
    rows = _read(
        conn,
        """
        SELECT t.ticker FROM unnest(%s::text[]) AS t(ticker)
        LEFT JOIN dim_stock d USING (ticker)
        WHERE COALESCE(d.company_name, d.sector, d.industry, d.exchange) IS NULL
        ORDER BY t.ticker
        """,
        (list(tickers),),
    )
    return [row[0] for row in rows]


def upsert_stocks(conn: Connection, records: Sequence[Row]) -> None:
    """Upsert (ticker, company_name, sector, industry, exchange) rows.

    A NULL field never overwrites a known value.
    """
    if not records:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_stock (ticker, company_name, sector, industry, exchange) VALUES %s
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, dim_stock.company_name),
                sector = COALESCE(EXCLUDED.sector, dim_stock.sector),
                industry = COALESCE(EXCLUDED.industry, dim_stock.industry),
                exchange = COALESCE(EXCLUDED.exchange, dim_stock.exchange)
            """,
            records,
            page_size=500,
        )
    conn.commit()


def upsert_etf(conn: Connection, etf_ticker: str, etf_name: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_etf (etf_ticker, etf_name) VALUES (%s, %s)
            ON CONFLICT (etf_ticker) DO UPDATE SET
                etf_name = COALESCE(EXCLUDED.etf_name, dim_etf.etf_name)
            """,
            (etf_ticker, etf_name),
        )
    conn.commit()


def link_stocks_to_etf(conn: Connection, etf_ticker: str, tickers: Sequence[str]) -> None:
    """Add holdings to an ETF, creating placeholder ``dim_stock`` rows as needed.

    Delisted tickers are not linked again.
    """
    if not tickers:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO dim_stock (ticker) VALUES %s ON CONFLICT (ticker) DO NOTHING",
            [(t,) for t in tickers],
        )
        execute_values(
            cur,
            """
            INSERT INTO junc_stock_etf (ticker, etf_ticker)
            SELECT v.ticker, v.etf_ticker FROM (VALUES %s) AS v(ticker, etf_ticker)
            WHERE v.ticker NOT IN (SELECT ticker FROM delisted_tickers)
            ON CONFLICT DO NOTHING
            """,
            [(t, etf_ticker) for t in tickers],
        )
    conn.commit()


def replace_sector_etfs(
    conn: Connection, sectors: dict[str, str], industries: dict[str, str]
) -> int:
    """Make the sector and industry → ETF maps exactly these.

    A stock's link to a sector or industry ETF is removed when the new maps no
    longer imply it, unless that ETF was onboarded directly (``dim_etf``), so
    its holdings links stay. Returns the number of links removed.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT etf_ticker FROM dim_sector UNION SELECT etf_ticker FROM dim_industry")
        before = {row[0] for row in cur.fetchall()}
        for table, mapping in (("dim_sector", sectors), ("dim_industry", industries)):
            cur.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table)))
            if mapping:
                execute_values(
                    cur,
                    sql.SQL("INSERT INTO {} (name, etf_ticker) VALUES %s").format(
                        sql.Identifier(table)
                    ),
                    list(mapping.items()),
                )
        # Remove links to formerly mapped ETFs that the new maps no longer
        # imply for that stock. Onboarded ETFs (dim_etf) keep their holdings.
        cur.execute(
            """
            DELETE FROM junc_stock_etf j
            WHERE j.etf_ticker = ANY(%s)
              AND j.etf_ticker NOT IN (SELECT etf_ticker FROM dim_etf)
              AND NOT EXISTS (
                  SELECT 1 FROM dim_stock s
                  LEFT JOIN dim_sector sec ON sec.name = s.sector
                  LEFT JOIN dim_industry ind ON ind.name = s.industry
                  WHERE s.ticker = j.ticker
                    AND j.etf_ticker IN (sec.etf_ticker, ind.etf_ticker)
              )
            """,
            (sorted(before),),
        )
        removed = int(cur.rowcount)
    conn.commit()
    return removed


PIPELINE_TABLES = frozenset(
    {
        "dim_stock", "dim_etf", "dim_sector", "dim_industry", "junc_stock_etf",
        "fact_commons_ohlc", "fact_index_ohlc", "fact_stock_metrics", "fact_etf_metrics",
        "ticker_fetch_failures", "delisted_tickers", "metrics_rebuild", "pipeline_state",
    }
)  # fmt: skip


def has_schema(conn: Connection) -> bool:
    return bool(_read(conn, "SELECT to_regclass('public.fact_stock_metrics') IS NOT NULL")[0][0])


def other_tables(conn: Connection) -> list[str]:
    """Tables in the public schema that this pipeline doesn't own."""
    rows = _read(conn, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")
    return [r[0] for r in rows if r[0] not in PIPELINE_TABLES]


def etfs_outside(conn: Connection, allowed: Sequence[str]) -> list[str]:
    """ETFs in the database that are not in *allowed* (used to protect real data).

    Safe to call before the schema exists: missing tables hold no ETFs.
    """
    if not has_schema(conn):
        return []
    rows = _read(
        conn,
        """
        SELECT etf_ticker FROM (
            SELECT etf_ticker FROM junc_stock_etf UNION SELECT etf_ticker FROM dim_etf
            UNION SELECT etf_ticker FROM dim_sector UNION SELECT etf_ticker FROM dim_industry
        ) e WHERE etf_ticker <> ALL(%s::text[]) ORDER BY 1
        """,
        (list(allowed),),
    )
    return [row[0] for row in rows]


def link_stocks_by_sector_and_industry(conn: Connection) -> int:
    """Link every listed stock to its sector's and industry's ETFs. Returns new links."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO junc_stock_etf (ticker, etf_ticker)
            SELECT ticker, etf_ticker FROM (
                SELECT s.ticker, sec.etf_ticker FROM dim_stock s
                JOIN dim_sector sec ON sec.name = s.sector
                UNION
                SELECT s.ticker, ind.etf_ticker FROM dim_stock s
                JOIN dim_industry ind ON ind.name = s.industry
            ) links
            WHERE ticker NOT IN (SELECT ticker FROM delisted_tickers)
            ON CONFLICT (ticker, etf_ticker) DO NOTHING
            """
        )
        added = int(cur.rowcount)
    conn.commit()
    if added:
        logger.info("Linked %d stocks to sector/industry ETFs", added)
    return added


# ---------------------------------------------------------------------------
# Fetch failures and delisting
# ---------------------------------------------------------------------------


def record_fetch_failures(conn: Connection, tickers: Sequence[str], on: date) -> dict[str, int]:
    """Count at most one failure per ticker per day. Returns the updated counts."""
    if not tickers:
        return {}
    with conn.cursor() as cur:
        rows = execute_values(
            cur,
            """
            INSERT INTO ticker_fetch_failures
                (ticker, failure_count, first_failure_date, last_failure_date)
            VALUES %s
            ON CONFLICT (ticker) DO UPDATE SET
                failure_count = ticker_fetch_failures.failure_count
                    + (ticker_fetch_failures.last_failure_date < EXCLUDED.last_failure_date)::int,
                last_failure_date = GREATEST(ticker_fetch_failures.last_failure_date,
                                             EXCLUDED.last_failure_date)
            RETURNING ticker, failure_count
            """,
            [(t, 1, on, on) for t in tickers],
            fetch=True,
        )
    conn.commit()
    return dict(rows)


def clear_fetch_failures(conn: Connection, tickers: Sequence[str]) -> None:
    if not tickers:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ticker_fetch_failures WHERE ticker = ANY(%s)", (list(tickers),))
    conn.commit()


def fetch_delisted(conn: Connection) -> set[str]:
    return {row[0] for row in _read(conn, "SELECT ticker FROM delisted_tickers")}


def mark_delisted(conn: Connection, ticker: str, on: date) -> list[str]:
    """Unlink *ticker* from its ETFs and record it as delisted.

    Its prices, metrics and ``dim_stock`` row are kept as history. Returns
    the ETFs it was removed from.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM junc_stock_etf WHERE ticker = %s RETURNING etf_ticker", (ticker,))
        etfs = sorted(row[0] for row in cur.fetchall())
        cur.execute(
            """
            INSERT INTO delisted_tickers (ticker, detected_date, original_etfs)
            VALUES (%s, %s, %s) ON CONFLICT (ticker) DO NOTHING
            """,
            (ticker, on, ",".join(etfs) or None),
        )
        cur.execute("DELETE FROM ticker_fetch_failures WHERE ticker = %s", (ticker,))
    conn.commit()
    logger.warning("Delisted %s (was in: %s)", ticker, ", ".join(etfs) or "no ETFs")
    return etfs


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

_METRIC_UPDATES = ", ".join(f"{c} = EXCLUDED.{c}" for c in METRIC_COLUMNS)
_METRIC_CHANGED = (
    f"({', '.join(f'm.{c}' for c in METRIC_COLUMNS)}) IS DISTINCT FROM "
    f"({', '.join(f'EXCLUDED.{c}' for c in METRIC_COLUMNS)})"
)
_RS_CHANGED = "m.relative_strength IS DISTINCT FROM EXCLUDED.relative_strength"


def _upsert_metrics(conn: Connection, table: str, keys: tuple[str, str], df: pd.DataFrame) -> int:
    """Write metric rows. Returns the number of rows inserted or changed.

    Unchanged rows are not rewritten. A row's ranks are cleared (so the
    ranking steps redo its partitions) only when it is new or its relative
    strength changed: ranks depend on nothing else.
    """
    if df.empty:
        return 0
    cols = ("date", *keys, *METRIC_COLUMNS)
    query = (
        f"INSERT INTO {table} AS m ({', '.join(cols)}) VALUES %s "  # noqa: S608 - constant identifiers
        f"ON CONFLICT (date, {', '.join(keys)}) DO UPDATE SET {_METRIC_UPDATES}, "
        f"rs_rank_index = CASE WHEN {_RS_CHANGED} THEN NULL ELSE m.rs_rank_index END, "
        f"rs_rank_overall = CASE WHEN {_RS_CHANGED} THEN NULL ELSE m.rs_rank_overall END "
        f"WHERE {_METRIC_CHANGED}"
    )
    dates = pd.to_datetime(df["date"]).dt.date
    metrics = [[_clean(v) for v in df[c]] for c in METRIC_COLUMNS]
    rows = [
        (d, k1, k2, *vals)
        for d, k1, k2, *vals in zip(dates, df[keys[0]], df[keys[1]], *metrics, strict=True)
    ]
    written = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), 1000):
            execute_values(cur, query, rows[i : i + 1000], page_size=1000)
            written += max(cur.rowcount, 0)
    conn.commit()
    return written


def prune_metrics(
    conn: Connection, table: str, keys: tuple[str, str], pair: tuple[str, str], keep: Sequence[date]
) -> int:
    """Delete a rebuilt series' rows for dates it no longer has, e.g. after the
    vendor dropped bars when restating history. The partitions those rows were
    ranked in are marked for re-ranking. Returns the number of rows deleted.
    """
    where = f"{keys[0]} = %(k1)s AND {keys[1]} = %(k2)s"
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {table} WHERE {where} "  # noqa: S608 - constant identifiers
            "AND NOT (date = ANY(%(keep)s::date[])) RETURNING date",
            {"k1": pair[0], "k2": pair[1], "keep": list(keep)},
        )
        gone = [row[0] for row in cur.fetchall()]
        if gone:
            # Ranks within the same ETF (or benchmark) on those dates changed;
            # for stocks, so did every overall rank on those dates.
            cur.execute(
                f"UPDATE {table} SET rs_rank_index = NULL "  # noqa: S608 - constant identifiers
                f"WHERE date = ANY(%(gone)s::date[]) AND {keys[1]} = %(k2)s",
                {"gone": gone, "k2": pair[1]},
            )
            cur.execute(
                f"UPDATE {table} SET rs_rank_overall = NULL "  # noqa: S608 - constant identifiers
                "WHERE date = ANY(%(gone)s::date[])"
                + (f" AND {keys[1]} = %(k2)s" if table == "fact_etf_metrics" else ""),
                {"gone": gone, "k2": pair[1]},
            )
    conn.commit()
    return len(gone)


def upsert_stock_metrics(conn: Connection, df: pd.DataFrame) -> int:
    return _upsert_metrics(conn, "fact_stock_metrics", ("ticker", "etf_ticker"), df)


def upsert_etf_metrics(conn: Connection, df: pd.DataFrame) -> int:
    return _upsert_metrics(conn, "fact_etf_metrics", ("etf_ticker", "ref_ticker"), df)


def last_stock_metric_dates(conn: Connection, etf_ticker: str) -> dict[str, date]:
    query = "SELECT ticker, MAX(date) FROM fact_stock_metrics WHERE etf_ticker = %s GROUP BY ticker"
    return dict(_read(conn, query, (etf_ticker,)))


def last_etf_metric_dates(conn: Connection, ref_ticker: str) -> dict[str, date]:
    query = (
        "SELECT etf_ticker, MAX(date) FROM fact_etf_metrics "
        "WHERE ref_ticker = %s GROUP BY etf_ticker"
    )
    return dict(_read(conn, query, (ref_ticker,)))


# ---------------------------------------------------------------------------
# Rankings
#
# Ranking runs in two steps. First the dirty dates are found: dates with an
# unranked (NULL) row, which partial indexes make cheap, plus the optional
# forced range. Then PERCENT_RANK is recomputed for those dates only, a chunk
# of dates per statement, so each statement uses the primary key's leading
# date column and stays small. A whole date (or date and ETF) is redone
# because one changed row shifts everyone's rank. Ranks are compared at the
# stored precision, NUMERIC(30, 10), and unchanged rows are not rewritten.
# ---------------------------------------------------------------------------

RANK_CHUNK_DATES = 50


def _dirty_dates(
    conn: Connection, null_check: str, table: str, start: date | None, end: date | None
) -> list[date]:
    rows = _read(
        conn,
        f"SELECT DISTINCT date FROM {table} WHERE {null_check} "  # noqa: S608 - constant identifiers
        f"UNION SELECT DISTINCT date FROM {table} WHERE date BETWEEN %s AND %s ORDER BY 1",
        (start, end),
    )
    return [row[0] for row in rows]


def _rank_in_chunks(
    conn: Connection, dates: list[date], statement: str, params: dict[str, Any]
) -> int:
    updated = 0
    with conn.cursor() as cur:
        for i in range(0, len(dates), RANK_CHUNK_DATES):
            cur.execute(statement, {**params, "dates": dates[i : i + RANK_CHUNK_DATES]})
            updated += int(cur.rowcount)
            conn.commit()
    return updated


def rank_stocks_within_etf(
    conn: Connection,
    etfs: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> int:
    """Set ``rs_rank_index``: a stock's RS percentile among one ETF's members on one date."""
    dates = _dirty_dates(conn, "rs_rank_index IS NULL", "fact_stock_metrics", start, end)
    return _rank_in_chunks(
        conn,
        dates,
        """
        WITH ranked AS (
            SELECT date, ticker, etf_ticker,
                   ROUND(PERCENT_RANK() OVER (PARTITION BY date, etf_ticker
                                              ORDER BY relative_strength NULLS FIRST)::numeric,
                         10) AS pct
            FROM fact_stock_metrics
            WHERE date = ANY(%(dates)s::date[])
              AND (%(etfs)s::text[] IS NULL OR etf_ticker = ANY(%(etfs)s))
        )
        UPDATE fact_stock_metrics m SET rs_rank_index = r.pct
        FROM ranked r
        WHERE m.date = r.date AND m.ticker = r.ticker AND m.etf_ticker = r.etf_ticker
          AND m.date = ANY(%(dates)s::date[])
          AND m.rs_rank_index IS DISTINCT FROM r.pct
        """,
        {"etfs": list(etfs) if etfs is not None else None},
    )


def rank_stocks_overall(
    conn: Connection, start: date | None = None, end: date | None = None
) -> int:
    """Set ``rs_rank_overall``: a stock's RS percentile among all stocks on one date.

    A stock that belongs to several ETFs has one RS per ETF. They are averaged
    first, so every row for that stock and date gets the same rank.
    """
    dates = _dirty_dates(conn, "rs_rank_overall IS NULL", "fact_stock_metrics", start, end)
    return _rank_in_chunks(
        conn,
        dates,
        """
        WITH per_stock AS (
            SELECT date, ticker, AVG(relative_strength) AS avg_rs
            FROM fact_stock_metrics WHERE date = ANY(%(dates)s::date[])
            GROUP BY date, ticker
        ), ranked AS (
            SELECT date, ticker,
                   ROUND(PERCENT_RANK() OVER (PARTITION BY date
                                              ORDER BY avg_rs NULLS FIRST)::numeric, 10) AS pct
            FROM per_stock
        )
        UPDATE fact_stock_metrics m SET rs_rank_overall = r.pct
        FROM ranked r
        WHERE m.date = r.date AND m.ticker = r.ticker
          AND m.date = ANY(%(dates)s::date[])
          AND m.rs_rank_overall IS DISTINCT FROM r.pct
        """,
        {},
    )


def rank_etfs(
    conn: Connection, ref_ticker: str, start: date | None = None, end: date | None = None
) -> int:
    """Set the ETF ranks among the ETFs measured against *ref_ticker*.

    ``rs_rank_index`` and ``rs_rank_overall`` are the same ranking here; both
    columns are kept so ETF and stock metrics share one shape.
    """
    dates = _dirty_dates(
        conn, "rs_rank_index IS NULL OR rs_rank_overall IS NULL", "fact_etf_metrics", start, end
    )
    return _rank_in_chunks(
        conn,
        dates,
        """
        WITH ranked AS (
            SELECT date, etf_ticker,
                   ROUND(PERCENT_RANK() OVER (PARTITION BY date
                                              ORDER BY relative_strength NULLS FIRST)::numeric,
                         10) AS pct
            FROM fact_etf_metrics
            WHERE date = ANY(%(dates)s::date[]) AND ref_ticker = %(ref)s
        )
        UPDATE fact_etf_metrics m SET rs_rank_index = r.pct, rs_rank_overall = r.pct
        FROM ranked r
        WHERE m.date = r.date AND m.etf_ticker = r.etf_ticker AND m.ref_ticker = %(ref)s
          AND m.date = ANY(%(dates)s::date[])
          AND (m.rs_rank_index IS DISTINCT FROM r.pct OR m.rs_rank_overall IS DISTINCT FROM r.pct)
        """,
        {"ref": ref_ticker},
    )


def top_stocks(conn: Connection, limit: int, on: date | None = None) -> pd.DataFrame:
    """Highest overall-ranked stocks on *on*, or on the latest ranked date."""
    return _query_df(
        conn,
        """
        WITH day AS (
            SELECT COALESCE(%(on)s::date, MAX(date)) AS date FROM fact_stock_metrics
            WHERE rs_rank_overall IS NOT NULL
        )
        SELECT m.date, m.ticker, s.company_name, s.sector,
               ROUND(AVG(m.relative_strength), 2) AS relative_strength,
               ROUND(MAX(m.rs_rank_overall) * 100, 1) AS percentile,
               STRING_AGG(m.etf_ticker, ',' ORDER BY m.etf_ticker) AS etfs
        FROM fact_stock_metrics m
        JOIN day USING (date)
        LEFT JOIN dim_stock s USING (ticker)
        GROUP BY m.date, m.ticker, s.company_name, s.sector
        ORDER BY percentile DESC NULLS LAST, m.ticker
        LIMIT %(limit)s
        """,
        {"on": on, "limit": limit},
    )
