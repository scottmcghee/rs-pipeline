"""Database helpers — connection pool, bulk operations, all queries."""

import logging
from contextlib import contextmanager
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import execute_values

from rs_pipeline.config import DB_CONFIG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

class ConnectionPool:
    """Thin wrapper around psycopg2 ThreadedConnectionPool."""

    def __init__(self, minconn: int = 1, maxconn: int = 10):
        self._pool = pool.ThreadedConnectionPool(
            minconn, maxconn, **DB_CONFIG
        )
        logger.info("DB connection pool created (%d–%d)", minconn, maxconn)

    @contextmanager
    def connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            try:
                self._pool.putconn(conn)
            except Exception:
                # Connection went stale (e.g. server closed it during long
                # downloads). Discard it so the pool creates a fresh one.
                try:
                    self._pool.putconn(conn, close=True)
                except Exception:
                    pass

    def close_all(self):
        self._pool.closeall()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    """Convert to float, mapping NaN/Inf to None."""
    if val is None:
        return None
    if isinstance(val, pd.Series):
        val = val.iloc[0]
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if np.isnan(f) or np.isinf(f):
        return None
    return f


# ---------------------------------------------------------------------------
# Table DDL (idempotent)
# ---------------------------------------------------------------------------

def ensure_delisted_table(conn) -> None:
    """Create the delisted_tickers table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS delisted_tickers (
            ticker VARCHAR(10) PRIMARY KEY,
            detected_date DATE NOT NULL,
            original_etfs TEXT
        );
        """)
    conn.commit()


def ensure_tables(conn) -> None:
    """Create metric tables if they don't exist."""
    ensure_delisted_table(conn)
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_stock_metrics (
            date DATE NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            etf_ticker VARCHAR(10) NOT NULL,
            naive_relative_strength NUMERIC(30, 10),
            naive_relative_strength_percentile NUMERIC(30, 10),
            naive_deviation_from_moving_average NUMERIC(30, 10),
            relative_strength NUMERIC(30, 10),
            relative_strength_percentile NUMERIC(30, 10),
            deviation_from_moving_average NUMERIC(30, 10),
            rs_rank_index NUMERIC(30, 10),
            rs_rank_overall NUMERIC(30, 10),
            rs_rolling_mean NUMERIC(30, 10),
            rs_rolling_std_dev NUMERIC(30, 10),
            rs_z_score NUMERIC(30, 10),
            rs_z_score_roc NUMERIC(30, 10),
            PRIMARY KEY (date, ticker, etf_ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_date
            ON fact_stock_metrics (date);
        CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_ticker
            ON fact_stock_metrics (ticker);
        CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_etf_ticker
            ON fact_stock_metrics (etf_ticker);
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_etf_metrics (
            date DATE NOT NULL,
            etf_ticker VARCHAR(20) NOT NULL,
            ref_ticker VARCHAR(20) NOT NULL,
            naive_relative_strength NUMERIC(30, 10),
            naive_relative_strength_percentile NUMERIC(30, 10),
            naive_deviation_from_moving_average NUMERIC(30, 10),
            relative_strength NUMERIC(30, 10),
            relative_strength_percentile NUMERIC(30, 10),
            deviation_from_moving_average NUMERIC(30, 10),
            rs_rank_index NUMERIC(30, 10),
            rs_rank_overall NUMERIC(30, 10),
            rs_rolling_mean NUMERIC(30, 10),
            rs_rolling_std_dev NUMERIC(30, 10),
            rs_z_score NUMERIC(30, 10),
            rs_z_score_roc NUMERIC(30, 10),
            PRIMARY KEY (date, etf_ticker, ref_ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_fact_etf_metrics_date
            ON fact_etf_metrics (date);
        CREATE INDEX IF NOT EXISTS idx_fact_etf_metrics_etf_ticker
            ON fact_etf_metrics (etf_ticker);
        CREATE INDEX IF NOT EXISTS idx_fact_etf_metrics_ref_ticker
            ON fact_etf_metrics (ref_ticker);
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Bulk OHLC upserts
# ---------------------------------------------------------------------------

_OHLC_UPSERT_TEMPLATE = """
INSERT INTO {} (date, ticker, open, high, low, close, volume)
VALUES %s
ON CONFLICT (date, ticker) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume
"""


def bulk_upsert_ohlc(conn, table: str, records: List[tuple]) -> int:
    """Bulk upsert OHLC rows using execute_values.

    *records* is a list of (date, ticker, open, high, low, close, volume).
    Returns number of rows upserted.
    """
    if not records:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, _OHLC_UPSERT_TEMPLATE.format(table), records,
                       page_size=1000)
    conn.commit()
    return len(records)


# ---------------------------------------------------------------------------
# Bulk metrics upserts
# ---------------------------------------------------------------------------

_STOCK_METRICS_UPSERT = """
INSERT INTO fact_stock_metrics (
    date, ticker, etf_ticker,
    naive_relative_strength, naive_relative_strength_percentile,
    naive_deviation_from_moving_average,
    relative_strength, relative_strength_percentile,
    deviation_from_moving_average,
    rs_rolling_mean, rs_rolling_std_dev,
    rs_z_score, rs_z_score_roc
) VALUES %s
ON CONFLICT (date, ticker, etf_ticker) DO UPDATE SET
    naive_relative_strength = EXCLUDED.naive_relative_strength,
    naive_relative_strength_percentile = EXCLUDED.naive_relative_strength_percentile,
    naive_deviation_from_moving_average = EXCLUDED.naive_deviation_from_moving_average,
    relative_strength = EXCLUDED.relative_strength,
    relative_strength_percentile = EXCLUDED.relative_strength_percentile,
    deviation_from_moving_average = EXCLUDED.deviation_from_moving_average,
    rs_rolling_mean = EXCLUDED.rs_rolling_mean,
    rs_rolling_std_dev = EXCLUDED.rs_rolling_std_dev,
    rs_z_score = EXCLUDED.rs_z_score,
    rs_z_score_roc = EXCLUDED.rs_z_score_roc
"""

_ETF_METRICS_UPSERT = """
INSERT INTO fact_etf_metrics (
    date, etf_ticker, ref_ticker,
    naive_relative_strength, naive_relative_strength_percentile,
    naive_deviation_from_moving_average,
    relative_strength, relative_strength_percentile,
    deviation_from_moving_average,
    rs_rolling_mean, rs_rolling_std_dev,
    rs_z_score, rs_z_score_roc
) VALUES %s
ON CONFLICT (date, etf_ticker, ref_ticker) DO UPDATE SET
    naive_relative_strength = EXCLUDED.naive_relative_strength,
    naive_relative_strength_percentile = EXCLUDED.naive_relative_strength_percentile,
    naive_deviation_from_moving_average = EXCLUDED.naive_deviation_from_moving_average,
    relative_strength = EXCLUDED.relative_strength,
    relative_strength_percentile = EXCLUDED.relative_strength_percentile,
    deviation_from_moving_average = EXCLUDED.deviation_from_moving_average,
    rs_rolling_mean = EXCLUDED.rs_rolling_mean,
    rs_rolling_std_dev = EXCLUDED.rs_rolling_std_dev,
    rs_z_score = EXCLUDED.rs_z_score,
    rs_z_score_roc = EXCLUDED.rs_z_score_roc
"""


def _metrics_df_to_tuples(df: pd.DataFrame, key_cols: List[str]) -> List[tuple]:
    """Convert a metrics DataFrame into a list of tuples for execute_values."""
    metric_cols = [
        'naive_relative_strength', 'naive_relative_strength_percentile',
        'naive_deviation_from_moving_average',
        'relative_strength', 'relative_strength_percentile',
        'deviation_from_moving_average',
        'rs_rolling_mean', 'rs_rolling_std_dev',
        'rs_z_score', 'rs_z_score_roc',
    ]
    rows = []
    for _, row in df.iterrows():
        keys = tuple(row[c] for c in key_cols)
        vals = tuple(_safe_float(row[c]) for c in metric_cols)
        rows.append(keys + vals)
    return rows


def bulk_upsert_stock_metrics(conn, df: pd.DataFrame) -> int:
    """Bulk upsert stock metrics from a DataFrame."""
    if df.empty:
        return 0
    tuples = _metrics_df_to_tuples(df, ['date', 'ticker', 'etf_ticker'])
    with conn.cursor() as cur:
        execute_values(cur, _STOCK_METRICS_UPSERT, tuples, page_size=1000)
    conn.commit()
    return len(tuples)


def bulk_upsert_etf_metrics(conn, df: pd.DataFrame) -> int:
    """Bulk upsert ETF metrics from a DataFrame."""
    if df.empty:
        return 0
    tuples = _metrics_df_to_tuples(df, ['date', 'etf_ticker', 'ref_ticker'])
    with conn.cursor() as cur:
        execute_values(cur, _ETF_METRICS_UPSERT, tuples, page_size=1000)
    conn.commit()
    return len(tuples)


# ---------------------------------------------------------------------------
# Read queries
# ---------------------------------------------------------------------------

def fetch_all_ohlc(conn, tickers: List[str], start: str, end: str,
                   table: str) -> Dict[str, pd.DataFrame]:
    """Load OHLC for multiple tickers → dict[ticker, DataFrame]."""
    if not tickers:
        return {}
    query = f"""
        SELECT date, ticker, open, high, low, close, volume
        FROM {table}
        WHERE ticker = ANY(%s) AND date BETWEEN %s AND %s
        ORDER BY ticker, date
    """
    df = pd.read_sql_query(query, conn, params=[tickers, start, end])
    if df.empty:
        return {}
    df['date'] = pd.to_datetime(df['date'])
    result = {}
    for ticker, group in df.groupby('ticker'):
        g = group.set_index('date').sort_index()
        result[ticker] = g
    return result


def fetch_all_holdings(conn) -> Dict[str, List[str]]:
    """Return {etf_ticker: [stock_tickers]} from junc_stock_etf."""
    query = "SELECT etf_ticker, ticker FROM junc_stock_etf ORDER BY etf_ticker"
    df = pd.read_sql_query(query, conn)
    result: Dict[str, List[str]] = {}
    for etf, group in df.groupby('etf_ticker'):
        result[etf] = group['ticker'].tolist()
    return result


def fetch_holdings_for_etf(conn, etf_ticker: str) -> List[str]:
    """Return list of stock tickers for a single ETF."""
    query = "SELECT ticker FROM junc_stock_etf WHERE etf_ticker = %s"
    df = pd.read_sql_query(query, conn, params=[etf_ticker])
    return df['ticker'].tolist()


def fetch_latest_ohlc_dates(conn, table: str) -> Dict[str, date]:
    """Return {ticker: max_date} from an OHLC table."""
    query = f"SELECT ticker, MAX(date) AS max_date FROM {table} GROUP BY ticker"
    df = pd.read_sql_query(query, conn)
    return dict(zip(df['ticker'], pd.to_datetime(df['max_date']).dt.date))


def fetch_last_metric_dates(conn, etf_ticker: str) -> Dict[str, date]:
    """Return {ticker: max_date} from fact_stock_metrics for an ETF."""
    query = """
        SELECT ticker, MAX(date) AS max_date
        FROM fact_stock_metrics
        WHERE etf_ticker = %s
        GROUP BY ticker
    """
    df = pd.read_sql_query(query, conn, params=[etf_ticker])
    if df.empty:
        return {}
    return dict(zip(df['ticker'], pd.to_datetime(df['max_date']).dt.date))


def fetch_last_etf_metric_dates(conn, ref_ticker: str = 'SPY') -> Dict[str, date]:
    """Return {etf_ticker: max_date} from fact_etf_metrics for a ref."""
    query = """
        SELECT etf_ticker, MAX(date) AS max_date
        FROM fact_etf_metrics
        WHERE ref_ticker = %s
        GROUP BY etf_ticker
    """
    df = pd.read_sql_query(query, conn, params=[ref_ticker])
    if df.empty:
        return {}
    return dict(zip(df['etf_ticker'], pd.to_datetime(df['max_date']).dt.date))


def fetch_all_etf_tickers(conn) -> List[str]:
    """All distinct ETF tickers from dim_industry, dim_sector, dim_etf."""
    query = """
        SELECT etf_ticker FROM dim_industry
        UNION
        SELECT etf_ticker FROM dim_sector
        UNION
        SELECT etf_ticker FROM dim_etf
        WHERE etf_ticker NOT IN ('SPY', 'QQQ', 'IWM')
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return sorted({r[0] for r in rows if r[0]})


def fetch_existing_stocks(conn) -> set:
    """Return set of tickers already in dim_stock."""
    query = "SELECT ticker FROM dim_stock"
    df = pd.read_sql_query(query, conn)
    return set(df['ticker'])


def fetch_all_etfs_from_db(conn) -> List[Tuple[str, str]]:
    """All ETFs with type from dim_etf, dim_sector, dim_industry."""
    etfs = []
    with conn.cursor() as cur:
        cur.execute("SELECT etf_ticker FROM dim_etf")
        etfs.extend((r[0], 'index') for r in cur.fetchall())
        cur.execute("SELECT DISTINCT etf_ticker FROM dim_sector WHERE etf_ticker IS NOT NULL")
        etfs.extend((r[0], 'sector') for r in cur.fetchall())
        cur.execute("SELECT DISTINCT etf_ticker FROM dim_industry WHERE etf_ticker IS NOT NULL")
        etfs.extend((r[0], 'industry') for r in cur.fetchall())
    return list(set(etfs))


# ---------------------------------------------------------------------------
# SQL-based rankings
# ---------------------------------------------------------------------------

def rank_index_by_sql(conn, etf_ticker: str, start: str, end: str) -> int:
    """Update rs_rank_index using PERCENT_RANK() — one SQL per ETF."""
    query = """
        UPDATE fact_stock_metrics AS m
        SET rs_rank_index = sub.pct_rank
        FROM (
            SELECT date, ticker, etf_ticker,
                   PERCENT_RANK() OVER (
                       PARTITION BY date, etf_ticker
                       ORDER BY relative_strength
                   ) AS pct_rank
            FROM fact_stock_metrics
            WHERE date BETWEEN %s AND %s AND etf_ticker = %s
        ) sub
        WHERE m.date = sub.date
          AND m.ticker = sub.ticker
          AND m.etf_ticker = sub.etf_ticker
    """
    with conn.cursor() as cur:
        cur.execute(query, [start, end, etf_ticker])
        count = cur.rowcount
    conn.commit()
    return count


def rank_overall_by_sql(conn, start: str, end: str) -> int:
    """Update rs_rank_overall across all stocks for a date range."""
    query = """
        UPDATE fact_stock_metrics AS m
        SET rs_rank_overall = sub.pct_rank
        FROM (
            SELECT date, ticker, etf_ticker,
                   PERCENT_RANK() OVER (
                       PARTITION BY date
                       ORDER BY relative_strength
                   ) AS pct_rank
            FROM fact_stock_metrics
            WHERE date BETWEEN %s AND %s
        ) sub
        WHERE m.date = sub.date
          AND m.ticker = sub.ticker
          AND m.etf_ticker = sub.etf_ticker
    """
    with conn.cursor() as cur:
        cur.execute(query, [start, end])
        count = cur.rowcount
    conn.commit()
    return count


def rank_etf_index_by_sql(conn, ref_ticker: str, start: str, end: str) -> int:
    """Update rs_rank_index on fact_etf_metrics."""
    query = """
        UPDATE fact_etf_metrics AS m
        SET rs_rank_index = sub.pct_rank
        FROM (
            SELECT date, etf_ticker, ref_ticker,
                   PERCENT_RANK() OVER (
                       PARTITION BY date, ref_ticker
                       ORDER BY relative_strength
                   ) AS pct_rank
            FROM fact_etf_metrics
            WHERE date BETWEEN %s AND %s AND ref_ticker = %s
        ) sub
        WHERE m.date = sub.date
          AND m.etf_ticker = sub.etf_ticker
          AND m.ref_ticker = sub.ref_ticker
    """
    with conn.cursor() as cur:
        cur.execute(query, [start, end, ref_ticker])
        count = cur.rowcount
    conn.commit()
    return count


def rank_etf_overall_by_sql(conn, start: str, end: str) -> int:
    """Update rs_rank_overall on fact_etf_metrics."""
    query = """
        UPDATE fact_etf_metrics AS m
        SET rs_rank_overall = sub.pct_rank
        FROM (
            SELECT date, etf_ticker, ref_ticker,
                   PERCENT_RANK() OVER (
                       PARTITION BY date
                       ORDER BY relative_strength
                   ) AS pct_rank
            FROM fact_etf_metrics
            WHERE date BETWEEN %s AND %s
        ) sub
        WHERE m.date = sub.date
          AND m.etf_ticker = sub.etf_ticker
          AND m.ref_ticker = sub.ref_ticker
    """
    with conn.cursor() as cur:
        cur.execute(query, [start, end])
        count = cur.rowcount
    conn.commit()
    return count


# ---------------------------------------------------------------------------
# Dimension table helpers (for ETF onboarding)
# ---------------------------------------------------------------------------

def upsert_dim_stock(conn, ticker: str, company_name: str = None,
                     sector: str = None, industry: str = None) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_stock (ticker, company_name, sector, industry)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, dim_stock.company_name),
                sector = COALESCE(EXCLUDED.sector, dim_stock.sector),
                industry = COALESCE(EXCLUDED.industry, dim_stock.industry)
        """, (ticker, company_name, sector, industry))
    conn.commit()


def upsert_dim_etf(conn, etf_ticker: str, etf_name: str = None) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_etf (etf_ticker, etf_name)
            VALUES (%s, %s)
            ON CONFLICT (etf_ticker) DO NOTHING
        """, (etf_ticker, etf_name))
    conn.commit()


def upsert_junc_stock_etf(conn, ticker: str, etf_ticker: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO junc_stock_etf (ticker, etf_ticker)
            VALUES (%s, %s)
            ON CONFLICT (ticker, etf_ticker) DO NOTHING
        """, (ticker, etf_ticker))
    conn.commit()


def bulk_upsert_dim_stock(conn, records: List[tuple]) -> None:
    """records: list of (ticker, company_name, sector, industry)."""
    if not records:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_stock (ticker, company_name, sector, industry)
            VALUES %s
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, dim_stock.company_name),
                sector = COALESCE(EXCLUDED.sector, dim_stock.sector),
                industry = COALESCE(EXCLUDED.industry, dim_stock.industry)
        """, records, page_size=500)
    conn.commit()


def bulk_upsert_junc_stock_etf(conn, records: List[tuple]) -> None:
    """records: list of (ticker, etf_ticker)."""
    if not records:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO junc_stock_etf (ticker, etf_ticker)
            VALUES %s
            ON CONFLICT (ticker, etf_ticker) DO NOTHING
        """, records, page_size=500)
    conn.commit()


# ---------------------------------------------------------------------------
# Delisted ticker management
# ---------------------------------------------------------------------------

def insert_delisted_ticker(conn, ticker: str, detected_date: str,
                           etf_list: List[str]) -> None:
    """Record a ticker as delisted."""
    etfs_str = ','.join(etf_list) if etf_list else None
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO delisted_tickers (ticker, detected_date, original_etfs)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO NOTHING
        """, (ticker, detected_date, etfs_str))
    conn.commit()


def fetch_delisted_tickers(conn) -> set:
    """Return set of tickers known to be delisted."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM delisted_tickers")
        return {row[0] for row in cur.fetchall()}


def soft_delete_ticker(conn, ticker: str) -> None:
    """Soft-delete a ticker: log to delisted_tickers and remove junction rows.

    Preserves dim_stock, fact_commons_ohlc, and fact_stock_metrics.
    """
    # Find which ETFs this ticker belongs to
    with conn.cursor() as cur:
        cur.execute(
            "SELECT etf_ticker FROM junc_stock_etf WHERE ticker = %s",
            (ticker,)
        )
        etf_list = [row[0] for row in cur.fetchall()]

    # Record in delisted_tickers
    insert_delisted_ticker(conn, ticker, date.today().isoformat(), etf_list)

    # Remove junction rows
    with conn.cursor() as cur:
        cur.execute("DELETE FROM junc_stock_etf WHERE ticker = %s", (ticker,))
    conn.commit()

    logger.info("Soft-deleted ticker %s (was in ETFs: %s)", ticker,
                ','.join(etf_list) if etf_list else 'none')
