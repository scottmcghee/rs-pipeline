"""Step 2: Compute RS metrics — incremental, vectorized."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd

from rs_pipeline import config
from rs_pipeline.db import (
    ConnectionPool,
    bulk_upsert_stock_metrics,
    fetch_all_holdings,
    fetch_all_ohlc,
    fetch_holdings_for_etf,
    fetch_last_metric_dates,
)
from rs_pipeline.rs import compute_all_metrics

logger = logging.getLogger(__name__)


def _compute_for_etf(pool: ConnectionPool,
                     etf_ticker: str,
                     stock_tickers: List[str],
                     window: int,
                     end_date: str,
                     full_recompute: bool) -> int:
    """Compute RS metrics for all stocks in one ETF. Returns rows written."""
    with pool.connection() as conn:
        # Determine last computed dates
        if full_recompute:
            last_dates: Dict[str, date] = {}
            global_start = config.FULL_HISTORY_START
        else:
            last_dates = fetch_last_metric_dates(conn, etf_ticker)
            global_start = config.FULL_HISTORY_START

        # Determine the earliest OHLC we need (2*window lookback)
        lookback_days = 2 * window + 63  # extra buffer for 4-quarter calc
        earliest_ohlc = str(
            pd.Timestamp(global_start) - pd.Timedelta(days=int(lookback_days * 1.5))
        )[:10]

        # Load all stock OHLC in one query
        stock_data = fetch_all_ohlc(
            conn, stock_tickers, earliest_ohlc, end_date, 'fact_commons_ohlc')

        # Load index OHLC
        index_data = fetch_all_ohlc(
            conn, [etf_ticker], earliest_ohlc, end_date, 'fact_index_ohlc')
        if etf_ticker not in index_data:
            logger.warning("No index OHLC for %s", etf_ticker)
            return 0
        idx_df = index_data[etf_ticker]

        total_written = 0
        for ticker in stock_tickers:
            if ticker not in stock_data:
                continue
            s_df = stock_data[ticker]

            # Compute all metrics across full loaded range
            metrics = compute_all_metrics(s_df, idx_df, window, ticker, etf_ticker)
            if metrics.empty:
                continue

            # Slice to only new dates (incremental)
            if not full_recompute and ticker in last_dates:
                cutoff = last_dates[ticker]
                metrics = metrics[metrics['date'] > pd.Timestamp(cutoff)]

            if metrics.empty:
                continue

            n = bulk_upsert_stock_metrics(conn, metrics)
            total_written += n

        return total_written


def run(pool: ConnectionPool, args, indices: Optional[List[str]] = None) -> None:
    logger.info("=== Step 2: compute-rs ===")

    end_date = args.end_date
    window = args.window
    max_workers = args.max_workers
    full_recompute = getattr(args, 'full_recompute', False)

    with pool.connection() as conn:
        if indices:
            all_holdings = {}
            for etf in indices:
                tickers = fetch_holdings_for_etf(conn, etf)
                if tickers:
                    all_holdings[etf] = tickers
        else:
            all_holdings = fetch_all_holdings(conn)

    if not all_holdings:
        logger.warning("No holdings found. Nothing to compute.")
        return

    logger.info("Computing RS for %d ETFs, %d total pairs",
                 len(all_holdings),
                 sum(len(v) for v in all_holdings.values()))

    total = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _compute_for_etf, pool, etf, tickers,
                window, end_date, full_recompute
            ): etf
            for etf, tickers in all_holdings.items()
        }
        for future in as_completed(futures):
            etf = futures[future]
            try:
                n = future.result()
                total += n
                if n > 0:
                    logger.info("ETF %s: %d metric rows written", etf, n)
            except Exception:
                logger.exception("Error computing RS for %s", etf)

    logger.info("=== Step 2 complete: %d total rows written ===", total)
