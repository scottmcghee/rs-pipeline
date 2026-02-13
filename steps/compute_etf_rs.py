"""Step 4: Compute ETF-vs-SPY RS metrics and rank."""

import logging
from typing import List, Optional

import pandas as pd

from rs_pipeline import config
from rs_pipeline.db import (
    ConnectionPool,
    bulk_upsert_etf_metrics,
    fetch_all_etf_tickers,
    fetch_all_ohlc,
    fetch_last_etf_metric_dates,
    rank_etf_index_by_sql,
    rank_etf_overall_by_sql,
)
from rs_pipeline.rs import compute_all_metrics

logger = logging.getLogger(__name__)

REF_TICKER = 'SPY'


def run(pool: ConnectionPool, args, indices: Optional[List[str]] = None) -> None:
    logger.info("=== Step 4: compute-etf-rs ===")

    end_date = args.end_date
    window = args.window
    full_recompute = getattr(args, 'full_recompute', False)

    with pool.connection() as conn:
        # Determine ETF list
        if indices:
            etf_list = [e for e in indices if e != REF_TICKER]
        else:
            etf_list = fetch_all_etf_tickers(conn)

        if not etf_list:
            logger.warning("No ETFs found.")
            return

        # Lookback
        lookback_days = 2 * window + 63
        global_start = config.FULL_HISTORY_START
        earliest = str(
            pd.Timestamp(global_start) - pd.Timedelta(days=int(lookback_days * 1.5))
        )[:10]

        # Load SPY once
        all_tickers = etf_list + [REF_TICKER]
        ohlc = fetch_all_ohlc(conn, all_tickers, earliest, end_date,
                              'fact_index_ohlc')
        if REF_TICKER not in ohlc:
            logger.error("No OHLC for %s", REF_TICKER)
            return
        spy_df = ohlc[REF_TICKER]

        # Incremental: last computed dates
        if full_recompute:
            last_dates = {}
        else:
            last_dates = fetch_last_etf_metric_dates(conn, REF_TICKER)

        total = 0
        for etf in etf_list:
            if etf not in ohlc:
                logger.info("No OHLC for %s, skipping", etf)
                continue

            etf_df = ohlc[etf]
            metrics = compute_all_metrics(etf_df, spy_df, window, etf, REF_TICKER)
            if metrics.empty:
                continue

            # Rename for ETF table: ticker→etf_ticker, etf_ticker→ref_ticker
            metrics = metrics.rename(columns={
                'ticker': 'etf_ticker',
                'etf_ticker': 'ref_ticker',
            })

            # Slice incremental
            if not full_recompute and etf in last_dates:
                cutoff = last_dates[etf]
                metrics = metrics[metrics['date'] > pd.Timestamp(cutoff)]

            if metrics.empty:
                continue

            n = bulk_upsert_etf_metrics(conn, metrics)
            total += n
            logger.info("ETF %s vs %s: %d rows", etf, REF_TICKER, n)

        # SQL-based rankings for ETFs
        start_date = args.start_date or end_date
        rank_etf_index_by_sql(conn, REF_TICKER, start_date, end_date)
        rank_etf_overall_by_sql(conn, start_date, end_date)

    logger.info("=== Step 4 complete: %d rows written ===", total)
