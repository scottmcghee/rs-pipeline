"""Step 1: Fetch OHLC price data — incremental, batch yfinance."""

import logging
from datetime import timedelta
from typing import List, Optional

import pandas as pd

from rs_pipeline import config
from rs_pipeline.db import (
    ConnectionPool,
    bulk_upsert_ohlc,
    ensure_delisted_table,
    fetch_all_etfs_from_db,
    fetch_all_holdings,
    fetch_delisted_tickers,
    fetch_holdings_for_etf,
    fetch_latest_ohlc_dates,
    soft_delete_ticker,
)
from rs_pipeline.fetch import (
    batch_download_ohlc,
    fetch_missing_stock_info,
    onboard_new_etf,
)
from rs_pipeline.db import bulk_upsert_dim_stock

logger = logging.getLogger(__name__)


def run(pool: ConnectionPool, args, indices: Optional[List[str]] = None) -> None:
    logger.info("=== Step 1: fetch-prices ===")

    start_date = args.start_date or config.FULL_HISTORY_START
    end_date = args.end_date

    with pool.connection() as conn:
        # Ensure delisted_tickers table exists
        ensure_delisted_table(conn)

        # Load known-delisted tickers to skip
        delisted = fetch_delisted_tickers(conn)
        if delisted:
            logger.info("Skipping %d known delisted tickers", len(delisted))

        # Determine which ETFs to process
        if indices:
            # Check for unknown ETFs and onboard them
            known_holdings = fetch_all_holdings(conn)
            for etf in indices:
                if etf not in known_holdings:
                    logger.info("ETF %s not in DB — onboarding", etf)
                    onboard_new_etf(conn, etf)

            # Collect tickers for requested ETFs
            all_holdings = {}
            for etf in indices:
                tickers = fetch_holdings_for_etf(conn, etf)
                if tickers:
                    all_holdings[etf] = tickers
        else:
            all_holdings = fetch_all_holdings(conn)

        if not all_holdings:
            logger.warning("No ETF holdings found. Nothing to fetch.")
            return

        # Deduplicate stock tickers across all ETFs, excluding delisted
        all_stock_tickers = sorted(
            {t for ts in all_holdings.values() for t in ts} - delisted
        )
        # Also collect the ETF tickers themselves (for fact_index_ohlc)
        all_etf_tickers = sorted(all_holdings.keys())

        logger.info("Total unique stocks: %d across %d ETFs",
                     len(all_stock_tickers), len(all_etf_tickers))

        # --- Fetch incremental stock OHLC ---
        latest_commons = fetch_latest_ohlc_dates(conn, 'fact_commons_ohlc')
        stocks_to_fetch = {}
        for ticker in all_stock_tickers:
            last = latest_commons.get(ticker)
            fetch_start = start_date
            if last is not None:
                next_day = last + timedelta(days=1)
                if str(next_day) > end_date:
                    continue  # already up to date
                fetch_start = str(next_day)
            stocks_to_fetch[ticker] = fetch_start

        if stocks_to_fetch:
            # Group by start_date to minimise yf.download calls
            by_start: dict[str, list[str]] = {}
            for ticker, s in stocks_to_fetch.items():
                by_start.setdefault(s, []).append(ticker)

            total_upserted = 0
            all_still_missing: list[str] = []
            for fetch_start, tickers in by_start.items():
                logger.info("Downloading %d stocks from %s to %s",
                             len(tickers), fetch_start, end_date)
                ohlc_data, still_missing = batch_download_ohlc(
                    tickers, fetch_start, end_date)
                all_still_missing.extend(still_missing)
                for ticker, df in ohlc_data.items():
                    records = [
                        (idx.date(), ticker,
                         float(row['open']), float(row['high']),
                         float(row['low']), float(row['close']),
                         int(row['volume']))
                        for idx, row in df.iterrows()
                    ]
                    total_upserted += bulk_upsert_ohlc(
                        conn, 'fact_commons_ohlc', records)

            logger.info("Stock OHLC: %d rows upserted", total_upserted)

            # --- Handle delisted tickers ---
            if all_still_missing:
                # Filter out tickers already known as delisted
                new_delistings = [t for t in all_still_missing
                                  if t not in delisted]
                if new_delistings:
                    logger.warning("Delisted %d tickers: %s",
                                   len(new_delistings),
                                   ', '.join(new_delistings[:20]))
                    for ticker in new_delistings:
                        soft_delete_ticker(conn, ticker)
        else:
            logger.info("All stock OHLC up to date")

        # --- Fetch incremental ETF/index OHLC ---
        latest_index = fetch_latest_ohlc_dates(conn, 'fact_index_ohlc')
        etfs_to_fetch = {}
        for etf in all_etf_tickers:
            last = latest_index.get(etf)
            fetch_start = start_date
            if last is not None:
                next_day = last + timedelta(days=1)
                if str(next_day) > end_date:
                    continue
                fetch_start = str(next_day)
            etfs_to_fetch[etf] = fetch_start

        if etfs_to_fetch:
            by_start_etf: dict[str, list[str]] = {}
            for etf, s in etfs_to_fetch.items():
                by_start_etf.setdefault(s, []).append(etf)

            total_etf = 0
            for fetch_start, etfs in by_start_etf.items():
                logger.info("Downloading %d ETFs from %s to %s",
                             len(etfs), fetch_start, end_date)
                ohlc_data, _ = batch_download_ohlc(etfs, fetch_start, end_date)
                for etf, df in ohlc_data.items():
                    records = [
                        (idx.date(), etf,
                         float(row['open']), float(row['high']),
                         float(row['low']), float(row['close']),
                         int(row['volume']))
                        for idx, row in df.iterrows()
                    ]
                    total_etf += bulk_upsert_ohlc(
                        conn, 'fact_index_ohlc', records)

            logger.info("ETF OHLC: %d rows upserted", total_etf)
        else:
            logger.info("All ETF OHLC up to date")

        # --- Update dim_stock for new tickers ---
        stock_info = fetch_missing_stock_info(conn, all_stock_tickers)
        if stock_info:
            bulk_upsert_dim_stock(conn, stock_info)
            logger.info("Updated dim_stock for %d new tickers", len(stock_info))

    logger.info("=== Step 1 complete ===")
