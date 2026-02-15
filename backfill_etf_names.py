"""One-off script to backfill etf_name in dim_etf via yfinance."""

import logging
import time

import yfinance as yf

from rs_pipeline.db import ConnectionPool, upsert_dim_etf, update_sector_industry_mappings

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    pool = ConnectionPool()

    # --- Part 1: Backfill ETF names ---
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT etf_ticker FROM dim_etf WHERE etf_name IS NULL ORDER BY etf_ticker")
            etfs = [row[0] for row in cur.fetchall()]

    logger.info("Found %d ETFs with NULL etf_name", len(etfs))

    updated = 0
    skipped = 0
    for i, ticker in enumerate(etfs, 1):
        try:
            info = yf.Ticker(ticker).info
            name = info.get('longName') or info.get('shortName')
            if name:
                with pool.connection() as conn:
                    upsert_dim_etf(conn, ticker, name)
                updated += 1
                logger.info("[%d/%d] %s → %s", i, len(etfs), ticker, name)
            else:
                skipped += 1
                logger.warning("[%d/%d] %s — no name found", i, len(etfs), ticker)
        except Exception as e:
            err_str = str(e).lower()
            if '429' in err_str or 'rate' in err_str:
                logger.warning("Rate limited at %s, sleeping 60s", ticker)
                time.sleep(60)
                # Retry once
                try:
                    info = yf.Ticker(ticker).info
                    name = info.get('longName') or info.get('shortName')
                    if name:
                        with pool.connection() as conn:
                            upsert_dim_etf(conn, ticker, name)
                        updated += 1
                        logger.info("[%d/%d] %s → %s (retry)", i, len(etfs), ticker, name)
                    else:
                        skipped += 1
                except Exception:
                    skipped += 1
                    logger.warning("[%d/%d] %s — failed after retry", i, len(etfs), ticker)
            else:
                skipped += 1
                logger.warning("[%d/%d] %s — error: %s", i, len(etfs), ticker, e)

    logger.info("ETF names: %d updated, %d skipped", updated, skipped)

    # --- Part 2: Run sector/industry mapping ---
    logger.info("Running sector/industry ETF mapping...")
    with pool.connection() as conn:
        total = update_sector_industry_mappings(conn)
    logger.info("Sector/industry mapping: %d new junction rows", total)

    pool.closeall()


if __name__ == '__main__':
    main()
