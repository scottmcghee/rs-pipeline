"""One-off script to backfill missing company_name, sector, industry, exchange in dim_stock."""

import logging
import time

import psycopg2
import yfinance as yf

from rs_pipeline.config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_COMMIT_SIZE = 50


def _update_ticker(cur, ticker: str, info: dict) -> bool:
    """Update dim_stock with info from yfinance. Returns True if updated."""
    name = info.get('longName')
    sector = info.get('sector')
    industry = info.get('industry')
    exchange = info.get('exchange')

    if name or sector or industry or exchange:
        cur.execute("""
            UPDATE dim_stock SET
                company_name = COALESCE(%s, company_name),
                sector = COALESCE(%s, sector),
                industry = COALESCE(%s, industry),
                exchange = COALESCE(%s, exchange)
            WHERE ticker = %s
        """, (name, sector, industry, exchange, ticker))
        return True
    return False


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT ticker FROM dim_stock
        WHERE company_name IS NULL OR sector IS NULL
           OR industry IS NULL OR exchange IS NULL
        ORDER BY ticker
    """)
    tickers = [r[0] for r in cur.fetchall()]
    logger.info("Found %d tickers to backfill", len(tickers))

    updated = 0
    skipped = 0
    for i, ticker in enumerate(tickers):
        try:
            info = yf.Ticker(ticker).info
            if _update_ticker(cur, ticker, info):
                updated += 1
            else:
                skipped += 1
        except Exception as e:
            err = str(e).lower()
            if '429' in err or 'rate' in err:
                logger.warning("Rate limited at %s, sleeping 60s", ticker)
                time.sleep(60)
                try:
                    info = yf.Ticker(ticker).info
                    if _update_ticker(cur, ticker, info):
                        updated += 1
                    else:
                        skipped += 1
                except Exception:
                    skipped += 1
            else:
                logger.debug("Failed for %s: %s", ticker, e)
                skipped += 1

        if (i + 1) % BATCH_COMMIT_SIZE == 0:
            conn.commit()
            logger.info("Progress: %d/%d (updated=%d, skipped=%d)",
                        i + 1, len(tickers), updated, skipped)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Done: %d updated, %d skipped out of %d", updated, skipped, len(tickers))


if __name__ == '__main__':
    main()
