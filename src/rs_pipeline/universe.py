"""Adding ETFs and keeping stock metadata (and therefore sector links) filled in."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from psycopg2.extensions import connection as Connection

from rs_pipeline import db
from rs_pipeline.sources import DataSources

logger = logging.getLogger(__name__)


def onboard_etf(conn: Connection, sources: DataSources, etf_ticker: str) -> list[str] | None:
    """Register an ETF and link its holdings. Returns the holdings, or None if unknown."""
    holdings = sources.holdings.holdings(etf_ticker)
    if not holdings:
        logger.error("No holdings found for %s; not onboarded", etf_ticker)
        return None
    db.upsert_etf(conn, etf_ticker, sources.info.etf_name(etf_ticker))
    db.link_stocks_to_etf(conn, etf_ticker, holdings)
    logger.info("Onboarded %s with %d holdings", etf_ticker, len(holdings))
    return holdings


def collect_stock_info(sources: DataSources, tickers: Sequence[str]) -> list[db.Row]:
    """Metadata rows for *tickers* (one vendor call each; needs no connection)."""
    records: list[db.Row] = []
    for ticker in tickers:
        info = sources.info.stock_info(ticker)
        if info is not None:
            records.append((ticker, *info))
    return records


def store_stock_info(conn: Connection, records: Sequence[db.Row]) -> None:
    """Store metadata, then link stocks to the ETFs of their sector and industry."""
    db.upsert_stocks(conn, records)
    if records:
        logger.info("Stored metadata for %d stocks", len(records))
    db.link_stocks_by_sector_and_industry(conn)
