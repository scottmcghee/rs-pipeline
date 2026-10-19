"""ETF holdings: local CSV files first, then the AlphaVantage ETF_PROFILE API."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

from rs_pipeline.sources.base import clean_tickers

logger = logging.getLogger(__name__)

ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"


class CsvHoldings:
    """Reads ``<dir>/<etf>.csv`` (lower-case file name) with a ``ticker`` column."""

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)

    def holdings(self, etf_ticker: str) -> list[str] | None:
        path = self._dir / f"{etf_ticker.lower()}.csv"
        if not path.is_file():
            return None
        logger.info("Holdings for %s from %s", etf_ticker, path)
        return clean_tickers(pd.read_csv(path)["ticker"].tolist())


class AlphaVantageHoldings:
    def __init__(self, api_key: str, timeout: float = 15) -> None:
        self._api_key = api_key
        self._timeout = timeout

    def holdings(self, etf_ticker: str) -> list[str] | None:
        try:
            resp = requests.get(
                ALPHAVANTAGE_URL,
                params={"function": "ETF_PROFILE", "symbol": etf_ticker, "apikey": self._api_key},
                timeout=self._timeout,
            )
            resp.raise_for_status()
            rows = resp.json().get("holdings", [])
        except (requests.RequestException, ValueError) as exc:
            # Only the error type: request errors embed the URL, which holds the key.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            logger.error(
                "AlphaVantage holdings failed for %s: %s%s",
                etf_ticker,
                type(exc).__name__,
                f" (HTTP {status})" if status else "",
            )
            return None
        return clean_tickers([row.get("symbol") for row in rows]) or None


class FirstAvailableHoldings:
    """Asks each source in turn and returns the first answer."""

    def __init__(self, *sources: CsvHoldings | AlphaVantageHoldings) -> None:
        self._sources = sources

    def holdings(self, etf_ticker: str) -> list[str] | None:
        for source in self._sources:
            found = source.holdings(etf_ticker)
            if found:
                return found
        return None
