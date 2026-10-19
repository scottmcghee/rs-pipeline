"""Twelve Data REST API, used as a fallback for tickers Yahoo misses.

Needs ``TWELVE_DATA_API_KEY`` and the optional ``twelvedata`` extra.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from datetime import date, timedelta

import pandas as pd

from rs_pipeline import config
from rs_pipeline.retry import with_backoff
from rs_pipeline.sources.base import normalize_ohlc

logger = logging.getLogger(__name__)


class TwelveDataPrices:
    def __init__(
        self,
        api_key: str,
        batch_size: int = config.TWELVE_DATA_BATCH_SIZE,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        import twelvedata

        self._client = twelvedata.TDClient(apikey=api_key)
        self._batch_size = batch_size
        self._sleep = sleep

    def download(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for i in range(0, len(tickers), self._batch_size):
            batch = list(tickers[i : i + self._batch_size])
            try:
                data = with_backoff(
                    lambda b=batch: self._fetch(b, start, end),  # type: ignore[misc]
                    what="Twelve Data",
                    sleep=self._sleep,
                )
            except Exception as exc:
                # Not logger.exception: the traceback can include the request URL and key.
                logger.error(
                    "Twelve Data failed for %d tickers: %s", len(batch), type(exc).__name__
                )
                continue
            for ticker, df in _split_by_symbol(data, batch).items():
                normalized = normalize_ohlc(df, start, end)
                if normalized is not None:
                    result[ticker] = normalized
        logger.info("Twelve Data returned %d of %d tickers", len(result), len(tickers))
        return result

    def _fetch(self, batch: list[str], start: date, end: date) -> object:
        # Request one extra day so the range is inclusive whichever way the
        # API treats end_date; normalize_ohlc clips the extra day off.
        series = self._client.time_series(
            symbol=",".join(batch),
            interval="1day",
            start_date=start.isoformat(),
            end_date=(end + timedelta(days=1)).isoformat(),
            outputsize=5000,
        )
        return series.as_pandas()

    def recheck(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        return {}


def _split_by_symbol(data: object, batch: list[str]) -> dict[str, pd.DataFrame]:
    """One frame per symbol from any of the shapes ``as_pandas()`` returns.

    A single symbol gives a plain frame; several give either a dict of frames
    or one frame indexed by (symbol, datetime), depending on the version.
    """
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items() if isinstance(v, pd.DataFrame)}
    if not isinstance(data, pd.DataFrame):
        return {}
    if isinstance(data.index, pd.MultiIndex):
        symbols = data.index.get_level_values(0).unique()
        return {
            str(sym): data.loc[sym] for sym in symbols if isinstance(data.loc[sym], pd.DataFrame)
        }
    return {batch[0]: data} if len(batch) == 1 else {}
