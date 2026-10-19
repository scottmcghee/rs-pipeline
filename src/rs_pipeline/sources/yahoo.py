"""Yahoo Finance via the ``yfinance`` package (no API key needed)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from rs_pipeline import config
from rs_pipeline.retry import is_rate_limit, with_backoff
from rs_pipeline.sources.base import StockInfo, normalize_ohlc

logger = logging.getLogger(__name__)

# Prices are split- and dividend-adjusted (auto_adjust=True), so a split
# never shows up as a crash in relative strength.
#
# A large batch coming back completely empty usually means Yahoo is
# throttling us; a small one usually means the tickers are dead.
_THROTTLE_SUSPECT_MIN_BATCH = 10


def _exclusive_end(end: date) -> str:
    # yfinance treats ``end`` as exclusive; we treat it as inclusive.
    return (end + timedelta(days=1)).isoformat()


def _column_for(df: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """The sub-frame for *ticker* from a multi-ticker ``yf.download`` result."""
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    for level in range(df.columns.nlevels):
        if ticker in df.columns.get_level_values(level):
            sub = df.xs(ticker, axis=1, level=level)
            return sub if isinstance(sub, pd.DataFrame) else None
    return None


class YahooPrices:
    def __init__(
        self,
        batch_size: int = config.YFINANCE_BATCH_SIZE,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._batch_size = batch_size
        self._sleep = sleep

    def download(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for i in range(0, len(tickers), self._batch_size):
            batch = list(tickers[i : i + self._batch_size])
            logger.info("Yahoo: tickers %d-%d of %d", i + 1, i + len(batch), len(tickers))
            result.update(self._download_batch(batch, start, end))
        return result

    def _download_batch(self, batch: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        retries = config.RATE_LIMIT_MAX_RETRIES if len(batch) > _THROTTLE_SUSPECT_MIN_BATCH else 0
        for attempt in range(retries + 1):
            try:
                df = yf.download(
                    batch,
                    start=start.isoformat(),
                    end=_exclusive_end(end),
                    group_by="ticker",
                    threads=True,
                    progress=False,
                    auto_adjust=True,
                )
            except Exception:
                logger.exception("yf.download failed (attempt %d)", attempt + 1)
                df = None
            if df is not None and not df.empty:
                frames = {}
                for ticker in batch:
                    normalized = normalize_ohlc(_column_for(df, ticker), start, end)
                    if normalized is not None:
                        frames[ticker] = normalized
                return frames
            if attempt < retries:
                delay = config.RATE_LIMIT_BACKOFF_BASE * 2**attempt
                logger.warning(
                    "Empty Yahoo result for %d tickers; retrying in %ds", len(batch), delay
                )
                self._sleep(delay)
        return {}

    def recheck(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        """``Ticker.history`` one at a time; batch downloads drop valid tickers."""
        recovered: dict[str, pd.DataFrame] = {}
        for ticker in tickers:
            try:
                df = yf.Ticker(ticker).history(
                    start=start.isoformat(), end=_exclusive_end(end), auto_adjust=True
                )
            except Exception as exc:
                logger.debug("Yahoo history failed for %s: %s", ticker, exc)
                continue
            normalized = normalize_ohlc(df, start, end)
            if normalized is not None:
                recovered[ticker] = normalized
        return recovered


class YahooInfo:
    """Company metadata. After a few lookups in a row fail for rate limiting,
    the rest of the run skips lookups (they are retried next run) instead of
    waiting minutes per ticker.
    """

    MAX_THROTTLED = 3

    def __init__(self, sleep: Callable[[float], None] = time.sleep) -> None:
        self._sleep = sleep
        self._throttled = 0

    def _info(self, ticker: str) -> dict[str, object] | None:
        if self._throttled >= self.MAX_THROTTLED:
            return None
        try:
            info = with_backoff(
                lambda: yf.Ticker(ticker).info, what=f"info {ticker}", sleep=self._sleep
            )
        except Exception as exc:
            if is_rate_limit(exc):
                self._throttled += 1
                if self._throttled == self.MAX_THROTTLED:
                    logger.warning("Yahoo keeps rate-limiting; skipping metadata until next run")
            else:
                logger.warning("Yahoo info failed for %s: %s", ticker, exc)
            return None
        self._throttled = 0
        return info if isinstance(info, dict) else None

    def stock_info(self, ticker: str) -> StockInfo | None:
        info = self._info(ticker)
        if not info:
            return None

        def text(key: str) -> str | None:
            value = info.get(key)
            return str(value) if value else None

        return StockInfo(text("longName"), text("sector"), text("industry"), text("exchange"))

    def etf_name(self, ticker: str) -> str | None:
        info = self._info(ticker) or {}
        name = info.get("longName") or info.get("shortName")
        return str(name) if name else None
