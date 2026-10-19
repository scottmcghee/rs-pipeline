"""Interfaces for the outside data the pipeline needs, and shared helpers.

All date ranges are inclusive at both ends. Price frames are "normalized":
a ``DatetimeIndex`` named ``date`` (sorted, tz-naive) and float columns
``open, high, low, close, volume``, with no rows missing a close.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date
from typing import NamedTuple, Protocol

import pandas as pd

OHLC = ("open", "high", "low", "close", "volume")


class StockInfo(NamedTuple):
    company_name: str | None
    sector: str | None
    industry: str | None
    exchange: str | None


class PriceSource(Protocol):
    def download(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        """Fast bulk download. Tickers with no data are left out of the result."""
        ...

    def recheck(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        """Slow per-ticker retry for tickers ``download`` missed."""
        ...


class InfoSource(Protocol):
    def stock_info(self, ticker: str) -> StockInfo | None: ...

    def etf_name(self, ticker: str) -> str | None: ...


class HoldingsSource(Protocol):
    def holdings(self, etf_ticker: str) -> list[str] | None:
        """Constituent tickers, or None when the ETF is unknown to this source."""
        ...


def normalize_ohlc(df: pd.DataFrame | None, start: date, end: date) -> pd.DataFrame | None:
    """Normalize a vendor frame and clip it to ``[start, end]``.

    Accepts title-case or lower-case columns, and two-level columns such as
    ``("AAPL", "Close")`` in either order. Returns None when nothing usable
    is left.
    """
    if df is None or df.empty:
        return None
    df = df.copy()
    df.columns = [_field_name(c) for c in df.columns]
    if not set(OHLC).issubset(df.columns):
        return None
    df = df[list(OHLC)].astype(float).dropna(subset=["close"])
    index = pd.to_datetime(df.index)
    if index.tz is not None:
        index = index.tz_localize(None)
    df.index = index.normalize()
    df.index.name = "date"
    # Vendors occasionally return a provisional and a final row for one day.
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df = df[(df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))]
    return df if not df.empty else None


def _field_name(column: object) -> str:
    parts = column if isinstance(column, tuple) else (column,)
    for part in parts:
        if str(part).lower() in OHLC:
            return str(part).lower()
    return str(parts[-1]).lower()


_JUNK_TICKERS = frozenset({"n/a", "na", "none", "null", "nan", ""})
# Holdings feeds write US share classes as BRK.B; Yahoo (and this database)
# use BRK-B. Only classes A-C are converted, so exchange suffixes such as
# VOD.L (London) are left alone.
_SHARE_CLASS = re.compile(r"^([A-Z]+)[./]([A-C])$")


def clean_tickers(raw: Sequence[object]) -> list[str]:
    """Upper-cased, de-duplicated tickers, without the junk holdings feeds contain.

    Share-class suffixes are written with a dash (``BRK.B`` becomes ``BRK-B``).
    """
    seen: dict[str, None] = {}
    for item in raw:
        ticker = str(item).strip().upper()
        if ticker.lower() not in _JUNK_TICKERS:
            seen.setdefault(_SHARE_CLASS.sub(r"\1-\2", ticker), None)
    return list(seen)
