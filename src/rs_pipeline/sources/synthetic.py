"""A fictional, reproducible market for demos and tests (no network).

Every ticker here is made up. Prices come from a factor model:

    stock return = beta * market + sector + idiosyncratic(drift(t), vol)

where each stock's drift rotates slowly, so relative-strength leadership
changes over time. Sector and industry ETFs are built from the same factors,
so ranks behave like real ones: a stock beating its sector shows up as
RS above 100.

Series are generated once for a fixed calendar, so any date range always
returns the same prices, and only dates up to ``today`` are ever returned.
Unknown tickers return no data, like a delisted symbol.
"""

from __future__ import annotations

import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from functools import cached_property

import numpy as np
import pandas as pd

from rs_pipeline.sources.base import StockInfo

SEED = 7
_CALENDAR = pd.bdate_range("2015-01-01", "2075-12-31", name="date")

BENCHMARK = "MKT"
THEMATIC = "ROBX"

# Sector and industry names are deliberately unlike the ones real vendors
# report, so demo mappings can never be mistaken for real ones.
SECTOR_ETFS = {
    "Tech": "TCHX",
    "Finance": "FINX",
    "Health": "HLTX",
    "Fuel": "ENGX",
}
INDUSTRY_ETFS = {
    "Chips": "SEMX",
    "Lenders": "BNKX",
}
_INDUSTRIES = {
    "Tech": ("Chips", "Cloud Software"),
    "Finance": ("Lenders", "Insurers"),
    "Health": ("Gene Therapy", "Devices"),
    "Fuel": ("Drillers", "Pipelines"),
}
ETF_NAMES = {
    BENCHMARK: "Synthetic Total Market",
    THEMATIC: "Synthetic Robotics & Automation",
    **{etf: f"Synthetic {name}" for name, etf in {**SECTOR_ETFS, **INDUSTRY_ETFS}.items()},
}

_SYLLABLES = ("ka", "ro", "ve", "lu", "ni", "to", "sa", "mi", "do", "re", "zu", "pa", "qi", "fe")
_SUFFIXES = ("Systems", "Holdings", "Labs", "Group", "Industries", "Partners", "Corp", "Works")
STOCKS_PER_SECTOR = 10


@dataclass(frozen=True)
class SyntheticStock:
    ticker: str
    company_name: str
    sector: str
    industry: str
    exchange: str


def _rng(*key: str) -> np.random.Generator:
    return np.random.default_rng([SEED, *(zlib.crc32(k.encode()) for k in key)])


def _universe() -> list[SyntheticStock]:
    rng = _rng("universe")
    reserved = set(ETF_NAMES)
    stocks: list[SyntheticStock] = []
    for sector, industries in _INDUSTRIES.items():
        for i in range(STOCKS_PER_SECTOR):
            while True:
                word = "".join(rng.choice(_SYLLABLES, size=3)).capitalize()
                ticker = word[:4].upper()
                if ticker not in reserved:
                    break
            reserved.add(ticker)
            stocks.append(
                SyntheticStock(
                    ticker=ticker,
                    company_name=f"{word} {rng.choice(_SUFFIXES)}",
                    sector=sector,
                    industry=industries[i % len(industries)],
                    exchange=str(rng.choice(["NMS", "NYQ"])),
                )
            )
    return stocks


class SyntheticMarket:
    """Implements PriceSource, InfoSource and HoldingsSource."""

    def __init__(self, today: Callable[[], date] = date.today) -> None:
        self._today = today

    @cached_property
    def stocks(self) -> dict[str, SyntheticStock]:
        return {s.ticker: s for s in _universe()}

    @cached_property
    def etf_holdings(self) -> dict[str, list[str]]:
        """Holdings of the ETFs that are onboarded directly (not via sectors)."""
        tickers = sorted(self.stocks)
        tech = [t for t in tickers if self.stocks[t].sector == "Tech"]
        others = [t for t in tickers if t not in tech]
        return {BENCHMARK: tickers, THEMATIC: tech[:6] + others[::7]}

    @cached_property
    def _closes(self) -> pd.DataFrame:
        n = len(_CALENDAR)
        market = _rng(BENCHMARK).normal(0.0004, 0.010, n)
        sectors = {s: _rng("sector", s).normal(0.0, 0.006, n) for s in _INDUSTRIES}
        t = np.arange(n)

        returns: dict[str, np.ndarray] = {BENCHMARK: market}
        for ticker, stock in self.stocks.items():
            rng = _rng("stock", ticker)
            beta = rng.uniform(0.8, 1.3)
            base_drift = rng.normal(0.0, 0.0003)
            swing = rng.uniform(0.0004, 0.0012)
            period = rng.uniform(150, 400)
            phase = rng.uniform(0, 2 * np.pi)
            drift = base_drift + swing * np.sin(2 * np.pi * t / period + phase)
            idio = rng.normal(0.0, rng.uniform(0.010, 0.022), n) + drift
            returns[ticker] = beta * market + sectors[stock.sector] + idio

        for sector, etf in SECTOR_ETFS.items():
            returns[etf] = market + sectors[sector]
        for etf, members in {
            **{
                e: [t for t, s in self.stocks.items() if s.industry == ind]
                for ind, e in INDUSTRY_ETFS.items()
            },
            THEMATIC: self.etf_holdings[THEMATIC],
        }.items():
            returns[etf] = np.mean([returns[m] for m in members], axis=0)

        start_prices = {k: _rng("price", k).uniform(20, 200) for k in returns}
        return pd.DataFrame(
            {
                k: start_prices[k] * np.cumprod(1 + np.clip(r, -0.5, 0.5))
                for k, r in returns.items()
            },
            index=_CALENDAR,
        )

    def _ohlc(self, ticker: str, start: date, end: date) -> pd.DataFrame | None:
        if ticker not in self._closes:
            return None
        end = min(end, self._today())
        close = self._closes[ticker]
        rng = _rng("ohlc", ticker)
        n = len(close)
        open_ = close.shift(1).fillna(close.iloc[0]) * (1 + rng.normal(0, 0.003, n))
        wick_up = 1 + np.abs(rng.normal(0, 0.006, n))
        wick_down = 1 - np.abs(rng.normal(0, 0.006, n))
        df = pd.DataFrame(
            {
                "open": open_,
                "high": np.maximum(open_, close) * wick_up,
                "low": np.minimum(open_, close) * wick_down,
                "close": close,
                "volume": np.round(rng.lognormal(14, 0.6, n)),
            }
        ).round({"open": 4, "high": 4, "low": 4, "close": 4})
        df = df.loc[pd.Timestamp(start) : pd.Timestamp(end)]
        return df if not df.empty else None

    # PriceSource

    def download(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        frames = {t: self._ohlc(t, start, end) for t in tickers}
        return {t: df for t, df in frames.items() if df is not None}

    def recheck(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        return {}

    # InfoSource

    def stock_info(self, ticker: str) -> StockInfo | None:
        s = self.stocks.get(ticker)
        return StockInfo(s.company_name, s.sector, s.industry, s.exchange) if s else None

    def etf_name(self, ticker: str) -> str | None:
        return ETF_NAMES.get(ticker)

    # HoldingsSource

    def holdings(self, etf_ticker: str) -> list[str] | None:
        return self.etf_holdings.get(etf_ticker)
