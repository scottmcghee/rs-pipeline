"""Where outside data comes from: live vendors, or a synthetic market."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

import pandas as pd

from rs_pipeline.config import Settings
from rs_pipeline.sources.base import HoldingsSource, InfoSource, PriceSource

logger = logging.getLogger(__name__)

SOURCE_NAMES = ("live", "synthetic")


class FallbackPrices:
    """Tries each price source in turn for the tickers the previous ones missed."""

    def __init__(self, *sources: PriceSource) -> None:
        self._sources = sources

    def download(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for source in self._sources:
            missing = [t for t in tickers if t not in result]
            if not missing:
                break
            result.update(source.download(missing, start, end))
        return result

    def recheck(self, tickers: Sequence[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for source in self._sources:
            missing = [t for t in tickers if t not in result]
            if not missing:
                break
            result.update(source.recheck(missing, start, end))
        return result


@dataclass(frozen=True)
class DataSources:
    prices: PriceSource
    info: InfoSource
    holdings: HoldingsSource


def build_sources(name: str, settings: Settings) -> DataSources:
    """``live``: Yahoo, plus Twelve Data and AlphaVantage when their keys are set.
    ``synthetic``: the offline fictional market.
    """
    if name == "synthetic":
        from rs_pipeline.sources.synthetic import SyntheticMarket

        market = SyntheticMarket()
        return DataSources(prices=market, info=market, holdings=market)
    if name != "live":
        raise ValueError(f"unknown data source {name!r}; expected one of {SOURCE_NAMES}")

    from rs_pipeline.sources.holdings import (
        AlphaVantageHoldings,
        CsvHoldings,
        FirstAvailableHoldings,
    )
    from rs_pipeline.sources.yahoo import YahooInfo, YahooPrices

    prices: list[PriceSource] = [YahooPrices()]
    if settings.twelve_data_api_key:
        try:
            from rs_pipeline.sources.twelvedata import TwelveDataPrices

            prices.append(TwelveDataPrices(settings.twelve_data_api_key))
        except ImportError:
            logger.warning("TWELVE_DATA_API_KEY is set but the twelvedata extra is not installed")

    holdings: list[CsvHoldings | AlphaVantageHoldings] = []
    if settings.holdings_csv_dir:
        holdings.append(CsvHoldings(settings.holdings_csv_dir))
    if settings.alphavantage_api_key:
        holdings.append(AlphaVantageHoldings(settings.alphavantage_api_key))

    return DataSources(
        prices=FallbackPrices(*prices),
        info=YahooInfo(),
        holdings=FirstAvailableHoldings(*holdings),
    )
