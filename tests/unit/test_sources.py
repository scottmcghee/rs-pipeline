"""Data sources: normalization, vendors (mocked), fallback chains, synthetic market."""

from __future__ import annotations

import sys
import types
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from rs_pipeline.config import Settings
from rs_pipeline.sources import FallbackPrices, build_sources
from rs_pipeline.sources.base import StockInfo, clean_tickers, normalize_ohlc
from rs_pipeline.sources.holdings import AlphaVantageHoldings, CsvHoldings, FirstAvailableHoldings
from rs_pipeline.sources.synthetic import BENCHMARK, THEMATIC, SyntheticMarket
from rs_pipeline.sources.yahoo import YahooInfo, YahooPrices

D1, D2, D3 = date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)


def vendor_frame(days: list[date], title_case: bool = True) -> pd.DataFrame:
    cols = ["Open", "High", "Low", "Close", "Volume"]
    df = pd.DataFrame(
        [[1.0, 2.0, 0.5, 1.5, 100.0]] * len(days), index=pd.to_datetime(days), columns=cols
    )
    return df if title_case else df.rename(columns=str.lower)


class TestNormalize:
    def test_lowercases_and_orders_columns(self) -> None:
        df = normalize_ohlc(vendor_frame([D1, D2]), D1, D2)
        assert df is not None
        assert list(df.columns) == ["open", "high", "low", "close", "volume"]
        assert df.index.name == "date"

    def test_clips_to_inclusive_range(self) -> None:
        df = normalize_ohlc(vendor_frame([D1, D2, D3]), D2, D2)
        assert df is not None
        assert list(df.index.date) == [D2]

    @pytest.mark.parametrize("order", ["ticker_first", "field_first"])
    def test_two_level_columns(self, order: str) -> None:
        raw = vendor_frame([D1])
        pairs = [("AAA", c) if order == "ticker_first" else (c, "AAA") for c in raw.columns]
        raw.columns = pd.MultiIndex.from_tuples(pairs)
        df = normalize_ohlc(raw, D1, D1)
        assert df is not None
        assert df["close"].iloc[0] == 1.5

    def test_strips_timezone_and_time(self) -> None:
        raw = vendor_frame([D1])
        raw.index = pd.DatetimeIndex(["2024-01-02 09:30"]).tz_localize("America/New_York")
        df = normalize_ohlc(raw, D1, D1)
        assert df is not None
        assert df.index[0] == pd.Timestamp("2024-01-02")

    def test_keeps_the_last_of_duplicate_days(self) -> None:
        raw = vendor_frame([D1, D1, D2])
        raw.iloc[1, raw.columns.get_loc("Close")] = 9.0  # a later row for the same day
        df = normalize_ohlc(raw, D1, D2)
        assert df is not None
        assert list(df.index.date) == [D1, D2]
        assert df["close"].iloc[0] == 9.0

    def test_drops_rows_without_close(self) -> None:
        raw = vendor_frame([D1, D2])
        raw.loc[raw.index[0], "Close"] = float("nan")
        df = normalize_ohlc(raw, D1, D2)
        assert df is not None
        assert list(df.index.date) == [D2]

    @pytest.mark.parametrize(
        "raw", [None, pd.DataFrame(), pd.DataFrame({"close": [1.0]}, index=[pd.Timestamp(D1)])]
    )
    def test_unusable_input_gives_none(self, raw: pd.DataFrame | None) -> None:
        assert normalize_ohlc(raw, D1, D2) is None


def test_clean_tickers() -> None:
    raw = [" aapl", "N/A", None, "nan", "AAPL", "msft", "", "BRK.B", "bf/b", "RY.TO", "VOD.L"]
    assert clean_tickers(raw) == ["AAPL", "MSFT", "BRK-B", "BF-B", "RY.TO", "VOD.L"]


class FakePrices:
    def __init__(self, have: dict[str, pd.DataFrame], recheck_have: dict[str, pd.DataFrame]):
        self.have, self.recheck_have = have, recheck_have
        self.asked: list[list[str]] = []

    def download(self, tickers: Any, start: date, end: date) -> dict[str, pd.DataFrame]:
        self.asked.append(list(tickers))
        return {t: self.have[t] for t in tickers if t in self.have}

    def recheck(self, tickers: Any, start: date, end: date) -> dict[str, pd.DataFrame]:
        return {t: self.recheck_have[t] for t in tickers if t in self.recheck_have}


def test_fallback_asks_later_sources_only_for_misses() -> None:
    frame = pd.DataFrame()
    first = FakePrices({"A": frame}, {})
    second = FakePrices({"B": frame}, {"C": frame})
    chain = FallbackPrices(first, second)
    assert set(chain.download(["A", "B", "C"], D1, D2)) == {"A", "B"}
    assert second.asked == [["B", "C"]]
    assert set(chain.recheck(["C"], D1, D2)) == {"C"}


def test_fallback_stops_when_nothing_is_missing() -> None:
    first = FakePrices({"A": pd.DataFrame()}, {})
    second = FakePrices({}, {})
    FallbackPrices(first, second).download(["A"], D1, D2)
    assert second.asked == []


class TestYahooPrices:
    def test_requests_one_extra_day_because_yahoo_end_is_exclusive(self) -> None:
        with patch(
            "rs_pipeline.sources.yahoo.yf.download", return_value=vendor_frame([D1, D2])
        ) as dl:
            result = YahooPrices().download(["AAA"], D1, D2)
        assert dl.call_args.kwargs["end"] == "2024-01-04"  # D2 + 1 day
        assert list(result["AAA"].index.date) == [D1, D2]

    def test_splits_multi_ticker_frames(self) -> None:
        a, b = vendor_frame([D1]), vendor_frame([D1]) * 2
        both = pd.concat({"AAA": a, "BBB": b}, axis=1)
        with patch("rs_pipeline.sources.yahoo.yf.download", return_value=both):
            result = YahooPrices().download(["AAA", "BBB", "CCC"], D1, D1)
        assert set(result) == {"AAA", "BBB"}
        assert result["BBB"]["close"].iloc[0] == 3.0

    def test_retries_an_empty_large_batch(self) -> None:
        tickers = [f"T{i}" for i in range(20)]
        frames = pd.concat({t: vendor_frame([D1]) for t in tickers}, axis=1)
        sleeps: list[float] = []
        with patch(
            "rs_pipeline.sources.yahoo.yf.download", side_effect=[pd.DataFrame(), frames]
        ) as dl:
            result = YahooPrices(sleep=sleeps.append).download(tickers, D1, D1)
        assert dl.call_count == 2
        assert sleeps == [30]
        assert len(result) == 20

    def test_does_not_retry_a_small_batch(self) -> None:
        with patch("rs_pipeline.sources.yahoo.yf.download", side_effect=RuntimeError("boom")) as dl:
            assert YahooPrices(sleep=lambda _: None).download(["AAA"], D1, D1) == {}
        assert dl.call_count == 1

    def test_batches_tickers(self) -> None:
        with patch("rs_pipeline.sources.yahoo.yf.download", return_value=pd.DataFrame()) as dl:
            YahooPrices(batch_size=2).download(["A", "B", "C"], D1, D1)
        assert [c.args[0] for c in dl.call_args_list] == [["A", "B"], ["C"]]

    def test_recheck_one_ticker_at_a_time(self) -> None:
        history = MagicMock(side_effect=[vendor_frame([D1]), RuntimeError("gone")])
        with patch("rs_pipeline.sources.yahoo.yf.Ticker") as ticker:
            ticker.return_value.history = history
            result = YahooPrices().recheck(["AAA", "BBB"], D1, D1)
        assert set(result) == {"AAA"}
        assert history.call_args_list[0].kwargs["end"] == "2024-01-03"


class TestYahooInfo:
    def test_stock_info(self) -> None:
        info = {"longName": "Acme", "sector": "Tech", "industry": "", "exchange": "NMS"}
        with patch("rs_pipeline.sources.yahoo.yf.Ticker") as ticker:
            ticker.return_value.info = info
            assert YahooInfo().stock_info("ACME") == StockInfo("Acme", "Tech", None, "NMS")
            assert YahooInfo().etf_name("ACME") == "Acme"

    def test_stops_asking_while_throttled(self) -> None:
        sleeps: list[float] = []
        with patch(
            "rs_pipeline.sources.yahoo.yf.Ticker",
            side_effect=RuntimeError("429 Too Many Requests"),
        ) as ticker:
            info = YahooInfo(sleep=sleeps.append)
            for t in ("A", "B", "C", "D", "E"):
                assert info.stock_info(t) is None
        assert ticker.call_count == 3 * 4  # three tickers, each tried 4 times; then it stops

    def test_failure_gives_none(self) -> None:
        with patch("rs_pipeline.sources.yahoo.yf.Ticker", side_effect=ValueError("bad")):
            assert YahooInfo().stock_info("ACME") is None
            assert YahooInfo().etf_name("ACME") is None


class TestTwelveData:
    @pytest.fixture
    def client(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        client = MagicMock()
        module = types.ModuleType("twelvedata")
        module.TDClient = MagicMock(return_value=client)  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "twelvedata", module)
        return client

    def test_multi_and_single_ticker_responses(self, client: MagicMock) -> None:
        from rs_pipeline.sources.twelvedata import TwelveDataPrices

        client.time_series.return_value.as_pandas.side_effect = [
            {"A": vendor_frame([D1], title_case=False), "B": vendor_frame([D1])},
            vendor_frame([D1]),
        ]
        prices = TwelveDataPrices("key", batch_size=2)
        assert set(prices.download(["A", "B", "C"], D1, D1)) == {"A", "B", "C"}
        assert client.time_series.call_args.kwargs["end_date"] == "2024-01-03"
        assert prices.recheck(["A"], D1, D1) == {}

    def test_multi_index_response(self, client: MagicMock) -> None:
        from rs_pipeline.sources.twelvedata import TwelveDataPrices

        both = pd.concat({"A": vendor_frame([D1]), "B": vendor_frame([D1]) * 2})
        client.time_series.return_value.as_pandas.return_value = both
        result = TwelveDataPrices("key").download(["A", "B"], D1, D1)
        assert result["B"]["close"].iloc[0] == 3.0
        assert set(result) == {"A", "B"}

    def test_errors_skip_the_batch_without_logging_the_key(
        self, client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        from rs_pipeline.sources.twelvedata import TwelveDataPrices

        client.time_series.side_effect = RuntimeError("GET /time_series?apikey=SECRET failed")
        assert TwelveDataPrices("key").download(["A"], D1, D1) == {}
        assert "SECRET" not in caplog.text


class TestHoldings:
    def test_csv(self, tmp_path: Path) -> None:
        (tmp_path / "abc.csv").write_text("ticker,weight\nAAA,1\nn/a,2\nbbb,3\n")
        source = CsvHoldings(tmp_path)
        assert source.holdings("ABC") == ["AAA", "BBB"]
        assert source.holdings("XYZ") is None

    def test_alphavantage(self) -> None:
        response = MagicMock()
        response.json.return_value = {"holdings": [{"symbol": "AAA"}, {"symbol": "n/a"}]}
        with patch("rs_pipeline.sources.holdings.requests.get", return_value=response) as get:
            assert AlphaVantageHoldings("k").holdings("ABC") == ["AAA"]
        assert get.call_args.kwargs["params"]["symbol"] == "ABC"

    def test_alphavantage_failure_does_not_log_the_key(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        error = requests.ConnectionError("Max retries exceeded with url: /query?apikey=SECRET")
        with patch("rs_pipeline.sources.holdings.requests.get", side_effect=error):
            assert AlphaVantageHoldings("SECRET").holdings("ABC") is None
        assert "ConnectionError" in caplog.text
        assert "SECRET" not in caplog.text

    def test_first_available(self, tmp_path: Path) -> None:
        (tmp_path / "abc.csv").write_text("ticker\nAAA\n")
        fallback = MagicMock()
        fallback.holdings.return_value = ["ZZZ"]
        chain = FirstAvailableHoldings(CsvHoldings(tmp_path), fallback)
        assert chain.holdings("ABC") == ["AAA"]
        assert chain.holdings("XYZ") == ["ZZZ"]
        fallback.holdings.return_value = None
        assert chain.holdings("XYZ") is None


class TestSyntheticMarket:
    def test_is_reproducible_and_range_independent(self) -> None:
        a = SyntheticMarket(today=lambda: date(2024, 12, 31))
        b = SyntheticMarket(today=lambda: date(2024, 12, 31))
        long = a.download([BENCHMARK], date(2024, 1, 1), date(2024, 6, 30))[BENCHMARK]
        short = b.download([BENCHMARK], date(2024, 3, 1), date(2024, 3, 31))[BENCHMARK]
        pd.testing.assert_frame_equal(short, long.loc["2024-03-01":"2024-03-31"])

    def test_never_returns_the_future(self) -> None:
        market = SyntheticMarket(today=lambda: date(2024, 1, 3))
        df = market.download([BENCHMARK], date(2024, 1, 1), date(2024, 12, 31))[BENCHMARK]
        assert df.index.max() == pd.Timestamp("2024-01-03")
        assert market.download([BENCHMARK], date(2024, 2, 1), date(2024, 2, 5)) == {}

    def test_prices_are_consistent_bars(self) -> None:
        market = SyntheticMarket(today=lambda: date(2025, 1, 1))
        for df in market.download(
            list(market.stocks)[:5], date(2023, 1, 1), date(2024, 12, 31)
        ).values():
            assert (df["high"] >= df[["open", "close"]].max(axis=1)).all()
            assert (df["low"] <= df[["open", "close"]].min(axis=1)).all()
            assert (df["close"] > 0).all()

    def test_unknown_ticker_has_no_data(self) -> None:
        market = SyntheticMarket()
        assert market.download(["NOPE"], D1, D3) == {}
        assert market.stock_info("NOPE") is None
        assert market.holdings("NOPE") is None
        assert market.recheck(["NOPE"], D1, D3) == {}

    def test_universe(self) -> None:
        market = SyntheticMarket()
        everyone = market.holdings(BENCHMARK)
        assert everyone is not None
        assert len(everyone) == len(market.stocks) == 40
        thematic = market.holdings(THEMATIC)
        assert thematic
        assert set(thematic) < set(everyone)
        info = market.stock_info(everyone[0])
        assert info is not None
        assert info.sector
        assert info.exchange in {"NMS", "NYQ"}
        assert market.etf_name(BENCHMARK) == "Synthetic Total Market"


class TestBuildSources:
    @staticmethod
    def settings(**overrides: str) -> Settings:
        values = {"twelve_data_api_key": "", "alphavantage_api_key": "", "holdings_csv_dir": ""}
        return Settings(**{**values, **overrides})  # type: ignore[arg-type]

    def test_synthetic(self) -> None:
        sources = build_sources("synthetic", self.settings())
        assert isinstance(sources.prices, SyntheticMarket)

    def test_live_without_keys_uses_yahoo_only(self) -> None:
        bare = build_sources("live", self.settings())
        assert isinstance(bare.prices, FallbackPrices)
        assert [type(s) for s in bare.prices._sources] == [YahooPrices]
        assert isinstance(bare.info, YahooInfo)
        assert isinstance(bare.holdings, FirstAvailableHoldings)
        assert bare.holdings._sources == ()

    def test_live_with_every_vendor(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        module = types.ModuleType("twelvedata")
        module.TDClient = MagicMock()  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "twelvedata", module)
        from rs_pipeline.sources.twelvedata import TwelveDataPrices

        sources = build_sources(
            "live",
            self.settings(
                twelve_data_api_key="k", alphavantage_api_key="k", holdings_csv_dir=str(tmp_path)
            ),
        )
        assert [type(s) for s in sources.prices._sources] == [YahooPrices, TwelveDataPrices]  # type: ignore[attr-defined]
        assert [type(s) for s in sources.holdings._sources] == [CsvHoldings, AlphaVantageHoldings]  # type: ignore[attr-defined]

    def test_live_without_twelvedata_extra(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setitem(sys.modules, "twelvedata", None)
        monkeypatch.delitem(sys.modules, "rs_pipeline.sources.twelvedata", raising=False)
        sources = build_sources("live", self.settings(twelve_data_api_key="key"))
        assert [type(s) for s in sources.prices._sources] == [YahooPrices]  # type: ignore[attr-defined]
        assert "twelvedata extra is not installed" in caplog.text

    def test_unknown_name(self) -> None:
        with pytest.raises(ValueError, match="unknown data source"):
            build_sources("bogus", self.settings())
