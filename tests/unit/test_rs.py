"""Relative-strength math."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from rs_pipeline.rs import (
    METRIC_COLUMNS,
    QUARTER,
    compute_all_metrics,
    deviation_from_ma,
    naive_relative_strength,
    relative_strength,
    required_history,
    rolling_mean,
    rolling_std,
    strength,
    trailing_percentile,
    z_score,
    z_score_roc,
)
from tests.conftest import random_walk_ohlc

WINDOW = 252


def _line(start: float, stop: float, n: int = 300) -> pd.Series:
    return pd.Series(np.linspace(start, stop, n), index=pd.bdate_range("2020-01-01", periods=n))


class TestStrength:
    def test_needs_four_quarters_of_history(self, stock: pd.DataFrame) -> None:
        result = strength(stock["close"])
        assert result.iloc[: 4 * QUARTER].isna().all()
        assert np.isfinite(result.iloc[4 * QUARTER :]).all()

    def test_is_weighted_sum_of_trailing_returns(self) -> None:
        closes = pd.Series([100.0] * (4 * QUARTER + 1))
        closes.iloc[-1 - QUARTER] = 105.0  # one quarter ago
        closes.iloc[-1] = 110.0  # today
        # 63-day return 110/105 - 1; 126-, 189- and 252-day returns are 10%.
        expected = 0.4 * (110 / 105 - 1) + 0.2 * 0.10 * 3
        assert strength(closes).iloc[-1] == pytest.approx(expected)


class TestRelativeStrength:
    def test_outperformer_is_above_100(self) -> None:
        rs = relative_strength(_line(100, 200), _line(100, 100))
        assert (rs.dropna() > 100).all()

    def test_underperformer_is_below_100(self) -> None:
        rs = relative_strength(_line(100, 50), _line(100, 100))
        assert (rs.dropna() < 100).all()

    def test_identical_series_is_100(self) -> None:
        prices = _line(100, 130)
        assert (relative_strength(prices, prices.copy()).dropna() == 100).all()


class TestHelpers:
    def test_naive_rs_is_price_ratio(self) -> None:
        result = naive_relative_strength(pd.Series([100.0, 200.0]), pd.Series([100.0, 100.0]))
        assert result.tolist() == [1.0, 2.0]

    def test_deviation_from_ma(self) -> None:
        # MA of [100, 200] is 150, so 200 is 33.3% above it.
        result = deviation_from_ma(pd.Series([100.0, 100.0, 200.0]), 2)
        assert result.iloc[2] == pytest.approx(100 / 3)

    def test_trailing_percentile_ignores_the_future(self) -> None:
        series = pd.Series([1.0, 2.0, 3.0, 0.0])
        # On day 3 (value 3.0) it is the highest so far: 100th percentile,
        # even though a lower value comes later.
        assert trailing_percentile(series, 10).tolist() == [1.0, 1.0, 1.0, 0.25]

    def test_z_score_roc(self) -> None:
        result = z_score_roc(pd.Series([1.0, 2.0, -4.0]))
        assert result.tolist() == [0.0, 100.0, -300.0]


class TestComputeAllMetrics:
    def test_columns_and_keys(self, stock: pd.DataFrame, index: pd.DataFrame) -> None:
        result = compute_all_metrics(stock, index, WINDOW, "AAA", "IDX")
        assert list(result.columns) == ["date", "ticker", "etf_ticker", *METRIC_COLUMNS]
        assert (result["ticker"] == "AAA").all()
        assert (result["etf_ticker"] == "IDX").all()
        assert len(result) == len(stock)

    def test_uses_only_common_dates(self, stock: pd.DataFrame, index: pd.DataFrame) -> None:
        result = compute_all_metrics(stock.iloc[::2], index, WINDOW, "AAA", "IDX")
        assert result.index.equals(stock.index[::2])

    def test_no_common_dates_gives_empty_frame(self, stock: pd.DataFrame) -> None:
        later = stock.copy()
        later.index = later.index.shift(5000, freq="D")  # no overlap
        assert compute_all_metrics(stock, later, WINDOW, "AAA", "IDX").empty

    def test_survives_missing_closes(self, stock: pd.DataFrame, index: pd.DataFrame) -> None:
        stock = stock.copy()
        stock.iloc[10, stock.columns.get_loc("close")] = np.nan
        result = compute_all_metrics(stock, index, WINDOW, "AAA", "IDX")
        assert not result.empty


def test_percentile_ignores_the_first_year_placeholders(
    stock: pd.DataFrame, index: pd.DataFrame
) -> None:
    result = compute_all_metrics(stock, index, WINDOW, "AAA", "IDX")
    first_rs = result["relative_strength"].first_valid_index()
    pos = result.index.get_loc(first_rs)
    # On RS's first day there is one deviation to rank: itself, the 100th percentile.
    assert result["relative_strength_percentile"].iloc[pos] == 1.0
    assert (result["relative_strength_percentile"].iloc[:pos] == 0).all()


class TestTrailingProperty:
    """The guarantee incremental runs rely on (see ``required_history``)."""

    @pytest.mark.parametrize("window", [20, 252])
    @pytest.mark.parametrize("seed", range(4))
    def test_history_beyond_the_bound_never_matters(self, window: int, seed: int) -> None:
        dates = pd.bdate_range("2019-01-01", periods=1400, name="date")
        stock = random_walk_ohlc(dates, 100.0, 0.0005, 0.02, seed)
        index = random_walk_ohlc(dates, 300.0, 0.0003, 0.01, seed + 100)
        full = compute_all_metrics(stock, index, window, "AAA", "IDX")
        n = required_history(window)
        for cut in (n, 900, 1300):
            # Rows from `cut` on, computed from only the n - 1 days before it.
            part = compute_all_metrics(
                stock.iloc[cut - n + 1 :], index.iloc[cut - n + 1 :], window, "AAA", "IDX"
            )
            expected = full.iloc[cut:][list(METRIC_COLUMNS)].to_numpy(float)
            actual = part.loc[full.index[cut:]][list(METRIC_COLUMNS)].to_numpy(float)
            # Exactly equal, not approximately: stored values must not depend on
            # how much history a run happened to load.
            assert np.array_equal(expected, actual, equal_nan=True)

    @pytest.mark.parametrize("window", [20, 252])
    def test_bound_is_tight(self, window: int) -> None:
        """The oldest of the n days can change today's metrics; the day before can't.

        That oldest close reaches today only through the percentile: it moves
        one RS value, hence the moving average behind the deviation at the
        far edge of the percentile's window. A tiny close there makes that RS
        (and the average) enormous, pushing the edge deviation far below
        today's. It changes today's percentile only if the edge deviation
        started above today's, so the test picks data where it does.
        """
        n = required_history(window)
        dates = pd.bdate_range("2019-01-01", periods=n + 50, name="date")
        t = len(dates) - 1
        for seed in range(100):
            index = random_walk_ohlc(dates, 300.0, 0.0003, 0.01, seed)
            stock = random_walk_ohlc(dates, 100.0, 0.0005, 0.02, seed + 1000)
            dev = compute_all_metrics(stock, index, window, "A", "I")[
                "deviation_from_moving_average"
            ]
            if dev.iloc[t - window + 1] > dev.iloc[t]:
                break

        def last_row(prices: pd.DataFrame) -> pd.Series:
            return compute_all_metrics(prices, index, window, "A", "I").iloc[-1]

        base = last_row(stock)
        inside, outside = stock.copy(), stock.copy()
        col = stock.columns.get_loc("close")
        inside.iloc[t - n + 1, col] /= 1000  # the oldest day within the bound
        outside.iloc[t - n, col] /= 1000  # one day further back
        pd.testing.assert_series_equal(last_row(outside), base, check_exact=True)
        changed = last_row(inside)
        assert changed["relative_strength_percentile"] != base["relative_strength_percentile"]

    def test_appending_days_never_changes_earlier_rows(
        self, stock: pd.DataFrame, index: pd.DataFrame
    ) -> None:
        before = compute_all_metrics(stock.iloc[:-50], index.iloc[:-50], WINDOW, "AAA", "IDX")
        after = compute_all_metrics(stock, index, WINDOW, "AAA", "IDX")
        pd.testing.assert_frame_equal(before, after.iloc[:-50], check_exact=True)


class TestTrailingStatistics:
    def test_match_pandas_rolling(self, stock: pd.DataFrame) -> None:
        s = stock["close"].copy()
        s.iloc[[3, 50, 51]] = np.nan
        pd.testing.assert_series_equal(
            rolling_mean(s, 20), s.rolling(20, min_periods=1).mean().fillna(0), rtol=1e-12
        )
        pd.testing.assert_series_equal(
            rolling_std(s, 20), s.rolling(20, min_periods=1).std().fillna(0), rtol=1e-9
        )

    def test_undefined_values_are_zero(self) -> None:
        flat = pd.Series([5.0, 5.0, 5.0])
        assert rolling_std(flat, 3).tolist() == [0.0, 0.0, 0.0]
        assert z_score(flat, flat, rolling_std(flat, 3)).tolist() == [0.0, 0.0, 0.0]
        assert z_score_roc(pd.Series([0.0, 1.0])).tolist() == [0.0, 0.0]  # yesterday was 0
