"""Tests for the vectorized RS calculation engine."""

import numpy as np
import pandas as pd
import pytest

from rs_pipeline.rs import (
    calculate_deviation_from_ma,
    calculate_naive_rs,
    calculate_percentile,
    calculate_z_score_roc,
    compute_all_metrics,
    relative_strength_vectorized,
    strength_vectorized,
)


class TestStrengthVectorized:
    def test_returns_series(self, sample_ohlc):
        closes = sample_ohlc['AAPL']['close']
        result = strength_vectorized(closes)
        assert isinstance(result, pd.Series)
        assert len(result) == len(closes)

    def test_early_dates_are_nan(self, sample_ohlc):
        """First 252 dates (4 quarters) should be NaN."""
        closes = sample_ohlc['AAPL']['close']
        result = strength_vectorized(closes)
        # The first 4*63=252 values need shift(4*63), so they're NaN
        assert result.iloc[:252].isna().all()

    def test_later_dates_are_finite(self, sample_ohlc):
        closes = sample_ohlc['AAPL']['close']
        result = strength_vectorized(closes)
        later = result.iloc[252:]
        assert later.notna().all()
        assert np.isfinite(later).all()


class TestRelativeStrength:
    def test_outperformer_above_100(self):
        """Stock that doubles while index is flat → RS > 100."""
        dates = pd.bdate_range('2020-01-01', periods=300)
        stock = pd.Series(np.linspace(100, 200, 300), index=dates)
        index = pd.Series(np.full(300, 100.0), index=dates)
        rs = relative_strength_vectorized(stock, index)
        # After enough lookback, RS should be > 100
        valid = rs.dropna()
        assert (valid.iloc[-50:] > 100).all()

    def test_underperformer_below_100(self):
        """Stock that halves while index is flat → RS < 100."""
        dates = pd.bdate_range('2020-01-01', periods=300)
        stock = pd.Series(np.linspace(100, 50, 300), index=dates)
        index = pd.Series(np.full(300, 100.0), index=dates)
        rs = relative_strength_vectorized(stock, index)
        valid = rs.dropna()
        assert (valid.iloc[-50:] < 100).all()

    def test_identical_series_near_100(self):
        """Stock == index → RS ≈ 100."""
        dates = pd.bdate_range('2020-01-01', periods=300)
        prices = pd.Series(np.linspace(100, 130, 300), index=dates)
        rs = relative_strength_vectorized(prices, prices.copy())
        valid = rs.dropna()
        assert ((valid - 100).abs() < 0.1).all()


class TestComputeAllMetrics:
    def test_output_columns(self, sample_ohlc, sample_index_ohlc):
        result = compute_all_metrics(
            sample_ohlc['AAPL'], sample_index_ohlc, 252, 'AAPL', 'SPY')
        expected_cols = {
            'date', 'ticker', 'etf_ticker',
            'naive_relative_strength', 'naive_relative_strength_percentile',
            'naive_deviation_from_moving_average',
            'relative_strength', 'relative_strength_percentile',
            'deviation_from_moving_average',
            'rs_rolling_mean', 'rs_rolling_std_dev',
            'rs_z_score', 'rs_z_score_roc',
        }
        assert expected_cols == set(result.columns)

    def test_ticker_values(self, sample_ohlc, sample_index_ohlc):
        result = compute_all_metrics(
            sample_ohlc['AAPL'], sample_index_ohlc, 252, 'AAPL', 'SPY')
        assert (result['ticker'] == 'AAPL').all()
        assert (result['etf_ticker'] == 'SPY').all()

    def test_short_series_returns_empty(self):
        """Very short series with no overlap → empty result."""
        dates_a = pd.bdate_range('2020-01-01', periods=5)
        dates_b = pd.bdate_range('2021-01-01', periods=5)
        stock = pd.DataFrame({'close': [1, 2, 3, 4, 5]}, index=dates_a)
        index = pd.DataFrame({'close': [1, 2, 3, 4, 5]}, index=dates_b)
        result = compute_all_metrics(stock, index, 252, 'X', 'Y')
        assert result.empty

    def test_nan_in_input(self, sample_dates):
        """NaN in close prices → handled without crash."""
        closes = np.random.uniform(100, 200, len(sample_dates)).astype(float)
        closes[10] = np.nan
        stock = pd.DataFrame({
            'close': closes, 'open': closes, 'high': closes,
            'low': closes, 'volume': 1000, 'ticker': 'X',
        }, index=sample_dates)
        index = pd.DataFrame({
            'close': np.random.uniform(100, 200, len(sample_dates)),
            'open': 100, 'high': 100, 'low': 100, 'volume': 1000,
            'ticker': 'SPY',
        }, index=sample_dates)
        # Should not raise
        result = compute_all_metrics(stock, index, 252, 'X', 'SPY')
        assert isinstance(result, pd.DataFrame)


class TestHelpers:
    def test_naive_rs(self):
        stock = pd.Series([100.0, 200.0, 300.0])
        index = pd.Series([100.0, 100.0, 100.0])
        result = calculate_naive_rs(stock, index)
        assert list(result) == [1.0, 2.0, 3.0]

    def test_deviation_from_ma(self):
        s = pd.Series([100.0, 100.0, 200.0])
        result = calculate_deviation_from_ma(s, 2)
        # Last value: ma of [100,200]=150, dev = (200-150)/150*100 ≈ 33.33
        assert abs(result.iloc[2] - 33.33) < 1.0

    def test_percentile_rank(self):
        s = pd.Series([10, 20, 30, 40, 50])
        result = calculate_percentile(s)
        assert result.iloc[-1] == 1.0  # max gets rank 1.0

    def test_z_score_roc(self):
        s = pd.Series([1.0, 2.0, 4.0])
        result = calculate_z_score_roc(s)
        assert result.iloc[0] == 0  # fillna
        assert abs(result.iloc[1] - 100.0) < 0.01  # (2-1)/|1|*100
