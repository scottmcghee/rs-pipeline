"""Relative-strength metrics, vectorized over whole price series.

Every metric is *trailing*: its value on day ``t`` depends only on prices on
or before ``t``, and on at most ``required_history(window)`` of them. That
bound lets an incremental run load a short slice of history and still
produce exactly the numbers a full recompute would.

Undefined values (too little history, division by zero) are returned as 0,
except relative strength itself, which stays NaN until it has a full year of
history. The naive price ratio is 1.0 where undefined.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

QUARTER = 63  # trading days
# Strength looks back four quarters, weighted toward the most recent one.
QUARTER_WEIGHTS = (0.4, 0.2, 0.2, 0.2)

METRIC_COLUMNS = (
    "naive_relative_strength",
    "naive_relative_strength_percentile",
    "naive_deviation_from_moving_average",
    "relative_strength",
    "relative_strength_percentile",
    "deviation_from_moving_average",
    "rs_rolling_mean",
    "rs_rolling_std_dev",
    "rs_z_score",
    "rs_z_score_roc",
)


def required_history(window: int) -> int:
    """Number of trading days of prices that determine one day's metrics.

    The deepest dependency chain is the RS percentile: a rolling window over
    the moving-average deviation (itself a rolling window over RS), and RS
    needs ``4 * QUARTER`` earlier closes.
    """
    return len(QUARTER_WEIGHTS) * QUARTER + 2 * window - 1


def strength(closes: pd.Series) -> pd.Series:
    """Weighted four-quarter momentum for every date in *closes*."""
    total = pd.Series(0.0, index=closes.index)
    for i, weight in enumerate(QUARTER_WEIGHTS, start=1):
        total += weight * (closes / closes.shift(i * QUARTER) - 1)
    return total


def relative_strength(stock_closes: pd.Series, index_closes: pd.Series) -> pd.Series:
    """Stock strength relative to the index; 100 means in line with it."""
    return ((1 + strength(stock_closes)) / (1 + strength(index_closes)) * 100).round(2)


def naive_relative_strength(stock_closes: pd.Series, index_closes: pd.Series) -> pd.Series:
    return (stock_closes / index_closes).fillna(1.0)


def _windows(series: pd.Series, window: int) -> np.ndarray:
    """Each row's trailing window of values (NaN-padded at the start)."""
    values = series.to_numpy(dtype=float)
    padded = np.concatenate([np.full(window - 1, np.nan), values])
    return sliding_window_view(padded, window)


def _trailing(series: pd.Series, window: int, stat: str) -> pd.Series:
    """Trailing mean or sample std over up to *window* non-missing values.

    Computed independently for each window, rather than with pandas' running
    sums, so a value never depends on how much earlier history was loaded:
    incremental and full runs agree to the last bit. Windows with too few
    values give NaN.
    """
    windows = _windows(series, window)
    present = ~np.isnan(windows)
    count = present.sum(axis=1)
    values = np.where(present, windows, 0.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = values.sum(axis=1) / count
        if stat == "mean":
            result = mean
        else:
            deviations = np.where(present, windows - mean[:, None], 0.0)
            result = np.sqrt((deviations**2).sum(axis=1) / (count - 1))
            result[count < 2] = np.nan
    return pd.Series(result, index=series.index, name=series.name)


def _deviation(series: pd.Series, window: int) -> pd.Series:
    ma = _trailing(series, window, "mean")
    return (series - ma) / ma * 100


def deviation_from_ma(series: pd.Series, window: int) -> pd.Series:
    """Percent distance of *series* from its trailing moving average."""
    return _deviation(series, window).fillna(0)


def trailing_percentile(series: pd.Series, window: int) -> pd.Series:
    """Percentile of each value within its own trailing window.

    Missing values are skipped, not ranked. A whole-series
    ``rank(pct=True)`` would also compare each day with later days, so stored
    values would silently change as history grows.
    """
    return series.rolling(window=window, min_periods=1).rank(pct=True).fillna(0)


def rolling_mean(series: pd.Series, window: int) -> pd.Series:
    return _trailing(series, window, "mean").fillna(0)


def rolling_std(series: pd.Series, window: int) -> pd.Series:
    return _trailing(series, window, "std").fillna(0)


def z_score(series: pd.Series, mean: pd.Series, std: pd.Series) -> pd.Series:
    return ((series - mean) / std).replace([np.inf, -np.inf], np.nan).fillna(0)


def z_score_roc(series: pd.Series) -> pd.Series:
    """Day-over-day percent change of the z-score (0 when yesterday's was 0)."""
    prev = series.shift(1)
    return ((series - prev) / prev.abs() * 100).replace([np.inf, -np.inf], np.nan).fillna(0)


def compute_all_metrics(
    stock_data: pd.DataFrame,
    index_data: pd.DataFrame,
    window: int,
    ticker: str,
    etf_ticker: str,
) -> pd.DataFrame:
    """Every metric for one (stock, ETF) pair, on the dates both have prices.

    Both frames need a ``DatetimeIndex`` and a ``close`` column. Returns one
    row per common date with ``date``, ``ticker``, ``etf_ticker`` and the
    ``METRIC_COLUMNS``, or an empty frame when there are no common dates.
    """
    common = stock_data.index.intersection(index_data.index)
    if len(common) == 0:
        return pd.DataFrame()

    sc = stock_data.loc[common, "close"].astype(float)
    ic = index_data.loc[common, "close"].astype(float)

    naive = naive_relative_strength(sc, ic)
    naive_dev = _deviation(naive, window)

    # Percentiles rank the deviations that exist; the 0 stored for days
    # without one (RS's first year) must not count as a value.
    rs = relative_strength(sc, ic)
    dev = _deviation(rs, window)
    mean = rolling_mean(rs, window)
    std = rolling_std(rs, window)
    z = z_score(rs, mean, std)

    return pd.DataFrame(
        {
            "date": common,
            "ticker": ticker,
            "etf_ticker": etf_ticker,
            "naive_relative_strength": naive.to_numpy(),
            "naive_relative_strength_percentile": trailing_percentile(naive_dev, window).to_numpy(),
            "naive_deviation_from_moving_average": naive_dev.fillna(0).to_numpy(),
            "relative_strength": rs.to_numpy(),
            "relative_strength_percentile": trailing_percentile(dev, window).to_numpy(),
            "deviation_from_moving_average": dev.fillna(0).to_numpy(),
            "rs_rolling_mean": mean.to_numpy(),
            "rs_rolling_std_dev": std.to_numpy(),
            "rs_z_score": z.to_numpy(),
            "rs_z_score_roc": z_score_roc(z).to_numpy(),
        },
        index=common,
    )
