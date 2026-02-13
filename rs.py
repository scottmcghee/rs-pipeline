"""Fully vectorized RS calculation engine.

Replaces the per-date loop in rs_redux/rs.py with pure pandas shifted
division, yielding ~100x speedup on the RS computation itself.
"""

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Core RS functions
# ---------------------------------------------------------------------------

def strength_vectorized(closes: pd.Series) -> pd.Series:
    """Weighted momentum score for every date in *closes*.

    Equivalent to the original ``strength()`` called per-date, but computed
    for the entire series at once using shifted division.

    Weights: Q1 40%, Q2-Q4 20% each  (Q = 63 trading days).
    """
    q = 252 // 4  # 63 trading days per quarter
    q1 = closes / closes.shift(q) - 1
    q2 = closes / closes.shift(2 * q) - 1
    q3 = closes / closes.shift(3 * q) - 1
    q4 = closes / closes.shift(4 * q) - 1
    return 0.4 * q1 + 0.2 * q2 + 0.2 * q3 + 0.2 * q4


def relative_strength_vectorized(stock_closes: pd.Series,
                                  index_closes: pd.Series) -> pd.Series:
    """RS ratio for every date — no loops."""
    stock_str = strength_vectorized(stock_closes)
    index_str = strength_vectorized(index_closes)
    return ((1 + stock_str) / (1 + index_str) * 100).round(2)


# ---------------------------------------------------------------------------
# Supporting vectorized metrics (unchanged logic from rs_redux, kept simple)
# ---------------------------------------------------------------------------

def calculate_naive_rs(stock_closes: pd.Series,
                       index_closes: pd.Series) -> pd.Series:
    return (stock_closes / index_closes).fillna(1.0)


def calculate_deviation_from_ma(series: pd.Series, window: int) -> pd.Series:
    ma = series.rolling(window=window, min_periods=1).mean()
    return ((series - ma) / ma * 100).fillna(0)


def calculate_percentile(series: pd.Series) -> pd.Series:
    return series.rank(pct=True).fillna(0)


def calculate_rolling_mean(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean().fillna(0)


def calculate_rolling_std(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).std().fillna(0)


def calculate_z_score(series: pd.Series, mean: pd.Series,
                      std: pd.Series) -> pd.Series:
    return ((series - mean) / std).fillna(0)


def calculate_z_score_roc(series: pd.Series) -> pd.Series:
    return ((series - series.shift(1)) / series.shift(1).abs() * 100).fillna(0)


# ---------------------------------------------------------------------------
# All-in-one metric computation
# ---------------------------------------------------------------------------

def compute_all_metrics(stock_data: pd.DataFrame,
                        index_data: pd.DataFrame,
                        window: int,
                        ticker: str,
                        etf_ticker: str) -> pd.DataFrame:
    """Compute every RS metric for one (stock, ETF) pair.

    Parameters
    ----------
    stock_data : DataFrame with DatetimeIndex and a ``close`` column.
    index_data : DataFrame with DatetimeIndex and a ``close`` column.
    window     : Rolling window (typically 252).
    ticker     : Stock ticker (written into output rows).
    etf_ticker : ETF ticker (written into output rows).

    Returns
    -------
    DataFrame indexed by date with all 12 metric columns, or empty DataFrame.
    """
    # Align to common dates
    common = stock_data.index.intersection(index_data.index)
    if len(common) == 0:
        return pd.DataFrame()

    sc = stock_data.loc[common, 'close'].astype(float)
    ic = index_data.loc[common, 'close'].astype(float)

    # Naive RS
    naive_rs = calculate_naive_rs(sc, ic)
    naive_dev = calculate_deviation_from_ma(naive_rs, window)
    naive_pct = calculate_percentile(naive_dev)

    # Strength-based RS
    rs = relative_strength_vectorized(sc, ic)
    dev = calculate_deviation_from_ma(rs, window)
    rs_pct = calculate_percentile(dev)

    # Rolling stats
    rs_mean = calculate_rolling_mean(rs, window)
    rs_std = calculate_rolling_std(rs, window)
    rs_z = calculate_z_score(rs, rs_mean, rs_std)
    rs_z_roc = calculate_z_score_roc(rs_z)

    result = pd.DataFrame({
        'date': common,
        'ticker': ticker,
        'etf_ticker': etf_ticker,
        'naive_relative_strength': naive_rs.values,
        'naive_relative_strength_percentile': naive_pct.values,
        'naive_deviation_from_moving_average': naive_dev.values,
        'relative_strength': rs.values,
        'relative_strength_percentile': rs_pct.values,
        'deviation_from_moving_average': dev.values,
        'rs_rolling_mean': rs_mean.values,
        'rs_rolling_std_dev': rs_std.values,
        'rs_z_score': rs_z.values,
        'rs_z_score_roc': rs_z_roc.values,
    }, index=common)

    return result
