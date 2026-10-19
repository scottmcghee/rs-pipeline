"""Fixtures shared by unit and integration tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def random_walk_ohlc(
    dates: pd.DatetimeIndex, start: float, drift: float, vol: float, seed: int
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    closes = start * np.cumprod(1 + rng.normal(drift, vol, len(dates)))
    df = pd.DataFrame(
        {
            "open": closes * (1 + rng.uniform(-0.01, 0.01, len(dates))),
            "high": closes * (1 + np.abs(rng.normal(0, 0.01, len(dates)))),
            "low": closes * (1 - np.abs(rng.normal(0, 0.01, len(dates)))),
            "close": closes,
            "volume": rng.integers(1_000_000, 50_000_000, len(dates)).astype(float),
        },
        index=dates,
    )
    df.index.name = "date"
    return df


@pytest.fixture
def dates() -> pd.DatetimeIndex:
    """900 business days: enough for every metric to be fully warmed up."""
    return pd.bdate_range("2021-01-04", periods=900, name="date")


@pytest.fixture
def stock(dates: pd.DatetimeIndex) -> pd.DataFrame:
    return random_walk_ohlc(dates, 150.0, 0.0006, 0.02, seed=1)


@pytest.fixture
def index(dates: pd.DatetimeIndex) -> pd.DataFrame:
    return random_walk_ohlc(dates, 450.0, 0.0003, 0.012, seed=2)
