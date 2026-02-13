"""Shared pytest fixtures for rs_pipeline tests."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_dates():
    """500 trading days of dates."""
    return pd.bdate_range('2023-01-02', periods=500, freq='B')


@pytest.fixture
def sample_ohlc(sample_dates):
    """Realistic OHLC data for 3 tickers."""
    np.random.seed(42)
    result = {}
    for ticker, base in [('AAPL', 150.0), ('MSFT', 300.0), ('GOOG', 120.0)]:
        # Random walk for close prices
        returns = np.random.normal(0.0005, 0.02, len(sample_dates))
        closes = base * np.cumprod(1 + returns)
        df = pd.DataFrame({
            'open': closes * (1 + np.random.uniform(-0.01, 0.01, len(closes))),
            'high': closes * (1 + np.abs(np.random.normal(0, 0.01, len(closes)))),
            'low': closes * (1 - np.abs(np.random.normal(0, 0.01, len(closes)))),
            'close': closes,
            'volume': np.random.randint(1_000_000, 50_000_000, len(closes)),
            'ticker': ticker,
        }, index=sample_dates)
        df.index.name = 'date'
        result[ticker] = df
    return result


@pytest.fixture
def sample_index_ohlc(sample_dates):
    """Index OHLC (e.g. SPY) matching sample_dates."""
    np.random.seed(99)
    returns = np.random.normal(0.0003, 0.012, len(sample_dates))
    closes = 450.0 * np.cumprod(1 + returns)
    df = pd.DataFrame({
        'open': closes * (1 + np.random.uniform(-0.005, 0.005, len(closes))),
        'high': closes * (1 + np.abs(np.random.normal(0, 0.005, len(closes)))),
        'low': closes * (1 - np.abs(np.random.normal(0, 0.005, len(closes)))),
        'close': closes,
        'volume': np.random.randint(50_000_000, 200_000_000, len(closes)),
        'ticker': 'SPY',
    }, index=sample_dates)
    df.index.name = 'date'
    return df


@pytest.fixture
def mock_db_pool():
    """Mock ConnectionPool that returns a mock connection."""
    pool = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
    pool.connection.return_value.__exit__ = MagicMock(return_value=False)
    return pool, mock_conn, mock_cursor


@pytest.fixture
def sample_metrics(sample_ohlc, sample_index_ohlc):
    """Pre-computed metrics DataFrame for AAPL vs SPY."""
    from rs_pipeline.rs import compute_all_metrics
    return compute_all_metrics(
        sample_ohlc['AAPL'], sample_index_ohlc, 252, 'AAPL', 'SPY'
    )
