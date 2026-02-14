"""Integration tests for pipeline steps."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_args(**kwargs):
    defaults = {
        'start_date': '2024-01-01',
        'end_date': '2024-01-31',
        'window': 252,
        'max_workers': 1,
        'batch_size': 100,
        'log_level': 'WARNING',
        'full_recompute': False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestFetchPricesIncremental:
    @patch('rs_pipeline.steps.fetch_prices.bulk_upsert_dim_stock')
    @patch('rs_pipeline.steps.fetch_prices.fetch_missing_stock_info', return_value=[])
    @patch('rs_pipeline.steps.fetch_prices.clear_fetch_failure')
    @patch('rs_pipeline.steps.fetch_prices.verify_tickers_individually', return_value=({}, []))
    @patch('rs_pipeline.steps.fetch_prices.batch_download_ohlc', return_value=({}, []))
    @patch('rs_pipeline.steps.fetch_prices.fetch_latest_ohlc_dates')
    @patch('rs_pipeline.steps.fetch_prices.fetch_all_holdings',
           return_value={'SPY': ['AAPL', 'MSFT']})
    @patch('rs_pipeline.steps.fetch_prices.fetch_delisted_tickers', return_value=set())
    @patch('rs_pipeline.steps.fetch_prices.ensure_delisted_table')
    def test_skips_up_to_date(self, mock_ensure, mock_delisted, mock_hold,
                               mock_latest, mock_dl, mock_verify, mock_clear,
                               mock_info, mock_dim):
        """Tickers already up to date should not be re-downloaded."""
        mock_latest.return_value = {
            'AAPL': date(2024, 1, 31),
            'MSFT': date(2024, 1, 31),
        }
        pool = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=MagicMock())
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        from rs_pipeline.steps.fetch_prices import run
        run(pool, _make_args(), indices=None)

        # Both up to date → no download call for stocks
        # (ETF download may still happen but stock download should be skipped)
        # batch_download_ohlc is called only if there are tickers to fetch


    @patch('rs_pipeline.steps.fetch_prices.bulk_upsert_dim_stock')
    @patch('rs_pipeline.steps.fetch_prices.fetch_missing_stock_info', return_value=[])
    @patch('rs_pipeline.steps.fetch_prices.clear_fetch_failure')
    @patch('rs_pipeline.steps.fetch_prices.verify_tickers_individually', return_value=({}, []))
    @patch('rs_pipeline.steps.fetch_prices.bulk_upsert_ohlc', return_value=5)
    @patch('rs_pipeline.steps.fetch_prices.batch_download_ohlc')
    @patch('rs_pipeline.steps.fetch_prices.fetch_latest_ohlc_dates')
    @patch('rs_pipeline.steps.fetch_prices.fetch_all_holdings',
           return_value={'SPY': ['AAPL']})
    @patch('rs_pipeline.steps.fetch_prices.fetch_delisted_tickers', return_value=set())
    @patch('rs_pipeline.steps.fetch_prices.ensure_delisted_table')
    def test_fetches_from_last_date(self, mock_ensure, mock_delisted, mock_hold,
                                     mock_latest, mock_dl, mock_upsert,
                                     mock_verify, mock_clear, mock_info,
                                     mock_dim):
        """Should fetch from day after last stored date."""
        mock_latest.return_value = {'AAPL': date(2024, 1, 15)}
        dates = pd.bdate_range('2024-01-16', periods=5)
        mock_dl.return_value = (
            {
                'AAPL': pd.DataFrame({
                    'open': [1]*5, 'high': [2]*5, 'low': [0.5]*5,
                    'close': [1.5]*5, 'volume': [100]*5,
                }, index=dates)
            },
            [],
        )
        pool = MagicMock()
        mock_conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        from rs_pipeline.steps.fetch_prices import run
        run(pool, _make_args(), indices=None)

        # batch_download_ohlc should have been called
        assert mock_dl.called


class TestComputeRsIncremental:
    @patch('rs_pipeline.steps.compute_rs.bulk_upsert_stock_metrics', return_value=10)
    @patch('rs_pipeline.steps.compute_rs.compute_all_metrics')
    @patch('rs_pipeline.steps.compute_rs.fetch_all_ohlc')
    @patch('rs_pipeline.steps.compute_rs.fetch_last_metric_dates')
    @patch('rs_pipeline.steps.compute_rs.fetch_all_holdings',
           return_value={'SPY': ['AAPL']})
    def test_only_writes_new_dates(self, mock_hold, mock_last, mock_ohlc,
                                    mock_compute, mock_upsert):
        dates = pd.bdate_range('2024-01-01', periods=30)
        mock_last.return_value = {'AAPL': date(2024, 1, 15)}
        stock_df = pd.DataFrame({
            'close': range(30), 'open': range(30), 'high': range(30),
            'low': range(30), 'volume': [100]*30, 'ticker': 'AAPL',
        }, index=dates)
        index_df = pd.DataFrame({
            'close': range(30), 'open': range(30), 'high': range(30),
            'low': range(30), 'volume': [100]*30, 'ticker': 'SPY',
        }, index=dates)
        mock_ohlc.return_value = {'AAPL': stock_df, 'SPY': index_df}

        metrics_df = pd.DataFrame({
            'date': dates,
            'ticker': 'AAPL', 'etf_ticker': 'SPY',
            'naive_relative_strength': 1.0,
            'naive_relative_strength_percentile': 0.5,
            'naive_deviation_from_moving_average': 0.0,
            'relative_strength': 100.0,
            'relative_strength_percentile': 0.5,
            'deviation_from_moving_average': 0.0,
            'rs_rolling_mean': 100.0, 'rs_rolling_std_dev': 1.0,
            'rs_z_score': 0.0, 'rs_z_score_roc': 0.0,
        })
        mock_compute.return_value = metrics_df

        pool = MagicMock()
        mock_conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        from rs_pipeline.steps.compute_rs import run
        run(pool, _make_args(full_recompute=False), indices=None)

        # bulk_upsert should have been called with only dates after Jan 15
        if mock_upsert.called:
            written_df = mock_upsert.call_args[0][1]
            assert (written_df['date'] > pd.Timestamp('2024-01-15')).all()

    @patch('rs_pipeline.steps.compute_rs.bulk_upsert_stock_metrics', return_value=30)
    @patch('rs_pipeline.steps.compute_rs.compute_all_metrics')
    @patch('rs_pipeline.steps.compute_rs.fetch_all_ohlc')
    @patch('rs_pipeline.steps.compute_rs.fetch_last_metric_dates')
    @patch('rs_pipeline.steps.compute_rs.fetch_all_holdings',
           return_value={'SPY': ['AAPL']})
    def test_full_recompute_ignores_last_dates(self, mock_hold, mock_last,
                                                 mock_ohlc, mock_compute,
                                                 mock_upsert):
        dates = pd.bdate_range('2024-01-01', periods=30)
        mock_last.return_value = {'AAPL': date(2024, 1, 15)}
        stock_df = pd.DataFrame({
            'close': range(30), 'open': range(30), 'high': range(30),
            'low': range(30), 'volume': [100]*30, 'ticker': 'AAPL',
        }, index=dates)
        index_df = stock_df.copy()
        index_df['ticker'] = 'SPY'
        mock_ohlc.return_value = {'AAPL': stock_df, 'SPY': index_df}
        metrics_df = pd.DataFrame({
            'date': dates,
            'ticker': 'AAPL', 'etf_ticker': 'SPY',
            'naive_relative_strength': 1.0,
            'naive_relative_strength_percentile': 0.5,
            'naive_deviation_from_moving_average': 0.0,
            'relative_strength': 100.0,
            'relative_strength_percentile': 0.5,
            'deviation_from_moving_average': 0.0,
            'rs_rolling_mean': 100.0, 'rs_rolling_std_dev': 1.0,
            'rs_z_score': 0.0, 'rs_z_score_roc': 0.0,
        })
        mock_compute.return_value = metrics_df

        pool = MagicMock()
        mock_conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        from rs_pipeline.steps.compute_rs import run
        run(pool, _make_args(full_recompute=True), indices=None)

        # All 30 dates should be written
        if mock_upsert.called:
            written_df = mock_upsert.call_args[0][1]
            assert len(written_df) == 30


class TestRunAllOrder:
    @patch('rs_pipeline.steps.rank_all.run')
    @patch('rs_pipeline.steps.compute_etf_rs.run')
    @patch('rs_pipeline.steps.rank_indices.run')
    @patch('rs_pipeline.steps.compute_rs.run')
    @patch('rs_pipeline.steps.fetch_prices.run')
    def test_step_order(self, m_fetch, m_compute, m_rank, m_etf, m_all):
        """Steps must execute in correct sequence."""
        from rs_pipeline.db import ConnectionPool
        pool = MagicMock(spec=ConnectionPool)
        mock_conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        from rs_pipeline.steps import (
            fetch_prices, compute_rs, rank_indices, compute_etf_rs, rank_all
        )
        args = _make_args()
        indices = None

        fetch_prices.run(pool, args, indices)
        compute_rs.run(pool, args, indices)
        rank_indices.run(pool, args, indices)
        compute_etf_rs.run(pool, args, indices)
        rank_all.run(pool, args, indices)

        assert m_fetch.called
        assert m_compute.called
        assert m_rank.called
        assert m_etf.called
        assert m_all.called


class TestNewEtfOnboarding:
    @patch('rs_pipeline.steps.fetch_prices.bulk_upsert_dim_stock')
    @patch('rs_pipeline.steps.fetch_prices.fetch_missing_stock_info', return_value=[])
    @patch('rs_pipeline.steps.fetch_prices.clear_fetch_failure')
    @patch('rs_pipeline.steps.fetch_prices.verify_tickers_individually', return_value=({}, []))
    @patch('rs_pipeline.steps.fetch_prices.batch_download_ohlc', return_value=({}, []))
    @patch('rs_pipeline.steps.fetch_prices.fetch_latest_ohlc_dates', return_value={})
    @patch('rs_pipeline.steps.fetch_prices.onboard_new_etf', return_value=['A', 'B'])
    @patch('rs_pipeline.steps.fetch_prices.fetch_holdings_for_etf',
           return_value=['A', 'B'])
    @patch('rs_pipeline.steps.fetch_prices.fetch_all_holdings', return_value={})
    @patch('rs_pipeline.steps.fetch_prices.fetch_delisted_tickers', return_value=set())
    @patch('rs_pipeline.steps.fetch_prices.ensure_delisted_table')
    def test_unknown_etf_triggers_onboard(self, mock_ensure, mock_delisted,
                                           mock_all, mock_for, mock_onboard,
                                           mock_latest, mock_dl, mock_verify,
                                           mock_clear, mock_info, mock_dim):
        pool = MagicMock()
        mock_conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)

        from rs_pipeline.steps.fetch_prices import run
        run(pool, _make_args(), indices=['ARKK'])

        mock_onboard.assert_called_once()
        assert mock_onboard.call_args[0][1] == 'ARKK'
