"""Tests for database operations."""

from datetime import date
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from rs_pipeline.db import (
    _safe_float,
    _metrics_df_to_tuples,
    bulk_upsert_ohlc,
    bulk_upsert_stock_metrics,
    ensure_delisted_table,
    fetch_delisted_tickers,
    insert_delisted_ticker,
    rank_index_by_sql,
    rank_overall_by_sql,
    soft_delete_ticker,
    ConnectionPool,
)


class TestSafeFloat:
    def test_normal(self):
        assert _safe_float(3.14) == 3.14

    def test_nan(self):
        import numpy as np
        assert _safe_float(np.nan) is None

    def test_inf(self):
        import numpy as np
        assert _safe_float(np.inf) is None

    def test_none(self):
        assert _safe_float(None) is None

    def test_series_extracts_first(self):
        s = pd.Series([42.0])
        assert _safe_float(s) == 42.0


class TestBulkUpsertOhlc:
    @patch('rs_pipeline.db.execute_values')
    def test_correct_sql(self, mock_ev):
        conn = MagicMock()
        records = [
            ('2024-01-02', 'AAPL', 150.0, 155.0, 149.0, 153.0, 1000000),
        ]
        n = bulk_upsert_ohlc(conn, 'fact_commons_ohlc', records)
        assert n == 1
        mock_ev.assert_called_once()
        sql_arg = mock_ev.call_args[0][1]
        assert 'fact_commons_ohlc' in sql_arg
        assert 'ON CONFLICT' in sql_arg

    def test_empty_records(self):
        conn = MagicMock()
        assert bulk_upsert_ohlc(conn, 'fact_commons_ohlc', []) == 0


class TestBulkUpsertStockMetrics:
    @patch('rs_pipeline.db.execute_values')
    def test_converts_dataframe(self, mock_ev):
        df = pd.DataFrame({
            'date': ['2024-01-02'],
            'ticker': ['AAPL'],
            'etf_ticker': ['SPY'],
            'naive_relative_strength': [1.5],
            'naive_relative_strength_percentile': [0.8],
            'naive_deviation_from_moving_average': [2.0],
            'relative_strength': [105.0],
            'relative_strength_percentile': [0.9],
            'deviation_from_moving_average': [3.0],
            'rs_rolling_mean': [100.0],
            'rs_rolling_std_dev': [5.0],
            'rs_z_score': [1.0],
            'rs_z_score_roc': [10.0],
        })
        conn = MagicMock()
        n = bulk_upsert_stock_metrics(conn, df)
        assert n == 1
        mock_ev.assert_called_once()

    def test_empty_df(self):
        conn = MagicMock()
        assert bulk_upsert_stock_metrics(conn, pd.DataFrame()) == 0


class TestRankBySQL:
    def test_rank_index_executes(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 42
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        n = rank_index_by_sql(conn, 'SPY', '2024-01-01', '2024-01-31')
        assert n == 42
        sql_used = mock_cursor.execute.call_args[0][0]
        assert 'PERCENT_RANK()' in sql_used
        assert 'etf_ticker' in sql_used
        conn.commit.assert_called_once()

    def test_rank_overall_executes(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        n = rank_overall_by_sql(conn, '2024-01-01', '2024-01-31')
        assert n == 100
        sql_used = mock_cursor.execute.call_args[0][0]
        assert 'PERCENT_RANK()' in sql_used
        assert 'rs_rank_overall' in sql_used


class TestMetricsDfToTuples:
    def test_basic(self):
        df = pd.DataFrame({
            'date': ['2024-01-02'],
            'ticker': ['AAPL'],
            'etf_ticker': ['SPY'],
            'naive_relative_strength': [1.5],
            'naive_relative_strength_percentile': [0.8],
            'naive_deviation_from_moving_average': [2.0],
            'relative_strength': [105.0],
            'relative_strength_percentile': [0.9],
            'deviation_from_moving_average': [3.0],
            'rs_rolling_mean': [100.0],
            'rs_rolling_std_dev': [5.0],
            'rs_z_score': [1.0],
            'rs_z_score_roc': [10.0],
        })
        result = _metrics_df_to_tuples(df, ['date', 'ticker', 'etf_ticker'])
        assert len(result) == 1
        assert result[0][:3] == ('2024-01-02', 'AAPL', 'SPY')
        assert len(result[0]) == 13  # 3 keys + 10 metrics


# ---------------------------------------------------------------------------
# Delisted ticker tests
# ---------------------------------------------------------------------------

class TestEnsureDelistedTable:
    def test_creates_table(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        ensure_delisted_table(conn)
        sql_used = mock_cursor.execute.call_args[0][0]
        assert 'delisted_tickers' in sql_used
        assert 'CREATE TABLE IF NOT EXISTS' in sql_used
        conn.commit.assert_called_once()


class TestInsertDelistedTicker:
    def test_inserts_with_etf_list(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        insert_delisted_ticker(conn, 'VERV', '2026-02-13', ['SPY', 'QQQ'])
        args = mock_cursor.execute.call_args[0]
        assert 'delisted_tickers' in args[0]
        assert args[1] == ('VERV', '2026-02-13', 'SPY,QQQ')
        conn.commit.assert_called_once()

    def test_inserts_with_empty_etf_list(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        insert_delisted_ticker(conn, 'GONE', '2026-02-13', [])
        args = mock_cursor.execute.call_args[0]
        assert args[1] == ('GONE', '2026-02-13', None)


class TestFetchDelistedTickers:
    def test_returns_set(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('VERV',), ('GONE',)]
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        result = fetch_delisted_tickers(conn)
        assert result == {'VERV', 'GONE'}

    def test_returns_empty_set(self):
        conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        result = fetch_delisted_tickers(conn)
        assert result == set()


class TestSoftDeleteTicker:
    @patch('rs_pipeline.db.insert_delisted_ticker')
    def test_soft_delete_removes_junction_preserves_rest(self, mock_insert):
        conn = MagicMock()
        mock_cursor = MagicMock()
        # First call: SELECT etf_ticker returns two ETFs
        # Second call: DELETE from junc_stock_etf
        mock_cursor.fetchall.return_value = [('SPY',), ('QQQ',)]
        conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        soft_delete_ticker(conn, 'VERV')

        # Should have queried for ETF memberships
        select_call = mock_cursor.execute.call_args_list[0]
        assert 'junc_stock_etf' in select_call[0][0]
        assert 'SELECT' in select_call[0][0]

        # Should have inserted into delisted_tickers
        mock_insert.assert_called_once()
        insert_args = mock_insert.call_args[0]
        assert insert_args[1] == 'VERV'
        assert insert_args[3] == ['SPY', 'QQQ']

        # Should have deleted junction rows
        delete_call = mock_cursor.execute.call_args_list[1]
        assert 'DELETE' in delete_call[0][0]
        assert 'junc_stock_etf' in delete_call[0][0]
        assert delete_call[0][1] == ('VERV',)
        conn.commit.assert_called()
