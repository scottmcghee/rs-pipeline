"""Tests for data fetching logic."""

from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from rs_pipeline.fetch import (
    _normalise_ohlc,
    _twelve_data_download,
    _fetch_ticker_info_with_retry,
    batch_download_ohlc,
    fetch_etf_holdings,
    fetch_missing_stock_info,
    onboard_new_etf,
    verify_tickers_individually,
)


class TestBatchDownload:
    @patch('rs_pipeline.fetch._twelve_data_download', return_value={})
    @patch('rs_pipeline.fetch._yf_download_with_retry')
    def test_single_ticker(self, mock_retry, mock_td):
        """Single ticker returns flat DataFrame from yf.download."""
        dates = pd.bdate_range('2024-01-02', periods=5)
        mock_retry.return_value = {
            'AAPL': pd.DataFrame({
                'open': [1]*5, 'high': [2]*5, 'low': [0.5]*5,
                'close': [1.5]*5, 'volume': [100]*5,
            }, index=dates)
        }
        result, missing = batch_download_ohlc(['AAPL'], '2024-01-02', '2024-01-08')
        assert 'AAPL' in result
        assert missing == []

    @patch('rs_pipeline.fetch._twelve_data_download', return_value={})
    @patch('rs_pipeline.fetch._yf_download_with_retry', return_value={})
    def test_empty_download(self, mock_retry, mock_td):
        result, missing = batch_download_ohlc(['FAKE'], '2024-01-02', '2024-01-08')
        assert result == {}
        assert missing == ['FAKE']

    @patch('rs_pipeline.fetch._twelve_data_download', return_value={})
    @patch('rs_pipeline.fetch.config.YFINANCE_BATCH_SIZE', 2)
    @patch('rs_pipeline.fetch._yf_download_with_retry', return_value={})
    def test_batching(self, mock_retry, mock_td):
        """Tickers are batched according to YFINANCE_BATCH_SIZE."""
        batch_download_ohlc(['A', 'B', 'C', 'D', 'E'], '2024-01-01', '2024-01-05')
        # 5 tickers / batch_size 2 = 3 calls
        assert mock_retry.call_count == 3

    @patch('rs_pipeline.fetch._twelve_data_download')
    @patch('rs_pipeline.fetch._yf_download_with_retry')
    def test_fallback_to_twelve_data(self, mock_retry, mock_td):
        """When yfinance misses tickers, Twelve Data is called for them."""
        dates = pd.bdate_range('2024-01-02', periods=3)
        # yfinance returns only AAPL
        mock_retry.return_value = {
            'AAPL': pd.DataFrame({
                'open': [1]*3, 'high': [2]*3, 'low': [0.5]*3,
                'close': [1.5]*3, 'volume': [100]*3,
            }, index=dates)
        }
        # Twelve Data returns GOOG
        mock_td.return_value = {
            'GOOG': pd.DataFrame({
                'open': [10]*3, 'high': [12]*3, 'low': [9]*3,
                'close': [11]*3, 'volume': [200]*3,
            }, index=dates)
        }
        result, missing = batch_download_ohlc(
            ['AAPL', 'GOOG', 'FAKE'], '2024-01-02', '2024-01-05')
        assert 'AAPL' in result
        assert 'GOOG' in result
        assert missing == ['FAKE']
        # Twelve Data called with the missing tickers
        mock_td.assert_called_once_with(['GOOG', 'FAKE'], '2024-01-02', '2024-01-05')

    @patch('rs_pipeline.fetch.time.sleep')
    @patch('rs_pipeline.fetch.yf.download')
    def test_retry_on_rate_limit_large_batch(self, mock_dl, mock_sleep):
        """Empty result for large batch (>10) triggers retry with backoff."""
        from rs_pipeline.fetch import _yf_download_with_retry
        tickers = [f'T{i}' for i in range(15)]
        mock_dl.return_value = pd.DataFrame()
        result = _yf_download_with_retry(tickers, '2024-01-02', '2024-01-08')
        assert result == {}
        # 3 retries + 1 initial = 4 calls total
        assert mock_dl.call_count == 4
        assert mock_sleep.call_count == 3

    @patch('rs_pipeline.fetch.time.sleep')
    @patch('rs_pipeline.fetch.yf.download')
    def test_no_retry_small_batch(self, mock_dl, mock_sleep):
        """Small batch (<=10) empty result skips retry (likely all delisted)."""
        from rs_pipeline.fetch import _yf_download_with_retry
        mock_dl.return_value = pd.DataFrame()
        result = _yf_download_with_retry(['FAKE', 'GONE', 'DEAD'], '2024-01-02', '2024-01-08')
        assert result == {}
        assert mock_dl.call_count == 1
        mock_sleep.assert_not_called()

    @patch('rs_pipeline.fetch.config.TWELVE_DATA_API_KEY', '')
    def test_no_twelve_data_key_graceful(self):
        """Empty API key → fallback skipped, returns empty dict."""
        result = _twelve_data_download(['AAPL'], '2024-01-02', '2024-01-08')
        assert result == {}


class TestNormaliseOhlc:
    def test_normalises_columns(self):
        df = pd.DataFrame({
            'Open': [1], 'High': [2], 'Low': [0.5],
            'Close': [1.5], 'Volume': [100],
        })
        result = _normalise_ohlc(df)
        assert set(result.columns) == {'open', 'high', 'low', 'close', 'volume'}

    def test_returns_none_for_empty(self):
        assert _normalise_ohlc(pd.DataFrame()) is None
        assert _normalise_ohlc(None) is None

    def test_returns_none_for_missing_columns(self):
        df = pd.DataFrame({'Foo': [1], 'Bar': [2]})
        assert _normalise_ohlc(df) is None


class TestFetchEtfHoldings:
    @patch('rs_pipeline.fetch.os.path.exists', return_value=True)
    @patch('rs_pipeline.fetch.pd.read_csv')
    def test_csv_path(self, mock_csv, mock_exists):
        mock_csv.return_value = pd.DataFrame({'ticker': ['AAPL', 'MSFT']})
        result = fetch_etf_holdings('SPY', csv_dir='/tmp')
        mock_exists.assert_called_once_with('/tmp/spy.csv')
        assert result == ['AAPL', 'MSFT']

    @patch('rs_pipeline.fetch.os.path.exists', return_value=False)
    @patch('rs_pipeline.fetch.requests.get')
    def test_alphavantage_fallback(self, mock_get, mock_exists):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            'holdings': [{'symbol': 'AAPL'}, {'symbol': 'GOOG'}]
        }
        mock_get.return_value = mock_resp
        result = fetch_etf_holdings('ARKK', csv_dir='/tmp')
        assert result == ['AAPL', 'GOOG']


class TestFetchMissingStockInfo:
    @patch('rs_pipeline.fetch.fetch_existing_stocks', return_value={'AAPL', 'MSFT'})
    @patch('rs_pipeline.fetch.yf.Ticker')
    def test_skips_existing(self, mock_ticker, mock_existing):
        """Only fetches info for tickers not already in dim_stock."""
        mock_conn = MagicMock()
        result = fetch_missing_stock_info(mock_conn, ['AAPL', 'MSFT'])
        assert result == []
        mock_ticker.assert_not_called()

    @patch('rs_pipeline.fetch._fetch_ticker_info_with_retry')
    @patch('rs_pipeline.fetch.fetch_existing_stocks', return_value={'AAPL'})
    def test_fetches_new(self, mock_existing, mock_info):
        mock_info.return_value = {
            'longName': 'Google', 'sector': 'Tech', 'industry': 'Internet',
            'exchange': 'NMS',
        }
        mock_conn = MagicMock()
        result = fetch_missing_stock_info(mock_conn, ['AAPL', 'GOOG'])
        assert len(result) == 1
        assert result[0] == ('GOOG', 'Google', 'Tech', 'Internet', 'NMS')

    @patch('rs_pipeline.fetch.time.sleep')
    @patch('rs_pipeline.fetch.yf.Ticker')
    def test_stock_info_retry_on_429(self, mock_ticker_cls, mock_sleep):
        """429 error triggers retry with backoff."""
        # First call raises 429, second succeeds
        mock_ticker_cls.return_value.info = property(lambda self: None)
        calls = [0]

        def side_effect(t):
            mock = MagicMock()
            if calls[0] == 0:
                calls[0] += 1
                mock.info = property(lambda s: (_ for _ in ()).throw(
                    Exception("429 Too Many Requests")))
                raise Exception("429 Too Many Requests")
            mock.info = {'longName': 'Test', 'sector': 'Tech', 'industry': 'SW'}
            return mock

        mock_ticker_cls.side_effect = side_effect
        result = _fetch_ticker_info_with_retry('GOOG')
        assert result == {'longName': 'Test', 'sector': 'Tech', 'industry': 'SW'}
        assert mock_sleep.call_count == 1


class TestOnboardNewEtf:
    @patch('rs_pipeline.fetch.yf.Ticker')
    @patch('rs_pipeline.fetch.fetch_etf_holdings', return_value=['A', 'B', 'C'])
    @patch('rs_pipeline.fetch.fetch_existing_stocks', return_value={'A'})
    @patch('rs_pipeline.fetch.bulk_upsert_dim_stock')
    @patch('rs_pipeline.fetch.bulk_upsert_junc_stock_etf')
    @patch('rs_pipeline.fetch.upsert_dim_etf')
    def test_onboards(self, mock_etf, mock_junc, mock_stock, mock_exist,
                      mock_hold, mock_yf_ticker):
        mock_yf_ticker.return_value.info = {'longName': 'ARK Innovation ETF'}
        conn = MagicMock()
        result = onboard_new_etf(conn, 'ARKK')
        assert result == ['A', 'B', 'C']
        mock_etf.assert_called_once_with(conn, 'ARKK', 'ARK Innovation ETF')
        # Only B and C are new
        mock_stock.assert_called_once()
        new_stocks = mock_stock.call_args[0][1]
        tickers = [r[0] for r in new_stocks]
        assert 'B' in tickers and 'C' in tickers and 'A' not in tickers
        # Stub rows should be 5-tuples with None fields
        assert all(len(r) == 5 for r in new_stocks)
        # Junction for all 3
        assert mock_junc.call_args[0][1] == [('A', 'ARKK'), ('B', 'ARKK'), ('C', 'ARKK')]


class TestVerifyTickersIndividually:
    @patch('rs_pipeline.fetch.yf.Ticker')
    def test_recovers_valid_tickers(self, mock_ticker_cls):
        """Valid tickers should be recovered via yf.Ticker().history()."""
        dates = pd.bdate_range('2024-01-02', periods=3)
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame({
            'Open': [1]*3, 'High': [2]*3, 'Low': [0.5]*3,
            'Close': [1.5]*3, 'Volume': [100]*3,
        }, index=dates)
        mock_ticker_cls.return_value = mock_ticker

        recovered, missing = verify_tickers_individually(
            ['AAPL'], '2024-01-02', '2024-01-05')
        assert 'AAPL' in recovered
        assert missing == []

    @patch('rs_pipeline.fetch.yf.Ticker')
    def test_truly_missing_tickers(self, mock_ticker_cls):
        """Tickers returning empty history should be truly missing."""
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_cls.return_value = mock_ticker

        recovered, missing = verify_tickers_individually(
            ['FAKE'], '2024-01-02', '2024-01-05')
        assert recovered == {}
        assert missing == ['FAKE']

    @patch('rs_pipeline.fetch.yf.Ticker')
    def test_mixed_results(self, mock_ticker_cls):
        """Mix of valid and invalid tickers."""
        dates = pd.bdate_range('2024-01-02', periods=3)

        def side_effect(ticker):
            mock = MagicMock()
            if ticker == 'GOOD':
                mock.history.return_value = pd.DataFrame({
                    'Open': [1]*3, 'High': [2]*3, 'Low': [0.5]*3,
                    'Close': [1.5]*3, 'Volume': [100]*3,
                }, index=dates)
            else:
                mock.history.return_value = pd.DataFrame()
            return mock

        mock_ticker_cls.side_effect = side_effect
        recovered, missing = verify_tickers_individually(
            ['GOOD', 'BAD'], '2024-01-02', '2024-01-05')
        assert 'GOOD' in recovered
        assert missing == ['BAD']

    def test_empty_input(self):
        recovered, missing = verify_tickers_individually(
            [], '2024-01-02', '2024-01-05')
        assert recovered == {}
        assert missing == []

    @patch('rs_pipeline.fetch.yf.Ticker')
    def test_exception_counts_as_missing(self, mock_ticker_cls):
        """Exceptions during verification should mark ticker as missing."""
        mock_ticker_cls.side_effect = Exception("network error")
        recovered, missing = verify_tickers_individually(
            ['ERR'], '2024-01-02', '2024-01-05')
        assert recovered == {}
        assert missing == ['ERR']
