"""Data fetching — yfinance batch downloads, stock info, ETF holdings."""

import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf

from rs_pipeline import config
from rs_pipeline.db import (
    bulk_upsert_dim_stock,
    bulk_upsert_junc_stock_etf,
    fetch_existing_stocks,
    upsert_dim_etf,
    upsert_dim_stock,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch OHLC download
# ---------------------------------------------------------------------------

def batch_download_ohlc(tickers: List[str], start: str,
                        end: str) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Download OHLC for many tickers via yfinance, with Twelve Data fallback.

    Returns (dict[ticker → DataFrame], list of still-missing tickers).
    """
    result: Dict[str, pd.DataFrame] = {}
    batch_size = config.YFINANCE_BATCH_SIZE

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        logger.info("Downloading OHLC batch %d–%d of %d tickers",
                     i + 1, min(i + batch_size, len(tickers)), len(tickers))

        batch_result = _yf_download_with_retry(batch, start, end)
        result.update(batch_result)

    # Identify tickers yfinance missed
    missing = [t for t in tickers if t not in result]

    if missing:
        logger.info("yfinance missed %d tickers, trying Twelve Data fallback",
                     len(missing))
        td_result = _twelve_data_download(missing, start, end)
        result.update(td_result)

    still_missing = [t for t in tickers if t not in result]
    logger.info("Downloaded OHLC for %d / %d tickers (%d still missing)",
                 len(result), len(tickers), len(still_missing))
    return result, still_missing


def _yf_download_with_retry(batch: List[str], start: str,
                            end: str) -> Dict[str, pd.DataFrame]:
    """Download a single batch from yfinance with retry on blanket rate limit.

    Only retries when batch has multiple tickers (empty = likely rate limited).
    Single-ticker empty results are assumed delisted and skip straight to fallback.
    """
    result: Dict[str, pd.DataFrame] = {}
    # Only retry large batches where blanket empty likely means rate limiting.
    # Small batches (<=10) returning empty are almost certainly all delisted.
    max_retries = config.RATE_LIMIT_MAX_RETRIES if len(batch) > 10 else 0

    for attempt in range(max_retries + 1):
        try:
            df = yf.download(batch, start=start, end=end,
                             threads=True, progress=False, group_by='ticker')
        except Exception as e:
            logger.error("yf.download failed (attempt %d): %s", attempt + 1, e)
            if attempt < max_retries:
                wait = config.RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
                logger.info("Rate limit backoff: waiting %ds", wait)
                time.sleep(wait)
                continue
            return result

        if df.empty:
            if attempt < max_retries:
                wait = config.RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
                logger.warning("Empty result for %d tickers (attempt %d), "
                               "retrying in %ds", len(batch), attempt + 1, wait)
                time.sleep(wait)
                continue
            return result

        # Got data — extract per-ticker DataFrames
        if len(batch) == 1:
            ticker = batch[0]
            sub = _normalise_ohlc(df)
            if sub is not None and not sub.empty:
                result[ticker] = sub
        else:
            for ticker in batch:
                try:
                    sub = df[ticker] if ticker in df.columns.get_level_values(0) else None
                except (KeyError, TypeError):
                    sub = None
                if sub is not None:
                    sub = _normalise_ohlc(sub)
                    if sub is not None and not sub.empty:
                        result[ticker] = sub
        break  # success, no retry needed

    return result


def _normalise_ohlc(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Normalise a yfinance DataFrame to lowercase columns.

    Handles both flat columns ('Open') and MultiIndex tuples (('Open', 'AAPL'))
    that newer yfinance versions return with group_by='ticker'.
    """
    if df is None or df.empty:
        return None
    df = df.copy()
    # Handle MultiIndex columns from yfinance group_by='ticker'
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower()
                      for c in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]
    needed = {'open', 'high', 'low', 'close', 'volume'}
    if not needed.issubset(set(df.columns)):
        return None
    df = df[['open', 'high', 'low', 'close', 'volume']].dropna(subset=['close'])
    df.index = pd.to_datetime(df.index)
    df.index.name = 'date'
    return df


# ---------------------------------------------------------------------------
# Twelve Data fallback
# ---------------------------------------------------------------------------

def _twelve_data_download(tickers: List[str], start: str,
                          end: str) -> Dict[str, pd.DataFrame]:
    """Fallback: fetch OHLC from Twelve Data for tickers yfinance missed."""
    api_key = config.TWELVE_DATA_API_KEY
    if not api_key:
        logger.info("No TWELVE_DATA_API_KEY configured, skipping fallback")
        return {}

    try:
        import twelvedata as td_lib
    except ImportError:
        logger.warning("twelvedata package not installed, skipping fallback")
        return {}

    result: Dict[str, pd.DataFrame] = {}
    batch_size = config.TWELVE_DATA_BATCH_SIZE

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        logger.info("Twelve Data batch %d–%d of %d tickers",
                     i + 1, min(i + batch_size, len(tickers)), len(tickers))

        for attempt in range(config.RATE_LIMIT_MAX_RETRIES + 1):
            try:
                td = td_lib.TDClient(apikey=api_key)
                ts = td.time_series(
                    symbol=','.join(batch),
                    interval='1day',
                    start_date=start,
                    end_date=end,
                    outputsize=5000,
                )
                data = ts.as_pandas()

                if isinstance(data, dict):
                    # Multiple tickers → dict of DataFrames
                    for ticker, df in data.items():
                        normed = _normalise_twelve_data(df)
                        if normed is not None:
                            result[ticker] = normed
                elif isinstance(data, pd.DataFrame) and len(batch) == 1:
                    normed = _normalise_twelve_data(data)
                    if normed is not None:
                        result[batch[0]] = normed
                break
            except Exception as e:
                err_str = str(e).lower()
                if '429' in err_str or 'rate' in err_str:
                    if attempt < config.RATE_LIMIT_MAX_RETRIES:
                        wait = config.RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
                        logger.warning("Twelve Data rate limit (attempt %d), "
                                       "retrying in %ds", attempt + 1, wait)
                        time.sleep(wait)
                        continue
                logger.error("Twelve Data fetch failed: %s", e)
                break

    logger.info("Twelve Data returned %d tickers", len(result))
    return result


def _normalise_twelve_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Normalise Twelve Data output to match yfinance format."""
    if df is None or df.empty:
        return None
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    needed = {'open', 'high', 'low', 'close', 'volume'}
    if not needed.issubset(set(df.columns)):
        return None
    df = df[['open', 'high', 'low', 'close', 'volume']].dropna(subset=['close'])
    df.index = pd.to_datetime(df.index)
    df.index.name = 'date'
    df = df.sort_index()
    return df


# ---------------------------------------------------------------------------
# Individual ticker verification (post-batch fallback)
# ---------------------------------------------------------------------------

def verify_tickers_individually(tickers: List[str], start: str,
                                end: str) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Verify supposedly-missing tickers one-by-one using yf.Ticker().history().

    Batch yf.download() can false-positive on valid tickers due to rate limits
    or batch-level errors. This rechecks each ticker individually as a final
    gate before recording a failure.

    Returns (recovered dict[ticker → DataFrame], truly_missing list).
    """
    if not tickers:
        return {}, []

    logger.info("Verifying %d tickers individually via yf.Ticker().history()",
                len(tickers))
    recovered: Dict[str, pd.DataFrame] = {}
    truly_missing: List[str] = []

    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            df = t.history(start=start, end=end)
            if df is not None and not df.empty:
                normed = _normalise_ohlc(df)
                if normed is not None and not normed.empty:
                    recovered[ticker] = normed
                    continue
        except Exception as e:
            logger.debug("Individual verify failed for %s: %s", ticker, e)
        truly_missing.append(ticker)

    logger.info("Individual verification: %d recovered, %d truly missing",
                len(recovered), len(truly_missing))
    return recovered, truly_missing


# ---------------------------------------------------------------------------
# ETF holdings discovery
# ---------------------------------------------------------------------------

def fetch_etf_holdings(etf_ticker: str,
                       csv_dir: str = None) -> Optional[List[str]]:
    """Discover ETF constituents: CSV cache first, AlphaVantage fallback."""
    csv_dir = csv_dir or config.CSV_DIR
    csv_path = os.path.join(csv_dir, f"{etf_ticker.lower()}.csv")

    if os.path.exists(csv_path):
        logger.info("Reading holdings from %s", csv_path)
        df = pd.read_csv(csv_path)
        return df['ticker'].tolist()

    # AlphaVantage fallback
    logger.info("Fetching holdings for %s from AlphaVantage", etf_ticker)
    url = (f"https://www.alphavantage.co/query?function=ETF_PROFILE"
           f"&symbol={etf_ticker}&apikey={config.AV_API_KEY}")
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [h['symbol'] for h in data.get('holdings', [])]
    except Exception as e:
        logger.error("AlphaVantage holdings fetch failed for %s: %s",
                     etf_ticker, e)
        return None


# ---------------------------------------------------------------------------
# Stock info (only for new tickers)
# ---------------------------------------------------------------------------

def fetch_missing_stock_info(conn, tickers: List[str]) -> List[Tuple[str, str, str, str]]:
    """Fetch yfinance stock info only for tickers not already in dim_stock.

    Retries on rate limit (429) with exponential backoff.
    Returns list of (ticker, company_name, sector, industry).
    """
    existing = fetch_existing_stocks(conn)
    new_tickers = [t for t in tickers if t not in existing]
    if not new_tickers:
        return []

    logger.info("Fetching stock info for %d new tickers", len(new_tickers))
    records = []
    for ticker in new_tickers:
        info = _fetch_ticker_info_with_retry(ticker)
        records.append((
            ticker,
            info.get('longName') if info else None,
            info.get('sector') if info else None,
            info.get('industry') if info else None,
        ))
    return records


def _fetch_ticker_info_with_retry(ticker: str) -> Optional[dict]:
    """Fetch yf.Ticker().info with retry on rate limit."""
    for attempt in range(config.RATE_LIMIT_MAX_RETRIES + 1):
        try:
            return yf.Ticker(ticker).info
        except Exception as e:
            err_str = str(e).lower()
            if ('429' in err_str or 'rate' in err_str) and \
               attempt < config.RATE_LIMIT_MAX_RETRIES:
                wait = config.RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
                logger.warning("Rate limit on stock info for %s (attempt %d), "
                               "retrying in %ds", ticker, attempt + 1, wait)
                time.sleep(wait)
                continue
            logger.warning("stock_info failed for %s: %s", ticker, e)
            return None
    return None


# ---------------------------------------------------------------------------
# New ETF onboarding
# ---------------------------------------------------------------------------

def onboard_new_etf(conn, etf_ticker: str,
                    csv_dir: str = None) -> Optional[List[str]]:
    """Discover holdings, register ETF + stocks + mappings in DB.

    Returns list of constituent tickers, or None on failure.
    """
    holdings = fetch_etf_holdings(etf_ticker, csv_dir)
    if not holdings:
        logger.error("Could not discover holdings for %s", etf_ticker)
        return None

    logger.info("Onboarding %s with %d holdings", etf_ticker, len(holdings))

    # Ensure ETF in dim_etf (with name from yfinance)
    etf_name = None
    try:
        info = yf.Ticker(etf_ticker).info
        etf_name = info.get('longName') or info.get('shortName')
    except Exception:
        pass
    upsert_dim_etf(conn, etf_ticker, etf_name)

    # Ensure all stocks in dim_stock (stub rows; stock_info fetched separately)
    existing = fetch_existing_stocks(conn)
    new_stocks = [(t, None, None, None) for t in holdings if t not in existing]
    if new_stocks:
        bulk_upsert_dim_stock(conn, new_stocks)

    # Junction mappings
    junc_records = [(t, etf_ticker) for t in holdings]
    bulk_upsert_junc_stock_etf(conn, junc_records)

    logger.info("Onboarded %s: %d stocks mapped", etf_ticker, len(holdings))
    return holdings
