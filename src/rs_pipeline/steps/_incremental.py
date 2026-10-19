"""Incremental metric computation shared by compute-rs and compute-etf-rs."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta

import pandas as pd
from psycopg2.extensions import connection as Connection

from rs_pipeline import db
from rs_pipeline.pipeline import BEGINNING, RunOptions, lookback_start
from rs_pipeline.rs import compute_all_metrics, required_history


def cutoffs(
    tickers: Sequence[str],
    last_dates: dict[str, date],
    opts: RunOptions,
    rebuild: set[str] | frozenset[str] = frozenset(),
) -> dict[str, date | None]:
    """For each ticker, the last date whose metrics are kept; None means recompute all.

    Tickers in *rebuild* (queued after a price restatement) are recomputed in full.
    """
    if opts.full_recompute:
        return dict.fromkeys(tickers)
    result: dict[str, date | None] = {}
    for ticker in tickers:
        last = None if ticker in rebuild else last_dates.get(ticker)
        if last is not None and opts.start is not None:
            last = min(last, opts.start - timedelta(days=1))
        result[ticker] = last
    return result


def has_enough_history(
    prices: pd.DataFrame, benchmark: pd.DataFrame, cutoff: date, window: int
) -> bool:
    """Whether the loaded slice covers the history the first new row depends on.

    The first row after *cutoff* needs ``required_history - 1`` earlier
    trading days that both series share. A calendar look-back usually covers
    that, but not across a long trading halt or a gap in the data.
    """
    common = prices.index.intersection(benchmark.index)
    return int((common <= pd.Timestamp(cutoff)).sum()) >= required_history(window) - 1


def new_metrics(
    prices: dict[str, pd.DataFrame],
    benchmark: pd.DataFrame,
    benchmark_ticker: str,
    ticker_cutoffs: dict[str, date | None],
    window: int,
) -> pd.DataFrame:
    """Metrics for every ticker against *benchmark*, only for dates after each cutoff."""
    frames = []
    for ticker, cutoff in ticker_cutoffs.items():
        if ticker not in prices:
            continue
        metrics = compute_all_metrics(prices[ticker], benchmark, window, ticker, benchmark_ticker)
        if cutoff is not None and not metrics.empty:
            metrics = metrics[metrics["date"] > pd.Timestamp(cutoff)]
        if not metrics.empty:
            frames.append(metrics)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def incremental_metrics(
    conn: Connection,
    *,
    tickers: Sequence[str],
    table: str,
    benchmark: str,
    last_dates: dict[str, date],
    opts: RunOptions,
    rebuild: set[str] | frozenset[str] = frozenset(),
) -> tuple[pd.DataFrame, dict[str, date | None]] | None:
    """New metric rows for *tickers* (prices in *table*) against *benchmark*.

    Tickers without metrics, or queued for *rebuild*, load their full
    history; the rest load only the look-back before the earliest cutoff,
    and fall back to full history if that slice turns out too short. Returns
    the rows and each ticker's cutoff, or None if the benchmark has no prices.
    """
    ticker_cutoffs = cutoffs(tickers, last_dates, opts, rebuild)
    full = [t for t, c in ticker_cutoffs.items() if c is None]
    known = {t: c for t, c in ticker_cutoffs.items() if c is not None}
    start = lookback_start(min(known.values()), opts.window) if known else BEGINNING

    def load_benchmark(since: date) -> pd.DataFrame | None:
        return db.fetch_ohlc(conn, db.INDEX_OHLC_TABLE, [benchmark], since, opts.end).get(benchmark)

    bench_start = BEGINNING if full else start
    bench = load_benchmark(bench_start)
    if bench is None:
        return None

    prices = db.fetch_ohlc(conn, table, full, BEGINNING, opts.end)
    loaded = db.fetch_ohlc(conn, table, list(known), start, opts.end)
    short = [
        t
        for t, df in loaded.items()
        if start > BEGINNING and not has_enough_history(df, bench, known[t], opts.window)
    ]
    if short:
        if bench_start > BEGINNING:
            bench = load_benchmark(BEGINNING)
            assert bench is not None
        loaded.update(db.fetch_ohlc(conn, table, short, BEGINNING, opts.end))
    prices.update(loaded)
    return new_metrics(prices, bench, benchmark, ticker_cutoffs, opts.window), ticker_cutoffs


def store_metrics(
    conn: Connection,
    *,
    kind: str,
    benchmark: str,
    metrics: pd.DataFrame,
    ticker_cutoffs: dict[str, date | None],
    rebuild: set[str] | frozenset[str],
    last_dates: dict[str, date],
    opts: RunOptions,
) -> int:
    """Upsert new rows, finish queued rebuilds, and drop rows a rebuild no longer has.

    A queued series is finished only once it was recomputed through its last
    stored date: a run that ends earlier (``--end-date`` in the past) writes
    what it computed but leaves the series queued, and deletes nothing.
    Queue entries for series that no longer exist are dropped.
    """
    table, keys = (
        ("fact_stock_metrics", ("ticker", "etf_ticker"))
        if kind == "stock"
        else ("fact_etf_metrics", ("etf_ticker", "ref_ticker"))
    )
    upsert = db.upsert_stock_metrics if kind == "stock" else db.upsert_etf_metrics
    frame = (
        metrics
        if kind == "stock"
        else metrics.rename(columns={"ticker": "etf_ticker", "etf_ticker": "ref_ticker"})
    )
    written = upsert(conn, frame)
    # A queued series is finished only once it was actually recomputed; one
    # with no stored prices yet stays queued rather than losing its rows.
    computed = set(metrics["ticker"]) if not metrics.empty else set()
    rebuilt = sorted(
        t
        for t in rebuild
        if t in ticker_cutoffs
        and t in computed
        and (t not in last_dates or opts.end >= last_dates[t])
    )
    for ticker in rebuilt:
        keep = list(pd.to_datetime(metrics.loc[metrics["ticker"] == ticker, "date"]).dt.date)
        db.prune_metrics(conn, table, keys, (ticker, benchmark), keep)
    orphans = sorted(t for t in rebuild if t not in ticker_cutoffs and opts.indices is None)
    db.finish_rebuilds(conn, kind, benchmark, rebuilt + orphans)
    return written
