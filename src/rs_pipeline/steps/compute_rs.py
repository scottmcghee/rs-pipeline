"""Step 2: relative strength of every stock against each ETF it belongs to.

ETFs are processed in parallel, one connection per worker.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from rs_pipeline import db
from rs_pipeline.pipeline import Context, check_window, finish_window
from rs_pipeline.steps._incremental import incremental_metrics, store_metrics

logger = logging.getLogger(__name__)


def compute_for_etf(ctx: Context, etf: str, tickers: list[str]) -> int:
    """Compute and store new metrics for one ETF's members. Returns rows written."""
    with ctx.pool.connection() as conn:
        rebuild = db.pending_rebuilds(conn, "stock", etf)
        last_dates = db.last_stock_metric_dates(conn, etf)
        result = incremental_metrics(
            conn,
            tickers=tickers,
            table=db.STOCK_OHLC_TABLE,
            benchmark=etf,
            last_dates=last_dates,
            opts=ctx.options,
            rebuild=rebuild,
        )
        if result is None:
            logger.warning("No prices for ETF %s; skipping its members", etf)
            return 0
        metrics, ticker_cutoffs = result
        return store_metrics(
            conn,
            kind="stock",
            benchmark=etf,
            metrics=metrics,
            ticker_cutoffs=ticker_cutoffs,
            rebuild=rebuild,
            last_dates=last_dates,
            opts=ctx.options,
        )


def run(ctx: Context) -> None:
    logger.info("=== compute-rs ===")
    with ctx.pool.connection() as conn:
        check_window(conn, "fact_stock_metrics", ctx.options)
        holdings = db.fetch_holdings(conn, ctx.options.indices)
    if not holdings:
        logger.warning("No ETF holdings found; nothing to compute")
        return

    total, failed = 0, 0
    with ThreadPoolExecutor(max_workers=ctx.options.max_workers) as executor:
        futures = {
            executor.submit(compute_for_etf, ctx, etf, tickers): etf
            for etf, tickers in holdings.items()
        }
        for future in as_completed(futures):
            etf = futures[future]
            try:
                rows = future.result()
            except Exception as exc:
                logger.exception("compute-rs failed for %s", etf)
                ctx.problem(f"compute-rs failed for {etf}: {type(exc).__name__}")
                failed += 1
                continue
            total += rows
            if rows:
                logger.info("%s: %d rows written", etf, rows)

    if not failed:
        with ctx.pool.connection() as conn:
            finish_window(conn, "fact_stock_metrics", ctx.options)
    logger.info("=== compute-rs done: %d rows written ===", total)
