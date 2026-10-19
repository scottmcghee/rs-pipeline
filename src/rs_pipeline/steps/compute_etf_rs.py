"""Step 4: relative strength of each ETF against the benchmark, then ETF ranks."""

from __future__ import annotations

import logging

from rs_pipeline import db
from rs_pipeline.pipeline import Context, check_window, finish_window
from rs_pipeline.steps._incremental import incremental_metrics, store_metrics

logger = logging.getLogger(__name__)


def run(ctx: Context) -> None:
    logger.info("=== compute-etf-rs ===")
    opts, ref = ctx.options, ctx.settings.ref_ticker
    # Broad-market ETFs are price sources only, never ranked as ETFs.
    excluded = {ref, *ctx.settings.benchmark_tickers}

    with ctx.pool.connection() as conn:
        check_window(conn, "fact_etf_metrics", opts)
        candidates = opts.indices or db.fetch_rankable_etfs(conn, sorted(excluded))
        etfs = [e for e in candidates if e not in excluded]
        if not etfs:
            logger.warning("No ETFs to compute")
            return

        rebuild = db.pending_rebuilds(conn, "etf", ref)
        last_dates = db.last_etf_metric_dates(conn, ref)
        result = incremental_metrics(
            conn,
            tickers=etfs,
            table=db.INDEX_OHLC_TABLE,
            benchmark=ref,
            last_dates=last_dates,
            opts=opts,
            rebuild=rebuild,
        )
        if result is None:
            raise RuntimeError(f"no prices for benchmark {ref}; run fetch-prices first")
        metrics, ticker_cutoffs = result
        written = store_metrics(
            conn,
            kind="etf",
            benchmark=ref,
            metrics=metrics,
            ticker_cutoffs=ticker_cutoffs,
            rebuild=rebuild,
            last_dates=last_dates,
            opts=opts,
        )
        finish_window(conn, "fact_etf_metrics", opts)
        ranked = db.rank_etfs(conn, ref, *opts.rerank_range)

    logger.info("=== compute-etf-rs done: %d rows written, %d ranks updated ===", written, ranked)
