"""Step 3: rank each stock's RS among the members of each of its ETFs."""

from __future__ import annotations

import logging

from rs_pipeline import db
from rs_pipeline.pipeline import Context

logger = logging.getLogger(__name__)


def run(ctx: Context) -> None:
    logger.info("=== rank-indices ===")
    with ctx.pool.connection() as conn:
        updated = db.rank_stocks_within_etf(conn, ctx.options.indices, *ctx.options.rerank_range)
    logger.info("=== rank-indices done: %d ranks updated ===", updated)
