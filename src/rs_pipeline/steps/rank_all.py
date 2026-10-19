"""Step 5: rank each stock's RS among all stocks, whatever ETFs hold them."""

from __future__ import annotations

import logging

from rs_pipeline import db
from rs_pipeline.pipeline import Context

logger = logging.getLogger(__name__)


def run(ctx: Context) -> None:
    logger.info("=== rank-all ===")
    with ctx.pool.connection() as conn:
        updated = db.rank_stocks_overall(conn, *ctx.options.rerank_range)
    logger.info("=== rank-all done: %d ranks updated ===", updated)
