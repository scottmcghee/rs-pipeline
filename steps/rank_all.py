"""Step 5: Overall RS rankings across all stocks via SQL."""

import logging
from typing import List, Optional

from rs_pipeline.db import ConnectionPool, rank_overall_by_sql

logger = logging.getLogger(__name__)


def run(pool: ConnectionPool, args, indices: Optional[List[str]] = None) -> None:
    logger.info("=== Step 5: rank-all ===")

    start_date = args.start_date or args.end_date
    end_date = args.end_date

    with pool.connection() as conn:
        n = rank_overall_by_sql(conn, start_date, end_date)
        logger.info("Overall ranking: %d rows updated", n)

    logger.info("=== Step 5 complete ===")
