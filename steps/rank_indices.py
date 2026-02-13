"""Step 3: Per-index RS rankings via SQL PERCENT_RANK()."""

import logging
from typing import List, Optional

from rs_pipeline.db import (
    ConnectionPool,
    fetch_all_holdings,
    rank_index_by_sql,
)

logger = logging.getLogger(__name__)


def run(pool: ConnectionPool, args, indices: Optional[List[str]] = None) -> None:
    logger.info("=== Step 3: rank-indices ===")

    start_date = args.start_date or args.end_date
    end_date = args.end_date

    with pool.connection() as conn:
        if indices:
            etf_list = indices
        else:
            holdings = fetch_all_holdings(conn)
            etf_list = sorted(holdings.keys())

        total = 0
        for etf in etf_list:
            try:
                n = rank_index_by_sql(conn, etf, start_date, end_date)
                total += n
                logger.info("Ranked %s: %d rows updated", etf, n)
            except Exception:
                logger.exception("Error ranking %s", etf)

    logger.info("=== Step 3 complete: %d total rows updated ===", total)
