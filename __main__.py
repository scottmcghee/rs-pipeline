"""CLI entry point: python -m rs_pipeline <subcommand> [options]"""

import argparse
import logging
import sys
from datetime import datetime

from rs_pipeline import config


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--start_date', type=str, default=None,
                        help='Start date (YYYY-MM-DD). Defaults vary per step.')
    parser.add_argument('--end_date', type=str,
                        default=datetime.now().strftime('%Y-%m-%d'),
                        help='End date (YYYY-MM-DD). Default: today.')
    parser.add_argument('--window', type=int, default=config.DEFAULT_WINDOW,
                        help=f'Rolling window (default {config.DEFAULT_WINDOW}).')
    parser.add_argument('--max_workers', type=int,
                        default=config.DEFAULT_MAX_WORKERS,
                        help=f'Thread pool size (default {config.DEFAULT_MAX_WORKERS}).')
    parser.add_argument('--batch_size', type=int,
                        default=config.DEFAULT_BATCH_SIZE,
                        help=f'Batch size (default {config.DEFAULT_BATCH_SIZE}).')
    parser.add_argument('--indices', type=str, default=None,
                        help='Comma-separated ETF tickers (subset or new).')
    parser.add_argument('--log_level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Log level (default INFO).')


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s %(levelname)-8s %(name)s — %(message)s',
        datefmt='%H:%M:%S',
    )


def _parse_indices(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    return [t.strip().upper() for t in raw.split(',') if t.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        prog='rs_pipeline',
        description='Unified RS Pipeline — fetch, compute, rank.',
    )
    subs = parser.add_subparsers(dest='command', required=True)

    # --- fetch-prices ---
    p_fetch = subs.add_parser('fetch-prices', help='Fetch OHLC price data.')
    _add_common_args(p_fetch)

    # --- compute-rs ---
    p_rs = subs.add_parser('compute-rs', help='Compute RS metrics.')
    _add_common_args(p_rs)
    p_rs.add_argument('--full-recompute', action='store_true',
                      help='Recompute from FULL_HISTORY_START.')

    # --- rank-indices ---
    p_rank = subs.add_parser('rank-indices',
                             help='Per-index RS rankings (SQL).')
    _add_common_args(p_rank)

    # --- compute-etf-rs ---
    p_etf = subs.add_parser('compute-etf-rs',
                            help='ETF-vs-SPY RS metrics.')
    _add_common_args(p_etf)
    p_etf.add_argument('--full-recompute', action='store_true',
                       help='Recompute from FULL_HISTORY_START.')

    # --- rank-all ---
    p_all = subs.add_parser('rank-all',
                            help='Overall RS rankings (SQL).')
    _add_common_args(p_all)

    # --- run-all ---
    p_run = subs.add_parser('run-all',
                            help='Run all steps in sequence.')
    _add_common_args(p_run)
    p_run.add_argument('--full-recompute', action='store_true',
                       help='Recompute from FULL_HISTORY_START.')

    args = parser.parse_args()
    _setup_logging(args.log_level)

    logger = logging.getLogger('rs_pipeline')
    logger.info("rs_pipeline — command=%s", args.command)

    indices = _parse_indices(args.indices)

    # Lazy imports so startup is fast and testable
    from rs_pipeline.db import ConnectionPool, ensure_tables

    pool = ConnectionPool()
    try:
        with pool.connection() as conn:
            ensure_tables(conn)

        if args.command == 'fetch-prices':
            from rs_pipeline.steps.fetch_prices import run
            run(pool, args, indices)

        elif args.command == 'compute-rs':
            from rs_pipeline.steps.compute_rs import run
            run(pool, args, indices)

        elif args.command == 'rank-indices':
            from rs_pipeline.steps.rank_indices import run
            run(pool, args, indices)

        elif args.command == 'compute-etf-rs':
            from rs_pipeline.steps.compute_etf_rs import run
            run(pool, args, indices)

        elif args.command == 'rank-all':
            from rs_pipeline.steps.rank_all import run
            run(pool, args, indices)

        elif args.command == 'run-all':
            from rs_pipeline.steps import fetch_prices, compute_rs, rank_indices
            from rs_pipeline.steps import compute_etf_rs, rank_all

            fetch_prices.run(pool, args, indices)
            compute_rs.run(pool, args, indices)
            rank_indices.run(pool, args, indices)
            compute_etf_rs.run(pool, args, indices)
            rank_all.run(pool, args, indices)

    except KeyboardInterrupt:
        logger.info("Interrupted.")
        sys.exit(1)
    except Exception:
        logger.exception("Fatal error")
        sys.exit(1)
    finally:
        pool.close_all()

    logger.info("Done.")


if __name__ == '__main__':
    main()
