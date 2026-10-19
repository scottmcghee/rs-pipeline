"""Command line: ``rs-pipeline <command> [options]`` (or ``python -m rs_pipeline``)."""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

from rs_pipeline import config, db
from rs_pipeline.config import Settings
from rs_pipeline.pipeline import Context, RunOptions, last_completed_session
from rs_pipeline.sources import SOURCE_NAMES, build_sources
from rs_pipeline.steps import compute_etf_rs, compute_rs, fetch_prices, rank_all, rank_indices
from rs_pipeline.universe import collect_stock_info, onboard_etf, store_stock_info

logger = logging.getLogger("rs_pipeline")

Step = Callable[[Context], None]
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"]

STEPS: dict[str, tuple[Step, str]] = {
    "fetch-prices": (fetch_prices.run, "Download new daily prices for stocks and ETFs."),
    "compute-rs": (compute_rs.run, "Compute stock-vs-ETF relative strength metrics."),
    "rank-indices": (rank_indices.run, "Rank stocks within each ETF."),
    "compute-etf-rs": (compute_etf_rs.run, "Compute and rank ETF-vs-benchmark metrics."),
    "rank-all": (rank_all.run, "Rank stocks across all ETFs."),
}


def _iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {text!r}") from exc


def _ticker_list(text: str) -> tuple[str, ...]:
    tickers = tuple(dict.fromkeys(t.strip().upper() for t in text.split(",") if t.strip()))
    if not tickers:
        raise argparse.ArgumentTypeError("expected comma-separated tickers")
    return tickers


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source",
        choices=SOURCE_NAMES,
        default=os.getenv("RS_SOURCE", "live"),
        help="where prices, metadata and holdings come from (env RS_SOURCE; default live)",
    )
    parser.add_argument(
        "--log-level",
        "--log_level",
        default="INFO",
        choices=LOG_LEVELS,
    )


def _run_options(parser: argparse.ArgumentParser, *, recompute: bool) -> None:
    parser.add_argument(
        "--start-date",
        "--start_date",
        type=_iso_date,
        help="reprocess from this date: re-download, recompute and re-rank it onward",
    )
    parser.add_argument(
        "--end-date",
        "--end_date",
        type=_iso_date,
        default=date.today(),
        help="last date to process (default today)",
    )
    parser.add_argument(
        "--indices",
        type=_ticker_list,
        help="only these ETFs (comma-separated); fetch-prices onboards unknown ones",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=config.DEFAULT_WINDOW,
        help=f"rolling window in trading days (default {config.DEFAULT_WINDOW})",
    )
    parser.add_argument(
        "--max-workers",
        "--max_workers",
        type=int,
        default=config.DEFAULT_MAX_WORKERS,
        help="ETFs computed in parallel by compute-rs (default: CPU count, up to 8)",
    )
    if recompute:
        parser.add_argument(
            "--full-recompute",
            "--full_recompute",
            action="store_true",
            help="recompute every metric from all stored prices",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rs-pipeline",
        description="Rank stocks by relative strength against the ETFs that hold them.",
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    for name, (_, help_text) in STEPS.items():
        p = sub.add_parser(name, help=help_text, description=help_text)
        _common(p)
        _run_options(p, recompute=name.startswith("compute"))

    text = "Run all five steps in order."
    p = sub.add_parser("run-all", help=text, description=text)
    _common(p)
    _run_options(p, recompute=True)

    text = "Store which ETF represents each sector and industry (see examples/)."
    p = sub.add_parser("map-sectors", help=text, description=text)
    _common(p)
    p.add_argument("file", type=Path, help="CSV with columns kind (sector|industry), name, etf")

    for name, text in (
        ("seed-demo", "Set up the fictional demo market in an empty database."),
        ("demo", "Offline demo: set up the fictional market, run every step, print the leaders."),
    ):
        p = sub.add_parser(name, help=text, description=text)
        p.add_argument("--log-level", "--log_level", default="INFO", choices=LOG_LEVELS)
        p.set_defaults(source="synthetic")

    text = "Print the highest-ranked stocks."
    p = sub.add_parser("top", help=text, description=text)
    _common(p)
    p.add_argument("--on", type=_iso_date, help="date to show (default: latest ranked)")
    p.add_argument("-n", "--limit", type=int, default=20, help="how many (default 20)")
    return parser


def read_sector_map(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    """``(sectors, industries)`` name-to-ETF maps from a ``kind,name,etf`` CSV."""
    maps: dict[str, dict[str, str]] = {"sector": {}, "industry": {}}
    with path.open(newline="") as f:
        for line, row in enumerate(csv.DictReader(f), start=2):
            kind = (row.get("kind") or "").strip().lower()
            name, etf = (row.get("name") or "").strip(), (row.get("etf") or "").strip().upper()
            if kind not in maps or not name or not etf:
                raise ValueError(f"{path}:{line}: expected kind (sector|industry), name, etf")
            maps[kind][name] = etf
    return maps["sector"], maps["industry"]


def _seed_demo(ctx: Context) -> None:
    from rs_pipeline.sources import synthetic

    with ctx.pool.connection() as conn:
        db.replace_sector_etfs(conn, synthetic.SECTOR_ETFS, synthetic.INDUSTRY_ETFS)
        tickers: set[str] = set()
        for etf in (synthetic.BENCHMARK, synthetic.THEMATIC):
            tickers.update(onboard_etf(conn, ctx.sources, etf) or [])
        store_stock_info(conn, collect_stock_info(ctx.sources, sorted(tickers)))


def _print_top(ctx: Context, limit: int, on: date | None) -> None:
    with ctx.pool.connection() as conn:
        df = db.top_stocks(conn, limit, on) if db.has_schema(conn) else None
    if df is None or df.empty:
        print("No ranked stocks yet. Run `rs-pipeline run-all` first.")
        return
    print(f"Top {len(df)} by overall RS percentile on {df['date'].iloc[0]}:\n")
    print(df.drop(columns="date").to_string(index=False))


def _run_steps(ctx: Context, names: Sequence[str]) -> None:
    """Run steps in order. A step that fails is reported, and the rest still run:
    later steps work on whatever is stored, and ranking catches up next time.
    """
    for name in names:
        try:
            STEPS[name][0](ctx)
        except Exception as exc:
            logger.exception("%s failed", name)
            ctx.problem(f"{name} failed: {exc}")


def _configure_logging(level: str) -> None:
    # Third-party libraries stay at WARNING: urllib3 logs request URLs at
    # DEBUG, and vendor URLs carry API keys.
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.setLevel(level)


SectorMap = tuple[dict[str, str], dict[str, str]]


def _check_demo_database(ctx: Context) -> None:
    """Refuse to put demo data into a database that holds anything else.

    Runs before the schema is applied, so a refused database is left untouched.
    """
    from rs_pipeline.sources import synthetic

    with ctx.pool.connection() as conn:
        found = db.other_tables(conn) or db.etfs_outside(conn, list(synthetic.ETF_NAMES))
    if found:
        raise RuntimeError(
            f"database {ctx.settings.db.dbname!r} already holds non-demo data "
            f"({', '.join(found[:5])}); set DB_NAME to an empty database"
        )


def _execute(args: argparse.Namespace, ctx: Context, sector_map: SectorMap | None) -> None:
    pool = ctx.pool
    if args.command in ("demo", "seed-demo"):
        _check_demo_database(ctx)
    if args.command != "top":  # top only reads, so it needs no DDL rights or locks
        with pool.connection() as conn:
            db.apply_schema(conn)
    if args.command in STEPS:
        STEPS[args.command][0](ctx)
    elif args.command == "run-all":
        _run_steps(ctx, list(STEPS))
    elif args.command == "map-sectors":
        assert sector_map is not None
        sectors, industries = sector_map
        with pool.connection() as conn:
            removed = db.replace_sector_etfs(conn, sectors, industries)
        logger.info(
            "Mapped %d sectors and %d industries (%d old links removed)",
            len(sectors),
            len(industries),
            removed,
        )
    elif args.command == "seed-demo":
        _seed_demo(ctx)
    elif args.command == "demo":
        _seed_demo(ctx)
        _run_steps(ctx, list(STEPS))
        _print_top(ctx, 15, None)
    elif args.command == "top":
        _print_top(ctx, args.limit, args.on)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)

    settings = Settings()
    if args.command in ("demo", "seed-demo"):
        from rs_pipeline.sources.synthetic import BENCHMARK

        settings = replace(settings, ref_ticker=BENCHMARK, benchmark_tickers=(BENCHMARK,))
    try:
        session = last_completed_session(datetime.now(UTC))
        if args.command in (*STEPS, "run-all"):
            options = RunOptions(
                start=args.start_date,
                end=args.end_date,
                window=args.window,
                max_workers=args.max_workers,
                indices=args.indices,
                full_recompute=getattr(args, "full_recompute", False),
                last_session=session,
            )
        else:
            options = RunOptions(end=date.today(), last_session=session)
        sector_map = read_sector_map(args.file) if args.command == "map-sectors" else None
    except (ValueError, OSError) as exc:
        parser.error(str(exc))

    pool = db.ConnectionPool(settings.db, maxconn=options.max_workers + 2)
    ctx = Context(settings, build_sources(args.source, settings), pool, options)
    try:
        if args.command == "top":
            _execute(args, ctx, sector_map)
        else:
            with db.run_lock(settings.db) as locked:
                if not locked:
                    logger.error("another rs-pipeline run is in progress; not starting")
                    return 1
                _execute(args, ctx, sector_map)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
        return 130
    except Exception:
        logger.exception("%s failed", args.command)
        return 1
    finally:
        pool.close_all()
    if ctx.problems:
        logger.error("%s finished with %d problem(s); see above", args.command, len(ctx.problems))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
