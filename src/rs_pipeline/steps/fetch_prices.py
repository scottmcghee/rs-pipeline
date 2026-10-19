"""Step 1: download new daily prices for every held stock and every ETF.

Slow vendor work never runs while a database transaction is open, and the
two slowest parts (metadata lookups and re-checking missed tickers, both one
call per ticker) run with no connection held at all:

0. Onboard requested ETFs; find stocks that lack metadata and look it up
   (no connection held).
1. Store the metadata and link stocks to their sector and industry ETFs;
   bulk-download prices; repair restated tickers.
2. Re-check, one at a time, the stocks that got no new bars (no connection held).
3. Store what was recovered (repairing it if restated); count failures.

Each incremental download also re-fetches the last two weeks of stored bars.
If the vendor's closes for those days no longer match ours, the vendor has
restated the ticker's history (after a split or dividend, or a correction).
The metric series built on the old prices are queued for a rebuild, then the
whole stored history is downloaded again, so the compute steps rebuild those
series on the new basis.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, timedelta

import pandas as pd
from psycopg2.extensions import connection as Connection

from rs_pipeline import config, db
from rs_pipeline.pipeline import Context
from rs_pipeline.universe import collect_stock_info, onboard_etf, store_stock_info

logger = logging.getLogger(__name__)

# How far back stored bars are re-fetched and compared. Vendors sometimes
# apply a split or dividend to past prices a few days late.
OVERLAP = timedelta(days=14)


def plan_downloads(
    tickers: Iterable[str],
    latest: dict[str, date],
    *,
    history_start: date,
    refresh_from: date | None,
    end: date,
) -> dict[date, list[str]]:
    """Group tickers by the first date each one needs, skipping up-to-date ones.

    A ticker with no stored prices needs everything from *history_start*.
    Otherwise it needs the days after its latest stored bar, plus the
    ``OVERLAP`` before it (re-fetched to detect restatements), or everything
    from *refresh_from* if that is earlier.
    """
    plan: dict[date, list[str]] = defaultdict(list)
    for ticker in sorted(set(tickers)):
        last = latest.get(ticker)
        if last is None:
            plan[history_start].append(ticker)
        elif last < end or (refresh_from is not None and refresh_from <= end):
            first = last - OVERLAP if refresh_from is None else min(last - OVERLAP, refresh_from)
            plan[first].append(ticker)
    return dict(plan)


def find_restated(
    frames: dict[str, pd.DataFrame],
    stored: dict[str, dict[date, float]],
    latest: dict[str, date],
    tolerance: float = config.READJUSTMENT_TOLERANCE,
) -> list[str]:
    """Tickers whose re-downloaded history no longer matches what is stored.

    Compares the stored bars before each ticker's latest one with the vendor's
    values for the same days: a close that moved by more than *tolerance*, or
    a day the vendor no longer has, means the vendor restated (re-adjusted)
    the ticker's history. The latest stored bar itself is left out, because
    vendors routinely revise the most recent close by a cent or two; a real
    restatement moves every earlier bar as well.
    """
    changed = []
    for ticker, old in sorted(stored.items()):
        df = frames.get(ticker)
        if df is None or df.empty or ticker not in latest:
            continue
        new = df["close"]
        first, last = new.index.min(), new.index.max()
        for day, old_close in old.items():
            ts = pd.Timestamp(day)
            if day >= latest[ticker] or not first <= ts <= last:
                continue
            if ts not in new.index or (
                old_close and abs(float(new.loc[ts]) / old_close - 1) > tolerance
            ):
                changed.append(ticker)
                break
    return changed


class _Table:
    """Downloads into one price table and sorts the outcome for the later phases.

    *fresh* tickers got bars newer than anything stored. *stale* ones were
    expected to (their latest bar is before the end of the range) but got
    none, mapped to the start of the range they were asked for. A vendor may
    keep serving a dead ticker's old bars, so getting only the overlap back
    doesn't count as success.
    """

    def __init__(self, ctx: Context, table: str, latest: dict[str, date]) -> None:
        self.ctx, self.table, self.latest = ctx, table, latest
        self.fresh: set[str] = set()
        self.stale: dict[str, date] = {}
        self.returned: set[str] = set()
        self.restated: list[str] = []

    def expects_new(self, ticker: str) -> bool:
        last = self.latest.get(ticker)
        return last is None or last < self.ctx.options.fetch_end

    def download(self, conn: Connection, plan: dict[date, list[str]]) -> None:
        end = self.ctx.options.fetch_end
        for first, tickers in sorted(plan.items()):
            logger.info(
                "%s: downloading %d tickers, %s to %s", self.table, len(tickers), first, end
            )
            frames = self.ctx.sources.prices.download(tickers, first, end)
            self.store(conn, frames, since=dict.fromkeys(frames, first))
            for ticker in tickers:
                if ticker not in self.fresh and self.expects_new(ticker):
                    self.stale[ticker] = first  # where a re-check starts, overlap included

    def store(
        self, conn: Connection, frames: dict[str, pd.DataFrame], since: dict[str, date]
    ) -> None:
        """Store bars, setting aside tickers whose history the vendor restated."""
        known = {t: d for t, d in since.items() if t in self.latest}
        changed = find_restated(frames, db.stored_closes(conn, self.table, known), self.latest)
        rows = sum(
            db.upsert_ohlc(conn, self.table, db.ohlc_records(t, df))
            for t, df in frames.items()
            if t not in changed
        )
        logger.info(
            "%s: stored %d rows for %d tickers", self.table, rows, len(frames) - len(changed)
        )
        self.returned.update(frames)
        for ticker, df in frames.items():
            last = self.latest.get(ticker)
            if not df.empty and (last is None or df.index.max().date() > last):
                self.fresh.add(ticker)
                self.stale.pop(ticker, None)
        self.restated += changed

    def repair(self, conn: Connection, *, is_etf: bool) -> None:
        """Repair restated tickers: queue their metric rebuilds, then replace their history.

        Queuing first means a repair that fails halfway still converges: the
        next run sees the same mismatch and tries again.
        """
        tickers, self.restated = sorted(set(self.restated)), []
        if not tickers:
            return
        logger.warning("%s: vendor restated history for %s", self.table, ", ".join(tickers))
        ref = self.ctx.settings.ref_ticker
        queued = db.queue_rebuild(
            conn,
            stocks=[] if is_etf else tickers,
            etfs=tickers if is_etf else [],
            benchmark=ref if is_etf and ref in tickers else None,
        )
        logger.info("Queued %d metric series to rebuild on the new prices", queued)

        stored_first = db.first_ohlc_dates(conn, self.table, tickers)
        by_first: dict[date, list[str]] = defaultdict(list)
        for ticker, first in stored_first.items():
            by_first[first].append(ticker)
        replaced: set[str] = set()
        prices, end = self.ctx.sources.prices, self.ctx.options.fetch_end
        for first, group in by_first.items():
            frames = prices.download(group, first, end)
            if misses := [t for t in group if t not in frames]:
                frames.update(prices.recheck(misses, first, end))
            for ticker, df in frames.items():
                db.replace_ohlc(conn, self.table, ticker, df)
                replaced.add(ticker)
                if df.index.min().date() > stored_first[ticker]:
                    self.ctx.problem(
                        f"{ticker}: the vendor's history now starts {df.index.min().date()}; "
                        "older stored bars still use the old adjustment"
                    )
        if missing := sorted(set(tickers) - replaced):
            self.ctx.problem(f"could not re-download restated history for {', '.join(missing)}")


def _counts_as_failure(failed: int, attempted: int) -> bool:
    """Whether misses look like dead tickers rather than a vendor problem.

    A few misses always count; beyond that, only if they are a small share
    of the stocks attempted.
    """
    limit = max(attempted * config.MAX_FAILURE_FRACTION, config.MIN_COUNTED_FAILURES)
    return 0 < failed <= limit


def run(ctx: Context) -> None:
    logger.info("=== fetch-prices ===")
    settings, opts = ctx.settings, ctx.options

    # Phase 0: the universe.
    with ctx.pool.connection() as conn:
        if opts.indices:
            known = db.fetch_holdings(conn, opts.indices)
            for etf in opts.indices:
                if etf not in known and onboard_etf(conn, ctx.sources, etf) is None:
                    ctx.problem(f"could not onboard {etf}: no holdings found")
        delisted = db.fetch_delisted(conn)
        held = {t for ts in db.fetch_holdings(conn, opts.indices).values() for t in ts}
        lacking_info = db.tickers_missing_info(conn, sorted(held - delisted))
    info = collect_stock_info(ctx.sources, lacking_info)

    # Phase 1: bulk download.
    with ctx.pool.connection() as conn:
        store_stock_info(conn, info)
        holdings = db.fetch_holdings(conn, opts.indices)
        if not holdings:
            logger.warning("No ETF holdings found; nothing to fetch")
            return
        stocks = sorted({t for ts in holdings.values() for t in ts} - delisted)
        etfs = sorted({*holdings, settings.ref_ticker})
        logger.info("%d stocks across %d ETFs", len(stocks), len(holdings))

        tables: dict[str, _Table] = {}
        for table, tickers in ((db.STOCK_OHLC_TABLE, stocks), (db.INDEX_OHLC_TABLE, etfs)):
            latest = db.latest_ohlc_dates(conn, table)
            tables[table] = _Table(ctx, table, latest)
            plan = plan_downloads(
                tickers,
                latest,
                history_start=settings.history_start,
                refresh_from=opts.start,
                end=opts.fetch_end,
            )
            tables[table].download(conn, plan)
        stock_table, etf_table = tables[db.STOCK_OHLC_TABLE], tables[db.INDEX_OHLC_TABLE]
        stock_table.repair(conn, is_etf=False)
        etf_table.repair(conn, is_etf=True)

        vendor_up = bool(stock_table.fresh or etf_table.fresh)
        if not vendor_up:
            if stock_table.returned or etf_table.returned:
                # Old bars came back but nothing new: no session since the
                # last run (a market holiday), not a failure.
                logger.info("No new trading session since the last run")
            elif stock_table.stale or etf_table.stale:
                ctx.problem("the price vendor returned nothing for any ticker; is it down?")
            logger.info("=== fetch-prices done ===")
            return
        if etf_table.stale:
            ctx.problem(f"no new prices for ETFs: {', '.join(sorted(etf_table.stale))}")

    # Phase 2: re-check stale stocks one at a time, over the same range.
    by_first: dict[date, list[str]] = defaultdict(list)
    for ticker, first in stock_table.stale.items():
        by_first[first].append(ticker)
    recovered: dict[str, pd.DataFrame] = {}
    for first, group in by_first.items():
        recovered.update(ctx.sources.prices.recheck(group, first, opts.fetch_end))

    # Phase 3: store what was recovered, and count failures.
    with ctx.pool.connection() as conn:
        if recovered:
            logger.info("Re-check returned %d tickers", len(recovered))
            since = {t: stock_table.stale[t] for t in recovered if t in stock_table.stale}
            stock_table.store(conn, recovered, since=since)
            stock_table.repair(conn, is_etf=False)

        attempted = sum(stock_table.expects_new(t) for t in stocks)
        failed = sorted(stock_table.stale)
        db.clear_fetch_failures(conn, sorted(stock_table.fresh))
        if _counts_as_failure(len(failed), attempted):
            counts = db.record_fetch_failures(conn, failed, opts.today)
            for ticker, count in sorted(counts.items()):
                if count >= config.DELIST_FAILURE_THRESHOLD:
                    db.mark_delisted(conn, ticker, opts.today)
        elif failed:
            # Too many misses at once (an outage, throttling) is a vendor
            # problem, not evidence that the tickers are gone.
            ctx.problem(
                f"no new prices for {len(failed)} of {attempted} stocks; likely a vendor "
                "problem, so not counted toward delisting"
            )

    logger.info("=== fetch-prices done ===")
