"""What every step receives: settings, data sources, DB pool and run options."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from psycopg2.extensions import connection as Connection

from rs_pipeline import config, db
from rs_pipeline.config import Settings
from rs_pipeline.db import ConnectionPool
from rs_pipeline.rs import required_history
from rs_pipeline.sources import DataSources

logger = logging.getLogger(__name__)

# Earliest possible date, for "load everything".
BEGINNING = date(1900, 1, 1)

MARKET_TZ = ZoneInfo("America/New_York")
# Daily bars are treated as final from 17:00, an hour after the 16:00 close.
BARS_FINAL_AT = time(17, 0)


def last_completed_session(now: datetime) -> date:
    """The latest weekday whose daily bar is final at *now* (exchange time).

    Holidays aren't modeled; on a holiday vendors simply return no bar.
    """
    local = now.astimezone(MARKET_TZ)
    day = local.date() if local.time() >= BARS_FINAL_AT else local.date() - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


@dataclass(frozen=True)
class RunOptions:
    """Options shared by the steps.

    ``start`` means "reprocess from this date": prices are re-downloaded,
    metrics recomputed and ranks recomputed from ``start`` to ``end``. When
    it is None, each step only handles what is new since its last run.

    ``last_session`` caps downloads at the last session with final bars, so
    a run during market hours never stores a provisional bar. None means no
    cap (used by tests that pin their own dates).
    """

    end: date
    start: date | None = None
    window: int = config.DEFAULT_WINDOW
    max_workers: int = config.DEFAULT_MAX_WORKERS
    indices: tuple[str, ...] | None = None
    full_recompute: bool = False
    today: date = field(default_factory=date.today)
    last_session: date | None = None

    def __post_init__(self) -> None:
        if self.start is not None and self.start > self.end:
            raise ValueError(f"start date {self.start} is after end date {self.end}")
        if self.window < 2:
            raise ValueError("window must be at least 2")
        if self.max_workers < 1:
            raise ValueError("max workers must be at least 1")

    @property
    def fetch_end(self) -> date:
        return self.end if self.last_session is None else min(self.end, self.last_session)

    @property
    def rerank_range(self) -> tuple[date | None, date | None]:
        """Dates to re-rank even if no rank is missing: from ``start``, or
        everything for a full recompute."""
        if self.full_recompute:
            return BEGINNING, self.end
        return (self.start, self.end) if self.start else (None, None)


@dataclass(frozen=True)
class Context:
    settings: Settings
    sources: DataSources
    pool: ConnectionPool
    options: RunOptions
    # Problems a step worked around but that should fail the run (exit 1),
    # e.g. an ETF that couldn't be onboarded or computed.
    problems: list[str] = field(default_factory=list)

    def problem(self, message: str) -> None:
        logger.error(message)
        self.problems.append(message)


def check_window(conn: Connection, table: str, opts: RunOptions) -> None:
    """Refuse to mix metrics computed with different rolling windows in *table*.

    Changing the window needs a full recompute of the whole table (no
    ``--indices``). Until one finishes the table is marked as in transition,
    and only another full recompute may run on it.
    """
    stored = db.stored_window(conn, table)
    wanted = str(opts.window)
    if stored in (None, wanted):
        if stored is None:
            db.save_window(conn, table, wanted)
        return
    if not opts.full_recompute or opts.indices is not None:
        raise ValueError(
            f"{table} uses --window {stored.rstrip('?')}"
            + (" (a full recompute is unfinished)" if stored.endswith("?") else "")
            + f"; changing it to {wanted} needs --full-recompute without --indices"
        )
    db.save_window(conn, table, wanted + "?")


def finish_window(conn: Connection, table: str, opts: RunOptions) -> None:
    """Record that *table* is now fully built with this run's window."""
    if opts.full_recompute and opts.indices is None:
        db.save_window(conn, table, str(opts.window))


def lookback_start(cutoff: date, window: int) -> date:
    """First calendar date to load so metrics after *cutoff* can be computed.

    Converts ``required_history(window)`` trading days to calendar days, with
    room for weekends and holidays. A trading halt can still leave too few
    bars; the metric steps check for that and fall back to full history.
    """
    calendar_days = math.ceil(required_history(window) * 7 / 5 * 1.05) + 14
    return cutoff - timedelta(days=calendar_days)
