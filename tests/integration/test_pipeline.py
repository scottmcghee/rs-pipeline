"""The whole pipeline against a real database and the synthetic market."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from psycopg2.extensions import connection as Connection

from rs_pipeline import cli, db
from rs_pipeline.pipeline import Context, lookback_start
from rs_pipeline.sources import DataSources
from rs_pipeline.sources.synthetic import BENCHMARK, THEMATIC, SyntheticMarket
from tests.integration.conftest import (
    SETTINGS_OVERRIDES,
    SMALL_WINDOW,
    T2,
    MakeContext,
    query,
    scalar,
)

T1 = date(2024, 3, 28)
# Run dates for the incremental tests: a gap of one quarter, then weekly, then daily.
RUN_DATES = [T1, date(2024, 5, 31), date(2024, 6, 7), date(2024, 6, 14), date(2024, 6, 27), T2]

STEPS = [step for step, _ in cli.STEPS.values()]
TABLES = ("fact_stock_metrics", "fact_etf_metrics")


def run_all(ctx: Context) -> None:
    for step in STEPS:
        step(ctx)


def seed(ctx: Context) -> None:
    cli._seed_demo(ctx)


def snapshot(conn: Connection, tables: tuple[str, ...] = TABLES) -> dict[str, pd.DataFrame]:
    return {t: query(conn, f"SELECT * FROM {t} ORDER BY 1, 2, 3") for t in tables}


def assert_same(a: dict[str, pd.DataFrame], b: dict[str, pd.DataFrame]) -> None:
    for table in a:
        assert len(a[table]) > 0, table
        pd.testing.assert_frame_equal(a[table], b[table], check_exact=True)


def wipe_facts(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE fact_stock_metrics, fact_etf_metrics, fact_commons_ohlc, fact_index_ohlc"
        )
    conn.commit()


@pytest.mark.seeded
def test_full_run_ranks_everything_consistently(conn: Connection) -> None:
    # The database is already seeded and run up to T2.

    assert scalar(conn, "SELECT COUNT(*) FROM dim_stock WHERE sector IS NOT NULL") == 40
    assert scalar(conn, "SELECT MAX(date) FROM fact_commons_ohlc") == T2
    assert scalar(conn, "SELECT MAX(date) FROM fact_stock_metrics") == T2

    unranked = scalar(
        conn,
        "SELECT COUNT(*) FROM fact_stock_metrics "
        "WHERE rs_rank_index IS NULL OR rs_rank_overall IS NULL",
    )
    assert unranked == 0
    assert scalar(conn, "SELECT COUNT(*) FROM fact_etf_metrics WHERE rs_rank_index IS NULL") == 0

    # Overall rank is one value per stock and date, whichever ETF row you read.
    inconsistent = scalar(
        conn,
        "SELECT COUNT(*) FROM (SELECT 1 FROM fact_stock_metrics GROUP BY date, ticker "
        "HAVING COUNT(DISTINCT rs_rank_overall) > 1) x",
    )
    assert inconsistent == 0

    # Within each ETF and date the ranks run from 0 to 1. (Before RS has a full
    # year of history it is NULL for everyone; those ties all rank 0.)
    bounds = query(
        conn,
        "SELECT MIN(rs_rank_index) lo, MAX(rs_rank_index) hi FROM fact_stock_metrics "
        "GROUP BY date, etf_ticker "
        "HAVING COUNT(*) > 1 AND COUNT(DISTINCT relative_strength) = COUNT(*)",
    )
    assert len(bounds) > 1000
    assert (bounds["lo"] == 0).all()
    assert (bounds["hi"] == 1).all()

    # Every sector/industry ETF and the thematic one got metrics; the benchmark did not.
    etfs = set(query(conn, "SELECT DISTINCT etf_ticker FROM fact_etf_metrics")["etf_ticker"])
    assert THEMATIC in etfs
    assert BENCHMARK not in etfs
    assert len(etfs) == 7

    top = db.top_stocks(conn, 5)
    assert len(top) == 5
    assert top["percentile"].iloc[0] == 100


@pytest.mark.seeded
def test_second_run_is_a_no_op(make_ctx: MakeContext, conn: Connection) -> None:
    ctx = make_ctx(T2)  # seeded and run up to T2 already
    before = snapshot(conn, (*TABLES, "fact_commons_ohlc", "fact_index_ohlc"))
    run_all(ctx)
    assert_same(before, snapshot(conn, (*TABLES, "fact_commons_ohlc", "fact_index_ohlc")))


def test_incremental_runs_match_a_full_recompute(make_ctx: MakeContext, conn: Connection) -> None:
    """The central invariant: running repeatedly gives the same numbers as one big run."""
    # Guard against the test going vacuous: the incremental runs must load a
    # truncated slice, not all stored history.
    assert lookback_start(T1, SMALL_WINDOW) > SETTINGS_OVERRIDES["history_start"]  # type: ignore[operator]

    seed(make_ctx(T1))
    for day in RUN_DATES:
        run_all(make_ctx(day, window=SMALL_WINDOW))
    incremental = snapshot(conn)

    wipe_facts(conn)
    run_all(make_ctx(T2, window=SMALL_WINDOW))
    assert_same(incremental, snapshot(conn))


def test_incremental_matches_full_across_a_trading_halt(
    make_ctx: MakeContext, conn: Connection
) -> None:
    """A long halt inside the look-back leaves too few bars; the step must notice."""
    market = SyntheticMarket(today=lambda: T2)
    halted = market.etf_holdings[BENCHMARK][3]

    class WithHalt:
        def download(self, tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            frames = SyntheticMarket(today=lambda: end).download(tickers, start, end)
            if halted in frames:
                df = frames[halted]
                frames[halted] = df[(df.index < "2023-09-01") | (df.index > "2024-02-29")]
            return frames

        def recheck(self, *_: object) -> dict[str, pd.DataFrame]:
            return {}

    sources = DataSources(prices=WithHalt(), info=market, holdings=market)
    seed(make_ctx(T1, sources=sources))
    for day in RUN_DATES:
        run_all(make_ctx(day, sources=sources, window=SMALL_WINDOW))
    incremental = snapshot(conn)

    wipe_facts(conn)
    run_all(make_ctx(T2, sources=sources, window=SMALL_WINDOW))
    assert_same(incremental, snapshot(conn))


@pytest.mark.seeded
def test_full_recompute_flag_reproduces_the_same_numbers(
    make_ctx: MakeContext, conn: Connection
) -> None:
    # The database is already seeded and run up to T2.
    before = snapshot(conn)
    with conn.cursor() as cur:  # stale ranks must be redone, not kept
        cur.execute("UPDATE fact_stock_metrics SET rs_rank_overall = 0.5, rs_rank_index = 0.5")
    conn.commit()
    run_all(make_ctx(T2, full_recompute=True))
    assert_same(before, snapshot(conn))


@pytest.mark.seeded
def test_start_date_reprocesses_from_that_date_only(
    make_ctx: MakeContext, conn: Connection
) -> None:
    # The database is already seeded and run up to T2.
    good = snapshot(conn)["fact_stock_metrics"]

    restart = date(2024, 6, 3)
    with conn.cursor() as cur:
        # Damage metrics on both sides of the restart date. (Damaged prices
        # would be caught as a vendor restatement and rebuild everything; see
        # TestReadjustment.)
        cur.execute(
            "UPDATE fact_stock_metrics SET relative_strength = -1 WHERE date >= '2024-05-01'"
        )
    conn.commit()

    run_all(make_ctx(T2, start=restart))
    repaired = snapshot(conn)["fact_stock_metrics"]

    after = pd.to_datetime(repaired["date"]) >= pd.Timestamp(restart)
    good_after = good[pd.to_datetime(good["date"]) >= pd.Timestamp(restart)]
    pd.testing.assert_frame_equal(
        repaired[after].reset_index(drop=True),
        good_after.reset_index(drop=True),
        check_exact=True,
    )
    before = repaired[~after & (pd.to_datetime(repaired["date"]) >= pd.Timestamp("2024-05-01"))]
    assert (before["relative_strength"] == -1).all()


def test_onboarding_with_indices(make_ctx: MakeContext, conn: Connection) -> None:
    run_all(make_ctx(T2, indices=(THEMATIC,)))
    assert scalar(conn, "SELECT etf_name FROM dim_etf WHERE etf_ticker = %s", (THEMATIC,))
    held = scalar(conn, "SELECT COUNT(*) FROM junc_stock_etf WHERE etf_ticker = %s", (THEMATIC,))
    computed = scalar(
        conn,
        "SELECT COUNT(DISTINCT ticker) FROM fact_stock_metrics WHERE etf_ticker = %s",
        (THEMATIC,),
    )
    assert held == computed == len(SyntheticMarket().etf_holdings[THEMATIC])
    # Metadata was filled in and sector links made in the same run.
    assert scalar(conn, "SELECT COUNT(*) FROM dim_stock WHERE sector IS NULL") == 0
    assert scalar(conn, "SELECT COUNT(*) FROM fact_stock_metrics WHERE rs_rank_index IS NULL") == 0


@pytest.mark.seeded
def test_benchmark_etfs_are_never_ranked_as_etfs(make_ctx: MakeContext, conn: Connection) -> None:
    # The database is already seeded and run up to T2.
    from rs_pipeline.steps import compute_etf_rs

    compute_etf_rs.run(make_ctx(T2, indices=(BENCHMARK, THEMATIC)))
    refs = set(query(conn, "SELECT DISTINCT etf_ticker FROM fact_etf_metrics")["etf_ticker"])
    assert BENCHMARK not in refs


def test_compute_rs_skips_etfs_without_prices_and_reports_errors(
    make_ctx: MakeContext, conn: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rs_pipeline.steps import compute_rs

    ctx = make_ctx(T2)
    seed(ctx)  # a fresh database, so compute-rs has every ETF to compute
    db.link_stocks_to_etf(conn, "NOPRICES", ["AAAA"])
    STEPS[0](ctx)  # fetch-prices: NOPRICES has no data anywhere
    assert compute_rs.compute_for_etf(ctx, "NOPRICES", ["AAAA"]) == 0

    real = compute_rs.compute_for_etf

    def broken(ctx: Context, etf: str, tickers: list[str]) -> int:
        if etf == THEMATIC:
            raise RuntimeError("boom")
        return real(ctx, etf, tickers)

    monkeypatch.setattr(compute_rs, "compute_for_etf", broken)
    compute_rs.run(ctx)
    assert ctx.problems == [
        "no new prices for ETFs: NOPRICES",  # from fetch-prices
        f"compute-rs failed for {THEMATIC}: RuntimeError",
    ]
    # The other ETFs were still computed.
    assert scalar(conn, "SELECT COUNT(DISTINCT etf_ticker) FROM fact_stock_metrics") == 7


def test_steps_with_nothing_to_do(make_ctx: MakeContext, conn: Connection) -> None:
    ctx = make_ctx(T2)
    for step in STEPS:
        step(ctx)  # nothing tracked yet: every step returns quietly
    assert ctx.problems == []
    for table in ("fact_commons_ohlc", "fact_stock_metrics", "fact_etf_metrics"):
        assert scalar(conn, f"SELECT COUNT(*) FROM {table}") == 0
