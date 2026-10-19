"""SQL behavior: rankings, upserts, metadata, failures, delisting, re-adjustment."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pandas as pd
import psycopg2
import psycopg2.extensions
import pytest
from psycopg2.extensions import connection as Connection

from rs_pipeline import cli, db
from rs_pipeline.rs import METRIC_COLUMNS
from rs_pipeline.sources import DataSources
from rs_pipeline.sources.synthetic import BENCHMARK, SECTOR_ETFS, SyntheticMarket
from rs_pipeline.steps import fetch_prices
from tests.integration.conftest import SMALL_WINDOW, MakeContext, query, scalar

D1, D2 = date(2024, 1, 2), date(2024, 1, 3)
JUNE_28 = date(2024, 6, 28)


def metrics(rows: list[tuple[date, str, str, float | None]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["date", "ticker", "etf_ticker", "relative_strength"])
    for col in METRIC_COLUMNS:
        if col != "relative_strength":
            df[col] = 0.0
    return df


def etf_metrics(rows: list[tuple[date, str, str, float | None]]) -> pd.DataFrame:
    return metrics(rows).rename(columns={"ticker": "etf_ticker", "etf_ticker": "ref_ticker"})


def ranks(conn: Connection, column: str) -> dict[tuple[date, str, str], float]:
    df = query(conn, f"SELECT date, ticker, etf_ticker, {column} FROM fact_stock_metrics")
    return {(r.date, r.ticker, r.etf_ticker): float(getattr(r, column)) for r in df.itertuples()}


class TestRanking:
    def test_percent_rank_within_etf_with_nulls_first(self, conn: Connection) -> None:
        db.upsert_stock_metrics(
            conn, metrics([(D1, "A", "E", 10.0), (D1, "B", "E", 20.0), (D1, "C", "E", None)])
        )
        assert db.rank_stocks_within_etf(conn) == 3
        assert ranks(conn, "rs_rank_index") == {
            (D1, "C", "E"): 0.0,
            (D1, "A", "E"): 0.5,
            (D1, "B", "E"): 1.0,
        }

    def test_overall_rank_averages_a_stock_across_etfs(self, conn: Connection) -> None:
        db.upsert_stock_metrics(
            conn,
            metrics(
                [
                    (D1, "A", "E1", 90.0),  # A averages 110
                    (D1, "A", "E2", 130.0),
                    (D1, "B", "E1", 100.0),
                    (D1, "C", "E2", 120.0),
                    (D1, "D", "E2", None),  # no RS yet: ranks lowest
                ]
            ),
        )
        db.rank_stocks_overall(conn)
        assert ranks(conn, "rs_rank_overall") == pytest.approx(
            {
                (D1, "D", "E2"): 0.0,
                (D1, "B", "E1"): 1 / 3,
                (D1, "A", "E1"): 2 / 3,
                (D1, "A", "E2"): 2 / 3,
                (D1, "C", "E2"): 1.0,
            }
        )

    def test_only_dirty_partitions_are_recomputed(self, conn: Connection) -> None:
        db.upsert_stock_metrics(
            conn,
            metrics([(d, t, "E", rs) for d in (D1, D2) for t, rs in (("A", 1.0), ("B", 2.0))]),
        )
        db.rank_stocks_within_etf(conn)
        with conn.cursor() as cur:  # tamper with a clean partition
            cur.execute("UPDATE fact_stock_metrics SET rs_rank_index = 0.25 WHERE date = %s", (D1,))
        conn.commit()

        # Rewriting one D2 row makes only D2 dirty again.
        db.upsert_stock_metrics(conn, metrics([(D2, "A", "E", 3.0)]))
        assert db.rank_stocks_within_etf(conn) == 2  # both D2 rows changed
        result = ranks(conn, "rs_rank_index")
        assert result[(D1, "A", "E")] == 0.25
        assert result[(D2, "A", "E")] == 1.0

        # A forced range re-ranks clean dates too.
        db.rank_stocks_within_etf(conn, start=D1, end=D1)
        assert ranks(conn, "rs_rank_index")[(D1, "A", "E")] == 0.0

    def test_unchanged_ranks_are_not_rewritten(self, conn: Connection) -> None:
        # Thirds don't survive NUMERIC(30,10) exactly; the comparison must round.
        rows = [(D1, t, "E", rs) for t, rs in (("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", 4.0))]
        db.upsert_stock_metrics(conn, metrics(rows))
        db.upsert_etf_metrics(conn, etf_metrics(rows))
        db.rank_stocks_within_etf(conn)
        db.rank_stocks_overall(conn)
        db.rank_etfs(conn, "E")
        assert db.rank_stocks_within_etf(conn, start=D1, end=D1) == 0
        assert db.rank_stocks_overall(conn, start=D1, end=D1) == 0
        assert db.rank_etfs(conn, "E", start=D1, end=D1) == 0

    def test_etf_filter(self, conn: Connection) -> None:
        db.upsert_stock_metrics(conn, metrics([(D1, "A", "E1", 1.0), (D1, "A", "E2", 1.0)]))
        db.rank_stocks_within_etf(conn, etfs=["E1"])
        unranked = query(
            conn, "SELECT etf_ticker FROM fact_stock_metrics WHERE rs_rank_index IS NULL"
        )
        assert unranked["etf_ticker"].tolist() == ["E2"]

    def test_etf_ranks_with_a_missing_rs(self, conn: Connection) -> None:
        db.upsert_etf_metrics(
            conn,
            etf_metrics(
                [
                    (D1, "X", "REF", 1.0),
                    (D1, "Y", "REF", 2.0),
                    (D1, "N", "REF", None),
                    (D1, "Z", "OTHER", 3.0),
                ]
            ),
        )
        db.rank_etfs(conn, "REF")
        df = query(
            conn,
            "SELECT etf_ticker, rs_rank_index, rs_rank_overall FROM fact_etf_metrics ORDER BY 1",
        )
        assert df["etf_ticker"].tolist() == ["N", "X", "Y", "Z"]
        # Ranked among the ETFs measured against REF; Z (another benchmark) is untouched.
        assert df["rs_rank_index"].tolist()[:3] == df["rs_rank_overall"].tolist()[:3]
        assert df["rs_rank_index"].astype(float).tolist()[:3] == [0.0, 0.5, 1.0]
        assert df["rs_rank_index"].isna().tolist()[3]

    def test_etf_row_missing_only_its_overall_rank_is_redone(self, conn: Connection) -> None:
        db.upsert_etf_metrics(conn, etf_metrics([(D1, "X", "REF", 1.0), (D1, "Y", "REF", 2.0)]))
        db.rank_etfs(conn, "REF")
        with conn.cursor() as cur:
            cur.execute("UPDATE fact_etf_metrics SET rs_rank_overall = NULL WHERE etf_ticker = 'Y'")
        conn.commit()
        assert db.rank_etfs(conn, "REF") == 1
        assert (
            scalar(conn, "SELECT rs_rank_overall FROM fact_etf_metrics WHERE etf_ticker='Y'") == 1
        )


class TestUpserts:
    def test_metric_upsert_overwrites_and_clears_both_ranks(self, conn: Connection) -> None:
        db.upsert_stock_metrics(conn, metrics([(D1, "A", "E", 1.0), (D1, "B", "E", 2.0)]))
        db.rank_stocks_within_etf(conn)
        db.rank_stocks_overall(conn)
        db.upsert_stock_metrics(conn, metrics([(D1, "A", "E", 5.0)]))
        df = query(
            conn,
            "SELECT ticker, relative_strength, rs_rank_index, rs_rank_overall "
            "FROM fact_stock_metrics ORDER BY 1",
        )
        assert df["relative_strength"].astype(float).tolist() == [5.0, 2.0]
        assert df["rs_rank_index"].isna().tolist() == [True, False]
        assert df["rs_rank_overall"].isna().tolist() == [True, False]

    def test_ranks_survive_changes_that_do_not_touch_rs(self, conn: Connection) -> None:
        rows = metrics([(D1, "A", "E", 1.0), (D1, "B", "E", 2.0)])
        db.upsert_stock_metrics(conn, rows)
        db.rank_stocks_within_etf(conn)
        db.rank_stocks_overall(conn)
        rows["naive_relative_strength"] = 9.0  # e.g. a dividend rescaled the price ratio
        assert db.upsert_stock_metrics(conn, rows) == 2
        assert db.upsert_stock_metrics(conn, rows) == 0  # identical rows aren't rewritten
        df = query(conn, "SELECT rs_rank_index, rs_rank_overall FROM fact_stock_metrics")
        assert df.notna().all().all()

    def test_nan_and_infinity_are_stored_as_null(self, conn: Connection) -> None:
        rows = metrics([(D1, "A", "E", float("nan"))])
        rows["rs_z_score"] = float("inf")
        db.upsert_stock_metrics(conn, rows)
        row = query(conn, "SELECT relative_strength, rs_z_score FROM fact_stock_metrics").iloc[0]
        assert row.isna().all()

    def test_ohlc_round_trip(self, conn: Connection) -> None:
        frame = pd.DataFrame(
            {"open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [3e9]},
            index=pd.DatetimeIndex([D1], name="date"),
        )
        db.upsert_ohlc(conn, db.STOCK_OHLC_TABLE, db.ohlc_records("A", frame))
        frame["close"] = 1.75
        db.upsert_ohlc(conn, db.STOCK_OHLC_TABLE, db.ohlc_records("A", frame))
        loaded = db.fetch_ohlc(conn, db.STOCK_OHLC_TABLE, ["A", "B"], D1, D2)
        assert list(loaded) == ["A"]
        assert loaded["A"]["close"].iloc[0] == 1.75
        assert loaded["A"]["volume"].iloc[0] == 3e9  # BIGINT: beyond int4
        assert db.latest_ohlc_dates(conn, db.STOCK_OHLC_TABLE) == {"A": D1}
        assert db.fetch_ohlc(conn, db.STOCK_OHLC_TABLE, [], D1, D2) == {}

    def test_missing_values_are_stored_as_null(self, conn: Connection) -> None:
        frame = pd.DataFrame(
            {"open": [float("nan")], "high": [2.0], "low": [0.5], "close": [1.5],
             "volume": [float("nan")]},
            index=pd.DatetimeIndex([D1], name="date"),
        )  # fmt: skip
        db.upsert_ohlc(conn, db.STOCK_OHLC_TABLE, db.ohlc_records("A", frame))
        row = query(conn, "SELECT open, volume, close FROM fact_commons_ohlc").iloc[0]
        assert pd.isna(row.open)
        assert pd.isna(row.volume)
        assert float(row.close) == 1.5

    def test_rejects_unknown_ohlc_table(self, conn: Connection) -> None:
        with pytest.raises(ValueError, match="not an OHLC table"):
            db.upsert_ohlc(conn, "dim_stock; DROP TABLE x", [(D1, "A", 1, 1, 1, 1, 1)])


class TestConnectionPool:
    def test_connections_dropped_by_the_server_are_never_reused(
        self, pool: db.ConnectionPool, db_settings: object
    ) -> None:
        with pool.connection() as conn:
            pid = conn.get_backend_pid()
        # The server drops that session, e.g. after a long vendor download.
        admin = psycopg2.connect(**db_settings.as_kwargs())  # type: ignore[attr-defined]
        with admin, admin.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
        admin.close()
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
            assert conn.get_backend_pid() != pid

    def test_an_error_rolls_back(self, pool: db.ConnectionPool) -> None:
        def insert_then_fail() -> None:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO dim_etf (etf_ticker) VALUES ('ROLLEDBACK')")
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            insert_then_fail()
        with pool.connection() as conn:
            assert scalar(conn, "SELECT COUNT(*) FROM dim_etf") == 0

    def test_reads_do_not_leave_a_transaction_open(self, conn: Connection) -> None:
        db.fetch_holdings(conn)
        db.latest_ohlc_dates(conn, db.STOCK_OHLC_TABLE)
        idle = psycopg2.extensions.TRANSACTION_STATUS_IDLE
        assert conn.get_transaction_status() == idle


class TestUniverse:
    def test_stock_metadata_and_sector_links(self, conn: Connection) -> None:
        db.replace_sector_etfs(conn, {"Tech": "XT"}, {"Chips": "XC"})
        db.link_stocks_to_etf(conn, "IDX", ["A", "B"])
        assert db.tickers_missing_info(conn, ["A", "B", "NEW"]) == ["A", "B", "NEW"]

        db.upsert_stocks(conn, [("A", "Acme", "Tech", "Chips", "NMS")])
        db.upsert_stocks(conn, [("A", None, None, None, "NYQ")])  # NULLs keep known values
        row = query(conn, "SELECT * FROM dim_stock WHERE ticker = 'A'").iloc[0]
        assert (row.company_name, row.sector, row.exchange) == ("Acme", "Tech", "NYQ")
        assert db.tickers_missing_info(conn, ["A", "B"]) == ["B"]

        assert db.link_stocks_by_sector_and_industry(conn) == 2
        assert db.link_stocks_by_sector_and_industry(conn) == 0
        assert db.fetch_holdings(conn) == {"IDX": ["A", "B"], "XC": ["A"], "XT": ["A"]}
        assert db.fetch_holdings(conn, ["XT"]) == {"XT": ["A"]}

    def test_delisted_stocks_are_never_linked_again(self, conn: Connection) -> None:
        db.replace_sector_etfs(conn, {"Tech": "XT"}, {})
        db.link_stocks_to_etf(conn, "IDX", ["A"])
        db.upsert_stocks(conn, [("A", "Acme", "Tech", None, None)])
        db.link_stocks_by_sector_and_industry(conn)
        assert db.mark_delisted(conn, "A", D1) == ["IDX", "XT"]

        db.link_stocks_by_sector_and_industry(conn)
        db.link_stocks_to_etf(conn, "IDX", ["A"])
        assert db.fetch_holdings(conn) == {}

    def test_replacing_the_sector_map_moves_individual_links(self, conn: Connection) -> None:
        # Tech moves off XT, but XT is still mapped (by an industry).
        db.replace_sector_etfs(conn, {"Tech": "XT"}, {})
        db.link_stocks_to_etf(conn, "IDX", ["CHIP", "SOFT"])
        db.upsert_etf(conn, "IDX", None)
        db.upsert_stocks(
            conn, [("CHIP", None, "Tech", "Semis", None), ("SOFT", None, "Tech", "Software", None)]
        )
        db.link_stocks_by_sector_and_industry(conn)
        db.replace_sector_etfs(conn, {"Tech": "XT2"}, {"Semis": "XT"})
        db.link_stocks_by_sector_and_industry(conn)
        holdings = db.fetch_holdings(conn)
        assert holdings["XT"] == ["CHIP"]  # SOFT's link to XT is gone
        assert holdings["XT2"] == ["CHIP", "SOFT"]

    def test_replacing_the_sector_map_unlinks_dropped_etfs(self, conn: Connection) -> None:
        db.replace_sector_etfs(conn, {"Tech": "XT", "Oil": "XO"}, {})
        db.upsert_etf(conn, "IDX", None)
        db.link_stocks_to_etf(conn, "IDX", ["A", "B"])
        db.upsert_stocks(conn, [("A", None, "Tech", None, None), ("B", None, "Oil", None, None)])
        db.link_stocks_by_sector_and_industry(conn)
        assert db.replace_sector_etfs(conn, {"Tech": "XT"}, {}) == 1  # B's link to XO
        assert db.fetch_holdings(conn) == {"IDX": ["A", "B"], "XT": ["A"]}
        assert query(conn, "SELECT name FROM dim_sector")["name"].tolist() == ["Tech"]

    def test_rankable_etfs_exclude_benchmarks(self, conn: Connection) -> None:
        db.replace_sector_etfs(conn, {"Tech": "XT"}, {"Chips": "XC"})
        db.upsert_etf(conn, "SPY", None)
        db.upsert_etf(conn, "THEME", None)
        db.upsert_etf(conn, "THEME", "Theme ETF")
        assert db.fetch_rankable_etfs(conn, ["SPY"]) == ["THEME", "XC", "XT"]
        assert (
            scalar(conn, "SELECT etf_name FROM dim_etf WHERE etf_ticker = 'THEME'") == "Theme ETF"
        )


class _Prices:
    """A price source built from functions, for scripted vendor behavior."""

    def __init__(
        self,
        download: Callable[[list[str], date, date], dict[str, pd.DataFrame]],
        recheck: Callable[[list[str], date, date], dict[str, pd.DataFrame]] | None = None,
    ) -> None:
        self._download = download
        self._recheck = recheck or (lambda *_: {})

    def download(self, tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        return self._download(list(tickers), start, end)

    def recheck(self, tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        return self._recheck(list(tickers), start, end)


def _with_prices(prices: _Prices) -> DataSources:
    market = SyntheticMarket()
    return DataSources(prices=prices, info=market, holdings=market)


class TestFailuresAndDelisting:
    def test_counts_once_per_day(self, conn: Connection) -> None:
        assert db.record_fetch_failures(conn, ["A"], D1) == {"A": 1}
        assert db.record_fetch_failures(conn, ["A"], D1) == {"A": 1}
        assert db.record_fetch_failures(conn, ["A", "B"], D2) == {"A": 2, "B": 1}
        db.clear_fetch_failures(conn, ["A"])
        assert db.record_fetch_failures(conn, ["A"], D2) == {"A": 1}

    def test_dead_ticker_is_delisted_after_three_failing_days(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        db.link_stocks_to_etf(conn, BENCHMARK, ["GONE", *market.etf_holdings[BENCHMARK][:10]])
        for day in (date(2024, 6, 26), date(2024, 6, 27), JUNE_28):
            fetch_prices.run(make_ctx(day))

        assert db.fetch_delisted(conn) == {"GONE"}
        assert scalar(conn, "SELECT original_etfs FROM delisted_tickers") == BENCHMARK
        assert "GONE" not in db.fetch_holdings(conn)[BENCHMARK]
        assert scalar(conn, "SELECT COUNT(*) FROM dim_stock WHERE ticker = 'GONE'") == 1  # kept
        assert scalar(conn, "SELECT COUNT(*) FROM ticker_fetch_failures") == 0

    def test_a_delisted_stock_with_a_sector_stays_unlinked(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        tickers = market.etf_holdings[BENCHMARK][:10]
        victim = tickers[0]
        cli._seed_demo(make_ctx(JUNE_28))

        def dead_victim(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            return {k: v for k, v in market.download(t, start, end).items() if k != victim}

        sources = _with_prices(_Prices(dead_victim))
        for day in (date(2024, 6, 26), date(2024, 6, 27), JUNE_28, date(2024, 7, 1)):
            fetch_prices.run(make_ctx(day, sources=sources))
        assert victim in db.fetch_delisted(conn)
        linked = scalar(conn, "SELECT COUNT(*) FROM junc_stock_etf WHERE ticker = %s", (victim,))
        assert linked == 0

    @pytest.mark.parametrize("missing", [4, 10])
    def test_mass_misses_are_not_counted(
        self, make_ctx: MakeContext, conn: Connection, missing: int
    ) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        tickers = market.etf_holdings[BENCHMARK][:10]
        db.link_stocks_to_etf(conn, BENCHMARK, tickers)

        def partial(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            return {
                k: v
                for k, v in market.download(t, start, end).items()
                if k not in tickers[:missing]
            }

        fetch_prices.run(make_ctx(JUNE_28, sources=_with_prices(_Prices(partial))))
        assert scalar(conn, "SELECT COUNT(*) FROM ticker_fetch_failures") == 0

    def test_a_ticker_served_only_old_bars_is_still_delisted(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        """A vendor may keep returning a dead ticker's old bars; that isn't success."""
        market = SyntheticMarket(today=lambda: JUNE_28)
        tickers = market.etf_holdings[BENCHMARK][:10]
        dead = tickers[0]
        db.link_stocks_to_etf(conn, BENCHMARK, tickers)
        fetch_prices.run(make_ctx(date(2024, 6, 21)))  # history up to the 21st

        def dead_after_21st(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            frames = SyntheticMarket(today=lambda: end).download(t, start, end)
            if dead in frames:
                frames[dead] = frames[dead].loc[:"2024-06-21"]
            return frames

        sources = _with_prices(_Prices(dead_after_21st, dead_after_21st))
        for day in (date(2024, 6, 24), date(2024, 6, 25), date(2024, 6, 26)):
            ctx = make_ctx(day, sources=sources)
            fetch_prices.run(ctx)
        assert db.fetch_delisted(conn) == {dead}

    def test_a_market_holiday_is_not_a_problem(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        db.link_stocks_to_etf(conn, BENCHMARK, market.etf_holdings[BENCHMARK][:10])
        fetch_prices.run(make_ctx(date(2024, 6, 21)))

        def closed(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            frames = SyntheticMarket(today=lambda: end).download(t, start, end)
            return {k: v.loc[:"2024-06-21"] for k, v in frames.items()}  # nothing new

        ctx = make_ctx(date(2024, 6, 24), sources=_with_prices(_Prices(closed, closed)))
        fetch_prices.run(ctx)
        assert ctx.problems == []
        assert scalar(conn, "SELECT COUNT(*) FROM ticker_fetch_failures") == 0

    def test_a_total_vendor_outage_is_reported(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        db.link_stocks_to_etf(conn, BENCHMARK, market.etf_holdings[BENCHMARK][:10])
        fetch_prices.run(make_ctx(date(2024, 6, 21)))
        ctx = make_ctx(date(2024, 6, 24), sources=_with_prices(_Prices(lambda *_: {})))
        fetch_prices.run(ctx)
        assert ctx.problems == ["the price vendor returned nothing for any ticker; is it down?"]
        assert scalar(conn, "SELECT COUNT(*) FROM ticker_fetch_failures") == 0

    def test_an_etf_that_stops_updating_is_reported(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        db.link_stocks_to_etf(conn, BENCHMARK, market.etf_holdings[BENCHMARK][:10])
        fetch_prices.run(make_ctx(date(2024, 6, 21)))

        def benchmark_frozen(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            frames = SyntheticMarket(today=lambda: end).download(t, start, end)
            if BENCHMARK in frames:
                frames[BENCHMARK] = frames[BENCHMARK].loc[:"2024-06-21"]
            return frames

        ctx = make_ctx(date(2024, 6, 24), sources=_with_prices(_Prices(benchmark_frozen)))
        fetch_prices.run(ctx)
        assert ctx.problems == [f"no new prices for ETFs: {BENCHMARK}"]

    def test_recheck_recovers_missed_tickers(self, make_ctx: MakeContext, conn: Connection) -> None:
        market = SyntheticMarket(today=lambda: JUNE_28)
        tickers = market.etf_holdings[BENCHMARK][:10]
        flaky = tickers[1]
        db.link_stocks_to_etf(conn, BENCHMARK, tickers)

        def drops_one(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            return market.download([x for x in t if x != flaky], start, end)

        sources = _with_prices(_Prices(drops_one, market.download))
        fetch_prices.run(make_ctx(JUNE_28, sources=sources))
        assert set(db.latest_ohlc_dates(conn, db.STOCK_OHLC_TABLE)) == set(tickers)
        assert scalar(conn, "SELECT COUNT(*) FROM ticker_fetch_failures") == 0


def _restated(
    victim: str, split_day: date, *, factor: float = 0.5, drop: date | None = None
) -> Callable[[list[str], date, date], dict[str, pd.DataFrame]]:
    """The synthetic market with *victim*'s prices before *split_day* scaled by
    *factor*, and optionally the bar on *drop* gone from its history."""

    def download(tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
        frames = SyntheticMarket(today=lambda: end).download(tickers, start, end)
        if victim in frames:
            df = frames[victim].copy()
            df.loc[df.index < pd.Timestamp(split_day), ["open", "high", "low", "close"]] *= factor
            if drop is not None:
                df = df.drop(index=pd.Timestamp(drop), errors="ignore")
            frames[victim] = df
        return frames

    return download


class TestReadjustment:
    """After a split the vendor rescales all earlier prices; we must follow exactly."""

    BEFORE, SPLIT, AFTER = date(2024, 6, 13), date(2024, 6, 14), JUNE_28

    def _run_all(self, make_ctx: MakeContext, day: date, sources: DataSources | None) -> None:
        # A short window, so incremental runs load a truncated look-back.
        ctx = make_ctx(day, sources=sources, window=SMALL_WINDOW)
        for step, _ in cli.STEPS.values():
            step(ctx)

    def _matches_fresh_run(
        self, make_ctx: MakeContext, conn: Connection, sources: DataSources
    ) -> None:
        tables = ("fact_commons_ohlc", "fact_index_ohlc", "fact_stock_metrics", "fact_etf_metrics")
        repaired = {t: query(conn, f"SELECT * FROM {t} ORDER BY 1, 2, 3") for t in tables}
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {', '.join(tables)}")
        conn.commit()
        self._run_all(make_ctx, self.AFTER, sources)
        for t in tables:
            fresh = query(conn, f"SELECT * FROM {t} ORDER BY 1, 2, 3")
            pd.testing.assert_frame_equal(repaired[t], fresh, check_exact=True)

    @pytest.mark.parametrize(
        "victim",
        [
            pytest.param(lambda m: m.etf_holdings[BENCHMARK][0], id="stock"),
            pytest.param(lambda m: SECTOR_ETFS["Tech"], id="sector-etf"),
            pytest.param(lambda m: BENCHMARK, id="benchmark"),
        ],
    )
    def test_restated_history_is_repaired(
        self, make_ctx: MakeContext, conn: Connection, victim: Callable[[SyntheticMarket], str]
    ) -> None:
        ticker = victim(SyntheticMarket())
        sources = _with_prices(_Prices(_restated(ticker, self.SPLIT)))
        cli._seed_demo(make_ctx(self.BEFORE))
        self._run_all(make_ctx, self.BEFORE, None)  # history stored before the split
        self._run_all(make_ctx, self.AFTER, sources)  # the vendor has now restated it

        table = (
            db.INDEX_OHLC_TABLE
            if ticker in (*SECTOR_ETFS.values(), BENCHMARK)
            else db.STOCK_OHLC_TABLE
        )
        stored = db.fetch_ohlc(conn, table, [ticker], date(2024, 6, 3), date(2024, 6, 3))[ticker]
        original = SyntheticMarket().download([ticker], date(2024, 6, 3), date(2024, 6, 3))[ticker]
        assert stored["close"].iloc[0] == pytest.approx(original["close"].iloc[0] / 2, rel=1e-6)
        assert scalar(conn, "SELECT COUNT(*) FROM metrics_rebuild") == 0  # queue drained
        self._matches_fresh_run(make_ctx, conn, sources)

    def test_a_restatement_of_older_bars_only_is_caught(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        """The vendor applied a split late: the latest stored bar matches, older ones don't."""
        ticker = SyntheticMarket().etf_holdings[BENCHMARK][0]
        sources = _with_prices(_Prices(_restated(ticker, date(2024, 6, 13))))
        cli._seed_demo(make_ctx(self.BEFORE))
        self._run_all(make_ctx, self.BEFORE, None)  # stored through the 13th
        self._run_all(make_ctx, self.AFTER, sources)
        assert scalar(conn, "SELECT COUNT(*) FROM metrics_rebuild") == 0
        self._matches_fresh_run(make_ctx, conn, sources)

    def test_a_restatement_that_drops_a_bar_is_matched_exactly(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        ticker = SyntheticMarket().etf_holdings[BENCHMARK][0]
        sources = _with_prices(_Prices(_restated(ticker, self.SPLIT, drop=date(2024, 5, 1))))
        cli._seed_demo(make_ctx(self.BEFORE))
        self._run_all(make_ctx, self.BEFORE, None)
        self._run_all(make_ctx, self.AFTER, sources)
        gone = scalar(
            conn,
            "SELECT COUNT(*) FROM fact_stock_metrics WHERE ticker = %s AND date = '2024-05-01'",
            (ticker,),
        )
        assert gone == 0
        self._matches_fresh_run(make_ctx, conn, sources)

    def test_a_queued_rebuild_survives_a_run_over_an_earlier_week(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        from rs_pipeline.steps import compute_rs

        cli._seed_demo(make_ctx(self.AFTER))
        self._run_all(make_ctx, self.AFTER, None)
        ticker = SyntheticMarket().etf_holdings[BENCHMARK][0]
        db.queue_rebuild(conn, stocks=[ticker], etfs=[], benchmark=None)
        before = scalar(
            conn, "SELECT COUNT(*) FROM fact_stock_metrics WHERE ticker = %s", (ticker,)
        )
        compute_rs.run(make_ctx(date(2024, 5, 3), start=date(2024, 4, 29), window=SMALL_WINDOW))
        after = scalar(conn, "SELECT COUNT(*) FROM fact_stock_metrics WHERE ticker = %s", (ticker,))
        assert after == before  # nothing past the run's end was deleted
        assert scalar(conn, "SELECT COUNT(*) FROM metrics_rebuild") > 0  # still queued

    def test_restatement_seen_only_on_recheck_is_repaired(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        ticker = SyntheticMarket().etf_holdings[BENCHMARK][0]
        restated = _restated(ticker, self.SPLIT)

        def bulk_misses_it(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            return {k: v for k, v in restated(t, start, end).items() if k != ticker}

        cli._seed_demo(make_ctx(self.BEFORE))
        self._run_all(make_ctx, self.BEFORE, None)
        self._run_all(make_ctx, self.AFTER, _with_prices(_Prices(bulk_misses_it, restated)))
        self._matches_fresh_run(make_ctx, conn, _with_prices(_Prices(restated)))

    def test_failed_repair_converges_on_the_next_run(
        self, make_ctx: MakeContext, conn: Connection
    ) -> None:
        ticker = SyntheticMarket().etf_holdings[BENCHMARK][0]
        restated = _restated(ticker, self.SPLIT)

        def history_unavailable(t: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
            # Recent bars come back, but the full-history re-download fails.
            return {} if start < date(2024, 6, 1) else restated(t, start, end)

        cli._seed_demo(make_ctx(self.BEFORE))
        self._run_all(make_ctx, self.BEFORE, None)
        self._run_all(make_ctx, date(2024, 6, 27), _with_prices(_Prices(history_unavailable)))
        self._run_all(make_ctx, self.AFTER, _with_prices(_Prices(restated)))
        self._matches_fresh_run(make_ctx, conn, _with_prices(_Prices(restated)))


class TestRebuildQueue:
    def test_queue_covers_current_links(self, conn: Connection) -> None:
        db.link_stocks_to_etf(conn, "E1", ["A", "B"])
        db.link_stocks_to_etf(conn, "E2", ["A"])
        db.upsert_etf_metrics(conn, etf_metrics([(D1, "E1", "REF", 1.0), (D1, "E2", "REF", 2.0)]))
        assert db.queue_rebuild(conn, stocks=["A"], etfs=[], benchmark=None) == 2
        assert db.pending_rebuilds(conn, "stock", "E1") == {"A"}
        assert db.queue_rebuild(conn, stocks=[], etfs=["E1"], benchmark=None) == 2  # B, and E1
        assert db.pending_rebuilds(conn, "stock", "E1") == {"A", "B"}
        assert db.pending_rebuilds(conn, "etf", "REF") == {"E1"}
        assert db.queue_rebuild(conn, stocks=[], etfs=[], benchmark="REF") == 1  # E2
        db.finish_rebuilds(conn, "stock", "E1", ["A"])
        assert db.pending_rebuilds(conn, "stock", "E1") == {"B"}

    def test_prune_marks_every_overall_rank_on_the_dates(self, conn: Connection) -> None:
        # A in E1 and E2; B only in E1; C only in E2. E1 loses D2 entirely.
        pairs = (("A", "E1", 3.0), ("A", "E2", 3.0), ("B", "E1", 1.0), ("C", "E2", 2.0))
        rows = [(d, t, e, rs) for d in (D1, D2) for t, e, rs in pairs]
        db.upsert_stock_metrics(conn, metrics(rows))
        db.rank_stocks_within_etf(conn)
        db.rank_stocks_overall(conn)
        keys = ("ticker", "etf_ticker")
        db.prune_metrics(conn, "fact_stock_metrics", keys, ("A", "E1"), [D1])
        db.prune_metrics(conn, "fact_stock_metrics", keys, ("B", "E1"), [D1])
        db.rank_stocks_within_etf(conn)
        db.rank_stocks_overall(conn)
        on_d2 = query(
            conn,
            "SELECT ticker, rs_rank_overall FROM fact_stock_metrics WHERE date = %s ORDER BY 1",
            (D2,),
        )
        assert on_d2.to_numpy().tolist() == [["A", 1], ["C", 0]]  # as a fresh ranking gives

    def test_prune_marks_the_affected_dates_for_ranking(self, conn: Connection) -> None:
        db.upsert_stock_metrics(
            conn, metrics([(d, t, "E", 1.0) for d in (D1, D2) for t in ("A", "B")])
        )
        db.rank_stocks_within_etf(conn)
        db.rank_stocks_overall(conn)
        assert (
            db.prune_metrics(conn, "fact_stock_metrics", ("ticker", "etf_ticker"), ("A", "E"), [D1])
            == 1
        )
        left = query(
            conn, "SELECT date, ticker, rs_rank_index FROM fact_stock_metrics ORDER BY 1, 2"
        )
        assert left[["date", "ticker"]].to_numpy().tolist() == [[D1, "A"], [D1, "B"], [D2, "B"]]
        assert left["rs_rank_index"].isna().tolist() == [False, False, True]

    def test_replace_ohlc_drops_bars_the_vendor_no_longer_has(self, conn: Connection) -> None:
        days = pd.DatetimeIndex([D1, D2, date(2024, 1, 4)], name="date")
        bars = pd.DataFrame(
            {"open": 1.0, "high": 1.0, "low": 1.0, "close": [1.0, 2.0, 3.0], "volume": 1.0},
            index=days,
        )
        db.upsert_ohlc(conn, db.STOCK_OHLC_TABLE, db.ohlc_records("A", bars))
        db.replace_ohlc(conn, db.STOCK_OHLC_TABLE, "A", bars.drop(index=days[1]) / 2)
        stored = db.fetch_ohlc(conn, db.STOCK_OHLC_TABLE, ["A"], D1, date(2024, 1, 4))["A"]
        assert stored["close"].tolist() == [0.5, 1.5]


class TestStateAndLocking:
    def test_window_changes_need_a_whole_table_recompute(self, conn: Connection) -> None:
        from rs_pipeline.pipeline import RunOptions, check_window, finish_window

        table = "fact_stock_metrics"
        check_window(conn, table, RunOptions(end=D2, window=252))
        assert db.stored_window(conn, table) == "252"
        with pytest.raises(ValueError, match="--full-recompute"):
            check_window(conn, table, RunOptions(end=D2, window=20))
        with pytest.raises(ValueError, match="without --indices"):
            check_window(
                conn, table, RunOptions(end=D2, window=20, full_recompute=True, indices=("X",))
            )

        full = RunOptions(end=D2, window=20, full_recompute=True)
        check_window(conn, table, full)
        assert db.stored_window(conn, table) == "20?"  # in transition until it finishes
        for window in (20, 252):  # an unfinished change blocks incremental runs either way
            with pytest.raises(ValueError, match="unfinished"):
                check_window(conn, table, RunOptions(end=D2, window=window))
        finish_window(conn, table, full)
        assert db.stored_window(conn, table) == "20"
        check_window(conn, table, RunOptions(end=D2, window=20))
        assert db.stored_window(conn, "fact_etf_metrics") is None  # tracked per table

    def test_only_one_run_at_a_time(self, db_settings: object) -> None:
        with db.run_lock(db_settings) as first:  # type: ignore[arg-type]
            assert first
            with db.run_lock(db_settings) as second:  # type: ignore[arg-type]
                assert not second
        with db.run_lock(db_settings) as again:  # type: ignore[arg-type]
            assert again


def test_onboarding_an_etf_later_matches_a_full_run(
    make_ctx: MakeContext, conn: Connection
) -> None:
    """A new ETF in a database that already has metrics loads its full history."""
    from rs_pipeline.sources.synthetic import THEMATIC

    def run(day: date, **opts: object) -> None:
        ctx = make_ctx(day, window=SMALL_WINDOW, **opts)
        for step, _ in cli.STEPS.values():
            step(ctx)

    db.replace_sector_etfs(conn, {}, {})
    run(date(2024, 3, 28), indices=(BENCHMARK,))
    run(JUNE_28)
    run(JUNE_28, indices=(THEMATIC,))  # onboarded after months of history exist
    run(JUNE_28)
    tables = ("fact_stock_metrics", "fact_etf_metrics")
    incremental = {t: query(conn, f"SELECT * FROM {t} ORDER BY 1, 2, 3") for t in tables}

    with conn.cursor() as cur:
        cur.execute("TRUNCATE fact_stock_metrics, fact_etf_metrics")
    conn.commit()
    run(JUNE_28)
    for t in tables:
        pd.testing.assert_frame_equal(
            incremental[t], query(conn, f"SELECT * FROM {t} ORDER BY 1, 2, 3"), check_exact=True
        )
