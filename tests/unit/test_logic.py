"""Pure step logic: planning, re-adjustment detection, cutoffs, sessions, retry, CLI parsing."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import pandas as pd
import psycopg2
import pytest

from rs_pipeline import cli, config, db
from rs_pipeline.config import DbSettings, Settings
from rs_pipeline.pipeline import RunOptions, last_completed_session, lookback_start
from rs_pipeline.retry import is_rate_limit, with_backoff
from rs_pipeline.rs import required_history
from rs_pipeline.steps._incremental import cutoffs, has_enough_history, new_metrics
from rs_pipeline.steps.fetch_prices import (
    OVERLAP,
    _counts_as_failure,
    find_restated,
    plan_downloads,
)

HISTORY = date(2020, 1, 1)
END = date(2024, 6, 28)


class TestPlanDownloads:
    def test_new_ticker_gets_full_history(self) -> None:
        assert plan_downloads(["NEW"], {}, history_start=HISTORY, refresh_from=None, end=END) == {
            HISTORY: ["NEW"]
        }

    def test_known_ticker_resumes_with_an_overlap(self) -> None:
        # The last two weeks are fetched again to detect restated history.
        plan = plan_downloads(
            ["A", "B"],
            {"A": date(2024, 6, 20), "B": date(2024, 6, 20)},
            history_start=HISTORY,
            refresh_from=None,
            end=END,
        )
        assert plan == {date(2024, 6, 6): ["A", "B"]}  # two weeks back
        assert date(2024, 6, 20) - OVERLAP == date(2024, 6, 6)

    def test_up_to_date_ticker_is_skipped(self) -> None:
        plan = plan_downloads(["A"], {"A": END}, history_start=HISTORY, refresh_from=None, end=END)
        assert plan == {}

    def test_refresh_reaches_back_but_not_for_new_tickers(self) -> None:
        refresh = date(2024, 6, 1)
        plan = plan_downloads(
            ["OLD", "NEW"], {"OLD": END}, history_start=HISTORY, refresh_from=refresh, end=END
        )
        assert plan == {refresh: ["OLD"], HISTORY: ["NEW"]}


class TestFindRestated:
    DAYS = (date(2024, 6, 19), date(2024, 6, 20), date(2024, 6, 21))
    LATEST: ClassVar[dict[str, date]] = {"A": date(2024, 6, 21)}  # the last stored bar

    def _frames(self, *closes: float) -> dict[str, pd.DataFrame]:
        idx = pd.DatetimeIndex(self.DAYS, name="date")
        return {"A": pd.DataFrame({"close": closes}, index=idx)}

    @pytest.mark.parametrize(
        ("new_close", "restated"),
        [
            (100.0, False),
            (100.0 * (1 + config.READJUSTMENT_TOLERANCE * 0.9), False),  # rounding noise
            (100.0 * (1 - config.READJUSTMENT_TOLERANCE * 1.1), True),  # just past tolerance
            (99.75, True),  # a 0.25% quarterly dividend
            (50.0, True),  # a 2:1 split
        ],
    )
    def test_threshold(self, new_close: float, restated: bool) -> None:
        stored = {"A": {self.DAYS[0]: 100.0, self.DAYS[1]: 100.0}}
        result = find_restated(self._frames(new_close, new_close, 101.0), stored, self.LATEST)
        assert result == (["A"] if restated else [])

    def test_a_late_restatement_of_an_older_bar_is_caught(self) -> None:
        # The vendor applied a split to the oldest day only.
        stored = {"A": {self.DAYS[0]: 100.0, self.DAYS[1]: 10.0}}
        assert find_restated(self._frames(10.0, 10.0, 10.1), stored, self.LATEST) == ["A"]

    def test_a_revised_latest_close_is_not_a_restatement(self) -> None:
        # Vendors revise the most recent close by a cent; that bar isn't compared.
        stored = {"A": {self.DAYS[1]: 10.00, self.DAYS[2]: 10.00}}
        assert find_restated(self._frames(9.0, 10.0, 10.01), stored, self.LATEST) == []

    def test_a_day_the_vendor_no_longer_has_is_a_restatement(self) -> None:
        frames = {"A": self._frames(10.0, 10.0, 10.0)["A"].drop(index=pd.Timestamp(self.DAYS[1]))}
        stored = {"A": {self.DAYS[0]: 10.0, self.DAYS[1]: 10.0}}
        assert find_restated(frames, stored, self.LATEST) == ["A"]

    def test_days_outside_the_download_are_ignored(self) -> None:
        stored = {"A": {date(2024, 6, 1): 100.0}}
        assert find_restated(self._frames(50.0, 50.0, 50.0), stored, self.LATEST) == []
        assert find_restated({}, {"A": {self.DAYS[0]: 100.0}}, self.LATEST) == []
        assert (
            find_restated(self._frames(5.0, 5.0, 5.0), {"A": {self.DAYS[0]: 0.0}}, self.LATEST)
            == []
        )


def test_failure_counting_ignores_mass_misses() -> None:
    assert _counts_as_failure(1, 100)
    assert _counts_as_failure(20, 100)
    assert not _counts_as_failure(21, 100)  # throttling or an outage
    assert not _counts_as_failure(0, 100)
    assert _counts_as_failure(1, 1)  # a few misses always count
    assert _counts_as_failure(3, 3)
    assert not _counts_as_failure(4, 4)


class TestCutoffs:
    def test_incremental(self) -> None:
        result = cutoffs(["A", "B"], {"A": date(2024, 6, 1)}, RunOptions(end=END))
        assert result == {"A": date(2024, 6, 1), "B": None}

    def test_start_pulls_cutoff_back(self) -> None:
        opts = RunOptions(end=END, start=date(2024, 5, 1))
        result = cutoffs(["A", "B"], {"A": date(2024, 6, 1), "B": date(2024, 4, 1)}, opts)
        assert result == {"A": date(2024, 4, 30), "B": date(2024, 4, 1)}

    def test_full_recompute(self) -> None:
        opts = RunOptions(end=END, full_recompute=True)
        assert cutoffs(["A"], {"A": date(2024, 6, 1)}, opts) == {"A": None}


class TestHistoryCoverage:
    def test_lookback_covers_required_trading_days_without_gaps(self) -> None:
        cutoff = date(2024, 6, 3)
        start = lookback_start(cutoff, 252)
        trading_days = len(pd.bdate_range(start, cutoff)) - 1
        holidays = 10 * ((cutoff - start).days / 365 + 1)  # about 10 a year
        assert trading_days - holidays >= required_history(252)

    def test_has_enough_history(self, stock: pd.DataFrame, index: pd.DataFrame) -> None:
        window = 20
        need = required_history(window) - 1
        cutoff = stock.index[need - 1].date()  # exactly `need` common days up to it
        assert has_enough_history(stock, index, cutoff, window)
        assert not has_enough_history(stock.iloc[1:], index, cutoff, window)

    def test_a_trading_halt_leaves_too_little_history(
        self, stock: pd.DataFrame, index: pd.DataFrame
    ) -> None:
        cutoff = stock.index[-1].date()
        start = pd.Timestamp(lookback_start(cutoff, 252))
        sliced = stock[stock.index >= start]
        assert has_enough_history(sliced, index, cutoff, 252)
        halted = sliced.drop(sliced.index[100:220])  # six months without trading
        assert not has_enough_history(halted, index, cutoff, 252)


def test_new_metrics_keeps_rows_after_cutoff(stock: pd.DataFrame, index: pd.DataFrame) -> None:
    cutoff = stock.index[-5].date()
    ticker_cutoffs = {"AAA": cutoff, "BBB": None, "ZZZ": None}
    result = new_metrics({"AAA": stock, "BBB": stock}, index, "IDX", ticker_cutoffs, 20)
    assert (result["ticker"] == "AAA").sum() == 4
    assert (result["ticker"] == "BBB").sum() == len(stock)
    assert new_metrics({}, index, "IDX", {"AAA": None}, 20).empty


class TestLastCompletedSession:
    @pytest.mark.parametrize(
        ("utc", "expected"),
        [
            ("2024-06-28 22:00", date(2024, 6, 28)),  # Fri 18:00 New York: today is final
            ("2024-06-28 18:00", date(2024, 6, 27)),  # Fri 14:00 New York: still trading
            ("2024-06-28 20:30", date(2024, 6, 27)),  # Fri 16:30 New York: closed, not final
            ("2024-06-28 21:00", date(2024, 6, 28)),  # Fri 17:00 New York: final
            ("2024-06-29 15:00", date(2024, 6, 28)),  # Saturday
            ("2024-07-01 03:00", date(2024, 6, 28)),  # Sunday night in New York
            ("2024-07-01 12:00", date(2024, 6, 28)),  # Monday before the open
        ],
    )
    def test_cases(self, utc: str, expected: date) -> None:
        now = datetime.fromisoformat(utc).replace(tzinfo=UTC)
        assert last_completed_session(now) == expected

    def test_caps_fetch_end(self) -> None:
        assert RunOptions(
            end=END, last_session=END - timedelta(days=1)
        ).fetch_end == END - timedelta(1)
        assert RunOptions(end=END).fetch_end == END


class TestRetry:
    def test_retries_rate_limits_then_succeeds(self) -> None:
        calls, sleeps = [], []

        def flaky() -> str:
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("HTTP 429 Too Many Requests")
            return "ok"

        assert with_backoff(flaky, what="t", base_delay=1, sleep=sleeps.append) == "ok"
        assert sleeps == [1, 2]

    def test_gives_up_after_retries(self) -> None:
        def limited() -> None:
            raise RuntimeError("rate limit exceeded")

        with pytest.raises(RuntimeError):
            with_backoff(limited, what="t", retries=2, sleep=lambda _: None)

    def test_other_errors_are_not_retried(self) -> None:
        sleeps: list[float] = []

        def broken() -> None:
            raise ValueError("bad input")

        with pytest.raises(ValueError, match="bad input"):
            with_backoff(broken, what="t", sleep=sleeps.append)
        assert sleeps == []

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("HTTP Error 429", True),
            ("Too Many Requests. Rate limited. Try after a while.", True),
            ("ratelimit hit", True),
            ("Failed to generate crumb", False),
            ("404 Not Found", False),
            ("corporate actions unavailable", False),
        ],
    )
    def test_is_rate_limit(self, message: str, expected: bool) -> None:
        assert is_rate_limit(RuntimeError(message)) is expected


class TestSettings:
    def test_reads_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PORT", "6543")
        monkeypatch.setenv("RS_BENCHMARK_TICKERS", " spy, qqq ,")
        monkeypatch.setenv("RS_HISTORY_START", "2018-01-02")
        monkeypatch.setenv("RS_REF_TICKER", "voo")
        settings = Settings()
        assert settings.db.as_kwargs()["port"] == 6543
        assert settings.benchmark_tickers == ("SPY", "QQQ")
        assert settings.history_start == date(2018, 1, 2)
        assert settings.ref_ticker == "VOO"

    def test_secrets_stay_out_of_repr(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PASSWORD", "hunter2")
        monkeypatch.setenv("AV_API_KEY", "av-secret")
        assert "hunter2" not in repr(DbSettings())
        assert "av-secret" not in repr(Settings())

    def test_no_secret_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("DB_PASSWORD", "AV_API_KEY", "TWELVE_DATA_API_KEY"):
            monkeypatch.delenv(var, raising=False)
        s = Settings()
        assert s.db.password == s.alphavantage_api_key == s.twelve_data_api_key == ""


class TestRunOptions:
    def test_rerank_range_only_with_start(self) -> None:
        assert RunOptions(end=END).rerank_range == (None, None)
        start = END - timedelta(days=7)
        assert RunOptions(end=END, start=start).rerank_range == (start, END)
        assert RunOptions(end=END, full_recompute=True).rerank_range == (date(1900, 1, 1), END)

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"start": END + timedelta(days=1)}, "after end"),
            ({"window": 1}, "window"),
            ({"max_workers": 0}, "workers"),
        ],
    )
    def test_rejects_bad_values(self, kwargs: dict[str, object], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            RunOptions(end=END, **kwargs)  # type: ignore[arg-type]


class TestCliParsing:
    @pytest.mark.parametrize(
        "argv",
        [
            [],
            ["run-all", "--start-date", "yesterday"],
            ["run-all", "--indices", ","],
            ["run-all", "--start-date", "2024-02-01", "--end-date", "2024-01-01"],
            ["compute-rs", "--max-workers", "0"],
            ["map-sectors", "/does/not/exist.csv"],
        ],
    )
    def test_bad_arguments_exit_with_usage_error(self, argv: list[str]) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(argv)
        assert exc.value.code == 2

    def test_legacy_spellings_are_accepted(self) -> None:
        args = cli.build_parser().parse_args(
            [
                "run-all",
                "--start_date",
                "2024-01-02",
                "--end_date",
                "2024-01-05",
                "--max_workers",
                "2",
            ]
        )
        assert (args.start_date, args.end_date, args.max_workers) == (
            date(2024, 1, 2),
            date(2024, 1, 5),
            2,
        )

    def test_full_recompute_only_where_it_means_something(self) -> None:
        with pytest.raises(SystemExit):
            cli.build_parser().parse_args(["rank-all", "--full-recompute"])
        assert cli.build_parser().parse_args(["compute-rs", "--full-recompute"]).full_recompute


class TestSectorMap:
    def test_reads_both_kinds(self, tmp_path: Path) -> None:
        path = tmp_path / "map.csv"
        path.write_text("kind,name,etf\nsector,Technology,xlk\nindustry, Semiconductors ,SMH\n")
        assert cli.read_sector_map(path) == ({"Technology": "XLK"}, {"Semiconductors": "SMH"})

    def test_rejects_bad_rows(self, tmp_path: Path) -> None:
        path = tmp_path / "map.csv"
        path.write_text("kind,name,etf\ncountry,US,SPY\n")
        with pytest.raises(ValueError, match=r"map.csv:2"):
            cli.read_sector_map(path)

    def test_example_file_is_valid(self) -> None:
        path = Path(__file__).parents[2] / "examples" / "sector_etfs.csv"
        sectors, industries = cli.read_sector_map(path)
        assert sectors["Technology"] == "XLK"
        assert len(sectors) == 11
        assert industries


class _DeadOnUnlock:
    """A connection that takes the run lock, then dies before releasing it."""

    closed = 0

    def __init__(self) -> None:
        self.calls = 0
        self.was_closed = False

    def cursor(self) -> _DeadOnUnlock:
        return self

    def __enter__(self) -> _DeadOnUnlock:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def execute(self, query: str, params: object = None) -> None:
        self.calls += 1
        if "unlock" in query:
            raise psycopg2.OperationalError("server closed the connection unexpectedly")

    def fetchone(self) -> tuple[bool]:
        return (True,)

    def commit(self) -> None:
        pass

    def close(self) -> None:
        self.was_closed = True


class TestRunLockRelease:
    @pytest.fixture
    def dead(self, monkeypatch: pytest.MonkeyPatch) -> _DeadOnUnlock:
        conn = _DeadOnUnlock()
        monkeypatch.setattr(db.psycopg2, "connect", lambda **_: conn)
        return conn

    def test_a_failed_release_does_not_fail_a_finished_run(self, dead: _DeadOnUnlock) -> None:
        with db.run_lock(DbSettings()) as acquired:
            assert acquired
        assert dead.calls == 2  # the unlock was attempted
        assert dead.was_closed

    def test_a_failed_release_does_not_hide_an_interrupt(self, dead: _DeadOnUnlock) -> None:
        with pytest.raises(KeyboardInterrupt), db.run_lock(DbSettings()):
            raise KeyboardInterrupt
        assert dead.was_closed
