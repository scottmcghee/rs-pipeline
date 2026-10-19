"""The command line, end to end against a real database."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest
from psycopg2.extensions import connection as Connection

from rs_pipeline import cli
from rs_pipeline.config import DbSettings
from tests.integration.conftest import query, scalar


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, db_settings: DbSettings, conn: Connection) -> None:
    for key, value in db_settings.as_kwargs().items():
        monkeypatch.setenv(f"DB_{'NAME' if key == 'dbname' else key.upper()}", str(value))
    monkeypatch.setenv("RS_SOURCE", "synthetic")
    monkeypatch.setenv("RS_REF_TICKER", "MKT")
    monkeypatch.setenv("RS_BENCHMARK_TICKERS", "MKT")
    monkeypatch.setenv("RS_HISTORY_START", (date.today() - timedelta(days=500)).isoformat())


@pytest.mark.usefixtures("env")
def test_demo_flow(capsys: pytest.CaptureFixture[str], conn: Connection) -> None:
    assert cli.main(["top"]) == 0
    assert "No ranked stocks yet" in capsys.readouterr().out

    assert cli.main(["seed-demo"]) == 0
    assert cli.main(["run-all", "--log-level", "WARNING"]) == 0
    unranked = "SELECT COUNT(*) FROM fact_stock_metrics WHERE rs_rank_overall IS NULL"
    assert scalar(conn, unranked) == 0

    assert cli.main(["top", "-n", "3"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("Top 3 by overall RS percentile on ")
    rows = [line.split() for line in lines[3:]]
    assert len(rows) == 3
    assert rows[0][-2] == "100.0"  # the leader is at the 100th percentile

    day = scalar(conn, "SELECT MIN(date) FROM fact_stock_metrics WHERE rs_rank_overall > 0")
    assert cli.main(["top", "--on", str(day), "-n", "1"]) == 0
    assert f"on {day}" in capsys.readouterr().out


@pytest.mark.usefixtures("env")
def test_single_steps_and_legacy_flag_spellings(conn: Connection) -> None:
    assert cli.main(["seed-demo"]) == 0
    end = date.today() - timedelta(days=7)
    for step in cli.STEPS:
        argv = [step, "--end_date", end.isoformat(), "--max_workers", "2", "--log_level", "ERROR"]
        assert cli.main(argv) == 0
    latest = scalar(
        conn, "SELECT MAX(date) FROM fact_stock_metrics WHERE rs_rank_overall IS NOT NULL"
    )
    assert end - timedelta(days=4) <= latest <= end  # type: ignore[operator]
    assert scalar(conn, "SELECT COUNT(*) FROM fact_etf_metrics WHERE rs_rank_index IS NULL") == 0


@pytest.mark.usefixtures("env")
def test_map_sectors(tmp_path: Path, conn: Connection) -> None:
    path = tmp_path / "map.csv"
    path.write_text("kind,name,etf\nsector,Energy,ENGX\nindustry,Banks - Regional,BNKX\n")
    assert cli.main(["map-sectors", str(path)]) == 0
    assert query(conn, "SELECT name, etf_ticker FROM dim_sector").values.tolist() == [
        ["Energy", "ENGX"]
    ]
    assert scalar(conn, "SELECT etf_ticker FROM dim_industry") == "BNKX"


@pytest.mark.usefixtures("env")
def test_failure_exits_nonzero(caplog: pytest.LogCaptureFixture) -> None:
    assert cli.main(["seed-demo"]) == 0
    # ETFs are known but no prices are stored yet, so the benchmark is missing.
    assert cli.main(["compute-etf-rs", "--log-level", "ERROR"]) == 1
    assert "no prices for benchmark MKT" in caplog.text


@pytest.mark.usefixtures("env")
def test_unknown_etf_is_reported_not_onboarded(
    caplog: pytest.LogCaptureFixture, conn: Connection
) -> None:
    assert cli.main(["fetch-prices", "--indices", "NOSUCH"]) == 1  # so cron notices
    assert "could not onboard NOSUCH" in caplog.text
    assert scalar(conn, "SELECT COUNT(*) FROM dim_etf") == 0


@pytest.mark.usefixtures("env")
def test_interrupt_exits_130(monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupted(_: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setitem(cli.STEPS, "rank-all", (interrupted, "test"))
    assert cli.main(["rank-all"]) == 130


@pytest.mark.usefixtures("env")
def test_the_benchmark_is_never_ranked_even_if_not_listed(
    monkeypatch: pytest.MonkeyPatch, conn: Connection
) -> None:
    assert cli.main(["seed-demo"]) == 0
    monkeypatch.setenv("RS_BENCHMARK_TICKERS", "QQQ")  # MKT is still RS_REF_TICKER
    assert cli.main(["run-all", "--log-level", "WARNING"]) == 0
    etfs = query(conn, "SELECT DISTINCT etf_ticker FROM fact_etf_metrics")["etf_ticker"]
    assert "MKT" not in set(etfs)
    assert len(etfs) > 0


@pytest.mark.usefixtures("env")
def test_downloads_stop_at_the_last_completed_session(
    monkeypatch: pytest.MonkeyPatch, conn: Connection
) -> None:
    session = date.today() - timedelta(days=10)
    monkeypatch.setattr(cli, "last_completed_session", lambda now: session)
    assert cli.main(["seed-demo"]) == 0
    assert cli.main(["fetch-prices", "--log-level", "WARNING"]) == 0
    latest = scalar(conn, "SELECT MAX(date) FROM fact_commons_ohlc")
    assert session - timedelta(days=4) <= latest <= session  # type: ignore[operator]


@pytest.mark.usefixtures("env")
def test_run_all_finishes_every_step_then_fails(
    monkeypatch: pytest.MonkeyPatch, conn: Connection
) -> None:
    assert cli.main(["seed-demo"]) == 0

    def broken(_: object) -> None:
        raise RuntimeError("compute exploded")

    monkeypatch.setitem(cli.STEPS, "compute-rs", (broken, "test"))
    assert cli.main(["run-all", "--log-level", "ERROR"]) == 1
    # fetch ran before the failure and compute-etf-rs after it.
    assert scalar(conn, "SELECT COUNT(*) FROM fact_commons_ohlc") > 0  # type: ignore[operator]
    assert scalar(conn, "SELECT COUNT(*) FROM fact_etf_metrics") > 0  # type: ignore[operator]


@pytest.mark.usefixtures("env")
def test_demo_refuses_a_database_with_real_data(
    caplog: pytest.LogCaptureFixture, conn: Connection
) -> None:
    from rs_pipeline import db

    db.link_stocks_to_etf(conn, "XLK", ["AAPL"])
    assert cli.main(["demo", "--log-level", "ERROR"]) == 1
    assert "already holds non-demo data" in caplog.text
    assert scalar(conn, "SELECT COUNT(*) FROM dim_sector") == 0


@pytest.mark.usefixtures("env")
def test_demo_command(capsys: pytest.CaptureFixture[str], conn: Connection) -> None:
    assert cli.main(["demo", "--log-level", "WARNING"]) == 0
    assert "Top 15 by overall RS percentile" in capsys.readouterr().out
    assert scalar(conn, "SELECT COUNT(*) FROM fact_stock_metrics WHERE rs_rank_index IS NULL") == 0


@pytest.mark.usefixtures("env")
def test_a_second_concurrent_run_is_refused(
    caplog: pytest.LogCaptureFixture, db_settings: DbSettings
) -> None:
    from rs_pipeline import db

    with db.run_lock(db_settings) as held:
        assert held
        assert cli.main(["rank-all"]) == 1
    assert "another rs-pipeline run is in progress" in caplog.text


def test_top_needs_no_schema(
    monkeypatch: pytest.MonkeyPatch, db_settings: DbSettings, capsys: pytest.CaptureFixture[str]
) -> None:
    import psycopg2

    conn = psycopg2.connect(**db_settings.as_kwargs())
    with conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    conn.close()
    for key, value in db_settings.as_kwargs().items():
        monkeypatch.setenv(f"DB_{'NAME' if key == 'dbname' else key.upper()}", str(value))
    assert cli.main(["top"]) == 0
    assert "No ranked stocks yet" in capsys.readouterr().out


@pytest.mark.usefixtures("env")
def test_demo_leaves_a_foreign_database_untouched(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE TABLE orders (id int)"
        )
    conn.commit()
    assert cli.main(["demo", "--log-level", "ERROR"]) == 1
    tables = query(conn, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    assert tables["tablename"].tolist() == ["orders"]


@pytest.mark.usefixtures("env")
def test_a_window_change_through_one_step_is_refused_later(conn: Connection) -> None:
    assert cli.main(["seed-demo"]) == 0
    assert cli.main(["run-all", "--log-level", "ERROR"]) == 0
    # Only stock metrics are rebuilt with the new window...
    assert (
        cli.main(["compute-rs", "--full-recompute", "--window", "30", "--log-level", "ERROR"]) == 0
    )
    # ...so an incremental run with it must not extend the ETF metrics.
    assert cli.main(["compute-etf-rs", "--window", "30", "--log-level", "ERROR"]) == 1
    assert cli.main(["run-all", "--window", "30", "--indices", "MKT", "--full-recompute"]) == 1
