"""A throwaway PostgreSQL server per test session, and a fresh database per test.

Each test gets its own database, cloned from a template: an empty schema by
default, or, for tests marked ``seeded``, the demo universe already run up to
``T2``. Cloning a template takes milliseconds, so the expensive setup runs
once per session.
"""

from __future__ import annotations

import itertools
import logging
import os
import re
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
from psycopg2.extensions import connection as Connection

from rs_pipeline import cli, db
from rs_pipeline.config import DbSettings, Settings
from rs_pipeline.pipeline import Context, RunOptions
from rs_pipeline.sources import DataSources
from rs_pipeline.sources.synthetic import SyntheticMarket

SMALL_WINDOW = 20  # short enough that incremental runs really load a truncated look-back
T2 = date(2024, 6, 28)  # the date the seeded template is run up to

SETTINGS_OVERRIDES = {
    "ref_ticker": "MKT",
    "benchmark_tickers": ("MKT",),
    "history_start": date(2022, 7, 1),
}

# The demo's image, pinned by digest, so the tests follow its Dependabot bumps.
_COMPOSE = Path(__file__).parents[2] / "docker-compose.yml"
_image = re.search(r"image: (postgres:\S+)", _COMPOSE.read_text())
assert _image, f"no postgres image in {_COMPOSE}"
POSTGRES_IMAGE = _image[1]

MakeContext = Callable[..., Context]
_names = itertools.count()


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
    except Exception:
        return False
    return True


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "seeded: start from the demo universe run up to T2")


@pytest.fixture(scope="session")
def server() -> Iterator[DbSettings]:
    testcontainers = pytest.importorskip("testcontainers.postgres")
    if not _docker_available():
        if os.getenv("CI"):
            pytest.fail("Docker is required for the integration tests in CI")
        pytest.skip("Docker is not available; skipping integration tests")
    # Any other error starting the container is a real failure, not a skip.
    container = testcontainers.PostgresContainer(POSTGRES_IMAGE, driver=None)
    container.start()
    try:
        yield DbSettings(
            dbname=container.dbname,
            user=container.username,
            password=container.password,
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
        )
    finally:
        try:
            container.stop()
        except Exception:  # a slow Docker daemon; testcontainers' reaper cleans up
            logging.getLogger(__name__).warning("could not stop the test container", exc_info=True)


@pytest.fixture(scope="session")
def admin(server: DbSettings) -> Iterator[Connection]:
    conn = psycopg2.connect(**server.as_kwargs())
    conn.autocommit = True  # CREATE/DROP DATABASE can't run in a transaction
    yield conn
    conn.close()


def _clone(admin: Connection, template: str) -> str:
    name = f"t{next(_names)}"
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}" TEMPLATE "{template}"')
    return name


def _make_ctx(pool: db.ConnectionPool, settings: DbSettings) -> MakeContext:
    def make(
        end: date, *, today: date | None = None, sources: DataSources | None = None, **opts: object
    ) -> Context:
        today = today or end
        if sources is None:
            market = SyntheticMarket(today=lambda: today)
            sources = DataSources(prices=market, info=market, holdings=market)
        full = Settings(db=settings, **SETTINGS_OVERRIDES)  # type: ignore[arg-type]
        options = RunOptions(end=end, today=today, max_workers=4, **opts)  # type: ignore[arg-type]
        return Context(full, sources, pool, options)

    return make


@pytest.fixture(scope="session")
def empty_template(admin: Connection, server: DbSettings) -> str:
    with admin.cursor() as cur:
        cur.execute('CREATE DATABASE "tpl_empty"')
    conn = psycopg2.connect(**replace(server, dbname="tpl_empty").as_kwargs())
    db.apply_schema(conn)
    conn.close()
    return "tpl_empty"


@pytest.fixture(scope="session")
def seeded_template(admin: Connection, server: DbSettings, empty_template: str) -> str:
    name = _clone(admin, empty_template)
    settings = replace(server, dbname=name)
    pool = db.ConnectionPool(settings, maxconn=6)
    ctx = _make_ctx(pool, settings)(T2)
    cli._seed_demo(ctx)
    for step, _ in cli.STEPS.values():
        step(ctx)
    pool.close_all()
    return name


@pytest.fixture
def db_settings(
    request: pytest.FixtureRequest, admin: Connection, server: DbSettings, empty_template: str
) -> Iterator[DbSettings]:
    seeded = request.node.get_closest_marker("seeded") is not None
    template = request.getfixturevalue("seeded_template") if seeded else empty_template
    name = _clone(admin, template)
    yield replace(server, dbname=name)
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


@pytest.fixture
def pool(db_settings: DbSettings) -> Iterator[db.ConnectionPool]:
    pool = db.ConnectionPool(db_settings, maxconn=6)
    yield pool
    pool.close_all()


@pytest.fixture
def conn(pool: db.ConnectionPool) -> Iterator[Connection]:
    with pool.connection() as c:
        yield c


@pytest.fixture
def make_ctx(pool: db.ConnectionPool, db_settings: DbSettings) -> MakeContext:
    """Context over the synthetic market as seen on *today* (default: end)."""
    return _make_ctx(pool, db_settings)


def query(conn: Connection, sql: str, params: object = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description or ()])
    conn.commit()
    return df


def scalar(conn: Connection, sql: str, params: object = None) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None
