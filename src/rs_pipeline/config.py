"""Runtime settings, read from environment variables.

Nothing secret has a default. API keys are optional (the sources that need
them are skipped when the key is empty); the database password must be
supplied for any non-trust authentication.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


def _env_list(name: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(t.strip().upper() for t in raw.split(",") if t.strip())


@dataclass(frozen=True)
class DbSettings:
    dbname: str = field(default_factory=lambda: os.getenv("DB_NAME", "stocks"))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""), repr=False)
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))

    def as_kwargs(self) -> dict[str, str | int]:
        """Keyword arguments for ``psycopg2.connect``."""
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }


@dataclass(frozen=True)
class Settings:
    db: DbSettings = field(default_factory=DbSettings)

    # Benchmark that every ETF is measured against in compute-etf-rs.
    ref_ticker: str = field(default_factory=lambda: os.getenv("RS_REF_TICKER", "SPY").upper())
    # Broad-market benchmarks. They are price sources, never ranked as ETFs.
    benchmark_tickers: tuple[str, ...] = field(
        default_factory=lambda: _env_list("RS_BENCHMARK_TICKERS", "SPY,QQQ,IWM")
    )
    # Earliest date fetched for a ticker that has no stored prices yet.
    history_start: date = field(
        default_factory=lambda: date.fromisoformat(os.getenv("RS_HISTORY_START", "2020-01-01"))
    )

    # Optional third-party sources.
    alphavantage_api_key: str = field(
        default_factory=lambda: os.getenv("AV_API_KEY", ""), repr=False
    )
    twelve_data_api_key: str = field(
        default_factory=lambda: os.getenv("TWELVE_DATA_API_KEY", ""), repr=False
    )
    # Directory of ``<etf>.csv`` holdings files (one ``ticker`` column),
    # consulted before AlphaVantage when onboarding an ETF.
    holdings_csv_dir: str = field(default_factory=lambda: os.getenv("HOLDINGS_CSV_DIR", ""))


# Rolling window, in trading days, for the moving-average deviation,
# percentile and z-score statistics.
DEFAULT_WINDOW = 252
DEFAULT_MAX_WORKERS = min(os.cpu_count() or 4, 8)

YFINANCE_BATCH_SIZE = 500
TWELVE_DATA_BATCH_SIZE = 120

# Rate-limited calls are retried after 30s, 60s and 120s.
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_BACKOFF_BASE = 30

# Relative change in a stored close that means the vendor restated history.
# 0.05% is far above storage rounding (6 decimals) and below a typical
# quarterly dividend. The latest stored bar is never compared, since vendors
# revise the most recent close by a cent or so (more than 0.05% for a stock
# under $20).
READJUSTMENT_TOLERANCE = 5e-4

# If more than this share of stocks get no prices in one run, the misses are
# blamed on the vendor (outage, throttling, holiday), not counted as failures.
MAX_FAILURE_FRACTION = 0.2
# ...though this many misses always count, however few stocks were attempted.
MIN_COUNTED_FAILURES = 3

# Fetch failures, on distinct days, before a ticker is treated as delisted.
DELIST_FAILURE_THRESHOLD = 3
