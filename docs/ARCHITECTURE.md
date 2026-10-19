# Architecture

## Modules

```text
src/rs_pipeline/
├── cli.py            argparse commands; builds the Context and runs steps
├── config.py         Settings from environment variables; tuning constants
├── pipeline.py       Context and RunOptions passed to every step; look-back, session and window checks
├── rs.py             the metrics (pure pandas/NumPy, no I/O)
├── db.py             connection pool, run lock, schema, every SQL statement
├── schema.sql        all tables and indexes (idempotent; applied on every run)
├── universe.py       onboarding ETFs; stock metadata and sector linking
├── retry.py          exponential backoff for rate-limited vendors
├── sources/
│   ├── base.py       PriceSource / InfoSource / HoldingsSource protocols, normalization
│   ├── __init__.py   build_sources("live" | "synthetic"); FallbackPrices
│   ├── yahoo.py      yfinance prices and metadata
│   ├── twelvedata.py Twelve Data price fallback (optional extra)
│   ├── holdings.py   CSV and AlphaVantage holdings
│   └── synthetic.py  fictional reproducible market for the demo and tests
└── steps/
    ├── fetch_prices.py    1. download, restatement detection and repair, re-check, failures
    ├── compute_rs.py      2. stock-vs-ETF metrics, parallel by ETF
    ├── rank_indices.py    3. rank within each ETF
    ├── compute_etf_rs.py  4. ETF-vs-benchmark metrics and ETF ranks
    ├── rank_all.py        5. rank across all stocks
    └── _incremental.py    cutoffs, look-back, history checks and rebuilds shared by steps 2 and 4
```

Imports only point downward: `cli` → `steps` → `pipeline` → `db`, `rs`, `sources`;
`cli` and `fetch-prices` also use `universe`. Nothing imports `cli` or `steps`.
`rs.py` and `sources/base.py` import nothing from the rest of the package. Steps
receive everything they need in a `Context` (settings, sources, pool,
options) instead of reading globals. That is what lets the tests swap in the
synthetic market, or a deliberately broken vendor.

## A daily run

```mermaid
sequenceDiagram
    participant Cron
    participant CLI as rs-pipeline run-all
    participant Src as Data vendors
    participant DB as PostgreSQL

    Cron->>CLI: run-all
    CLI->>DB: take the run lock, apply schema.sql
    Note over CLI,DB: 1 fetch-prices
    CLI->>DB: read holdings and the stocks lacking metadata
    CLI->>Src: metadata for new stocks (no connection held)
    CLI->>DB: store metadata, link stocks to sector ETFs
    CLI->>Src: bars from two weeks before each latest stored bar, in batches
    CLI->>DB: compare the overlap, upsert new and changed bars
    opt vendor restated a ticker's history
        CLI->>DB: queue its metric series for a rebuild
        CLI->>Src: re-download the whole history
        CLI->>DB: replace stored prices
    end
    CLI->>Src: re-check misses one by one (no connection held)
    CLI->>DB: store recoveries, count failures, retire dead stocks
    Note over CLI,DB: 2 compute-rs (ETFs in a thread pool)
    CLI->>DB: prices since cutoff minus look-back, full history for rebuilds
    CLI->>DB: upsert changed metric rows (ranks cleared where RS changed)
    Note over CLI,DB: 3 rank-indices, 4 compute-etf-rs, 5 rank-all
    CLI->>DB: rank within ETFs, ETF metrics and ranks, overall ranks (dates with cleared ranks only)
```

## Schema

```mermaid
erDiagram
    dim_stock ||--o{ junc_stock_etf : "belongs to"
    dim_etf |o--o{ junc_stock_etf : "holdings"
    dim_sector |o--o{ junc_stock_etf : "links by sector"
    dim_industry |o--o{ junc_stock_etf : "links by industry"
    dim_stock ||--o{ fact_commons_ohlc : "prices"
    dim_stock ||--o{ fact_stock_metrics : "metrics"
    dim_etf |o--o{ fact_index_ohlc : "prices"
    dim_etf |o--o{ fact_etf_metrics : "metrics"
    dim_stock {
        text ticker PK
        text company_name
        text sector
        text industry
        text exchange
    }
    junc_stock_etf {
        text ticker PK,FK
        text etf_ticker PK
    }
    dim_etf {
        text etf_ticker PK
        text etf_name
    }
    dim_sector {
        text name PK
        text etf_ticker
    }
    dim_industry {
        text name PK
        text etf_ticker
    }
    fact_commons_ohlc {
        date date PK
        text ticker PK
        numeric open
        numeric high
        numeric low
        numeric close
        bigint volume
    }
    fact_index_ohlc {
        date date PK
        text ticker PK
        numeric open
        numeric high
        numeric low
        numeric close
        bigint volume
    }
    fact_stock_metrics {
        date date PK
        text ticker PK
        text etf_ticker PK
        numeric relative_strength
        numeric rs_rank_index
        numeric rs_rank_overall
        numeric other_metrics "deviation, percentiles, z-score, ..."
    }
    fact_etf_metrics {
        date date PK
        text etf_ticker PK
        text ref_ticker PK
        numeric relative_strength
        numeric rs_rank_index
        numeric rs_rank_overall
        numeric other_metrics "same columns as fact_stock_metrics"
    }
```

`junc_stock_etf.etf_ticker` points at an onboarded ETF in `dim_etf`, or at the
ETF named by `dim_sector` / `dim_industry`. Sector and industry ETFs (and a
benchmark that isn't onboarded) have prices and metrics but no `dim_etf` row.
The relationships in the diagram are logical: only `junc_stock_etf.ticker` is a
declared foreign key.

Bookkeeping tables support the invariants in [DESIGN.md](DESIGN.md):
- `ticker_fetch_failures` and `delisted_tickers` for delisting (invariant 7);
- `metrics_rebuild` for queued restatement rebuilds (invariant 8);
- `pipeline_state` for the rolling window the stored metrics use.

The full DDL is in [`src/rs_pipeline/schema.sql`](../src/rs_pipeline/schema.sql),
plain PostgreSQL, tested on 18.

## Testing

| Layer | Where | Needs | What it shows |
|---|---|---|---|
| Unit | `tests/unit` | nothing | Metric math and its exact trailing bound, download planning, restatement detection, cutoffs and history checks, sessions, vendor clients (mocked), synthetic market, settings, CLI parsing |
| Integration | `tests/integration` | Docker | Real SQL on PostgreSQL 18: rankings, upserts, the rebuild queue, connection handling and the run lock, failures and delisting, restatement repair, the CLI, and whole-pipeline invariants (incremental = full, idempotence) |
| Demo | `docker compose up` | Docker | A clean checkout builds, seeds, runs and reports |

All three run in CI. Each integration test gets its own database, cloned from
a template (an empty schema, or the demo already run to a fixed date), so the
expensive setup runs once per session.
