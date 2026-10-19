-- rs_pipeline schema. Every statement is idempotent; it runs on each start.
--
-- Naming: dim_* are dimensions, fact_* are time series keyed by date,
-- junc_* are many-to-many links.

CREATE TABLE IF NOT EXISTS dim_stock (
    ticker        TEXT PRIMARY KEY,
    company_name  TEXT,
    sector        TEXT,
    industry      TEXT,
    exchange      TEXT
);

-- ETFs tracked directly (broad-market and thematic).
CREATE TABLE IF NOT EXISTS dim_etf (
    etf_ticker  TEXT PRIMARY KEY,
    etf_name    TEXT
);

-- Sector and industry names (as reported for each stock) mapped to the ETF
-- that represents them. Stocks are linked to these ETFs automatically.
CREATE TABLE IF NOT EXISTS dim_sector (
    name        TEXT PRIMARY KEY,
    etf_ticker  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_industry (
    name        TEXT PRIMARY KEY,
    etf_ticker  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS junc_stock_etf (
    ticker      TEXT NOT NULL REFERENCES dim_stock (ticker),
    etf_ticker  TEXT NOT NULL,
    PRIMARY KEY (ticker, etf_ticker)
);
CREATE INDEX IF NOT EXISTS idx_junc_stock_etf_etf ON junc_stock_etf (etf_ticker);

CREATE TABLE IF NOT EXISTS fact_commons_ohlc (
    date    DATE NOT NULL,
    ticker  TEXT NOT NULL,
    open    NUMERIC(18, 6),
    high    NUMERIC(18, 6),
    low     NUMERIC(18, 6),
    close   NUMERIC(18, 6),
    volume  BIGINT,
    PRIMARY KEY (date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_fact_commons_ohlc_ticker ON fact_commons_ohlc (ticker, date);

CREATE TABLE IF NOT EXISTS fact_index_ohlc (
    date    DATE NOT NULL,
    ticker  TEXT NOT NULL,
    open    NUMERIC(18, 6),
    high    NUMERIC(18, 6),
    low     NUMERIC(18, 6),
    close   NUMERIC(18, 6),
    volume  BIGINT,
    PRIMARY KEY (date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_fact_index_ohlc_ticker ON fact_index_ohlc (ticker, date);

-- One row per (date, stock, ETF it belongs to). A NULL rank means the row
-- is new, or its relative strength changed, since it was last ranked; the
-- ranking steps rely on this to find their work.
CREATE TABLE IF NOT EXISTS fact_stock_metrics (
    date                                 DATE NOT NULL,
    ticker                               TEXT NOT NULL,
    etf_ticker                           TEXT NOT NULL,
    naive_relative_strength              NUMERIC(30, 10),
    naive_relative_strength_percentile   NUMERIC(30, 10),
    naive_deviation_from_moving_average  NUMERIC(30, 10),
    relative_strength                    NUMERIC(30, 10),
    relative_strength_percentile         NUMERIC(30, 10),
    deviation_from_moving_average        NUMERIC(30, 10),
    rs_rank_index                        NUMERIC(30, 10),
    rs_rank_overall                      NUMERIC(30, 10),
    rs_rolling_mean                      NUMERIC(30, 10),
    rs_rolling_std_dev                   NUMERIC(30, 10),
    rs_z_score                           NUMERIC(30, 10),
    rs_z_score_roc                       NUMERIC(30, 10),
    PRIMARY KEY (date, ticker, etf_ticker)
);
CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_ticker ON fact_stock_metrics (ticker);
CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_etf ON fact_stock_metrics (etf_ticker, ticker, date);
CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_unranked_index
    ON fact_stock_metrics (etf_ticker, date) WHERE rs_rank_index IS NULL;
CREATE INDEX IF NOT EXISTS idx_fact_stock_metrics_unranked_overall
    ON fact_stock_metrics (date) WHERE rs_rank_overall IS NULL;

-- One row per (date, ETF, benchmark).
CREATE TABLE IF NOT EXISTS fact_etf_metrics (
    date                                 DATE NOT NULL,
    etf_ticker                           TEXT NOT NULL,
    ref_ticker                           TEXT NOT NULL,
    naive_relative_strength              NUMERIC(30, 10),
    naive_relative_strength_percentile   NUMERIC(30, 10),
    naive_deviation_from_moving_average  NUMERIC(30, 10),
    relative_strength                    NUMERIC(30, 10),
    relative_strength_percentile         NUMERIC(30, 10),
    deviation_from_moving_average        NUMERIC(30, 10),
    rs_rank_index                        NUMERIC(30, 10),
    rs_rank_overall                      NUMERIC(30, 10),
    rs_rolling_mean                      NUMERIC(30, 10),
    rs_rolling_std_dev                   NUMERIC(30, 10),
    rs_z_score                           NUMERIC(30, 10),
    rs_z_score_roc                       NUMERIC(30, 10),
    PRIMARY KEY (date, etf_ticker, ref_ticker)
);
CREATE INDEX IF NOT EXISTS idx_fact_etf_metrics_etf ON fact_etf_metrics (etf_ticker, date);

-- Fetch-failure bookkeeping: tickers that fail on DELIST_FAILURE_THRESHOLD
-- distinct days are moved to delisted_tickers and unlinked from their ETFs.
CREATE TABLE IF NOT EXISTS ticker_fetch_failures (
    ticker              TEXT PRIMARY KEY,
    failure_count       INTEGER NOT NULL DEFAULT 1,
    first_failure_date  DATE NOT NULL,
    last_failure_date   DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS delisted_tickers (
    ticker         TEXT PRIMARY KEY,
    detected_date  DATE NOT NULL,
    original_etfs  TEXT
);

-- Metric series to recompute from full history, queued when a vendor
-- restates prices. kind 'stock': (stock, holding ETF); kind 'etf':
-- (ETF, benchmark). The compute steps rebuild a pair and then remove it.
CREATE TABLE IF NOT EXISTS metrics_rebuild (
    kind    TEXT NOT NULL CHECK (kind IN ('stock', 'etf')),
    ticker  TEXT NOT NULL,
    ref     TEXT NOT NULL,
    PRIMARY KEY (kind, ref, ticker)
);

-- Settings the stored metrics depend on, e.g. the rolling window.
CREATE TABLE IF NOT EXISTS pipeline_state (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
