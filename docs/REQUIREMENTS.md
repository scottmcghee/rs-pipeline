# Requirements

Each requirement names the tests that verify it, or says how it is met when
it isn't directly testable.

## Functional

| ID | Requirement | Verified by |
|---|---|---|
| FR-1 | Download daily OHLC bars for every tracked stock, every tracked ETF, and the benchmark. | `test_full_run_ranks_everything_consistently` |
| FR-2 | Download only what is missing, plus a two-week overlap that is compared to detect restatements; a new ticker gets its full history. | `TestPlanDownloads` |
| FR-3 | `--start-date D` re-downloads, recomputes and re-ranks from `D`, leaving earlier data untouched. | `test_start_date_reprocesses_from_that_date_only` |
| FR-4 | Fall back to a secondary price vendor for tickers the primary misses, then re-check remaining misses one at a time. | `test_fallback_asks_later_sources_only_for_misses`, `test_recheck_recovers_missed_tickers` |
| FR-5 | Onboard an unknown ETF named in `--indices`: find its holdings (CSV, then AlphaVantage), register it, and link its stocks; report it if none are found. | `test_onboarding_with_indices`, `test_unknown_etf_is_reported_not_onboarded`, `TestHoldings` |
| FR-6 | Fill in company, sector, industry and exchange for new stocks, and link each stock to its sector's and industry's ETFs. | `TestUniverse`, `test_onboarding_with_indices` |
| FR-7 | Replace the sector/industry → ETF map from a CSV (`map-sectors`), unlinking ETFs the new map drops. | `test_map_sectors`, `test_replacing_the_sector_map_unlinks_dropped_etfs`, `TestSectorMap` |
| FR-8 | Compute the metrics defined in [DESIGN.md](DESIGN.md) for each (stock, ETF) pair and each (ETF, benchmark) pair. | `tests/unit/test_rs.py` |
| FR-9 | Rank each stock within each of its ETFs and across all stocks, and rank the ETFs. | `TestRanking` |
| FR-10 | Retire a stock after 3 separate days without new bars (while others got them), keep its record, and never link it again. | `test_dead_ticker_is_delisted_after_three_failing_days`, `test_a_ticker_served_only_old_bars_is_still_delisted`, `test_a_delisted_stock_with_a_sector_stays_unlinked` |
| FR-11 | When a vendor restates a ticker's history (split, dividend, correction), replace the stored history and rebuild every metric series derived from it: for a stock, an ETF or the benchmark. | `TestReadjustment`, `TestFindRestated`, `TestRebuildQueue` |
| FR-12 | Show the top-ranked stocks for a date (`top`). | `test_demo_flow` |
| FR-13 | Run fully offline against a synthetic market (`demo`, `--source synthetic`), and refuse to write demo data into a database that holds other data. | `test_demo_command`, `test_demo_refuses_a_database_with_real_data`, CI `docker` job |

## Nonfunctional

| ID | Requirement | How it is met / verified |
|---|---|---|
| NFR-1 **Correctness** | Any sequence of incremental runs stores exactly the same metric and rank values as one full recompute, including across trading halts and for ETFs onboarded later. | `test_incremental_runs_match_a_full_recompute`, `test_incremental_matches_full_across_a_trading_halt`, `test_onboarding_an_etf_later_matches_a_full_run`; the bound is checked exactly, in both directions, in `TestTrailingProperty` |
| NFR-2 **No look-ahead** | No stored value depends on prices after its date. | `test_appending_days_never_changes_earlier_rows`, `test_trailing_percentile_ignores_the_future` |
| NFR-3 **Idempotence** | Re-running any step with the same inputs changes nothing, and two runs never overlap. | `test_second_run_is_a_no_op`, `test_a_second_concurrent_run_is_refused` |
| NFR-4 **Final data only** | Only completed sessions are stored; a run during market hours never stores a provisional bar. | `TestLastCompletedSession`, `test_downloads_stop_at_the_last_completed_session` |
| NFR-5 **Resilience** | Vendor rate limits are retried with backoff (30, 60, 120 s), and persistent throttling stops metadata lookups for the run. Mass misses and market holidays aren't mistaken for delistings; a total outage is reported. A failing ETF or step doesn't stop the others. A failed repair converges on the next run. | `TestRetry`, `test_stops_asking_while_throttled`, `test_mass_misses_are_not_counted`, `test_a_market_holiday_is_not_a_problem`, `test_a_total_vendor_outage_is_reported`, `test_compute_rs_skips_etfs_without_prices_and_reports_errors`, `test_run_all_finishes_every_step_then_fails`, `test_failed_repair_converges_on_the_next_run` |
| NFR-6 **Observability** | Each step logs what it did (rows written, ranks updated, misses, repairs). Every failure, including problems a step works around, ends the run with exit code 1 and the cause in the log. | `test_failure_exits_nonzero`, `test_unknown_etf_is_reported_not_onboarded`, `test_interrupt_exits_130` |
| NFR-7 **Performance** | A daily run's database work scales with the new data, not the history: ranking touches only dates with changed RS. | Benchmarked at a third of a 3,500-stock install (5.6M metric rows): see [Performance](#performance) |
| NFR-8 **Bounded memory** | Memory grows with max-workers × the largest ETF's membership × the history loaded: the look-back on a normal day, full history for series without metrics. It never grows with the whole metrics table. | `compute-rs` loads one ETF at a time per worker; ranking happens in SQL |
| NFR-9 **No stale connections** | No vendor call runs inside an open transaction; the metadata lookups and the re-check of missed tickers hold no connection; connections aren't kept idle between uses. | `test_reads_do_not_leave_a_transaction_open`, `test_connections_dropped_by_the_server_are_never_reused` |
| NFR-10 **Security** | No secrets in code or defaults. Credentials come from the environment and never appear in logs, including third-party debug logs. SQL identifiers are constants or composed with `psycopg2.sql`, and values are always bound parameters. | `test_no_secret_defaults`, `test_secrets_stay_out_of_repr`, `test_alphavantage_failure_does_not_log_the_key`, `test_errors_skip_the_batch_without_logging_the_key`, `test_rejects_unknown_ohlc_table`, gitleaks in CI and pre-commit |
| NFR-11 **Portability** | Python 3.12+ and PostgreSQL (tested on 17). The container runs as a non-root user that can't modify the installed code. | CI matrix (3.12, 3.13); Dockerfile |
| NFR-12 **Maintainability** | Strict typing, lint, a 90% coverage floor (lines and branches), vendors isolated behind protocols, and pinned CI actions and base images. | CI `lint` and `test` jobs; Dependabot |

## Performance

Measured in October 2026 on a laptop (AMD Ryzen 5 7530U, 6 cores) with
PostgreSQL 17 in Docker.

**Ranking at scale** ([`scripts/benchmark_ranking.py`](../scripts/benchmark_ranking.py)).
A synthetic metrics table of 5.6 million rows (1,200
stocks × 3 ETFs × 1,566 trading days, about a third of a 3,500-stock
installation):

| Operation | Time |
|---|---|
| Daily `rank-indices`: one new trading day (3,600 rows) | 0.9 s |
| Daily `rank-all`: the same day | 1.6 s |
| A stock's history rewritten by a dividend restatement (4,700 rows) | 0.4 s, and 0.3 s of ranking, since no RS changed |
| First-ever ranking of the whole table (one-time) | 9 min (`rank-indices`) + 8 min (`rank-all`) |

**Metric math** (`timeit` over synthetic random walks). `compute_all_metrics`
takes a median of 6.7 ms per (stock, ETF) pair for a daily run's look-back
(755 days), and 9.6 ms for six years of history. About 10,000 pairs therefore
cost roughly a minute of CPU, overlapped with database I/O across
`--max-workers` threads.

**The demo.** 40 stocks, 8 ETFs, history since 2022: 15–20 s end to end, in
about 230 MB of memory.

A live run also depends on vendor latency and rate limits, which aren't
measured here.
