# Design

This document defines exactly what the pipeline computes, lists the
invariants the code relies on, and records the main design decisions.

## Metrics

For each (stock, ETF) pair, on every date both have a price, with closes `c`
and a rolling window `w` (default 252 trading days, about a year):

| Metric | Definition |
|---|---|
| strength | `0.4·R63 + 0.2·R126 + 0.2·R189 + 0.2·R252`, where `Rk = c[t] / c[t−k] − 1` is the trailing k-day return. The newest quarter is inside all four returns, so it dominates: quarters count roughly 1.0, 0.6, 0.4 and 0.2, newest to oldest. This is the commonly cited approximation of the RS Rating published by Investor's Business Daily (IBD), whose exact formula is proprietary. |
| `relative_strength` (RS) | `100 · (1 + strength_stock) / (1 + strength_etf)`, rounded to 2 decimals. 100 means in line with the ETF; 120 means 20% stronger. NULL until the stock and ETF share 252 earlier trading days, whatever `w` is. |
| `naive_relative_strength` | `c_stock / c_etf`, the plain price ratio. |
| `deviation_from_moving_average` | `100 · (RS − MA_w(RS)) / MA_w(RS)`: how far RS is above its own trailing average, in percent. |
| `relative_strength_percentile` | Percentile (0–1) of today's *deviation* (despite the name, not of RS) within the last `w` days. |
| `naive_deviation_from_moving_average`, `naive_relative_strength_percentile` | The same two statistics for the price ratio. |
| `rs_rolling_mean`, `rs_rolling_std_dev` | Trailing `w`-day mean and sample standard deviation of RS. |
| `rs_z_score` | `(RS − mean) / std`. |
| `rs_z_score_roc` | Day-over-day change of the z-score, in percent. |
| `rs_rank_index` | Percentile rank of RS among the stocks of the same ETF on the same date. |
| `rs_rank_overall` | Percentile rank of the stock's RS (averaged over all of its ETFs) among all stocks on that date. |

The ranks use SQL's `PERCENT_RANK = (rank − 1) / (rows − 1)`, which runs from 0
for the weakest to 1 for the strongest; `top` shows ranks × 100. A NULL RS
ranks lowest.

ETF metrics are the same, with each ETF in the place of the stock and the
benchmark (`RS_REF_TICKER`) in the place of the ETF. ETFs are ranked among the
ETFs measured against the same benchmark. For ETFs the two rank columns hold
the same value; both exist so the two metrics tables share one shape.

**Undefined values.** Rolling statistics use up to `w` values, so early rows use
a shorter window. Percentiles rank only the deviations that exist. Undefined
results (too little history, division by zero) are stored as 0; the naive ratio
is stored as 1.0. During a stock's first year RS is NULL, and the RS-derived
columns read 0, which should be ignored. The naive columns are real values from
the first day.

## Invariants

Each invariant names, in brackets, the tests that check it.

1. **Metrics are trailing and bounded.** A row for date `t` depends only on
   prices on or before `t`. It uses at most `required_history(w) = 4·63 + 2w − 1`
   trading days that the stock and ETF share, ending at `t` (755 for `w = 252`).
   The deepest dependency is the percentile: a `w`-day window over the
   deviation, which is itself a `w`-day window over RS, which needs 252
   earlier closes. The bound is exact: the oldest of those days can change
   today's percentile, and the day before it can't. Rolling statistics are
   computed separately for each window, not with running sums, so a value is
   bit-for-bit the same however much earlier history was loaded.
   [`TestTrailingProperty`]

2. **Incremental equals full.** Any sequence of runs (daily, weekly, or after a
   gap) stores exactly the same metrics and ranks as one run over the whole
   period. This follows from (1). An incremental run loads the look-back before
   the earliest date it will write, converted generously to calendar days.
   Then, for each stock, it checks that the slice really holds
   `required_history − 1` shared trading days. A long trading halt can defeat
   the calendar estimate, and that stock is then reloaded in full.
   [`test_incremental_runs_match_a_full_recompute`,
   `test_incremental_matches_full_across_a_trading_halt`,
   `test_onboarding_an_etf_later_matches_a_full_run`]

3. **A NULL rank means "needs ranking".** A metric row's ranks are cleared
   when the row is new or its RS changes, since ranks depend on nothing else.
   Unchanged rows are not rewritten at all. Each ranking step first finds the
   dates that hold a cleared rank, which partial indexes make cheap. It then
   recomputes `PERCENT_RANK` for those dates only, because one changed row
   shifts everyone's percentile on that date. So ranking needs no date range,
   always catches up (after a backfill, a newly linked stock, or a rebuilt
   series), and on a normal day re-ranks just one date. Ranks are compared at
   the stored precision, so unchanged ranks aren't rewritten either.
   [`TestRanking`, `test_ranks_survive_changes_that_do_not_touch_rs`]

4. **One overall rank per stock and date.** All of a stock's rows on a date
   (one per ETF) carry the same `rs_rank_overall`.
   [`test_full_run_ranks_everything_consistently`]

5. **Re-running is harmless, and runs never overlap.** Every write is an
   upsert on the table's primary key, and every step skips work that's already
   done, so a second run changes nothing. A database advisory lock makes a run
   refuse to start while another is in progress. Without it, one run could
   store metrics computed from prices the other had just replaced.
   [`test_second_run_is_a_no_op`, `TestStateAndLocking`]

6. **Dates are inclusive, and bars are final.** `--start-date` and `--end-date`
   include both ends everywhere. Yahoo's `end` is exclusive, so its client asks
   for one extra day. Downloads stop at the last session whose bar is final
   (bars count as final from 17:00 New York time), so a run during market hours
   never stores a provisional bar. [`test_requests_one_extra_day_because_yahoo_end_is_exclusive`,
   `TestLastCompletedSession`, `test_downloads_stop_at_the_last_completed_session`]

7. **Delisting needs evidence.** A stock is retired after getting no new bars
   on three separate days, with no success in between, while the vendor was
   serving new bars for other tickers. Old bars alone don't count as success: a
   vendor may keep serving a dead ticker's history. When more than 20% of the
   stocks attempted miss at once, the misses aren't counted, since that points
   to an outage or throttling (up to 3 misses always count). A day when no
   ticker gets a new bar is a market holiday, not a failure. A retired stock is
   unlinked from its ETFs and never linked again, but its prices and metrics
   are kept. An ETF that gets no new bars is reported, never retired.
   [`TestFailuresAndDelisting`]

8. **Stored prices follow the vendor's adjustments.** Prices are split- and
   dividend-adjusted. After a split or dividend, vendors *restate* a ticker's
   past prices (the code calls this re-adjustment), so the series stays
   continuous. Mixing old and new bars would show a fake crash or jump in RS.
   So every incremental fetch also re-downloads the last two weeks of stored
   bars, which covers vendors that apply an adjustment a few days late. If any
   of those bars, apart from the very latest (whose close vendors routinely
   revise by a cent), differs from ours by more than 0.05%, or is gone, the
   ticker is repaired:
   1. The metric series built on its prices are queued for a rebuild: the
      stock's series against each of its ETFs; for an ETF, every member's
      series against it and its own; for the benchmark, every ETF's.
   2. The ticker's whole stored history is downloaded again and replaces what
      is stored.
   3. The compute steps rebuild the queued series from full history, delete
      rows for dates the new history no longer has (marking those dates for
      re-ranking), then remove the series from the queue. A run that ends
      before a series' last stored date leaves it queued.

   This happens on every path that stores bars, including tickers recovered by
   the one-at-a-time re-check. Queuing first means a repair that fails halfway
   converges: the next run sees the same mismatch and tries again. The result
   is identical to a fresh run on the adjusted data. [`TestReadjustment`,
   `TestFindRestated`, `TestRebuildQueue`]

## How a run works

### Choosing what to download

For each ticker, `fetch-prices` downloads from two weeks before its latest
stored bar onward (invariant 8). A ticker with no stored prices gets everything
from `RS_HISTORY_START`. With `--start-date D`, already-stored tickers are
re-downloaded from `D` (new tickers still get full history), and the whole
re-downloaded range is compared. Tickers that need the same start date are
downloaded together, in batches of 500, and bars that haven't changed aren't
rewritten.

Comparing re-downloaded bars detects a restatement without needing
corporate-action data, so it works the same for every vendor. It also catches
restatements that have nothing to do with a split or dividend.

**The cost:** each dividend makes Yahoo restate the payer, four times a year for
a quarterly payer and twelve for a monthly one. For 3,500 quarterly payers
that's about 55 stock rebuilds a trading day, each rebuilding one series per
ETF the stock belongs to. Each ETF dividend also rebuilds its members' series
against it, and a benchmark dividend rebuilds every ETF's series. ETF dividends
cluster (sector fund families tend to share ex-dates), so a few days a year
rebuild most series. A rebuild rewrites one series' history, but because a dividend scales all
earlier prices by the same factor, RS before the ex-date doesn't change. So
almost no ranks are cleared, and ranking stays cheap.

### Phases and connections

Network calls never run inside an open database transaction: every query
function ends its own transaction. Every `with pool.connection()` block opens a
fresh connection and closes it at the end, so a step that spends minutes on
vendor calls never resumes on a connection the server or a firewall has
dropped. (The exception is the run lock's own connection, which stays open
for the run, with TCP keepalives.) `fetch-prices` runs in four phases,
matching its module docstring:

0. Onboard requested ETFs, and find stocks that lack metadata. Then, with no
   connection held, look up that metadata (one call per stock).
1. Store the metadata and link stocks to their sector ETFs. Bulk-download
   prices, and repair restated tickers.
2. With no connection held, re-check the stocks that got no new bars, one at
   a time.
3. Store what was recovered (and repair it if it was restated), then count
   failures.

Repairs re-check misses one at a time while holding a connection; they are
rare, and the connection has TCP keepalives.

### Incremental metrics

For each ETF, `compute-rs` finds each member's last metric date (its
*cutoff*). `--start-date D` pulls the cutoff back to `D − 1`. A series queued
for a rebuild has no cutoff, and `--full-recompute` ignores cutoffs altogether.
Members without a cutoff load their full history; the rest load the look-back
before the earliest cutoff. Then it computes every metric and keeps only rows
after each member's cutoff. Each metrics table records the window it was built
with. A different `--window` is refused unless the run is a full recompute of
the whole table (no `--indices`), and until such a recompute finishes, the
table is marked as in transition and only another full recompute may run.

### Concurrency and failure

`compute-rs` computes ETFs in a thread pool of `--max-workers`, one
connection per worker. Workers share no mutable state and write disjoint keys
(`etf_ticker` is part of the primary key), so they never conflict. Threads help
because much of each worker's time is database I/O, which runs without the GIL.

Problems a step can work around are reported, and the run then exits 1. These
include an ETF that fails to compute or has no prices, a failed onboarding or
repair, and an apparent vendor outage. In `run-all`, a step that fails outright
doesn't stop the later steps: they work on what is stored, and anything left
unranked is caught up next time (invariant 3).

## Data model

Stocks are linked to ETFs in two ways. Onboarded ETFs (`--indices`) link their
holdings, and sector and industry ETFs link stocks whose reported sector or
industry is in the `map-sectors` map. See [ARCHITECTURE.md](ARCHITECTURE.md)
for the schema.

## Decisions

**Rank in SQL, not pandas.** Rankings span every ticker on a date, while metric
computation is partitioned by ETF. Ranking in the database avoids reading tens
of millions of rows back out. Ranking runs in chunks of 50 dirty dates per
statement, so each statement uses the primary key's leading `date` column. A first full ranking is therefore many small transactions rather
than one huge one.

**One metrics row per (stock, ETF) pair.** A stock with three ETFs has three
rows per day, which multiplies the table by the average number of ETFs per stock
(about 3). In return, "how does this stock rank inside this ETF?" is a plain
filter, and every row carries its own context. The overall rank is copied onto
each row (invariant 4) so readers don't need a join. A normalized alternative
(per-stock rows plus a per-ETF RS table) would save space at the cost of every
reader query.

**NULL rank as the work queue, cleared only when RS changes.** A separate
"dirty dates" table would need to be kept consistent with the metrics table
across failures. Clearing a rank in the same statement that writes the metric
keeps the two in step. Clearing it only when RS actually changed keeps
restatement rebuilds (which mostly rewrite other columns) from dirtying every
date in history. Partial indexes (`WHERE rs_rank_index IS NULL`) make the
lookup cheap.

**A rebuild queue rather than deleting metrics.** Deleting a restated series'
rows and letting the incremental logic recreate them would clear their ranks on
every date, and leave holes if the rebuild failed. Queuing the series instead
lets the compute step recompute it in place. That also lets a run that covers
only some ETFs (`--indices`) leave the rest queued for the next full run.

**Per-window statistics instead of running sums.** pandas' rolling mean and
standard deviation keep running totals, so a value carries floating-point
residue from every earlier row. Two runs that start from different dates then
disagree in the last digits. Computing each window on its own costs a few
milliseconds per pair (see [Performance](REQUIREMENTS.md#performance)), and
makes invariant 2 exact rather than approximate.

**Trailing percentile.** A percentile over each run's whole history would
compare every day with later days (look-ahead), and stored values would
change as history grows. A trailing window avoids both, and its meaning is
unchanged: how unusual today is compared with the last year.

**A synthetic market that implements the real interfaces.** The demo and the
integration tests go through the same `PriceSource` / `InfoSource` /
`HoldingsSource` code paths as live vendors. That covers onboarding, failure
tracking, restatements and sector linking, not just the math. Prices come
from a seeded factor model (market + sector + rotating per-stock drift), so
rankings behave like real ones and every run sees identical data. Its sector
names (`Tech`, `Fuel`, ...) are deliberately unlike real vendors' names, so
demo mappings can't be mistaken for real ones.

**Vendors behind small protocols.** Yahoo, Twelve Data and AlphaVantage are
each one class. Fallbacks are composed (`FallbackPrices`,
`FirstAvailableHoldings`), so adding a vendor touches only `build_sources`.
Yahoo needs no key. The others are used only when their keys are set. In live
mode at least one holdings source (CSV files or AlphaVantage) is needed to
onboard an ETF.
