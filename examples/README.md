# Examples

## `sector_etfs.csv`: sector and industry ETFs

Maps the sector and industry names Yahoo Finance reports for stocks to ETFs
that track them (SPDR sector funds, plus a few industry funds). Load it with:

```bash
uv run rs-pipeline map-sectors examples/sector_etfs.csv
```

On each `fetch-prices`, every tracked stock whose sector or industry matches a
row is linked to that ETF and ranked within it. Sector ETFs don't add stocks
to the universe; stocks come from the ETFs you onboard. Running `map-sectors`
again replaces the whole map, and links to ETFs it no longer mentions are
removed.

## `holdings/`: ETF holdings for onboarding

`--indices XLK` onboards an ETF from `<HOLDINGS_CSV_DIR>/xlk.csv` (lower-case
name, one `ticker` column), or from AlphaVantage when `AV_API_KEY` is set.
`holdings/xlk.csv` is a short illustrative list for trying the pipeline, not
the fund's current holdings. For real use, download each fund's holdings from
its issuer (SPDR, iShares and Vanguard publish them daily) and keep the
`ticker` column.

Share-class tickers such as `BRK.B` are stored in Yahoo's form, `BRK-B`.
Holdings are read once, when an ETF is first onboarded.
