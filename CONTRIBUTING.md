# Contributing

## Setup

```bash
uv sync                    # Python 3.12+; installs the package and dev tools into .venv
uv run pre-commit install  # lint, format, type-check and secret-scan on each commit
```

Docker is needed for the integration tests and the demo. Without Docker the
integration tests are skipped locally (and coverage then falls below the 90%
floor). In CI, where `CI` is set, a missing Docker fails the run instead.

## Checks

```bash
uv run ruff check .                  # lint
uv run ruff format .                 # format (CI runs --check)
uv run mypy                          # strict type check of src/ and scripts/
uv run pytest -m "not integration"   # fast unit tests
uv run pytest --cov                  # everything, with the coverage floor
docker compose up --build            # the offline demo
```

CI runs the same checks on every push to `main` and on every pull request.

## Conventions

- Steps take a `Context` and get everything from it: no module-level state and
  no direct vendor calls. New outside data goes behind a protocol in
  `sources/base.py`, with a synthetic implementation so it works offline.
- All SQL lives in `db.py`. Values are always bound parameters. Identifiers
  are constants, or are composed with `psycopg2.sql`. Query functions end
  their own transaction.
- Schema changes go in `schema.sql` and must stay idempotent. Use
  `CREATE ... IF NOT EXISTS` for new objects, and
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for changes to existing tables.
- A change to a metric or to ranking must keep the invariants in
  [docs/DESIGN.md](docs/DESIGN.md). The integration tests check them.
- Date ranges are inclusive at both ends.
