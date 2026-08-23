# reroll-data
Static pypi data for determining reroll compatibility percentages with real packages

`reroll-data` incrementally scrapes every `.whl` filename on PyPI, plus its
PEP 658 `METADATA` body, into two SQLite databases (`main.db`/`pypi.db`, see
`reroll_data.db2`), then runs [`reroll`](../reroll)'s own translator over
every wheel and records whether it converted. See the `Makefile` for the
full pipeline; the short version:

```sh
make db-init          # create main.db/pypi.db if missing
make sync-filenames   # discover + fetch every .whl filename
make sync-metadata    # download PEP 658 METADATA bodies
make sync-reroll       # run reroll's translator over every outstanding wheel
make reroll-status     # coverage %, by category
```

or directly:

```sh
uv run reroll-data db init
uv run reroll-data refresh
uv run reroll-data crawl --rate 900 --workers 8
uv run reroll-data metadata sync
uv run reroll-data metadata fetch --rate 900 --workers 8
uv run reroll-data convert --retry-errors --retry-stale-version
uv run reroll-data reroll-status
```

`--help` on any of the above lists every flag.

## Status and diagnostics

```sh
make status           # crawl + metadata counts (pypi.db)
make metadata-status   # metadata download counts, optionally with --bytes
make reroll-status     # reroll's own conversion counts by category, + coverage %
```

`reroll-status` breaks conversion attempts down by
`scope`/`invalid`/`unconvertable`/`unavailable`/`unexpected`/`ok`/
`outstanding` (see `reroll_data.db2.stats_main`), plus `coverage`
(`ok / wheels`) and `unconvertable` (`unconvertable / (unconvertable + ok)`)
percentages.

## Migrating from the legacy `v.db` corpus

`reroll_data.db2_backfill` is a one-off, idempotent, resumable migration that
copies the pypi-index/metadata halves of an existing legacy `v.db` corpus
into `main.db`/`pypi.db`. It never writes back to `v.db`, and deliberately
does not copy `repodata_conversion` or `metadata_blob.parsed_json` -- see
that module's docstring for the full scope and rationale.

```sh
make db2-backfill DB=data/v.db
```

## Development

`reroll` is a required dependency (distribution name `py-reroll`), pinned in
`pyproject.toml` to a published release rather than a local editable
checkout. Bump the pin alongside a `uv lock --upgrade-package py-reroll`
when a new release is needed.
