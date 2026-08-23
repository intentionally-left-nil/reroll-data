# Overridable on the command line, e.g. `make sync-metadata RATE=1800 WORKERS=12`.
RATE    ?= 1800
WORKERS ?= 12

# main.db/pypi.db (see reroll_data.db2) live under this directory.
DATA_DIR ?= data
# Set to cap a run, e.g. `make crawl LIMIT=500`, so a step can be tried at
# small scale before being let loose on the full corpus.
LIMIT   ?=

# db2-backfill (see reroll_data.db2_backfill): the legacy v.db corpus this
# one-off migration reads from. Only used by that target.
DB ?= data/v.db

# reroll-convert (see reroll_data.reroll_convert): purely local CPU work,
# runs in the ordinary uv venv -- `reroll` is an ordinary, required
# dependency, kept current by a plain `uv sync`. Left empty by default so
# the tool's own default (all cores) applies, e.g.
# `make reroll-convert REROLL_WORKERS=4`.
REROLL_WORKERS ?=

RUN         := uv run reroll-data --data-dir $(DATA_DIR)
LIMIT_FLAG  = $(if $(LIMIT),--limit $(LIMIT),)
REROLL_WORKERS_FLAG = $(if $(REROLL_WORKERS),--workers $(REROLL_WORKERS),)

.PHONY: help status \
	db-init db2-backfill \
	refresh crawl sync-filenames sync-consistency \
	metadata-status metadata-sync metadata-fetch sync-metadata \
	reroll-convert reroll-status \
	refresh-mapping

help:
	@echo "Targets (override DATA_DIR, RATE, WORKERS, LIMIT as needed):"
	@echo "  db-init           create main.db/pypi.db (per-database schema, see reroll_data.db2)"
	@echo "  db2-backfill      one-off: migrate v.db's pypi index/metadata into main.db/pypi.db (resumable)"
	@echo "  sync-filenames    refresh + crawl -- discover and fetch .whl filenames (main.db/pypi.db)"
	@echo "  sync-consistency  rare: full reconciliation of main.db.wheel against pypi.db.pypi_index"
	@echo "  sync-metadata     metadata sync + fetch -- download METADATA bodies"
	@echo "  reroll-convert    run reroll's own translator over every outstanding main.db.wheel row, always retrying past errors + stale reroll_version (ordinary uv env)"
	@echo "  refresh-mapping   re-run reroll's mapper chain over pypi_conda_names, re-arming any affected wheel"
	@echo "  status            counts for the wheel/project crawl (pypi.db)"
	@echo "  metadata-status   counts for the metadata download"
	@echo "  reroll-status     reroll's own conversion counts by error category, + coverage %"
	@echo
	@echo "Finer-grained steps: refresh, crawl, metadata-sync, metadata-fetch"

status:
	$(RUN) status

metadata-status:
	$(RUN) metadata status

reroll-status:
	$(RUN) reroll-status

# --------------------------------------------------------------------------- #
# db: create main.db/pypi.db, the per-database schema (see reroll_data.db2)
# --------------------------------------------------------------------------- #

# One-off / idempotent: creates main.db and pypi.db under DATA_DIR if missing,
# and only verifies (never alters or drops) either one if it already exists --
# see reroll_data.db2's module docstring and SchemaMismatch.
db-init:
	$(RUN) db init

# One-off, idempotent, resumable (see reroll_data.db2_backfill): copies the
# pypi-index/metadata halves of the legacy v.db corpus into main.db/pypi.db.
# Never touches v.db (read-only); never copies reroll_data/repodata_conversion
# or metadata_blob.parsed_json -- see that module's docstring. Safe to
# interrupt and re-run; each step picks up exactly where it left off.
db2-backfill:
	uv run python -m reroll_data.db2_backfill --db $(DB) --data-dir $(DATA_DIR) $(LIMIT_FLAG)

# --------------------------------------------------------------------------- #
# filenames: discover every .whl on PyPI, into main.db/pypi.db
# (see reroll_data.crawl)
# --------------------------------------------------------------------------- #

refresh:
	$(RUN) refresh

crawl:
	$(RUN) crawl --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# refresh deletes any project the index no longer reports (from both
# pypi.db and main.db) and queues whatever the root index reports as
# changed; crawl then drains that queue, mirroring each fetched project's
# wheels into main.db as it goes. Run as one target (rather than making
# crawl depend on refresh) so the order is fixed regardless of `make -j`.
sync-filenames:
	$(RUN) refresh
	$(RUN) crawl --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# Rare: a full reconciliation of main.db.wheel against pypi.db.pypi_index
# (two anti-joins -- add whatever main.db is missing, drop whatever it has
# that pypi.db no longer lists). sync-filenames's own incremental path never
# needs this; run it only for occasional hygiene or to recover after an
# error. See reroll_data.crawl.sync_consistency.
sync-consistency:
	$(RUN) sync-consistency

# --------------------------------------------------------------------------- #
# metadata: download PEP 658 core-metadata bodies (see reroll_data.metadata)
# --------------------------------------------------------------------------- #

metadata-sync:
	$(RUN) metadata sync

metadata-fetch:
	$(RUN) metadata fetch --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# sync reconciles pypi_index -> wheel_metadata (picks up anything crawl just
# added, and is a no-op once converged); fetch then drains it. Both idempotent
# and resumable -- safe to re-run after an interrupted fetch.
sync-metadata:
	$(RUN) metadata sync
	$(RUN) metadata fetch --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# --------------------------------------------------------------------------- #
# convert: reroll's own translator over main.db.wheel (see reroll_data.reroll_convert)
# --------------------------------------------------------------------------- #

# Runs reroll's own translator over every outstanding main.db.wheel row
# (main.db/pypi.db, see reroll_data.reroll_convert's module docstring),
# writing reroll_data/resolutions/conversion_status/requires_prerelease/
# reroll_version back per row. Idempotent and resumable -- safe to re-run
# after an interrupted pass, and safe to re-run once converged (a no-op
# scan). Runs in the ordinary uv env ($(RUN)): reroll is a normal, required
# dependency here, kept current by a plain `uv sync`.
#
# Always passes --retry-errors, so every run also re-arms and re-attempts
# rows left over from a previous non-ok conversion_status -- fine as a no-op
# when there are none, and means a `pypi_conda_names` curation pass or a
# reroll fix takes effect on the very next `make reroll-convert` with no
# separate step. Always passes --retry-stale-version too, so a `py-reroll`
# upgrade automatically re-attempts every row (including previously-`ok`
# ones) its own last run's reroll_version disagrees with. Deliberately does
# *not* pass --allow-pre: the job itself already retries a genuine
# pre-release rejection with allow_pre=True on its own (see
# reroll_data.reroll_convert's "Pre-release retry" docstring section) --
# forcing it on for every wheel up front would just mean paying for a second
# attempt reroll's own retry logic already limits to the wheels that
# actually need it.
reroll-convert:
	$(RUN) convert --retry-errors --retry-stale-version $(REROLL_WORKERS_FLAG) $(LIMIT_FLAG)

# --------------------------------------------------------------------------- #
# names: curate main.db.pypi_conda_names (see reroll_data.refresh_names)
# --------------------------------------------------------------------------- #

# Re-runs reroll's default mapper chain (built once for the whole run) over
# every pypi_conda_names row, replacing conda_name in place wherever it
# disagrees, then re-arms any main.db.wheel row whose resolutions used a
# name that changed. Idempotent but not incremental -- every run re-checks
# the whole table. Run on its own schedule (e.g. weekly), then
# `make reroll-convert` to pick up whatever this re-armed.
refresh-mapping:
	$(RUN) names refresh $(LIMIT_FLAG)
