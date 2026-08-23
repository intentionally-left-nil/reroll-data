# Overridable on the command line, e.g. `make sync-metadata RATE=1800 WORKERS=12`.
DB      ?= data/v.db
RATE    ?= 1800
WORKERS ?= 12

# db-init (see reroll_data.db2): directory main.db/pypi.db are created under,
# alongside the legacy v.db above -- separate from DB since db2 is a
# per-database, not a single-file, schema. Defaults to the same directory
# `data/v.db` lives in.
DATA_DIR ?= data
# Set to cap a run, e.g. `make crawl LIMIT=500`, so a step can be tried at
# small scale before being let loose on the full corpus.
LIMIT   ?=

# Diagnostics (see reroll_data.investigate). PWORKERS is separate from WORKERS
# because they mean opposite things: WORKERS is politeness towards PyPI, while
# PWORKERS is local parallelism. 1 is right for a filename-only probe.
PROBE    ?= filename
OUT      ?= failures.csv
PACKAGE  ?=
PWORKERS ?= 1

# metadata backfill (see reroll_data.backfill): purely local CPU work, no
# network, so unlike WORKERS above there is no politeness reason to cap it.
# Left empty so the tool's own default (all cores, via
# os.process_cpu_count()) applies unless overridden, e.g.
# `make metadata-backfill BACKFILL_WORKERS=4`.
BACKFILL_WORKERS ?=

# retry-metadata-conversion (see reroll_data.retry_metadata_conversion): the
# single metadata_blob.sha256 digest to force re-parse, e.g.
# `make retry-metadata-conversion SHA256=abc123...`. No default -- there is no
# sensible one for a single-row one-off, so the target fails loudly if unset.
SHA256 ?=

# reroll-convert (see reroll_data.reroll_convert): purely local CPU work like
# metadata-backfill, runs in the ordinary uv venv -- `reroll` is an ordinary,
# required dependency, kept current by a plain `uv sync`. Left empty by
# default so the tool's own default (all cores) applies, e.g.
# `make reroll-convert REROLL_WORKERS=4`.
REROLL_WORKERS ?=

RUN        := uv run reroll-data --db $(DB) --data-dir $(DATA_DIR)
INVESTIGATE := uv run reroll-investigate --db $(DB)
LIMIT_FLAG  = $(if $(LIMIT),--limit $(LIMIT),)
PKG_FLAG    = $(if $(PACKAGE),--package $(PACKAGE),)
BACKFILL_WORKERS_FLAG = $(if $(BACKFILL_WORKERS),--workers $(BACKFILL_WORKERS),)
REROLL_WORKERS_FLAG = $(if $(REROLL_WORKERS),--workers $(REROLL_WORKERS),)

.PHONY: help status \
	db-init db2-backfill \
	refresh crawl sync-filenames sync-consistency \
	metadata-status metadata-sync metadata-fetch sync-metadata metadata-backfill \
	retry-metadata-conversion \
	repodata-status sync-repodata reroll-convert \
	reroll-status \
	investigate

help:
	@echo "Targets (override DB, RATE, WORKERS, LIMIT as needed):"
	@echo "  db-init           create main.db/pypi.db (new per-database schema, see reroll_data.db2)"
	@echo "  db2-backfill      one-off: migrate v.db's pypi index/metadata into main.db/pypi.db (resumable)"
	@echo "  sync-filenames    refresh + crawl -- discover and fetch .whl filenames (main.db/pypi.db)"
	@echo "  sync-consistency  rare: full reconciliation of main.db.wheel against pypi.db.pypi_index"
	@echo "  sync-metadata     metadata sync + fetch -- download METADATA bodies"
	@echo "  sync-repodata     reconcile wheel -> repodata_conversion (local, no network)"
	@echo "  reroll-convert    run reroll's own translator over every outstanding main.db.wheel row, always retrying past errors + stale reroll_version (ordinary uv env)"
	@echo "  status            counts for the wheel/project crawl (legacy v.db)"
	@echo "  metadata-status   counts for the metadata download"
	@echo "  repodata-status   counts for the legacy repodata_conversion table (reroll's own historical conversion stats)"
	@echo "  reroll-status     reroll's own conversion counts by error category, + coverage %"
	@echo "  metadata-backfill one-off: parse stored bodies into parsed_json (local, no network)"
	@echo "  retry-metadata-conversion  one-off: force re-parse one blob's parsed_json (needs SHA256)"
	@echo
	@echo "Finer-grained steps: refresh, crawl, metadata-sync, metadata-fetch"
	@echo
	@echo "Diagnostics (override PROBE, OUT, PACKAGE, LIMIT, PWORKERS):"
	@echo "  probes:           $(shell ls probes/*.py 2>/dev/null | xargs -n1 basename | sed 's/\.py$$//' | tr '\n' ' ')"
	@echo "  investigate       probe every wheel, write $(OUT)"

status:
	$(RUN) status

metadata-status:
	$(RUN) metadata status

repodata-status:
	$(RUN) repodata status

reroll-status:
	$(RUN) repodata reroll-status

# --------------------------------------------------------------------------- #
# db: create main.db/pypi.db, the new per-database schema (see reroll_data.db2)
# --------------------------------------------------------------------------- #

# One-off / idempotent: creates main.db and pypi.db under DATA_DIR if missing,
# and only verifies (never alters or drops) either one if it already exists --
# see reroll_data.db2's module docstring and SchemaMismatch. Deliberately does
# not touch the existing v.db every other target here uses; this is purely
# additive as part of the migration to the new schema.
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
# (see reroll_data.crawl; targets the new per-database schema exclusively --
# refresh/crawl no longer touch the legacy v.db at all)
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
# and resumable -- safe to re-run after an interrupted fetch. Both write to
# pypi.db (DATA_DIR), not the legacy v.db (DB).
sync-metadata:
	$(RUN) metadata sync
	$(RUN) metadata fetch --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# One-off: parse already-stored bodies into metadata_blob.parsed_json. Local
# only (no network, no RATE/WORKERS), idempotent and resumable the same way
# the rest of the metadata pipeline is -- safe to re-run after Ctrl-C.
metadata-backfill:
	$(RUN) metadata backfill $(BACKFILL_WORKERS_FLAG) $(LIMIT_FLAG)

# One-off: force re-parse a single metadata_blob row, overwriting parsed_json
# even if it is already set -- unlike metadata-backfill above, which skips
# rows that already have one. For re-running one row after a parser fix.
retry-metadata-conversion:
	@if [ -z "$(SHA256)" ]; then \
		echo "usage: make retry-metadata-conversion SHA256=<metadata_blob.sha256>" >&2; \
		exit 1; \
	fi
	$(RUN) metadata retry-conversion $(SHA256)

# --------------------------------------------------------------------------- #
# repodata: legacy v.db repodata_conversion bookkeeping
# (see reroll_data.repodata_sync). Originally also compared reroll's
# translator against upstream conda-pypi's; that comparison (and the pixi
# environment it needed) has been removed -- see git history if it is ever
# worth reviving. What is left is purely reroll's own historical conversion
# data (reroll-status below) plus the sync bookkeeping that still tags new
# rows.
# --------------------------------------------------------------------------- #

# Idempotent and resumable, same as metadata-sync -- picks up anything crawl
# just added, and is a no-op once converged. Purely local (no RATE/WORKERS):
# this only creates rows in the legacy repodata_conversion table.
sync-repodata:
	$(RUN) repodata sync

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
# diagnostics: run a probe over the corpus (see reroll_data.investigate)
# --------------------------------------------------------------------------- #

investigate:
	$(INVESTIGATE) --probe $(PROBE) -o $(OUT) --workers $(PWORKERS) \
		$(LIMIT_FLAG) $(PKG_FLAG)
