# Overridable on the command line, e.g. `make sync-metadata RATE=1800 WORKERS=12`.
DB      ?= data/v.db
RATE    ?= 1800
WORKERS ?= 12
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

# repodata convert (see reroll_data.repodata_convert): runs inside
# conda-pypi's own pixi environment (see pyproject.toml's [tool.pixi.*]
# tables), not the ordinary uv venv -- see CONVERT below. Purely local CPU
# work like metadata-backfill, so CONVERT_WORKERS is left empty by default so
# the tool's own default (all cores) applies, e.g.
# `make repodata-convert CONVERT_WORKERS=4`.
CONVERT_WORKERS ?=

# reroll-convert (see reroll_data.reroll_convert): purely local CPU work like
# repodata-convert above, but runs in the ordinary uv venv, not the pixi one
# -- `reroll` (unlike `conda_pypi`) is an ordinary optional dependency, see
# `sync-probe` below. Left empty by default so the tool's own default (all
# cores) applies, e.g. `make reroll-convert REROLL_WORKERS=4`.
REROLL_WORKERS ?=

RUN        := uv run reroll-data --db $(DB)
INVESTIGATE := uv run reroll-investigate --db $(DB)
# Runs the same `reroll-data` console script, but from inside the pixi
# environment `pyproject.toml` describes rather than the uv one `RUN` uses --
# that is what makes `import conda_pypi` work. `pixi run` itself also
# auto-installs/syncs that environment if it is missing or stale, but
# `repodata-convert` below depends on `repodata-convert-env` explicitly
# anyway, rather than relying on that implicit behavior.
CONVERT    := pixi run --manifest-path pyproject.toml reroll-data --db $(DB)
LIMIT_FLAG  = $(if $(LIMIT),--limit $(LIMIT),)
PKG_FLAG    = $(if $(PACKAGE),--package $(PACKAGE),)
BACKFILL_WORKERS_FLAG = $(if $(BACKFILL_WORKERS),--workers $(BACKFILL_WORKERS),)
CONVERT_WORKERS_FLAG = $(if $(CONVERT_WORKERS),--workers $(CONVERT_WORKERS),)
REROLL_WORKERS_FLAG = $(if $(REROLL_WORKERS),--workers $(REROLL_WORKERS),)

.PHONY: help status \
	refresh crawl sync-filenames \
	metadata-status metadata-sync metadata-fetch sync-metadata metadata-backfill \
	retry-metadata-conversion \
	repodata-status sync-repodata repodata-convert-env repodata-convert reroll-convert \
	investigate sync-probe

help:
	@echo "Targets (override DB, RATE, WORKERS, LIMIT as needed):"
	@echo "  sync-filenames    refresh + crawl -- discover and fetch .whl filenames"
	@echo "  sync-metadata     metadata sync + fetch -- download METADATA bodies"
	@echo "  sync-repodata     reconcile wheel -> repodata_conversion (local, no network)"
	@echo "  repodata-convert  run conda-pypi's translator over compatible wheels (needs pixi env)"
	@echo "  reroll-convert    run reroll's own translator over every wheel, always retrying past errors (ordinary uv env)"
	@echo "  status            counts for the wheel/project crawl"
	@echo "  metadata-status   counts for the metadata download"
	@echo "  repodata-status   counts for the reroll-vs-conda-pypi repodata comparison"
	@echo "  metadata-backfill one-off: parse stored bodies into parsed_json (local, no network)"
	@echo "  retry-metadata-conversion  one-off: force re-parse one blob's parsed_json (needs SHA256)"
	@echo
	@echo "Finer-grained steps: refresh, crawl, metadata-sync, metadata-fetch"
	@echo
	@echo "Diagnostics (override PROBE, OUT, PACKAGE, LIMIT, PWORKERS):"
	@echo "  probes:           $(shell ls probes/*.py 2>/dev/null | xargs -n1 basename | sed 's/\.py$$//' | tr '\n' ' ')"
	@echo "  investigate       probe every wheel, write $(OUT)"
	@echo "  sync-probe        reinstall ../reroll (only needed if its deps changed)"

status:
	$(RUN) status

metadata-status:
	$(RUN) metadata status

repodata-status:
	$(RUN) repodata status

# --------------------------------------------------------------------------- #
# filenames: discover every .whl on PyPI (see reroll_data.crawl)
# --------------------------------------------------------------------------- #

refresh:
	$(RUN) refresh

crawl:
	$(RUN) crawl --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# refresh queues whatever the root index reports as changed; crawl then drains
# that queue. Run as one target (rather than making crawl depend on refresh)
# so the order is fixed regardless of `make -j`.
sync-filenames:
	$(RUN) refresh
	$(RUN) crawl --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# --------------------------------------------------------------------------- #
# metadata: download PEP 658 core-metadata bodies (see reroll_data.metadata)
# --------------------------------------------------------------------------- #

metadata-sync:
	$(RUN) metadata sync

metadata-fetch:
	$(RUN) metadata fetch --rate $(RATE) --workers $(WORKERS) $(LIMIT_FLAG)

# sync reconciles wheel -> wheel_metadata (picks up anything crawl just added,
# and is a no-op once converged); fetch then drains it. Both idempotent and
# resumable -- safe to re-run after an interrupted fetch.
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
# repodata: compare reroll's vs conda-pypi's repodata conversion
# (see reroll_data.repodata_sync)
# --------------------------------------------------------------------------- #

# Idempotent and resumable, same as metadata-sync -- picks up anything crawl
# just added, and is a no-op once converged. Purely local (no RATE/WORKERS):
# this only creates rows and computes conda_pypi_compatible from filenames
# already in `wheel`, it does not run either converter.
sync-repodata:
	$(RUN) repodata sync

# One-time (or after pyproject.toml's [tool.pixi.*] tables change): solve and
# install conda-pypi's pixi environment with this project (and reroll)
# pip-installed editable into it. A prerequisite of repodata-convert below
# (not just something to remember to run first) so the environment is always
# created or synced automatically -- cheap and safe to depend on unconditionally:
# `pixi install` no-ops in well under a second once the lockfile already
# matches pyproject.toml, and only actually solves/reinstalls when something
# relevant changed. Safe to also run by hand; same idempotent behavior either way.
repodata-convert-env:
	pixi install --manifest-path pyproject.toml

# Runs conda-pypi's own translator over every wheel repodata-sync marked
# conda_pypi_compatible, writing conda_pypi_data/conda_pypi_error back per
# row. Idempotent and resumable like metadata-fetch -- safe to re-run after an
# interrupted pass, and safe to re-run once converged (a no-op scan). Purely
# local CPU work (no RATE): CONVERT_WORKERS defaults to the tool's own
# default (all cores). Depends on repodata-convert-env, so the pixi
# environment is always up to date before this runs -- never assumed.
repodata-convert: repodata-convert-env
	$(CONVERT) repodata convert $(CONVERT_WORKERS_FLAG) $(LIMIT_FLAG)

# Runs reroll's own translator over every wheel in repodata_conversion, no
# compatibility pre-filter (see reroll_data.reroll_convert's module
# docstring), writing reroll_data/reroll_error back per row. Idempotent and
# resumable like repodata-convert above -- safe to re-run after an
# interrupted pass, and safe to re-run once converged (a no-op scan). Runs in
# the ordinary uv env ($(RUN)), not the pixi one: unlike conda_pypi, reroll
# is a normal (if optional) dependency here -- see `sync-probe`. Always
# passes --retry-errors, so every run also re-arms and re-attempts rows left
# over from a previous reroll_error -- fine as a no-op when there are none,
# and means a reroll fix takes effect on the very next `make reroll-convert`
# with no separate step. Always passes --allow-pre too: this pipeline builds
# an archival mirror of the whole PyPI corpus, which is exactly the
# "channel should include -alpha/-beta/-rc packages" case reroll's own
# allow_pre docs (matchspec.md) describe as the intended reason to turn it
# on -- without it, reroll rejects every pre-release wheel as out of scope
# before it ever reaches the interpreter/platform checks, which is not what
# a full-corpus conversion should do by default.
reroll-convert:
	$(RUN) repodata reroll-convert --retry-errors --allow-pre $(REROLL_WORKERS_FLAG) $(LIMIT_FLAG)

# --------------------------------------------------------------------------- #
# diagnostics: run a probe over the corpus (see reroll_data.investigate)
# --------------------------------------------------------------------------- #

investigate:
	$(INVESTIGATE) --probe $(PROBE) -o $(OUT) --workers $(PWORKERS) \
		$(LIMIT_FLAG) $(PKG_FLAG)

# ../reroll is installed editable, so ordinary source edits need nothing. This
# is only for when reroll's own dependencies change.
sync-probe:
	uv sync --group probe
