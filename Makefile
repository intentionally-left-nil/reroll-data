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

RUN        := uv run reroll-data --db $(DB)
INVESTIGATE := uv run reroll-investigate --db $(DB)
LIMIT_FLAG  = $(if $(LIMIT),--limit $(LIMIT),)
PKG_FLAG    = $(if $(PACKAGE),--package $(PACKAGE),)

.PHONY: help status \
	refresh crawl sync-filenames \
	metadata-status metadata-sync metadata-fetch sync-metadata \
	investigate sync-probe

help:
	@echo "Targets (override DB, RATE, WORKERS, LIMIT as needed):"
	@echo "  sync-filenames    refresh + crawl -- discover and fetch .whl filenames"
	@echo "  sync-metadata     metadata sync + fetch -- download METADATA bodies"
	@echo "  status            counts for the wheel/project crawl"
	@echo "  metadata-status   counts for the metadata download"
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
