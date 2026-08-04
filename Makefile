# Overridable on the command line, e.g. `make sync-metadata RATE=1800 WORKERS=12`.
DB      ?= data/v.db
RATE    ?= 1800
WORKERS ?= 12
# Set to cap a run, e.g. `make crawl LIMIT=500`, so a step can be tried at
# small scale before being let loose on the full corpus.
LIMIT   ?=

RUN        := uv run reroll-data --db $(DB)
LIMIT_FLAG  = $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: help status \
	refresh crawl sync-filenames \
	metadata-status metadata-sync metadata-fetch sync-metadata

help:
	@echo "Targets (override DB, RATE, WORKERS, LIMIT as needed):"
	@echo "  sync-filenames    refresh + crawl -- discover and fetch .whl filenames"
	@echo "  sync-metadata     metadata sync + fetch -- download METADATA bodies"
	@echo "  status            counts for the wheel/project crawl"
	@echo "  metadata-status   counts for the metadata download"
	@echo
	@echo "Finer-grained steps: refresh, crawl, metadata-sync, metadata-fetch"

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
