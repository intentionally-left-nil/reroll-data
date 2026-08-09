"""One-off backfill of `metadata_blob.parsed_json`.

`reroll_data.metadata` only populates `parsed_json` for bodies fetched from
here on (see its module docstring); rows stored before that column existed --
the bulk of a multi-day corpus -- are left with `parsed_json IS NULL`. This
module fills those in.

Everything this needs is already on disk in `metadata_blob`: no network
request is made, so unlike :mod:`reroll_data.metadata` there is nothing to
rate limit. The only cost is local CPU (zlib decompression, `packaging`'s
email parser, pydantic validation) and disk I/O reading `z_body` back out.

Parsing is CPU-bound pure-Python work, which the GIL serialises within a
single process -- threads would not use more than one core for it. This uses
a `ProcessPoolExecutor` instead, sized to the machine's cores by default, so
a run actually saturates them rather than being limited to one.

Resumable the same way the rest of the module is, without any extra state:
work is selected with `WHERE parsed_json IS NULL`, so a row that already got
a value (success) simply stops matching, and an interrupted run just leaves
more rows for the next one to pick up. Rows that fail to parse are also left
NULL (not marked some other way -- there is no error-tracking column here,
unlike `wheel_metadata`), so they are retried on every subsequent run; that
is deliberate for a one-off tool, since a real parse bug needs a code fix
before retrying would help anyway.
"""

from __future__ import annotations

import os
import sys
import time
import zlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import db as _db
from . import metadata as _metadata

#: Rows pulled from `metadata_blob` per round trip, and roughly the unit of
#: work between progress reports / commits. Large enough that a commit is a
#: rare event relative to parsing, small enough that a `ProcessPoolExecutor`
#: (which -- see `pool.map`'s docs -- keeps the whole batch's tasks and
#: results in memory) never has to hold more than one batch's worth of
#: decompressed-body-sized data at a time.
READ_BATCH = 4000

#: Rows handed to a single worker-process call at a time. Bigger than 1 so
#: that the (small, but nonzero for ~7M rows) per-task IPC overhead is paid
#: once per chunk rather than once per row, without making any one chunk slow
#: enough to stall the pool waiting for a straggler.
CHUNKSIZE = 64


def _parse_blob(row: tuple[int, str, bytes]) -> tuple[int, str | None]:
    """Run in a worker process: decompress and parse one stored body.

    Returns `(id, parsed_json)`; `parsed_json` is None on any failure
    (corrupt/undecompressible `z_body`, undecodable body, or a METADATA the
    parser rejects) -- the caller counts that as an error and leaves the
    row's `parsed_json` untouched so a later run retries it.
    """
    row_id, sha256, z_body = row
    try:
        raw = zlib.decompress(z_body)
    except zlib.error as exc:
        print(f"  ! backfill decompress failed for {sha256}: {exc}", file=sys.stdout)
        return row_id, None
    return row_id, _metadata._parse_metadata_json(raw, context=sha256)


def backfill_parsed(
    db_path: Path,
    *,
    workers: int | None = None,
    read_batch: int = READ_BATCH,
    limit: int | None = None,
    progress_every: float = 5.0,
) -> dict:
    """Populate `parsed_json` for every `metadata_blob` row missing it.

    `workers` defaults to the machine's core count (`os.process_cpu_count()`)
    since this is purely CPU-bound local work with no politeness constraint
    towards a remote server, unlike `metadata.fetch`.
    """
    if _metadata.parse_metadata is None:
        # Every row would otherwise "fail" identically and get logged/counted
        # as an error, which is a misleading way to say "wrong environment".
        raise RuntimeError(
            "reroll is not importable in this environment -- install the "
            "'probe' dependency group (`uv sync --group probe`) before "
            "running the backfill."
        )

    workers = workers or os.process_cpu_count() or os.cpu_count() or 1

    # Two connections, one each way, mirroring the single-writer model the
    # rest of this codebase uses (see reroll_data.db's module docstring):
    # many readers are fine, but only one connection should ever write.
    read_db = _db.connect(db_path, read_only=True)
    write_db = _db.connect(db_path)

    # `table_total`/`missing_before` are the whole-table baseline, used only
    # to report overall coverage (what fraction of *every* row in
    # `metadata_blob` has parsed_json, not just this run's slice of it).
    # `total` stays the run-scoped count -- clamped to `limit` -- that the
    # rest of this function already used for its own progress/remaining math.
    table_total = read_db.execute("SELECT count(*) FROM metadata_blob").fetchone()[0]
    missing_before = read_db.execute(
        "SELECT count(*) FROM metadata_blob WHERE parsed_json IS NULL"
    ).fetchone()[0]
    total = missing_before if limit is None else min(missing_before, limit)
    coverage_before = (
        (table_total - missing_before) / table_total * 100 if table_total else 0.0
    )

    print(
        f"backfilling {total:,} row(s) missing parsed_json with {workers} "
        f"worker process(es) ({coverage_before:.1f}% of {table_total:,} rows "
        f"already covered) ...",
        file=sys.stderr,
    )

    counters = {"processed": 0, "errors": 0}
    started = time.monotonic()
    next_report = started + progress_every
    interrupted = False
    remaining_limit = limit

    cur = read_db.execute(
        "SELECT id, sha256, z_body FROM metadata_blob WHERE parsed_json IS NULL"
    )

    def report(force: bool = False) -> None:
        nonlocal next_report
        now = time.monotonic()
        if not force and now < next_report:
            return
        next_report = now + progress_every
        done = counters["processed"]
        success = done - counters["errors"]
        remaining = max(total - done, 0)
        elapsed = now - started
        rpm = done / elapsed * 60 if elapsed else 0.0
        # Rows this run has *actually* newly covered (errors leave a row
        # still NULL, so they do not count) against the whole table, not
        # just this run's slice of it -- see `table_total`/`missing_before`
        # above.
        covered = table_total - missing_before + success
        coverage = covered / table_total * 100 if table_total else 0.0
        print(
            f"  {done:>9,}/{total:,} processed  {remaining:>9,} remaining  "
            f"{counters['errors']:>7,} errors  {rpm:8.0f} rows/min  "
            f"{coverage:5.1f}% covered",
            file=sys.stderr,
        )

    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            while True:
                n = read_batch if remaining_limit is None else min(
                    read_batch, remaining_limit
                )
                if n <= 0:
                    break
                rows = cur.fetchmany(n)
                if not rows:
                    break
                if remaining_limit is not None:
                    remaining_limit -= len(rows)

                updates = []
                for row_id, parsed_json in pool.map(
                    _parse_blob, rows, chunksize=CHUNKSIZE
                ):
                    counters["processed"] += 1
                    if parsed_json is not None:
                        updates.append((parsed_json, row_id))
                    else:
                        counters["errors"] += 1

                if updates:
                    # One transaction per read batch, not per row -- same
                    # trade-off `reroll_data.metadata._writer` makes: enough
                    # rows per commit to amortise fsync overhead, small
                    # enough that a crash mid-run loses at most one batch.
                    write_db.execute("BEGIN IMMEDIATE")
                    try:
                        write_db.executemany(
                            "UPDATE metadata_blob SET parsed_json = ? WHERE id = ?",
                            updates,
                        )
                        write_db.execute("COMMIT")
                    except BaseException:
                        write_db.execute("ROLLBACK")
                        raise

                report()
    except KeyboardInterrupt:
        interrupted = True
        print(
            "\n  interrupted -- already-committed rows stay done; re-run to "
            "pick up where this left off.",
            file=sys.stderr,
        )
    finally:
        report(force=True)
        cur.close()
        read_db.close()
        write_db.close()

    counters["interrupted"] = interrupted
    return counters
