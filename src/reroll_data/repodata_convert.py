"""Run conda-pypi's own repodata translator over every compatible wheel.

Environment: this must run *inside* conda-pypi's pixi environment
-------------------------------------------------------------------
``conda_pypi`` is a conda plugin: it (transitively) needs the real ``conda``
package, which is not pip-installable and is therefore never part of this
repo's ordinary uv environment (see :mod:`reroll_data.conda_pypi_index_demo`'s
module docstring, which covers a one-off single-wheel lookup the same way).

For a one-off lookup, reaching across to a *different* interpreter for that
one call is fine. It is not fine to do that per wheel across ~12M rows: each
call would pay conda_pypi's own (non-trivial: conda, conda_index, packaging,
...) import cost and a fresh interpreter start/stop, and shuttling every
result back over a pipe or subprocess boundary adds IPC and version-drift
risk between two separately-installed copies of this same package's code.

So instead, this project (and ``reroll``, its comparison partner -- see
``reroll_data.metadata``) gets pip-installed *editable* directly into
conda-pypi's own pixi environment, via the ``[tool.pixi.*]`` tables in this
repo's ``pyproject.toml``. That gives the ordinary ``reroll-data`` console
script a second, fully working home: run from inside that environment
(``pixi run --manifest-path pyproject.toml reroll-data repodata convert``, or
just ``make repodata-convert``, which does the same), ``import conda_pypi``
just works, with zero cross-interpreter calls anywhere in this module.

That environment is created once (`pixi install`, or the first `pixi run`)
and only ever reused after that -- solving/installing it again is what would
be slow, not running it. Nothing in this module ever creates or touches that
environment; it assumes it already exists and simply imports what it needs
from it, exactly like any other dependency.

Concurrency: process pool, no subprocess-per-call
--------------------------------------------------
Once running inside that one environment, every worker is an ordinary
``ProcessPoolExecutor`` process forked/spawned from *the same* interpreter --
there is no second, differently-installed Python involved anywhere in this
run, so there is no ABI or version-drift risk to manage. This is therefore
architecturally identical to :mod:`reroll_data.backfill`: purely CPU-bound
local work (packaging's email parser plus conda-pypi's own translation, no
network, no rate limit), so the pool is sized to the machine's cores by
default and each worker keeps one read-only connection open across every
wheel routed to it (see `_init_worker`), rather than reconnecting per row.

Idempotency and resumability
-----------------------------
Mirrors :func:`reroll_data.backfill.backfill_parsed`: work is selected with
``WHERE conda_pypi_compatible = 1 AND conda_pypi_data IS NULL AND
conda_pypi_error IS NULL``, so a row that already succeeded or already failed
simply stops matching -- no lease/claim machinery is needed the way
:mod:`reroll_data.metadata` needs one, because there is no network request
that could be left dangling mid-flight. An interrupted run just leaves more
rows for the next one to pick up; :func:`reset_errors` re-arms rows that
failed (mirroring :func:`reroll_data.metadata.reset_errors`) for another try
after a translator fix.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import db as _db
from . import conda_pypi_index_demo as _demo

#: Rows pulled from `repodata_conversion` per round trip, and roughly the unit
#: of work between progress reports / commits. Mirrors `backfill.READ_BATCH`.
READ_BATCH = 2000

#: Wheels handed to a single worker-process call at a time. Smaller than
#: `backfill.CHUNKSIZE` (64): each task here does far more work per item (a
#: full conda-pypi translation: parse METADATA, build the PyPI-API dict, run
#: `pypi_to_repodata`) than a zlib decompress, so a smaller chunk keeps one
#: slow wheel from stalling the pool for as long.
CHUNKSIZE = 16

#: Rows committed per write transaction. Independent of READ_BATCH/CHUNKSIZE
#: so commit frequency can be tuned without touching pool scheduling.
WRITE_BATCH = 500

# Set once per worker process by `_init_worker`; never touched by the main
# process. A read-only connection held open for the process's lifetime, so
# `_convert_one` avoids a fresh sqlite3.connect()/close() per wheel -- the
# same optimization `reroll_data.metadata.fetch`'s per-worker `probe`
# connection makes, just across a process boundary instead of a thread.
_READ_DB: sqlite3.Connection | None = None


def _init_worker(db_path: str) -> None:
    """`ProcessPoolExecutor` initializer: open this worker's own connection."""
    global _READ_DB
    _READ_DB = _db.connect(db_path, read_only=True)


def _convert_one(item: tuple[str, str]) -> tuple[str, str, str | None, str | None]:
    """Run in a worker process: convert one wheel.

    Returns `(project, filename, data_json, error)` -- exactly one of the
    last two is non-None. Any failure (a real `NotPureWheel`/`WheelNotFound`/
    etc, or something conda-pypi itself raises on a malformed METADATA body)
    is caught here and turned into data rather than propagated: one bad wheel
    must not take down the pool or the rest of the batch.
    """
    project, filename = item
    assert _READ_DB is not None, "worker process not initialized"
    try:
        entry = _demo._entry_from_db(_READ_DB, filename, project=project)
    except Exception as exc:  # noqa: BLE001 - any conversion failure is data
        return project, filename, None, f"{type(exc).__name__}: {exc}"[:1000]
    return project, filename, json.dumps(entry, separators=(",", ":")), None


def reset_errors(db: sqlite3.Connection) -> int:
    """Re-arm rows with a `conda_pypi_error` for another attempt.

    Mirrors :func:`reroll_data.metadata.reset_errors`: :func:`convert`'s
    selection predicate excludes any row with `conda_pypi_error IS NOT NULL`,
    so a wheel that failed once is otherwise skipped forever. Only relevant
    after a translator fix -- re-running `convert()` unmodified would just
    reproduce the same errors.
    """
    now = int(time.time())
    db.execute("BEGIN IMMEDIATE")
    try:
        n = (
            db.execute(
                "UPDATE repodata_conversion SET conda_pypi_error = NULL, "
                "updated_at = ? WHERE conda_pypi_error IS NOT NULL",
                (now,),
            ).rowcount
            or 0
        )
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return n


def convert(
    db_path: Path,
    *,
    workers: int | None = None,
    read_batch: int = READ_BATCH,
    chunksize: int = CHUNKSIZE,
    write_batch: int = WRITE_BATCH,
    limit: int | None = None,
    progress_every: float = 5.0,
) -> dict:
    """Populate `conda_pypi_data`/`conda_pypi_error` for every outstanding row.

    `workers` defaults to the machine's core count (`os.process_cpu_count()`)
    since this is purely CPU-bound local work with no politeness constraint,
    unlike `reroll_data.metadata.fetch`.

    Raises `RuntimeError` immediately, before starting the pool, if
    `conda_pypi` is not importable -- i.e. this was run from the ordinary uv
    environment instead of conda-pypi's pixi one (see the module docstring).
    Letting every task discover that independently would just print the same
    "wrong environment" error `conda_pypi_compatible or 0` times.
    """
    if _demo.package_metadata_from_metadata_body is None or _demo.pypi_to_repodata is None:
        raise RuntimeError(
            "conda_pypi is not importable in this environment -- this command "
            "must run from inside conda-pypi's own pixi environment, e.g. "
            "`make repodata-convert`, or `pixi run --manifest-path pyproject.toml "
            "reroll-data --db ... repodata convert`. See the "
            f"{__name__} module docstring."
        )

    workers = workers or os.process_cpu_count() or os.cpu_count() or 1

    # Two connections, one each way, mirroring the single-writer model the
    # rest of this codebase uses (see reroll_data.db's module docstring).
    read_db = _db.connect(db_path, read_only=True)
    write_db = _db.connect(db_path)

    table_total = read_db.execute(
        "SELECT count(*) FROM repodata_conversion WHERE conda_pypi_compatible = 1"
    ).fetchone()[0]
    outstanding_before = read_db.execute(
        "SELECT count(*) FROM repodata_conversion WHERE conda_pypi_compatible = 1 "
        "AND conda_pypi_data IS NULL AND conda_pypi_error IS NULL"
    ).fetchone()[0]
    total = outstanding_before if limit is None else min(outstanding_before, limit)
    coverage_before = (
        (table_total - outstanding_before) / table_total * 100 if table_total else 0.0
    )

    print(
        f"converting {total:,} wheel(s) with {workers} worker process(es) "
        f"({coverage_before:.1f}% of {table_total:,} conda-pypi-compatible "
        "wheels already attempted) ...",
        file=sys.stderr,
    )

    counters = {"ok": 0, "error": 0}
    started = time.monotonic()
    next_report = started + progress_every
    interrupted = False
    remaining_limit = limit
    check_wal = _db.wal_monitor(db_path)

    pending_writes: list[tuple[str, str, str | None, str | None]] = []
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if not pending_writes:
            last_flush = time.monotonic()
            return
        now = int(time.time())
        write_db.execute("BEGIN IMMEDIATE")
        try:
            write_db.executemany(
                "UPDATE repodata_conversion SET conda_pypi_data = ?, "
                "conda_pypi_error = ?, updated_at = ? "
                "WHERE project = ? AND filename = ?",
                [(d, e, now, p, f) for (p, f, d, e) in pending_writes],
            )
            write_db.execute("COMMIT")
        except BaseException:
            write_db.execute("ROLLBACK")
            raise
        pending_writes.clear()
        last_flush = time.monotonic()

    def report(force: bool = False) -> None:
        nonlocal next_report
        now = time.monotonic()
        if not force and now < next_report:
            return
        next_report = now + progress_every
        check_wal()
        done = counters["ok"] + counters["error"]
        remaining = max(total - done, 0)
        elapsed = now - started
        rpm = done / elapsed * 60 if elapsed else 0.0
        attempted = table_total - outstanding_before + done
        coverage = attempted / table_total * 100 if table_total else 0.0
        print(
            f"  {done:>9,}/{total:,} converted  {remaining:>9,} remaining  "
            f"{counters['error']:>7,} errors  {rpm:8.0f} wheels/min  "
            f"{coverage:5.1f}% attempted",
            file=sys.stderr,
        )

    try:
        with ProcessPoolExecutor(
            max_workers=workers, initializer=_init_worker, initargs=(str(db_path),)
        ) as pool:
            while True:
                n = read_batch if remaining_limit is None else min(
                    read_batch, remaining_limit
                )
                if n <= 0:
                    break
                # Re-issued fresh every batch (and fully drained by
                # `fetchall()`) rather than one cursor held open across the
                # whole run -- see `reroll_data.db.wal_monitor`'s docstring
                # for why a long-lived `SELECT` pins the WAL and blocks every
                # checkpoint until the run ends. Already-converted rows drop
                # out of this WHERE clause as soon as `flush()` commits them.
                rows = read_db.execute(
                    "SELECT project, filename FROM repodata_conversion "
                    "WHERE conda_pypi_compatible = 1 "
                    "AND conda_pypi_data IS NULL AND conda_pypi_error IS NULL "
                    "LIMIT ?",
                    (n,),
                ).fetchall()
                if not rows:
                    break
                if remaining_limit is not None:
                    remaining_limit -= len(rows)

                for project, filename, data_json, error in pool.map(
                    _convert_one, rows, chunksize=chunksize
                ):
                    counters["error" if error else "ok"] += 1
                    pending_writes.append((project, filename, data_json, error))
                    if len(pending_writes) >= write_batch:
                        flush()
                flush()
                report()
    except KeyboardInterrupt:
        interrupted = True
        print(
            "\n  interrupted -- already-committed rows stay done; re-run to "
            "pick up where this left off.",
            file=sys.stderr,
        )
    finally:
        flush()
        report(force=True)
        read_db.close()
        write_db.close()

    counters["interrupted"] = interrupted
    return counters
