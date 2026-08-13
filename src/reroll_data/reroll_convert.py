"""Run reroll's own wheel-to-repodata translator over every corpus wheel.

Mirrors :mod:`reroll_data.repodata_convert` (the conda-pypi comparison job),
with two deliberate differences:

No compatibility pre-filter
----------------------------
conda-pypi only ever converts pure-Python ``*-none-any.whl`` wheels, so
:mod:`reroll_data.repodata_convert` selects
``WHERE conda_pypi_compatible = 1`` up front (that flag is computed once,
from the filename alone, by :mod:`reroll_data.repodata_sync`). reroll's own
scope is much wider -- CPython (and generic ``py3``) wheels from 3.4 up,
across every platform it recognises, not just noarch -- so this module
selects every row instead: ``WHERE reroll_data IS NULL AND reroll_error IS
NULL``, full stop. A wheel outside even reroll's broader scope still gets a
row here, just one whose `reroll_error` starts with the ``scope`` category
(see below) rather than being silently skipped by a pre-filter that would
have hidden that count entirely.

Ordinary uv environment, not a second pixi one
-------------------------------------------------
Unlike ``conda_pypi`` (a conda plugin, needing the real ``conda``, which is
not pip-installable), ``reroll`` is an ordinary optional dependency of this
project -- see ``pyproject.toml``'s ``probe`` group -- so this runs from the
regular ``uv`` environment (``uv sync --group probe`` once), no pixi
environment involved anywhere.

Skipping wheel parsing: hooking into ``reroll.stages``
--------------------------------------------------------
``reroll()`` itself expects a real ``.whl`` file on disk, to unzip its
``*.dist-info/METADATA`` out (``reroll.stages.extract_metadata_file``). This
corpus already stores that exact body -- the PEP 658 sidecar, see
:mod:`reroll_data.metadata` -- so :func:`reroll_index_demo._entry_from_db`
(reused here, one call per wheel, exactly like
:mod:`reroll_data.repodata_convert` reuses
:mod:`reroll_data.conda_pypi_index_demo`'s) calls ``reroll.stages``'
``parse_metadata`` and ``get_wheel_records`` directly on the stored text,
never ``extract_metadata_file``, and therefore never needs the actual wheel
bytes at all.

Error categories, not just messages
-------------------------------------
Every failure reroll raises falls into exactly one of four documented
categories (``docs/errors_and_logging.md`` in the reroll checkout):
``RerollScopeError``, ``RerollInvalidWheelError``, ``RerollUnconvertableError``,
``RerollRuntimeError``. :func:`reroll_index_demo.format_error` prefixes every
stored ``reroll_error`` with that category (or ``unavailable``/``unexpected``
for the two cases reroll itself never gets a chance to raise -- see that
function), so a query can slice the failure taxonomy without re-parsing
exception names, and the per-run progress report below breaks counts down
the same way.

Runtime errors stop the batch
-------------------------------
``docs/errors_and_logging.md`` is explicit that a `RerollRuntimeError` "says
nothing about the wheel" and batch processing "should generally stop ...
until the underlying host environment is stable" -- unlike the other three
categories, which are ordinary, expected per-wheel outcomes worth recording
and moving on from. So a runtime failure is deliberately *not* written to
`reroll_error` (leaving the row NULL for a real retry once whatever is
unstable -- network, on-disk cache, sqlite itself -- is fixed) and stops
:func:`convert` after flushing whatever else that batch already decided,
rather than ploughing through the rest of the corpus hitting the same
problem row after row.

Idempotency and resumability
-----------------------------
Identical shape to :func:`reroll_data.repodata_convert.convert`: work is
selected with ``WHERE reroll_data IS NULL AND reroll_error IS NULL``, so a
row that already succeeded or already failed simply stops matching -- no
lease/claim machinery needed, since there is no network request that could
be left dangling mid-flight. An interrupted run just leaves more rows for the
next one to pick up; :func:`reset_errors` re-arms rows that failed (mirroring
:func:`reroll_data.repodata_convert.reset_errors`) for another try after a
reroll fix.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import db as _db
from . import reroll_index_demo as _demo

#: Rows pulled from `repodata_conversion` per round trip, and roughly the unit
#: of work between progress reports / commits. Mirrors
#: `repodata_convert.READ_BATCH`.
READ_BATCH = 2000

#: Wheels handed to a single worker-process call at a time. Mirrors
#: `repodata_convert.CHUNKSIZE` -- reroll's own conversion (name mapping,
#: dependency/MatchSpec construction) is at least as much work per wheel as
#: conda-pypi's, so the same small chunk keeps one slow wheel from stalling
#: the pool for as long.
CHUNKSIZE = 16

#: Rows committed per write transaction. Mirrors `repodata_convert.WRITE_BATCH`.
WRITE_BATCH = 500

# Set once per worker process by `_init_worker`; never touched by the main
# process. Mirrors `repodata_convert._READ_DB` -- a read-only connection held
# open for the process's lifetime, one `sqlite3.connect()` instead of one per
# wheel.
_READ_DB: sqlite3.Connection | None = None

# Also set once per worker process by `_init_worker`, alongside `_READ_DB`.
# `reroll.default_mappers.default_mappers()` is not cheap to call -- its
# `parselmouth_mapper` opens (and may refresh over the network) a local
# sqlite evidence cache every time it is built, see
# `reroll.parselmouth_mapper.parselmouth_mapper` -- so `get_wheel_records`
# defaulting `mappers` to `None` and calling `default_mappers()` itself
# would otherwise pay that cost once per wheel instead of once per worker
# process. `None` here (rather than a real chain) only ever means "reroll
# isn't importable", exactly like `_demo.get_wheel_records` being `None`.
_NAME_MAPPERS: object | None = None


def _init_worker(db_path: str) -> None:
    """`ProcessPoolExecutor` initializer: open this worker's own connection
    and build this worker's own name-mapper chain.

    Each worker process gets its own `_READ_DB` connection and its own
    `_NAME_MAPPERS` chain -- built here, once, and reused by every
    `_convert_one` call this process ever makes, rather than defaulting to
    `None` and having `reroll.wheel_record.get_wheel_records` rebuild
    `default_mappers()` from scratch for every wheel (see `_NAME_MAPPERS`'s
    own comment above). `ProcessPoolExecutor` workers are separate OS
    processes, each with its own private memory and its own single-threaded
    task loop, so caching this in a plain module global here is safe -- no
    locking needed, unlike a thread pool sharing one process's globals.

    Also quiets `reroll`'s own per-instance logging (every `RerollError`
    subclass logs itself at construction -- see `reroll.errors`) down to
    `ERROR`. That logging is meant for interactive/small-scale callers who
    want a live narration; across millions of wheels and many worker
    processes it is pure noise here, since :func:`_convert_one` already
    captures and returns every failure's category and message for
    `reroll_error` to persist -- nothing reroll logs is otherwise lost.
    `RerollRuntimeError` still logs at `ERROR`, so the one category that
    should actually interrupt a human (see the module docstring's "Runtime
    errors stop the batch") still surfaces on the console immediately.
    """
    global _READ_DB, _NAME_MAPPERS
    _READ_DB = _db.connect(db_path, read_only=True)
    _NAME_MAPPERS = _demo.default_mappers() if _demo.default_mappers is not None else None
    logging.getLogger("reroll").setLevel(logging.ERROR)


def _convert_one(item: tuple[str, str]) -> tuple[str, str, str | None, str | None, str]:
    """Run in a worker process: convert one wheel.

    Returns `(project, filename, data_json, error, category)`. `category` is
    always set (`"ok"` on success); `error` -- one of
    :data:`reroll_index_demo.CATEGORIES`-prefixed via
    :func:`reroll_index_demo.format_error` -- is set instead of `data_json`
    on any failure. Every failure is caught here rather than propagated: one
    bad wheel must not take down the pool or the rest of the batch. Whether a
    `"runtime"`-categorized failure should stop the whole run is decided by
    the caller (:func:`convert`), not here, since that decision needs to see
    across the whole batch, not just one wheel.

    Passes this worker's `_NAME_MAPPERS` (built once by `_init_worker`)
    straight through to `_entry_from_db` rather than leaving it `None`, so
    `get_wheel_records` reuses the same chain -- and the same
    `parselmouth_mapper` sqlite connection within it -- for every wheel this
    process ever converts instead of rebuilding it from scratch each call.
    """
    project, filename = item
    assert _READ_DB is not None, "worker process not initialized"
    try:
        records = _demo._entry_from_db(
            _READ_DB, filename, project=project, mappers=_NAME_MAPPERS
        )
    except Exception as exc:  # noqa: BLE001 - any conversion failure is data
        category = _demo.categorize_error(exc)
        return project, filename, None, _demo.format_error(exc), category
    return (
        project,
        filename,
        json.dumps(records, separators=(",", ":")),
        None,
        "ok",
    )


def reset_errors(db: sqlite3.Connection) -> int:
    """Re-arm rows with a `reroll_error` for another attempt.

    Mirrors :func:`reroll_data.repodata_convert.reset_errors`:
    :func:`convert`'s selection predicate excludes any row with
    `reroll_error IS NOT NULL`, so a wheel that failed once is otherwise
    skipped forever. Only relevant after a reroll fix -- re-running
    `convert()` unmodified would just reproduce the same errors.
    """
    now = int(time.time())
    db.execute("BEGIN IMMEDIATE")
    try:
        n = (
            db.execute(
                "UPDATE repodata_conversion SET reroll_error = NULL, "
                "updated_at = ? WHERE reroll_error IS NOT NULL",
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
    """Populate `reroll_data`/`reroll_error` for every outstanding row.

    `workers` defaults to the machine's core count (`os.process_cpu_count()`)
    since this is purely CPU-bound local work (name mapping and dependency
    resolution against sqlite/local caches, no per-wheel network request --
    see the module docstring) with no politeness constraint, unlike
    `reroll_data.metadata.fetch`.

    Raises `RuntimeError` immediately, before starting the pool, if `reroll`
    is not importable -- i.e. the `probe` dependency group was never synced
    (`uv sync --group probe`). Letting every task discover that
    independently would just print the same "wrong environment" error
    `total` times.

    Stops early (`interrupted=True`, distinguishable from a real
    `KeyboardInterrupt` via the returned `"runtime_error"` key) the first
    time any wheel fails with reroll's `"runtime"` category -- see the module
    docstring's "Runtime errors stop the batch" section. That row's
    `reroll_error` is deliberately left unwritten, so it is retried (not
    skipped) once the run is retried.
    """
    if _demo.get_wheel_records is None or _demo.parse_metadata is None:
        raise RuntimeError(
            "reroll is not importable in this environment -- install the "
            "'probe' dependency group (`uv sync --group probe`) before "
            f"running this. See the {_demo.__name__} module docstring."
        )

    workers = workers or os.process_cpu_count() or os.cpu_count() or 1

    # Two connections, one each way, mirroring the single-writer model the
    # rest of this codebase uses (see reroll_data.db's module docstring).
    read_db = _db.connect(db_path, read_only=True)
    write_db = _db.connect(db_path)

    table_total = read_db.execute("SELECT count(*) FROM repodata_conversion").fetchone()[0]
    outstanding_before = read_db.execute(
        "SELECT count(*) FROM repodata_conversion "
        "WHERE reroll_data IS NULL AND reroll_error IS NULL"
    ).fetchone()[0]
    total = outstanding_before if limit is None else min(outstanding_before, limit)
    coverage_before = (
        (table_total - outstanding_before) / table_total * 100 if table_total else 0.0
    )

    print(
        f"converting {total:,} wheel(s) with {workers} worker process(es) "
        f"({coverage_before:.1f}% of {table_total:,} corpus wheels already "
        "attempted; no compatibility pre-filter) ...",
        file=sys.stderr,
    )

    counters: Counter[str] = Counter()
    started = time.monotonic()
    next_report = started + progress_every
    interrupted = False
    runtime_error: tuple[str, str, str] | None = None
    remaining_limit = limit

    cur = read_db.execute(
        "SELECT project, filename FROM repodata_conversion "
        "WHERE reroll_data IS NULL AND reroll_error IS NULL"
    )

    pending_writes: list[tuple[str, str, str | None, str | None]] = []

    def flush() -> None:
        if not pending_writes:
            return
        now = int(time.time())
        write_db.execute("BEGIN IMMEDIATE")
        try:
            write_db.executemany(
                "UPDATE repodata_conversion SET reroll_data = ?, "
                "reroll_error = ?, updated_at = ? "
                "WHERE project = ? AND filename = ?",
                [(d, e, now, p, f) for (p, f, d, e) in pending_writes],
            )
            write_db.execute("COMMIT")
        except BaseException:
            write_db.execute("ROLLBACK")
            raise
        pending_writes.clear()

    def report(force: bool = False) -> None:
        nonlocal next_report
        now = time.monotonic()
        if not force and now < next_report:
            return
        next_report = now + progress_every
        done = sum(counters.values())
        errors = done - counters["ok"]
        remaining = max(total - done, 0)
        elapsed = now - started
        rpm = done / elapsed * 60 if elapsed else 0.0
        attempted = table_total - outstanding_before + done
        coverage = attempted / table_total * 100 if table_total else 0.0
        breakdown = "  ".join(
            f"{cat}={counters[cat]:,}"
            for cat in _demo.CATEGORIES
            if counters[cat]
        )
        print(
            f"  {done:>9,}/{total:,} converted  {remaining:>9,} remaining  "
            f"{errors:>7,} errors  {rpm:8.0f} wheels/min  "
            f"{coverage:5.1f}% attempted"
            + (f"  ({breakdown})" if breakdown else ""),
            file=sys.stderr,
        )

    try:
        with ProcessPoolExecutor(
            max_workers=workers, initializer=_init_worker, initargs=(str(db_path),)
        ) as pool:
            while runtime_error is None:
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

                for project, filename, data_json, error, category in pool.map(
                    _convert_one, rows, chunksize=chunksize
                ):
                    if category == "runtime":
                        # Per the module docstring: a runtime failure says
                        # nothing about this wheel, and every remaining wheel
                        # in this batch is likely to hit the same unstable
                        # environment -- stop rather than burn through the
                        # rest of the corpus reproducing it. Left unwritten
                        # (not appended to pending_writes) so a retry lands
                        # on a clean NULL row, not a stale error.
                        runtime_error = (project, filename, error or "")
                        counters["runtime"] += 1
                        break
                    counters[category] += 1
                    pending_writes.append((project, filename, data_json, error))
                    if len(pending_writes) >= write_batch:
                        flush()
                flush()
                report()
                if runtime_error is not None:
                    break
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
        cur.close()
        read_db.close()
        write_db.close()

    if runtime_error is not None:
        project, filename, error = runtime_error
        print(
            f"\n  stopping: runtime error converting {project}/{filename}: "
            f"{error}\n  this indicates the host environment (network, "
            "local cache, sqlite) is unstable, not a bad wheel -- fix that, "
            "then re-run to pick up where this left off.",
            file=sys.stderr,
        )

    out = dict(counters)
    out["interrupted"] = interrupted or runtime_error is not None
    out["runtime_error"] = (
        f"{runtime_error[0]}/{runtime_error[1]}: {runtime_error[2]}"
        if runtime_error is not None
        else None
    )
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="reroll-data-reroll-convert",
        description=(
            "Run reroll's own translator over every corpus wheel, no "
            "compatibility pre-filter (idempotent, resumable)."
        ),
    )
    parser.add_argument(
        "--db",
        default=str(_db.DEFAULT_DB),
        type=Path,
        help=f"SQLite database path (default: {_db.DEFAULT_DB})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes (default: all cores, via os.process_cpu_count())",
    )
    parser.add_argument("--limit", type=int, default=None, help="only convert N wheels")
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt wheels previously marked reroll_error",
    )
    parser.add_argument("--read-batch", type=int, default=READ_BATCH)
    parser.add_argument("--chunksize", type=int, default=CHUNKSIZE)
    parser.add_argument("--write-batch", type=int, default=WRITE_BATCH)
    args = parser.parse_args(argv)

    db = _db.connect(args.db)
    _db.init(db)
    if args.retry_errors:
        rearmed = reset_errors(db)
        print(f"re-armed {rearmed:,} previously-failed wheels", file=sys.stderr)
    db.close()

    out = convert(
        args.db,
        workers=args.workers,
        limit=args.limit,
        read_batch=args.read_batch,
        chunksize=args.chunksize,
        write_batch=args.write_batch,
    )
    print("convert finished:", file=sys.stderr)
    for key, value in out.items():
        print(f"  {key:<16} {value}", file=sys.stderr)
    return 1 if out.get("interrupted") else 0


if __name__ == "__main__":
    raise SystemExit(main())
