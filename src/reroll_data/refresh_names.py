"""Refresh `main.db.pypi_conda_names` against reroll's own default mapper
chain, then re-arm every `main.db.wheel` row whose already-recorded
`resolutions` used a name this refresh changed.

This is a from-scratch job, distinct from `reroll_data.reroll_convert`:
where `reroll_convert` treats `pypi_conda_names` as a curated override
*ahead of* reroll's live mapper chain (a hit there ends the chain
immediately, never second-guessed), this job runs the live chain itself
and writes its answer *into* `pypi_conda_names`, unconditionally
overwriting whatever was there before. Deliberately a separate, human
(or cron-)triggered job -- run it (e.g. weekly) on your own schedule, not
as part of every `reroll-convert`, precisely because it changes what
`reroll_convert`'s next run will treat as curated truth.

One mapper build for the whole run
-----------------------------------
`reroll.default_mappers.default_mappers()` is built exactly **once**, in
the caller's own process, and reused for every row -- never rebuilt per
worker process or thread the way `reroll_convert._init_worker` rebuilds
it per `ProcessPoolExecutor` worker. That divergence is deliberate:
`conda_lock_mapper`'s table is fetched over HTTP through
`conda_lock.lookup_cache.cached_download_file`, a *time-based* (5 minute
freshness window, 2 day TTL) disk cache pointed at
`regro/cf-graph-countyfair`'s `master` branch -- a file a bot commits to
continuously. Rebuilding the chain more than once within a single run
risks two different builds seeing two different upstream snapshots if
that branch changes mid-run, and any such mismatch would show up as two
different worker processes silently disagreeing about the same
`pypi_name`, depending purely on timing. Building once up front (in a
single, plain Python loop -- see module docstring's "Concurrency" note
below) makes the whole run self-consistent by construction: there is
only ever one snapshot in play.

`grayskull_mapper`/`overrides_mapper` are pure, already-local (bundled
package data / a hardcoded table) with no such concern; `conda_lock_mapper`
is the only piece of the default chain this reasoning actually applies to,
but building once covers every mapper in the chain uniformly rather than
special-casing one.

Concurrency
-----------
After construction, every mapper in the chain resolves to a plain,
read-only `dict.get()` against an already-built table -- no I/O, no
shared mutable state left to race on (see each mapper's own module: this
was traced, not assumed). That means the concurrency question this
module started with is largely moot: a single-threaded pass over
`pypi_conda_names` is the simplest correct answer, and is expected to be
fast enough on its own (the real costs here are the one-time mapper
build, and the SQLite read/write I/O -- not the per-name resolution
itself). If a real run shows resolution itself is the bottleneck, a
`ThreadPoolExecutor` sharing the single already-built `mappers` chain
across threads is safe to add later (see the module docstring above for
why it is safe) -- deliberately not implemented up front.

Replace-in-place, never a second row
-------------------------------------
`pypi_conda_names` is `WITHOUT ROWID` with `pypi_name` as its sole
`PRIMARY KEY` (see `reroll_data.db2`'s module docstring) -- there is no
way to hold two rows for the same `pypi_name`, so a changed mapping is
always an `UPDATE` of the existing row, never an `INSERT` of a new one.
Every row this run touches shares one `updated_at` timestamp, captured
once before the pass starts.

Invalidating affected wheels
------------------------------
`reroll_data.db2`'s module docstring already gives the general form of
this invalidation sweep (comparing every wheel's `resolutions` against
the *current* `pypi_conda_names` table via a `LEFT JOIN`). This module
uses a narrower version instead: a `json_each` over just the `{pypi_name:
new_conda_name}` object this run actually changed, joined directly
against each wheel's `resolutions` -- never against `pypi_conda_names`
itself. That avoids depending on `pypi_conda_names`'s committed state
(no ordering requirement against the writes `refresh_names` just made),
and skips the join entirely for the overwhelming majority of resolved
names that did not change this run. The general, `pypi_conda_names`-joined
query in `db2`'s docstring remains the right tool for an occasional full
hygiene pass; this module's tightened version is for this job's normal,
diff-driven case.

`w.resolutions` is still expanded via `json_each` for every wheel with a
non-NULL `resolutions` (i.e. every successfully-converted wheel) -- there
is no index into a JSONB object's keys, so that part of the cost is
unavoidable with the current schema regardless of how few names changed.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Callable

from reroll.default_mappers import default_mappers
from reroll.errors import UnresolvedCondaNameError
from reroll.name_mapping import NameMappers, map_name

from . import db2 as _db2

#: Rows read from `pypi_conda_names` per round trip.
READ_BATCH = 2000

#: Rows committed per `pypi_conda_names` write transaction.
WRITE_BATCH = 500

#: Wheel ids re-armed per `wheel` write transaction.
INVALIDATE_BATCH = 500


def build_mappers() -> NameMappers:
    """Build reroll's default mapper chain exactly once for the whole run.

    See the module docstring's "One mapper build for the whole run"
    section for why this must not be called more than once per run.
    """
    return default_mappers()


def _resolve(pypi_name: str, mappers: NameMappers) -> str | None:
    """The conda name `mappers` currently resolves `pypi_name` to, or
    `None` if the chain can't agree (`UnresolvedCondaNameError` --
    competing, low-confidence candidates with no consensus and no single
    mapper confident enough on its own). `None` means "leave the existing
    row untouched", not "map to nothing".
    """
    try:
        winner = map_name(pypi_name, mappers)
    except UnresolvedCondaNameError:
        return None
    return winner.conda_name


def refresh_names(
    main_db,
    mappers: NameMappers,
    *,
    read_batch: int = READ_BATCH,
    write_batch: int = WRITE_BATCH,
    limit: int | None = None,
    progress_every: float = 5.0,
    check_wal: Callable[[], int] | None = None,
) -> tuple[dict, dict[str, str]]:
    """Re-run `mappers` over every `main.db.pypi_conda_names` row,
    replacing `conda_name` in place wherever the freshly-computed value
    disagrees with what is currently stored.

    Iterates by keyset pagination on `pypi_name` (its own primary key)
    rather than `LIMIT`/`OFFSET`, so each page is a cheap B-tree seek
    rather than an ever-more-expensive re-scan from the start -- there is
    no `WHERE`-filterable "already processed" column on this table the
    way `wheel_todo` gives `reroll_convert`, since every row is in-scope
    on every run.

    Returns `(stats, changed)`. `changed` maps every `pypi_name` this pass
    actually rewrote to its new `conda_name` -- exactly the input
    `invalidate_wheels` needs.

    Not resumable mid-run the way `reroll_convert` is (there is no status
    column to persist a partial pass): an interrupted run simply leaves
    whatever was already committed in place, and a fresh run redoes the
    full table -- correct either way, just not incremental, which is fine
    for a full-table refresh run on its own schedule.
    """
    total = main_db.execute("SELECT count(*) FROM pypi_conda_names").fetchone()[0]
    if limit is not None:
        total = min(total, limit)
    now = int(time.time())

    changed: dict[str, str] = {}
    checked = 0
    unresolved = 0
    interrupted = False
    last_name = ""
    pending: list[tuple[str, int, str]] = []

    started = time.monotonic()
    next_report = started + progress_every

    def flush() -> None:
        if not pending:
            return
        main_db.execute("BEGIN IMMEDIATE")
        try:
            main_db.executemany(
                "UPDATE pypi_conda_names SET conda_name = ?, updated_at = ? "
                "WHERE pypi_name = ?",
                pending,
            )
            main_db.execute("COMMIT")
        except BaseException:
            main_db.execute("ROLLBACK")
            raise
        pending.clear()

    def report(force: bool = False) -> None:
        nonlocal next_report
        now_mono = time.monotonic()
        if not force and now_mono < next_report:
            return
        next_report = now_mono + progress_every
        if check_wal is not None:
            check_wal()
        elapsed = now_mono - started
        rpm = checked / elapsed * 60 if elapsed else 0.0
        print(
            f"  {checked:>9,}/{total:,} checked  {len(changed):>7,} changed  "
            f"{unresolved:>7,} unresolved  {rpm:8.0f} names/min",
            file=sys.stderr,
        )

    try:
        while limit is None or checked < limit:
            n = read_batch if limit is None else min(read_batch, limit - checked)
            rows = main_db.execute(
                "SELECT pypi_name, conda_name FROM pypi_conda_names "
                "WHERE pypi_name > ? ORDER BY pypi_name LIMIT ?",
                (last_name, n),
            ).fetchall()
            if not rows:
                break
            last_name = rows[-1][0]
            for pypi_name, old_conda_name in rows:
                new_conda_name = _resolve(pypi_name, mappers)
                checked += 1
                if new_conda_name is None:
                    unresolved += 1
                elif new_conda_name != old_conda_name:
                    changed[pypi_name] = new_conda_name
                    pending.append((new_conda_name, now, pypi_name))
                    if len(pending) >= write_batch:
                        flush()
            flush()
            report()
    except KeyboardInterrupt:
        interrupted = True
        print(
            "\n  interrupted -- already-committed rows stay updated; re-run to "
            "refresh the rest (this job always does a full pass, so a re-run "
            "is safe, just not incremental).",
            file=sys.stderr,
        )
    finally:
        flush()
        report(force=True)

    stats = {
        "checked": checked,
        "changed": len(changed),
        "unresolved": unresolved,
        "interrupted": interrupted,
    }
    return stats, changed


def invalidate_wheels(
    main_db,
    changed: dict[str, str],
    *,
    batch_size: int = INVALIDATE_BATCH,
    check_wal: Callable[[], int] | None = None,
) -> dict:
    """Re-arm every `main.db.wheel` row whose already-committed
    `resolutions` used one of `changed`'s old values.

    See module docstring's "Invalidating affected wheels" section for why
    this compares against `changed` directly (via `json_each`) rather than
    re-joining `pypi_conda_names`. `changed` empty is a no-op -- nothing to
    invalidate.

    The `SELECT` is fully drained with `fetchall()` before any `UPDATE`
    runs, mirroring `reroll_convert.convert`'s own reasoning for never
    holding a read cursor open across writes on the same connection: here
    that matters even more directly, since the writes below null out the
    exact `resolutions` column the `SELECT` scans.
    """
    if not changed:
        return {"invalidated": 0}
    changed_json = json.dumps(changed, separators=(",", ":"))
    ids = [
        row[0]
        for row in main_db.execute(
            "SELECT DISTINCT w.id FROM wheel w, json_each(w.resolutions) r "
            "JOIN json_each(?) c ON c.key = r.key "
            "WHERE w.yanked = 0 AND r.value <> c.value",
            (changed_json,),
        ).fetchall()
    ]
    now = int(time.time())
    invalidated = 0
    for i in range(0, len(ids), batch_size):
        chunk = ids[i : i + batch_size]
        placeholders = ",".join("?" for _ in chunk)
        main_db.execute("BEGIN IMMEDIATE")
        try:
            main_db.execute(
                "UPDATE wheel SET conversion_status = NULL, reroll_data = NULL, "
                "resolutions = NULL, requires_prerelease = NULL, reroll_version = NULL, "
                f"updated_at = ? WHERE id IN ({placeholders})",
                (now, *chunk),
            )
            main_db.execute("COMMIT")
        except BaseException:
            main_db.execute("ROLLBACK")
            raise
        invalidated += len(chunk)
        if check_wal is not None:
            check_wal()
    return {"invalidated": invalidated}


def refresh(
    data_dir: Path | str,
    *,
    limit: int | None = None,
    read_batch: int = READ_BATCH,
    write_batch: int = WRITE_BATCH,
    invalidate_batch: int = INVALIDATE_BATCH,
    progress_every: float = 5.0,
) -> dict:
    """Refresh every `main.db.pypi_conda_names` row against reroll's
    current default mapper chain, then re-arm every `main.db.wheel` row
    whose already-recorded `resolutions` used one of the names that
    changed. See module docstring.
    """
    data_dir = Path(data_dir)
    main_db = _db2.connect_main(data_dir)
    _db2.init_main(main_db)
    check_wal = _db2.wal_monitor(data_dir / _db2.MAIN_DB_FILENAME)

    print(
        "building reroll's default mapper chain (one build, reused for the "
        "whole run) ...",
        file=sys.stderr,
    )
    mappers = build_mappers()

    print("refreshing pypi_conda_names ...", file=sys.stderr)
    names_stats, changed = refresh_names(
        main_db,
        mappers,
        read_batch=read_batch,
        write_batch=write_batch,
        limit=limit,
        progress_every=progress_every,
        check_wal=check_wal,
    )
    print("pypi_conda_names refresh finished:", file=sys.stderr)
    for key, value in names_stats.items():
        print(f"  {key:<16} {value}", file=sys.stderr)

    print(
        f"invalidating wheels referencing {len(changed):,} changed name(s) ...",
        file=sys.stderr,
    )
    invalidate_stats = invalidate_wheels(
        main_db, changed, batch_size=invalidate_batch, check_wal=check_wal
    )
    print("invalidation finished:", file=sys.stderr)
    for key, value in invalidate_stats.items():
        print(f"  {key:<16} {value:,}", file=sys.stderr)

    main_db.close()

    return {**names_stats, **invalidate_stats}


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="reroll-data-refresh-names",
        description=(
            "Refresh main.db.pypi_conda_names against reroll's default mapper "
            "chain, then re-arm any main.db.wheel row it affects."
        ),
    )
    parser.add_argument(
        "--data-dir",
        default=str(_db2.DEFAULT_DATA_DIR),
        type=Path,
        help=f"directory main.db/pypi.db live under (default: {_db2.DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="only check N pypi_conda_names rows"
    )
    args = parser.parse_args(argv)

    out = refresh(args.data_dir, limit=args.limit)
    print("refresh-names finished:", file=sys.stderr)
    for key, value in out.items():
        print(f"  {key:<16} {value}", file=sys.stderr)
    return 1 if out.get("interrupted") else 0


if __name__ == "__main__":
    raise SystemExit(main())
