"""One-off: migrate the legacy ``v.db`` corpus into ``main.db``/``pypi.db``
(:mod:`reroll_data.db2`).

Scope -- what gets copied, and what deliberately does not
-----------------------------------------------------------
Only the *pypi index* and *fetched metadata* halves of ``v.db`` move. Per the
migration's own brief:

* ``repodata_conversion`` (reroll's own conversion attempt, and the
  conda-pypi comparison) is **not** copied at all -- there is no equivalent
  table in :mod:`reroll_data.db2`'s schema. This is "reroll data"/"error
  data" and stays behind in ``v.db``.
* ``metadata_blob.parsed_json`` (reroll's parse of a stored METADATA body,
  see :mod:`reroll_data.backfill`) is **not** copied -- also a product of the
  reroll conversion step. Every migrated row's ``parsed_json`` is left NULL;
  a fresh backfill against the new ``pypi.db`` repopulates it later if
  wanted.
* ``pypi_conda_names`` (the pypi->conda name mapping) is untouched --
  outside this migration's scope entirely, per the module docstring of
  :mod:`reroll_data.db2`.

Four tables *do* move, each a straight column-for-column copy with type
conversions where the two schemas disagree (hex-text digests -> BLOB, most
notably):

======================  =========================================
``v.db`` table            ``db2`` destination
======================  =========================================
``project``               ``pypi.db.project``
``wheel``                 ``pypi.db.pypi_index`` **and** ``main.db.wheel``
``wheel_metadata``        ``pypi.db.wheel_metadata``
``metadata_blob``         ``pypi.db.metadata_blob`` (sans ``parsed_json``)
======================  =========================================

``main.db.wheel`` gets the bare minimum
----------------------------------------
Per the migration brief, each ``main.wheel`` row this script writes carries
only ``filename``, the PEP 503 *normalized* ``project`` (see
:func:`normalize`), the real ``yanked`` flag carried over from ``v.db``
(``main.wheel.yanked`` is ``NOT NULL``, so it cannot be left unset), and
``updated_at`` set to this migration run's timestamp. Every conversion-facing
column -- ``requires_prerelease``, ``reroll_version``, ``reroll_data``,
``resolutions``, ``conversion_status`` -- is left NULL/default, exactly as
if the row had never been touched by a conversion run, because none of that
data is being migrated.

``pypi_index.pypi_metadata`` folds five ``v.db.wheel`` columns into one JSONB
object, using PEP 691's own dashed key spelling (``upload-time``,
``requires-python``, ``provenance-url``, ``yanked-reason``,
``has-metadata``), plus a ``hashes`` object combining ``sha256`` (kept as hex
text -- JSON has no binary type) with whatever ``hashes_json`` already held.
``extra_json``'s keys are merged in underneath those (so a same-named
explicit column always wins on collision, though none is expected). See
:func:`_build_pypi_metadata`.

Row-identity conflicts
-----------------------
``v.db.wheel``/``wheel_metadata`` key on ``(project, filename)``; the
``db2`` destination tables key on ``filename`` alone (PyPI's filename
namespace is already global -- see :mod:`reroll_data.db2`'s own module
docstring). That collapse is not merely theoretical: real corpus data has
been observed to carry the same filename under more than one project
spelling. ``pypi_index`` and ``wheel_metadata`` therefore insert one row at
a time (see :func:`_insert_reporting_conflicts`) rather than as one
``executemany``, so a ``(project, filename)`` collision does not roll back
its whole batch -- the conflicting row, plus whatever already occupies that
``filename``, is printed in full to make the collision inspectable, then
this still raises (it does not skip/continue past a conflict that reaches
it -- see that function's own docstring).
``main.wheel`` gets the same treatment via
:func:`_insert_or_ignore_counting`, using ``ON CONFLICT(filename) DO
NOTHING`` rather than a bare ``OR IGNORE`` -- naming the conflict target
matters, since a bare ``OR IGNORE`` would also swallow a CHECK violation on
the normalized ``project`` column (see :func:`normalize`), silently
dropping a row instead of counting it as a conflict. See
:func:`migrate_wheel`/:func:`migrate_wheel_metadata` for how both counts
surface in each step's returned dict and progress output.

Why most of those collisions never reach the conflict handler at all
----------------------------------------------------------------------
Investigation traced the collision to PyPI's root ``/simple/`` index
reporting a project's *raw, non-normalized* display name (per PEP 691),
which can change over a project's lifetime (e.g. a later release re-declares
``Name: APICORE_Python`` where an earlier one had ``Name: APICORE-Python``).
:mod:`reroll_data.crawl` keys ``v.db.project`` on that raw name with no PEP
503 normalization, so a rename is crawled as an unrelated *new* project
rather than recognized as the same one -- the old raw name's row is left
behind marked ``status = 'gone'`` (PyPI's index no longer reports it) while
the new spelling gets a full, independent re-crawl of the very same
releases. Both spellings' wheel rows then collide on ``filename`` here.

Confirmed against the full ``v.db.project`` corpus: of 248 ``status='gone'``
rows, only 50 are this exact rename case (a different-spelled twin with
``status='done'``); those 50 pairs account for literally all colliding
``filename``\\s found. The other ~198 ``gone`` rows are either genuinely
deleted-from-PyPI projects with no duplicate at all (unrelated to this bug),
or a twin that has not been crawled yet (0 wheel rows -- dropping the
``gone`` side there would delete the *only* copy). A precise fix would key
off "this project's PEP 503-normalized name has a different-spelled sibling
with ``status='done'``", not off ``status='gone'`` alone.

This migration takes the blunter, interim shortcut instead: ``_WHEEL_SELECT``/
``_WHEEL_METADATA_SELECT`` skip *every* row whose ``project`` is currently
``status='gone'`` in ``v.db.project`` (see ``_SKIP_GONE_PROJECT``/
``_SKIP_GONE_PROJECT_WM``), full stop -- this is a one-off backfill of a
corpus that will be re-crawled properly under the new schema, so losing the
~198 unrelated ``gone`` projects' already-stale wheel rows from *this*
migration is an acceptable, deliberate trade for not having to reimplement
normalized-name reconciliation here. The real fix -- teaching the crawler
itself to recognize a raw-name change as a rename rather than a new
project -- belongs in :mod:`reroll_data.crawl` against the new ``db2``
corpus going forward, not in this one-off migration.

Resumability
------------
Each step keeps its own progress bookmark in the *destination* database's
own ``meta`` table (``pypi.db.meta`` for every step -- even
:func:`migrate_wheel`, which also writes to ``main.db``, treats
``pypi.db``'s commit as the record of truth; see its docstring), keyed by
table name. A batch's insert(s) and its bookmark advance commit together, so
an interrupted run resumes exactly where it left off -- no rescanning
already-migrated rows, unlike an ``INSERT OR IGNORE``-over-everything
approach would require. Bookmarks use SQLite row-value comparison
(``WHERE (project, filename) > (?, ?)``) to match each source table's own
``WITHOUT ROWID``/primary-key physical order, so pagination is a seek, never
a re-sort.

Nothing here touches ``v.db``; every read against it uses a read-only
connection.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

from . import db as _db
from . import db2 as _db2

#: Rows per read/write round trip. One number for every step except
#: `metadata_blob`, whose rows carry a compressed METADATA body (`z_body`)
#: and are therefore much heavier per row -- see `BLOB_BATCH_SIZE`.
BATCH_SIZE = 5000

#: Smaller than `BATCH_SIZE`: `metadata_blob.z_body` holds a whole compressed
#: METADATA body per row (up to a few hundred KB), so a batch this size
#: still keeps peak memory well below what `BATCH_SIZE` heavy rows would use.
BLOB_BATCH_SIZE = 1000

_PROGRESS_EVERY = 5.0

# PEP 503 normalization: lowercase, and every run of `-`/`_`/`.` collapsed to
# one `-`. Matches `reroll_data.db2._NORMALIZED_NAME_CHECK` exactly *except*
# for the no-leading/trailing-`-` clause -- a source name pathological enough
# to trip that (e.g. one starting with `_`) is intentionally left to raise a
# CHECK-constraint `IntegrityError` rather than being silently reshaped
# further, consistent with this module's "raise, don't paper over" stance on
# row-identity conflicts.
_NAME_RUNS = re.compile(r"[-_.]+")


def normalize(name: str) -> str:
    """PEP 503 normalize a project display name."""
    return _NAME_RUNS.sub("-", name).lower()


def _hex_to_blob(value: str | None) -> bytes | None:
    """Hex-text digest (`v.db`'s spelling) -> raw bytes (`db2`'s spelling)."""
    return None if value is None else bytes.fromhex(value)


def _load_json_object(text: str | None) -> dict:
    """Best-effort parse of a `v.db` JSON-text column into a dict.

    Returns `{}` for NULL, unparseable, or non-object JSON -- `hashes_json`/
    `extra_json` are both meant to hold a JSON object; anything else is a
    pre-existing data anomaly this migration is not the place to fix, so it
    is treated the same as "nothing extra to merge" rather than raised.
    """
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _build_pypi_metadata(
    *,
    url: str | None,
    size: int | None,
    upload_time: str | None,
    requires_python: str | None,
    sha256: str | None,
    hashes_json: str | None,
    yanked_reason: str | None,
    has_metadata: int | None,
    provenance_url: str | None,
    extra_json: str | None,
) -> str:
    """Fold nine `v.db.wheel` columns into one `pypi_index.pypi_metadata`
    JSON object (JSONB-encoded by the caller via `jsonb(?)`).

    `extra_json`'s keys are merged in *first*, so every explicitly-typed
    field below is applied on top and wins on any (unexpected) name
    collision -- see the module docstring.
    """
    merged: dict = dict(_load_json_object(extra_json))

    if url is not None:
        merged["url"] = url
    if size is not None:
        merged["size"] = size
    if upload_time is not None:
        merged["upload-time"] = upload_time
    if requires_python is not None:
        merged["requires-python"] = requires_python

    hashes: dict = dict(_load_json_object(hashes_json))
    if sha256 is not None:
        hashes["sha256"] = sha256
    if hashes:
        merged["hashes"] = hashes

    if yanked_reason is not None:
        merged["yanked-reason"] = yanked_reason
    if has_metadata is not None:
        merged["has-metadata"] = bool(has_metadata)
    if provenance_url is not None:
        merged["provenance-url"] = provenance_url

    return json.dumps(merged, separators=(",", ":"))


# --------------------------------------------------------------------------- #
# bookmarks
# --------------------------------------------------------------------------- #

# One key per step, all namespaced so they cannot collide with a meta key
# some other job writes (e.g. `index_serial`-alikes, were db2 to ever grow
# one).
_CURSOR_PROJECT = "migrate_v1_project_cursor"
_CURSOR_WHEEL = "migrate_v1_wheel_cursor"
_CURSOR_WHEEL_METADATA = "migrate_v1_wheel_metadata_cursor"
_CURSOR_METADATA_BLOB = "migrate_v1_metadata_blob_cursor"


def _load_pair_cursor(db: sqlite3.Connection, key: str) -> tuple[str, str]:
    """Read a `(project, filename)`-shaped bookmark; `("", "")` if unset.

    Empty strings sort before every real project/filename, so this is a
    correct "start from the very first row" sentinel for the row-value
    comparison each step's SELECT uses.
    """
    raw = _db2.get_meta(db, key)
    if raw is None:
        return "", ""
    project, filename = json.loads(raw)
    return project, filename


def _save_pair_cursor(db: sqlite3.Connection, key: str, project: str, filename: str) -> None:
    _db2.set_meta(db, key, json.dumps([project, filename]))


def _load_scalar_cursor(db: sqlite3.Connection, key: str, default):
    raw = _db2.get_meta(db, key)
    return default if raw is None else type(default)(raw)


def _save_scalar_cursor(db: sqlite3.Connection, key: str, value) -> None:
    _db2.set_meta(db, key, str(value))


# --------------------------------------------------------------------------- #
# conflict-diagnosing inserts
# --------------------------------------------------------------------------- #

# `v.db.wheel`/`wheel_metadata` key on `(project, filename)`; the destination
# tables key on `filename` alone (see the module docstring's "Row-identity
# conflicts" section). That collapse is not just theoretical -- real corpus
# data has been observed to carry the same filename under more than one
# project spelling. A bare `executemany` raising `IntegrityError` on that
# gives no way to tell which row of the batch was the culprit; this inserts
# one row at a time instead so the offending row -- plus whatever already
# occupies that `filename` -- can be printed before the error propagates.


def _insert_reporting_conflicts(
    db: sqlite3.Connection,
    insert_sql: str,
    rows: list[tuple],
    *,
    table: str,
    filename_index: int,
    source_label: str,
) -> None:
    """INSERT each row in `rows` individually, so that a UNIQUE/PK conflict
    can be attributed to one specific row instead of an entire batch.

    Still raises on a conflict -- this does not skip or continue past it,
    the run stops exactly as it did with a plain `executemany`. The only
    difference is that the offending row and whatever `table` already
    holds under the same `filename` are printed first.

    `table` is only ever one of this module's own hardcoded destination
    table names -- never user input -- so interpolating it into the
    diagnostic `SELECT` below is safe.
    """
    for row in rows:
        try:
            db.execute(insert_sql, row)
        except sqlite3.IntegrityError:
            filename = row[filename_index]
            existing = db.execute(
                f"SELECT * FROM {table} WHERE filename = ?", (filename,)
            ).fetchall()
            print(
                f"  ! CONFLICT inserting into {table}:\n"
                f"    from {source_label}: {row}\n"
                f"    already present in {table}: {existing}",
                file=sys.stderr,
            )
            raise


# --------------------------------------------------------------------------- #
# project -> pypi.db.project
# --------------------------------------------------------------------------- #

_PROJECT_SELECT = """
SELECT name, index_serial, crawled_serial, status, n_wheels, error, fetched_at
  FROM project
 WHERE name > ?
 ORDER BY name
 LIMIT ?
"""

_PROJECT_INSERT = """
INSERT INTO project (name, index_serial, crawled_serial, status, n_wheels, error, fetched_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def migrate_project(
    v_db: sqlite3.Connection,
    pypi_db: sqlite3.Connection,
    *,
    batch_size: int = BATCH_SIZE,
    progress_every: float = _PROGRESS_EVERY,
    limit: int | None = None,
) -> dict:
    """Copy every `v.db.project` row into `pypi.db.project`."""
    total_source = v_db.execute("SELECT count(*) FROM project").fetchone()[0]
    already = pypi_db.execute("SELECT count(*) FROM project").fetchone()[0]
    print(
        f"project: {already:,}/{total_source:,} already migrated ...",
        file=sys.stderr,
    )

    last_name = _load_scalar_cursor(pypi_db, _CURSOR_PROJECT, "")
    check_wal = _db.wal_monitor(_pypi_path(pypi_db))
    migrated = 0
    started = time.monotonic()
    next_report = started + progress_every
    remaining_limit = limit
    interrupted = False

    try:
        while True:
            n = batch_size if remaining_limit is None else min(batch_size, remaining_limit)
            if n <= 0:
                break
            rows = v_db.execute(_PROJECT_SELECT, (last_name, n)).fetchall()
            if not rows:
                break
            if remaining_limit is not None:
                remaining_limit -= len(rows)

            pypi_db.execute("BEGIN IMMEDIATE")
            try:
                pypi_db.executemany(_PROJECT_INSERT, rows)
                last_name = rows[-1][0]
                _save_scalar_cursor(pypi_db, _CURSOR_PROJECT, last_name)
                pypi_db.execute("COMMIT")
            except BaseException:
                pypi_db.execute("ROLLBACK")
                raise
            migrated += len(rows)

            now = time.monotonic()
            if now >= next_report:
                next_report = now + progress_every
                check_wal()
                print(f"  project: {migrated:,} migrated this run", file=sys.stderr)
    except KeyboardInterrupt:
        interrupted = True
        print("\n  project: interrupted -- resumable, re-run to continue", file=sys.stderr)

    print(f"  project: {migrated:,} migrated this run (done)", file=sys.stderr)
    return {"migrated": migrated, "interrupted": interrupted}


# --------------------------------------------------------------------------- #
# wheel -> pypi.db.pypi_index + main.db.wheel
# --------------------------------------------------------------------------- #

#: Interim workaround for the raw-name-rename duplication described in
#: `migrate_wheel`'s docstring: skip every `wheel` row whose `project` is
#: currently `status = 'gone'` in `v.db.project`. `project.name` is that
#: table's own `TEXT PRIMARY KEY` (see `reroll_data.db`), so this `EXISTS`
#: is an indexed point lookup per row, not a scan.
_SKIP_GONE_PROJECT = "EXISTS (SELECT 1 FROM project p WHERE p.name = wheel.project AND p.status = 'gone')"

_WHEEL_SELECT = f"""
SELECT project, filename, url, size, upload_time, requires_python, sha256,
       hashes_json, yanked, yanked_reason, has_metadata, metadata_sha256,
       provenance_url, extra_json, first_seen, last_seen
  FROM wheel
 WHERE (project, filename) > (?, ?)
   AND NOT {_SKIP_GONE_PROJECT}
 ORDER BY project, filename
 LIMIT ?
"""

_PYPI_INDEX_INSERT = """
INSERT INTO pypi_index (filename, project, yanked, metadata_sha256, pypi_metadata, first_seen, last_seen)
VALUES (?, ?, ?, ?, jsonb(?), ?, ?)
"""

# ON CONFLICT(filename) DO NOTHING, not OR IGNORE -- see the module
# docstring's "Row-identity conflicts" section for the resumability reason
# this table alone tolerates a conflict. Naming the conflict target matters:
# a bare `OR IGNORE` swallows *any* constraint violation on the row,
# including the CHECK on `project` (normalize() can turn a pathological
# source name, e.g. one starting with `_`, into one starting with `-`,
# which the CHECK rejects) -- that would silently drop a wheel from
# main.db with no error at all, exactly what this module's docstring
# promises never happens. Naming `filename` here means only *that*
# UNIQUE constraint is suppressed; a CHECK violation still raises.
_MAIN_WHEEL_INSERT = """
INSERT INTO wheel (filename, project, yanked, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(filename) DO NOTHING
"""


def migrate_wheel(
    v_db: sqlite3.Connection,
    main_db: sqlite3.Connection,
    pypi_db: sqlite3.Connection,
    *,
    batch_size: int = BATCH_SIZE,
    progress_every: float = _PROGRESS_EVERY,
    limit: int | None = None,
) -> dict:
    """Copy every `v.db.wheel` row into `pypi.db.pypi_index` and `main.db.wheel`.

    `main.wheel` gets only `filename`, the normalized `project`, the real
    `yanked` flag, and `updated_at` -- see the module docstring's "main.db
    gets the bare minimum" section. `pypi_index` gets the full row, with
    `pypi_metadata` built by :func:`_build_pypi_metadata`.

    `_WHEEL_SELECT` skips any row whose `project` is `status='gone'` in
    `v.db.project` -- an interim, corpus-wide workaround for the raw-name
    rename duplication described in the module docstring's "Why most of
    those collisions never reach the conflict handler at all" section, not
    a general-purpose filter. See that section before changing this.
    """
    total_source = v_db.execute("SELECT count(*) FROM wheel").fetchone()[0]
    already = pypi_db.execute("SELECT count(*) FROM pypi_index").fetchone()[0]
    print(
        f"wheel: {already:,}/{total_source:,} already migrated ...",
        file=sys.stderr,
    )

    last_project, last_filename = _load_pair_cursor(pypi_db, _CURSOR_WHEEL)
    check_main_wal = _db.wal_monitor(_main_path(main_db))
    check_pypi_wal = _db.wal_monitor(_pypi_path(pypi_db))
    migrated = 0
    started = time.monotonic()
    next_report = started + progress_every
    remaining_limit = limit
    interrupted = False
    now_ts = int(time.time())

    try:
        while True:
            n = batch_size if remaining_limit is None else min(batch_size, remaining_limit)
            if n <= 0:
                break
            rows = v_db.execute(_WHEEL_SELECT, (last_project, last_filename, n)).fetchall()
            if not rows:
                break
            if remaining_limit is not None:
                remaining_limit -= len(rows)

            main_rows = [
                (filename, normalize(project), yanked, now_ts)
                for (
                    project, filename, _url, _size, _upload_time, _requires_python,
                    _sha256, _hashes_json, yanked, _yanked_reason, _has_metadata,
                    _metadata_sha256, _provenance_url, _extra_json, _first_seen, _last_seen,
                ) in rows
            ]
            main_db.execute("BEGIN IMMEDIATE")
            try:
                main_db.executemany(_MAIN_WHEEL_INSERT, main_rows)
                main_db.execute("COMMIT")
            except BaseException:
                main_db.execute("ROLLBACK")
                raise

            pypi_rows = [
                (
                    filename,
                    project,
                    yanked,
                    _hex_to_blob(metadata_sha256),
                    _build_pypi_metadata(
                        url=url, size=size, upload_time=upload_time,
                        requires_python=requires_python, sha256=sha256,
                        hashes_json=hashes_json, yanked_reason=yanked_reason,
                        has_metadata=has_metadata, provenance_url=provenance_url,
                        extra_json=extra_json,
                    ),
                    first_seen,
                    last_seen,
                )
                for (
                    project, filename, url, size, upload_time, requires_python,
                    sha256, hashes_json, yanked, yanked_reason, has_metadata,
                    metadata_sha256, provenance_url, extra_json, first_seen, last_seen,
                ) in rows
            ]
            pypi_db.execute("BEGIN IMMEDIATE")
            try:
                _insert_reporting_conflicts(
                    pypi_db, _PYPI_INDEX_INSERT, pypi_rows,
                    table="pypi_index", filename_index=0, source_label="v.db.wheel",
                )
                last_project, last_filename = rows[-1][0], rows[-1][1]
                _save_pair_cursor(pypi_db, _CURSOR_WHEEL, last_project, last_filename)
                pypi_db.execute("COMMIT")
            except BaseException:
                pypi_db.execute("ROLLBACK")
                raise
            migrated += len(rows)

            now = time.monotonic()
            if now >= next_report:
                next_report = now + progress_every
                check_main_wal()
                check_pypi_wal()
                elapsed = now - started
                rpm = migrated / elapsed * 60 if elapsed else 0.0
                print(
                    f"  wheel: {migrated:,} migrated this run  {rpm:8.0f} rows/min",
                    file=sys.stderr,
                )
    except KeyboardInterrupt:
        interrupted = True
        print("\n  wheel: interrupted -- resumable, re-run to continue", file=sys.stderr)

    print(f"  wheel: {migrated:,} migrated this run (done)", file=sys.stderr)
    return {"migrated": migrated, "interrupted": interrupted}


# --------------------------------------------------------------------------- #
# wheel_metadata -> pypi.db.wheel_metadata
# --------------------------------------------------------------------------- #

#: Same interim workaround as `_SKIP_GONE_PROJECT` above, restated against
#: `wheel_metadata`'s own `project` column (both tables key on
#: `(project, filename)` in `v.db`, so both need the same exclusion).
_SKIP_GONE_PROJECT_WM = (
    "EXISTS (SELECT 1 FROM project p WHERE p.name = wheel_metadata.project AND p.status = 'gone')"
)

_WHEEL_METADATA_SELECT = f"""
SELECT project, filename, state, blob_sha256, lease_until, attempts, error, updated_at
  FROM wheel_metadata
 WHERE (project, filename) > (?, ?)
   AND NOT {_SKIP_GONE_PROJECT_WM}
 ORDER BY project, filename
 LIMIT ?
"""

_WHEEL_METADATA_INSERT = """
INSERT INTO wheel_metadata (filename, project, state, blob_sha256, lease_until, attempts, error, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def migrate_wheel_metadata(
    v_db: sqlite3.Connection,
    pypi_db: sqlite3.Connection,
    *,
    batch_size: int = BATCH_SIZE,
    progress_every: float = _PROGRESS_EVERY,
    limit: int | None = None,
) -> dict:
    """Copy every `v.db.wheel_metadata` row into `pypi.db.wheel_metadata`.

    `parser_version` (a `db2`-only column, with no `v.db` equivalent) is
    left NULL on every migrated row.

    `_WHEEL_METADATA_SELECT` applies the same `status='gone'`-project skip
    as `migrate_wheel`'s `_WHEEL_SELECT` -- see the module docstring.
    """
    total_source = v_db.execute("SELECT count(*) FROM wheel_metadata").fetchone()[0]
    already = pypi_db.execute("SELECT count(*) FROM wheel_metadata").fetchone()[0]
    print(
        f"wheel_metadata: {already:,}/{total_source:,} already migrated ...",
        file=sys.stderr,
    )

    last_project, last_filename = _load_pair_cursor(pypi_db, _CURSOR_WHEEL_METADATA)
    check_wal = _db.wal_monitor(_pypi_path(pypi_db))
    migrated = 0
    started = time.monotonic()
    next_report = started + progress_every
    remaining_limit = limit
    interrupted = False

    try:
        while True:
            n = batch_size if remaining_limit is None else min(batch_size, remaining_limit)
            if n <= 0:
                break
            rows = v_db.execute(
                _WHEEL_METADATA_SELECT, (last_project, last_filename, n)
            ).fetchall()
            if not rows:
                break
            if remaining_limit is not None:
                remaining_limit -= len(rows)

            dest_rows = [
                (filename, project, state, _hex_to_blob(blob_sha256), lease_until, attempts, error, updated_at)
                for (project, filename, state, blob_sha256, lease_until, attempts, error, updated_at) in rows
            ]
            pypi_db.execute("BEGIN IMMEDIATE")
            try:
                _insert_reporting_conflicts(
                    pypi_db, _WHEEL_METADATA_INSERT, dest_rows,
                    table="wheel_metadata", filename_index=0, source_label="v.db.wheel_metadata",
                )
                last_project, last_filename = rows[-1][0], rows[-1][1]
                _save_pair_cursor(pypi_db, _CURSOR_WHEEL_METADATA, last_project, last_filename)
                pypi_db.execute("COMMIT")
            except BaseException:
                pypi_db.execute("ROLLBACK")
                raise
            migrated += len(rows)

            now = time.monotonic()
            if now >= next_report:
                next_report = now + progress_every
                check_wal()
                elapsed = now - started
                rpm = migrated / elapsed * 60 if elapsed else 0.0
                print(
                    f"  wheel_metadata: {migrated:,} migrated this run  {rpm:8.0f} rows/min",
                    file=sys.stderr,
                )
    except KeyboardInterrupt:
        interrupted = True
        print("\n  wheel_metadata: interrupted -- resumable, re-run to continue", file=sys.stderr)

    print(f"  wheel_metadata: {migrated:,} migrated this run (done)", file=sys.stderr)
    return {"migrated": migrated, "interrupted": interrupted}


# --------------------------------------------------------------------------- #
# metadata_blob -> pypi.db.metadata_blob
# --------------------------------------------------------------------------- #

_METADATA_BLOB_SELECT = """
SELECT id, sha256, n_bytes, z_body, stored_at
  FROM metadata_blob
 WHERE id > ?
 ORDER BY id
 LIMIT ?
"""

# codec is hardcoded to 'zlib6' (the schema's own default): every `v.db`
# body was compressed with `zlib.compress(body, 6)` -- see
# `reroll_data.metadata.ZLIB_LEVEL` -- so this is simply making that fact
# explicit rather than leaving it to the column default. `parsed_json` is
# omitted (left NULL) -- see the module docstring.
_METADATA_BLOB_INSERT = """
INSERT INTO metadata_blob (sha256, n_bytes, codec, z_body, stored_at)
VALUES (?, ?, 'zlib6', ?, ?)
"""


def migrate_metadata_blob(
    v_db: sqlite3.Connection,
    pypi_db: sqlite3.Connection,
    *,
    batch_size: int = BLOB_BATCH_SIZE,
    progress_every: float = _PROGRESS_EVERY,
    limit: int | None = None,
) -> dict:
    """Copy every `v.db.metadata_blob` row into `pypi.db.metadata_blob`,
    dropping `parsed_json` and `id` (a fresh surrogate id is assigned;
    nothing else references the old one -- see the module docstring).
    """
    total_source = v_db.execute("SELECT count(*) FROM metadata_blob").fetchone()[0]
    already = pypi_db.execute("SELECT count(*) FROM metadata_blob").fetchone()[0]
    print(
        f"metadata_blob: {already:,}/{total_source:,} already migrated ...",
        file=sys.stderr,
    )

    last_id = _load_scalar_cursor(pypi_db, _CURSOR_METADATA_BLOB, 0)
    check_wal = _db.wal_monitor(_pypi_path(pypi_db))
    migrated = 0
    z_bytes = 0
    started = time.monotonic()
    next_report = started + progress_every
    remaining_limit = limit
    interrupted = False

    try:
        while True:
            n = batch_size if remaining_limit is None else min(batch_size, remaining_limit)
            if n <= 0:
                break
            rows = v_db.execute(_METADATA_BLOB_SELECT, (last_id, n)).fetchall()
            if not rows:
                break
            if remaining_limit is not None:
                remaining_limit -= len(rows)

            dest_rows = [
                (_hex_to_blob(sha256), n_bytes, z_body, stored_at)
                for (_id, sha256, n_bytes, z_body, stored_at) in rows
            ]
            pypi_db.execute("BEGIN IMMEDIATE")
            try:
                pypi_db.executemany(_METADATA_BLOB_INSERT, dest_rows)
                last_id = rows[-1][0]
                _save_scalar_cursor(pypi_db, _CURSOR_METADATA_BLOB, last_id)
                pypi_db.execute("COMMIT")
            except BaseException:
                pypi_db.execute("ROLLBACK")
                raise
            migrated += len(rows)
            # dest_rows entries are (sha256_blob, n_bytes, z_body, stored_at) --
            # z_body is index 2, not 3 (that's stored_at, an int).
            z_bytes += sum(len(r[2]) for r in dest_rows)

            now = time.monotonic()
            if now >= next_report:
                next_report = now + progress_every
                check_wal()
                elapsed = now - started
                rpm = migrated / elapsed * 60 if elapsed else 0.0
                print(
                    f"  metadata_blob: {migrated:,} migrated this run  "
                    f"{z_bytes / 1e6:9.1f} MB  {rpm:8.0f} rows/min",
                    file=sys.stderr,
                )
    except KeyboardInterrupt:
        interrupted = True
        print("\n  metadata_blob: interrupted -- resumable, re-run to continue", file=sys.stderr)

    print(f"  metadata_blob: {migrated:,} migrated this run (done)", file=sys.stderr)
    return {"migrated": migrated, "interrupted": interrupted}


# --------------------------------------------------------------------------- #
# orchestration
# --------------------------------------------------------------------------- #


def _db_path(db: sqlite3.Connection) -> Path:
    """The on-disk path a connection was opened against (for `wal_monitor`)."""
    return Path(db.execute("PRAGMA database_list").fetchone()[2])


# Aliases so each call site reads as "the path of *this* database" rather
# than a generic helper -- purely for readability at the call sites below.
_main_path = _db_path
_pypi_path = _db_path


def migrate_all(
    v_db_path: Path,
    data_dir: Path | str = _db2.DEFAULT_DATA_DIR,
    *,
    batch_size: int = BATCH_SIZE,
    blob_batch_size: int = BLOB_BATCH_SIZE,
    progress_every: float = _PROGRESS_EVERY,
    limit: int | None = None,
) -> dict:
    """Run every migration step against fresh connections to all three databases.

    The four steps are independent of one another (none reads a row another
    step just wrote), so the order they run in here is arbitrary. `limit`
    (if given) caps *each* step separately, for a small trial run before
    letting this loose on the full corpus.
    """
    v_db = _db.connect(v_db_path, read_only=True)
    main_db = _db2.connect_main(data_dir)
    pypi_db = _db2.connect_pypi(data_dir)
    _db2.init_main(main_db)
    _db2.init_pypi(pypi_db)

    out: dict[str, dict] = {}
    try:
        out["project"] = migrate_project(
            v_db, pypi_db, batch_size=batch_size, progress_every=progress_every, limit=limit
        )
        out["wheel"] = migrate_wheel(
            v_db, main_db, pypi_db, batch_size=batch_size, progress_every=progress_every, limit=limit
        )
        out["wheel_metadata"] = migrate_wheel_metadata(
            v_db, pypi_db, batch_size=batch_size, progress_every=progress_every, limit=limit
        )
        out["metadata_blob"] = migrate_metadata_blob(
            v_db, pypi_db, batch_size=blob_batch_size, progress_every=progress_every, limit=limit
        )
    finally:
        v_db.close()
        main_db.close()
        pypi_db.close()
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="reroll-data-db2-backfill",
        description=(
            "One-off: migrate the pypi-index/metadata halves of the legacy "
            "v.db corpus into main.db/pypi.db (idempotent, resumable). "
            "Never touches v.db; never copies reroll_data/repodata_conversion "
            "or metadata_blob.parsed_json -- see this module's docstring."
        ),
    )
    parser.add_argument(
        "--db",
        default=str(_db.DEFAULT_DB),
        type=Path,
        help=f"legacy v.db path to migrate from (default: {_db.DEFAULT_DB})",
    )
    parser.add_argument(
        "--data-dir",
        default=str(_db2.DEFAULT_DATA_DIR),
        help=(
            "directory main.db/pypi.db live under "
            f"(default: {_db2.DEFAULT_DATA_DIR})"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--blob-batch-size", type=int, default=BLOB_BATCH_SIZE)
    parser.add_argument(
        "--limit", type=int, default=None, help="cap each step to N rows (for a trial run)"
    )
    args = parser.parse_args(argv)

    out = migrate_all(
        args.db,
        args.data_dir,
        batch_size=args.batch_size,
        blob_batch_size=args.blob_batch_size,
        limit=args.limit,
    )
    print("migration finished:", file=sys.stderr)
    interrupted = False
    for step, info in out.items():
        print(f"  {step:<16} {info}", file=sys.stderr)
        interrupted = interrupted or info.get("interrupted", False)
    return 1 if interrupted else 0


if __name__ == "__main__":
    raise SystemExit(main())
