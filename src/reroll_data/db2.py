"""SQLite storage for the curated, conversion-facing corpus: ``main.db`` and
``pypi.db``.

Both live under one ``data_dir`` (default ``data/``). Historically this sat
alongside a legacy crawl database, ``data/v.db`` (see
:mod:`reroll_data.db2_backfill`, which one-off migrated it into these two
files); ``v.db``'s own schema module has since been removed. ``main.db`` and
``pypi.db`` are **separate files**:

``main.db``
    What the conversion job reads and writes -- one small ``wheel`` table
    plus the pypi->conda name mapping it depends on.

``pypi.db``
    The raw, as-crawled PyPI index: which projects/files exist, their PEP 658
    METADATA fetch state, and the content-addressed body store.

That split is deliberate: SQLite allows exactly **one writer per file**, not
per database server. Separate files means separate write locks -- the crawl
can keep writing `pypi.db` while conversion writes `main.db`, with no
contention between them, and a notebook holding an idle connection open
against one cannot stall checkpointing on the other (the `-wal` growth
failure mode :func:`wal_monitor` exists to catch in the first place).

Design history
---------------
This schema is the outcome of a long back-and-forth (see the project's
design discussion) that started from much wider tables -- version, tags,
conda_name, per-category error columns, a separate errors database,
per-file serials, a dozen promoted PEP 691 columns -- and was deliberately
narrowed back down. The decisions that survived, condensed:

``filename`` is the only unique key, on both `wheel` and `pypi_index`
    Not ``(project, filename)``. PyPI's filename namespace is already
    global.

``project`` is PEP 503 normalized on `main.wheel`, but *not* on `pypi_index`
    `pypi_index.project` keeps the display spelling PyPI actually returned --
    it's the raw, as-observed layer. `main.wheel.project` is normalized
    because it must equal the same normalized form used as the key into
    `pypi_conda_names` (dependency names arrive normalized; a join against a
    display-spelled column would silently miss). Normalizing `wheel.project`
    is safe specifically because `filename` -- not `(project, filename)` --
    is this table's unique key, so normalizing can never collapse two
    identity-bearing rows into one.

``pypi_conda_names`` is a two-column mapping plus a timestamp, nothing more
    `pypi_name` (normalized, PK), `conda_name` (nullable), `updated_at`. The
    tri-state -- never checked / checked-and-unmappable / mapped -- is
    expressed entirely by `(conda_name, updated_at)`: `(NULL, NULL)` is
    never-checked, `(NULL, ts)` is checked-with-no-conda-equivalent,
    `(name, ts)` is mapped.

``resolutions`` is kept on ``main.wheel``, as a JSONB *object*
    `{pypi_name: conda_name actually used}`, including the wheel's own
    project. Keeping it here (rather than in a side table, or in `pypi.db`)
    means the conversion job only ever writes to `main.db` -- never to
    `pypi.db` -- and it makes the mapping-change invalidation sweep an exact
    comparison rather than a timestamp race:

        SELECT w.id FROM wheel w, json_each(w.resolutions) r
          LEFT JOIN pypi_conda_names n ON n.pypi_name = r.key
         WHERE w.yanked = 0
           AND coalesce(n.conda_name, r.key) <> r.value;

    `coalesce(n.conda_name, r.key)` reproduces the mapper's own passthrough
    behaviour (no row, or `conda_name IS NULL`, means "use the pypi name
    unchanged"), so a wheel that resolved everything via passthrough and
    still does today compares equal and is never falsely invalidated. No
    generation counter or clock comparison is needed for correctness; this
    is intentionally exact rather than conservative. `yanked = 0` is excluded
    because nothing consumes a yanked wheel's `reroll_data` under normal
    dependency resolution (PEP 592), so re-converting it when a mapping
    changes underneath it is wasted work.

JSON columns are stored as JSONB, not text JSON
    Declared `BLOB`; written with `jsonb(?)`, read with `json(...)` or
    queried directly with `json_each`/`->`/`->>`, all of which accept the
    JSONB binary encoding natively (SQLite 3.45+). This is a storage
    *format*, not a separate declared type -- there is no `JSONB` type
    keyword. Every `CHECK` on one of these columns must use
    `json_valid(col, 8)`, not the bare `json_valid(col)`: the second
    argument is a bitmask of which encodings to accept (1 = text JSON,
    which is the default if omitted; 8 = JSONB). Passing `8` is what makes
    the CHECK accept the JSONB blobs these columns actually hold --
    omitting it makes `json_valid` report every well-formed JSONB write as
    invalid, since by default it is only checking "is this valid JSON
    *text*."

Column promoted vs. folded into a JSONB blob: filtered-or-joined-on, or not
    Every PEP 691/PEP 658 field that nothing in this codebase actually
    filters or joins on in SQL -- checked by grepping every module, not by
    guessing -- is folded into one `pypi_metadata`/`parsed_json` JSONB blob
    per row rather than promoted to its own column. A promoted column only
    earns its cost (index space, migration friction) when a `WHERE`/`JOIN`
    predicate needs it; a value that is only ever read *after* the row was
    already located by its `filename` primary key costs the same either way
    -- `json_extract`/`->>` on an already-fetched row vs. reading a column.
    Concretely, `metadata_sha256` (compared in `wheel_metadata` sync/claim
    queries) and `yanked` (filtered directly, and synced into `main.wheel`)
    are the only two fields that survive as real columns on `pypi_index`;
    `url`, `size`, `upload_time`, `requires_python`, `sha256`,
    `yanked_reason`, `has_metadata`, and any as-yet-unmodelled PEP 691 keys
    all live inside `pypi_metadata` instead.

No `first_seen`/`last_seen` on `pypi_index`
    The initial schema carried these over from the legacy `v.db.wheel`
    table, but nothing in this codebase ever reads either one -- no
    `SELECT`, `WHERE`, or stats query touches them, only the crawl's own
    insert/upsert. `last_seen` in particular stopped meaning "last time we
    crawled this file" once the incremental upsert (`crawl.py`'s
    `_INSERT_PYPI_INDEX`) was conditioned on `yanked` actually differing --
    a re-crawl of an unchanged wheel touches neither column, so it really
    means "last time `yanked` changed." The mechanism these two columns
    would support -- staleness-based removal of a wheel/project nothing has
    "seen" in a while -- is exactly what "Deletions, not a 'gone' status"
    (`crawl.py`'s module docstring) replaced with an explicit diff against
    the root index. Per the "filtered-or-joined-on" rule directly above,
    that leaves both columns with no functional justification, so they were
    dropped (`ALTER TABLE pypi_index DROP COLUMN ...`) rather than kept as
    unused metadata.

No per-file `observed_serial`
    `__last_serial` is project-granular (PEP 700), not per-file, and the
    crawler already writes every file of one project plus that project's
    own `crawled_serial` in a single transaction (see `crawl.py`'s
    `_writer`: "Wheels and the project's new serial commit together, so a
    project is never left half-recorded"). A per-file copy of that same
    value would be identical across every row of one crawl pass and
    identical to `project.crawled_serial` at that moment -- pure
    duplication under the crawler's current one-page-at-a-time write
    pattern, so it is not included here. `project.index_serial >
    project.crawled_serial` remains the one staleness signal, unchanged
    from the legacy ``v.db`` schema this replaced.

A separate ``reroll_errors`` table, not a ``wheel.conversion_status`` column
    `wheel.reroll_data` non-NULL *is* the "converted ok" signal (see below),
    so the only other state a wheel needs recorded is "why didn't this
    convert" -- and that is exactly the shape a one-row-per-failure table
    fits, not another column on `wheel` itself. `reroll_errors.wheel_id` is
    both this table's primary key and its foreign key into `wheel.id`: at
    most one error row per wheel, replaced in place by the next attempt
    (`INSERT ... ON CONFLICT(wheel_id) DO UPDATE`), never accumulated.
    Presence of a row is "this wheel has an error"; absence is "no error" --
    which, combined with `reroll_data`, is enough to recover the old
    three-way state (never attempted / ok / failed) without a fourth column:
    see `reroll_data.reroll_convert._categorize` for the taxonomy, and that
    module's own docstring for how the worklist query is built from
    `reroll_data IS NULL` plus an anti-join against this table.

    `category` is one of reroll's four documented error categories, plus
    `unavailable`/`unexpected` for cases reroll itself never gets a chance
    to raise, plus `runtime` -- unlike the legacy `conversion_status` column
    this replaced, `runtime` *is* written here, so a `RerollRuntimeError`
    (reroll's own "says nothing about this wheel, stop the batch" category --
    almost always an unstable environment: network, local cache, sqlite) is
    visible in the accounting instead of silently vanishing. It must
    nonetheless never count as a *settled* failure: a wheel whose only row
    has `category = 'runtime'` has to stay in the worklist for a genuine
    retry once the environment is stable, exactly like the old column's
    NULL-on-runtime behaviour -- so every reader that decides "is this
    wheel's failure final" filters out `category = 'runtime'` explicitly
    (via `reroll_errors_category`, below) rather than treating any row's
    mere presence as final. `sub_category` (the raising exception's class
    name) and `description` (`str(exception)`) exist for one row's error at
    a time; unlike `category` there is no CHECK on either -- reroll's
    exception surface is not a fixed enum the way the five/six recorded
    categories are, and a free-text description in particular can be
    arbitrarily long (e.g. `MissingPypiCondaStaticMapping` lists every
    offending dependency name).

    `ON DELETE CASCADE` on `wheel_id`: `reroll_data.crawl` deletes `wheel`
    rows outright once PyPI stops listing a project (`_delete_project`) or a
    consistency sweep finds an orphan (`sync_consistency`) -- both run with
    `PRAGMA foreign_keys=ON` (see `_connect`), so without CASCADE either
    delete would fail outright with a FOREIGN KEY constraint violation the
    moment the wheel being removed already has an error row.

``STRICT`` and ``CHECK`` from the start, on every table in both files
    Both require a full table rebuild to add after the fact (SQLite's
    `ALTER TABLE ... ADD COLUMN` cannot add a CHECK, and cannot turn a table
    STRICT). This is the one chance to have them at zero migration cost.

Concurrency
-----------
SQLite supports exactly **one writer at a time** per file. WAL mode permits
many concurrent readers alongside that single writer, but two simultaneous
write transactions will always cause one to fail with ``database is locked``
(``busy_timeout`` only converts that failure into a blocking retry, which
serialises them anyway). Every mutation is therefore routed through a single
dedicated writer thread/process per file; see :mod:`reroll_data.crawl` and
:mod:`reroll_data.reroll_convert`. This now simply applies to two files
instead of one.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Callable

#: Directory both databases live under. Each `connect_*` function appends
#: its own filename to this.
DEFAULT_DATA_DIR = Path("data")

MAIN_DB_FILENAME = "main.db"
PYPI_DB_FILENAME = "pypi.db"

#: A PEP 503 normalized name: lowercase, no `.`/`_`, no run of `-`, and no
#: leading/trailing `-`. Applied to `main.wheel.project` and
#: `pypi_conda_names.pypi_name` so the join between them never silently
#: misses on a spelling difference -- see module docstring for why
#: `pypi_index.project` deliberately does *not* get this check. This is a
#: *sanity* check on writers (real normalization happens in Python before
#: the value ever reaches SQLite) -- it catches a normalization bug loudly
#: via a constraint violation instead of letting a wrong spelling sit in the
#: table.
_NORMALIZED_NAME_CHECK = """(
    {col} = lower({col})
    AND instr({col}, '_')  = 0
    AND instr({col}, '.')  = 0
    AND instr({col}, '--') = 0
    AND {col} NOT GLOB '-*'
    AND {col} NOT GLOB '*-'
)"""

# --------------------------------------------------------------------------- #
# main.db
# --------------------------------------------------------------------------- #

MAIN_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
) STRICT;

-- One row per pypi->conda name mapping. Deliberately just these three
-- columns -- see module docstring for the tri-state this encodes without
-- a fourth column, and why passthrough (no row) is not the same as an
-- explicit identity mapping (a row where conda_name = pypi_name).
CREATE TABLE IF NOT EXISTS pypi_conda_names (
    pypi_name  TEXT PRIMARY KEY CHECK {_NORMALIZED_NAME_CHECK.format(col="pypi_name")},
    -- NULL + updated_at NULL     -> never checked by the mapper job
    -- NULL + updated_at NOT NULL -> checked; no conda equivalent exists
    -- name + updated_at NOT NULL -> mapped
    conda_name TEXT CHECK (conda_name IS NULL OR conda_name = lower(conda_name)),
    updated_at INTEGER
) WITHOUT ROWID;

-- One row per .whl file. `filename` is the sole unique key (PyPI's filename
-- namespace is already global); `id` is a short surrogate for other tables
-- in this same file to reference instead of repeating the filename string.
CREATE TABLE IF NOT EXISTS wheel (
    id                  INTEGER PRIMARY KEY,
    filename            TEXT NOT NULL UNIQUE CHECK (filename LIKE '%.whl'),
    -- PEP 503 normalized, so this joins directly against
    -- pypi_conda_names.pypi_name. NOT part of any unique index -- identity
    -- is `filename` alone; see module docstring for why normalizing this
    -- is safe here even though `pypi_index.project` (raw layer) is not
    -- normalized.
    project             TEXT NOT NULL CHECK {_NORMALIZED_NAME_CHECK.format(col="project")},
    yanked              INTEGER NOT NULL DEFAULT 0 CHECK (yanked IN (0, 1)),
    -- NULL: not yet determined. 0/1: whether this wheel's dependency set (or
    -- the wheel's own version) required accepting a pre-release to convert.
    requires_prerelease INTEGER CHECK (requires_prerelease IN (0, 1)),
    -- The reroll version used to produce `reroll_data`/`reroll_errors`.
    reroll_version      TEXT,
    -- JSONB (see module docstring): the repodata entry reroll produced, or
    -- NULL if not yet attempted or the attempt did not succeed. Non-NULL is
    -- this wheel's *entire* "converted ok" signal -- see `reroll_errors`
    -- below for why there is no separate status column.
    reroll_data         BLOB CHECK (reroll_data IS NULL OR json_valid(reroll_data, 8)),
    -- JSONB object {{pypi_name: conda_name actually used}}, including this
    -- wheel's own project -- every name this conversion resolved through
    -- pypi_conda_names (or passthrough). Drives the invalidation sweep in
    -- the module docstring; see there for why this column, kept here rather
    -- than in `pypi.db`, keeps the conversion job a `main.db`-only writer.
    resolutions         BLOB CHECK (resolutions IS NULL OR json_valid(resolutions, 8)),
    updated_at          INTEGER
) STRICT;

-- Covering for "every wheel of project P" without touching the wheel table
-- itself (id is appended automatically as the rowid, filename is spelled
-- out explicitly so it does not have to be looked up separately).
CREATE INDEX IF NOT EXISTS wheel_project
    ON wheel(project, filename);

-- Partial index over every wheel that has not (yet) converted ok.
-- Deliberately a conjunction of two equalities/IS-NULLs: SQLite's
-- partial-index prover only reliably matches that shape (an `IN`/`<>`
-- predicate gets ignored and falls back to a full scan). Excludes yanked
-- wheels: nothing consumes a yanked wheel's reroll_data under normal
-- dependency resolution, so it is not worklist work.
--
-- This covers both "never attempted" and "attempted and settled on a
-- failure" rows -- unlike the `conversion_status`-based index this
-- replaced, a single-table predicate here cannot also exclude a wheel with
-- a `reroll_errors` row, since that lives in a different table. Consumers
-- (`reroll_data.reroll_convert`'s worklist query) narrow the rest of the
-- way with an anti-join against `reroll_errors` -- one indexed probe per
-- candidate row, filtering out `category <> 'runtime'` rows specifically
-- (see `reroll_errors` below for why `runtime` must stay eligible).
CREATE INDEX IF NOT EXISTS wheel_todo
    ON wheel(id)
    WHERE reroll_data IS NULL AND yanked = 0;

-- One row per wheel reroll has ever failed to convert on its most recent
-- attempt -- replaced in place (never accumulated) by the next attempt, and
-- deleted entirely once a wheel is re-armed or actually converts. See
-- module docstring's "A separate reroll_errors table" section for the full
-- design rationale (why this is a table rather than a `wheel` column, the
-- `runtime` carve-out, `ON DELETE CASCADE`).
CREATE TABLE IF NOT EXISTS reroll_errors (
    wheel_id     INTEGER PRIMARY KEY REFERENCES wheel(id) ON DELETE CASCADE,
    category     TEXT NOT NULL CHECK (category IN (
                     'scope', 'invalid', 'unconvertable', 'unavailable',
                     'unexpected', 'runtime'
                 )),
    -- The raising exception's class name, e.g. 'UnsupportedPrereleaseError'
    -- or 'MissingPypiCondaStaticMapping'. No CHECK/enum: reroll's exception
    -- surface is not a fixed set the way `category` is.
    sub_category TEXT,
    -- str(exception). Free text, deliberately unbounded -- see module
    -- docstring.
    description  TEXT,
    updated_at   INTEGER
) STRICT;

-- Lets a query cheaply isolate or exclude one or more categories -- e.g.
-- reroll_convert's worklist anti-join filtering out 'runtime' specifically,
-- or an ad hoc "how many unconvertable" count -- without a full table scan.
CREATE INDEX IF NOT EXISTS reroll_errors_category
    ON reroll_errors(category);
"""

#: The "this wheel still needs a `reroll_convert.convert` attempt" predicate,
#: aliasing `wheel` as `w` -- never converted ok, not yanked, and not already
#: settled on a non-`runtime` failure (a `runtime`-only row never settles a
#: wheel; see `reroll_errors`'s module docstring). Shared, verbatim, by
#: `stats_main` and `reroll_data.reroll_convert.convert`'s worklist/count
#: queries so the two can never silently disagree on what "outstanding"
#: means. Matches `wheel_todo`'s partial index (`reroll_data IS NULL AND
#: yanked = 0`) for the candidate rows it can seek, then narrows the rest of
#: the way with one indexed anti-join probe per candidate against
#: `reroll_errors`.
OUTSTANDING_WHEEL = """(
    w.reroll_data IS NULL AND w.yanked = 0 AND NOT EXISTS (
        SELECT 1 FROM reroll_errors e
        WHERE e.wheel_id = w.id AND e.category <> 'runtime'
    )
)"""

# --------------------------------------------------------------------------- #
# pypi.db
# --------------------------------------------------------------------------- #

PYPI_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
) STRICT;

-- One row per project as listed in the root /simple/ index. Unchanged in
-- shape from the legacy ``v.db`` `project` table -- nothing in this schema
-- pass touched the crawl's own worklist.
CREATE TABLE IF NOT EXISTS project (
    -- Display name exactly as returned by the root index (not normalised;
    -- that only happens on main.wheel.project, see module docstring).
    name           TEXT PRIMARY KEY,
    -- __last-serial from the root index: the state we want to reach.
    index_serial   INTEGER NOT NULL,
    -- Serial of the data actually stored. NULL means never crawled.
    crawled_serial INTEGER,
    status         TEXT CHECK (status IS NULL OR status IN ('pending', 'done', 'gone', 'error')),
    n_wheels       INTEGER,
    error          TEXT,
    fetched_at     INTEGER
) STRICT;

-- Partial index over just the outstanding work, so building the worklist
-- stays fast once the vast majority of 860k+ projects are done.
CREATE INDEX IF NOT EXISTS project_pending
    ON project(name)
    WHERE crawled_serial IS NULL OR index_serial > crawled_serial;

-- One row per .whl file, as observed in its project's /simple/ page (the
-- PEP 691 file record). `filename` is the sole key, same reasoning as
-- main.wheel: PyPI's filename namespace is already global. Only the two
-- fields anything in this codebase actually filters or joins on in SQL are
-- promoted to real columns -- see module docstring's "filtered-or-joined-on"
-- rule; everything else lives inside `pypi_metadata`.
CREATE TABLE IF NOT EXISTS pypi_index (
    filename        TEXT PRIMARY KEY CHECK (filename LIKE '%.whl'),
    project         TEXT NOT NULL,
    -- Yanked status genuinely changes after the fact (PEP 592), so this is
    -- refreshed on every crawl rather than left to a one-time write.
    yanked          INTEGER NOT NULL DEFAULT 0 CHECK (yanked IN (0, 1)),
    -- Compared directly against wheel_metadata.blob_sha256 (sync/claim
    -- queries in reroll_data.metadata) -- the one field here that is a real
    -- join key, hence promoted rather than folded into pypi_metadata.
    metadata_sha256 BLOB CHECK (metadata_sha256 IS NULL OR length(metadata_sha256) = 32),
    -- JSONB object: url, size, upload_time, requires_python, sha256 (hex --
    -- JSON has no binary type, so this one copy of the digest is text
    -- unlike metadata_sha256 above), yanked_reason, has_metadata,
    -- provenance_url, any non-sha256 hashes, and whatever PEP 691 keys this
    -- version does not (yet) model explicitly. Read only ever after the row
    -- has already been located by `filename`; never filtered on directly.
    pypi_metadata   BLOB CHECK (pypi_metadata IS NULL OR json_valid(pypi_metadata, 8))
) STRICT;

CREATE INDEX IF NOT EXISTS pypi_index_project
    ON pypi_index(project, filename);

-- Per-wheel PEP 658 metadata fetch state machine. Narrow rows only; bodies
-- live in metadata_blob (content-addressed, see below). `filename` is the
-- sole key -- splitting away from the sha256 join-key scheme the original
-- crawl-era schema used.
CREATE TABLE IF NOT EXISTS wheel_metadata (
    filename       TEXT NOT NULL PRIMARY KEY CHECK (filename LIKE '%.whl'),
    project        TEXT NOT NULL,
    -- 'todo'    -- not looked at yet
    -- 'lease'   -- in flight; reclaimable once lease_until has passed
    -- 'done'    -- body stored, blob_sha256 points at it
    -- 'missing' -- index says there is no sidecar (verified: PyPI 404s)
    -- 'error'   -- gave up after repeated failures; see `error`
    state          TEXT NOT NULL CHECK (state IN ('todo', 'lease', 'done', 'missing', 'error')),
    -- The digest actually stored. Compared against pypi_index.metadata_sha256
    -- to detect upstream changes, mirroring crawled_serial vs index_serial.
    blob_sha256    BLOB CHECK (blob_sha256 IS NULL OR length(blob_sha256) = 32),
    -- Unix time this lease expires. A killed worker's rows are reclaimed
    -- then, so progress is never permanently stuck on an in-flight row.
    lease_until    INTEGER,
    attempts       INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    -- Which reroll.wheel_metadata parser version parsed the *linked*
    -- metadata_blob row's `parsed_json` -- denormalized here rather than
    -- onto metadata_blob itself, since this describes this wheel's fetch
    -- attempt, not an intrinsic property of the (deduplicated, shared) body.
    parser_version TEXT,
    updated_at     INTEGER
) STRICT;

-- Partial indexes over just the open work, so claiming a batch stays a seek
-- rather than a scan once most rows are 'done'. Single-equality predicates
-- deliberately: SQLite's partial-index prover only reliably matches that
-- shape.
CREATE INDEX IF NOT EXISTS wheel_metadata_todo
    ON wheel_metadata(attempts)
    WHERE state = 'todo';

CREATE INDEX IF NOT EXISTS wheel_metadata_lease
    ON wheel_metadata(lease_until)
    WHERE state = 'lease';

-- Content-addressed store: one row per *distinct* metadata body, not per
-- wheel. Measured on a full 11.1M-wheel crawl, 11,135,055 wheels carry only
-- 7,402,585 distinct metadata_sha256 values (1.50x) -- since the digest is
-- known from the index before downloading, a body already held needs no
-- request at all, so this table is what saves ~3.7M fetches, not just disk.
-- Deliberately a rowid table (not WITHOUT ROWID): the ~11M existence probes
-- ("do we already have this digest?") then touch only the slim UNIQUE index
-- instead of descending a tree whose leaves are padded with body bytes.
CREATE TABLE IF NOT EXISTS metadata_blob (
    id          INTEGER PRIMARY KEY,
    sha256      BLOB NOT NULL UNIQUE CHECK (length(sha256) = 32),
    -- Uncompressed length, so consumers can size a buffer without inflating.
    n_bytes     INTEGER NOT NULL,
    codec       TEXT NOT NULL DEFAULT 'zlib6',
    -- BLOB, never TEXT: METADATA is not reliably valid UTF-8 and may carry
    -- embedded NULs. TEXT breaks both ways here: binding a
    -- surrogateescape-decoded str raises UnicodeEncodeError, and SQLite's
    -- string functions silently truncate at the first NUL.
    z_body      BLOB NOT NULL,
    -- JSONB: structured METADATA fields (name, version, license,
    -- requires_dist, ...). NULL means not parsed yet, or parsing failed.
    parsed_json BLOB CHECK (parsed_json IS NULL OR json_valid(parsed_json, 8)),
    stored_at   INTEGER
) STRICT;
"""


def _connect(path: Path, *, read_only: bool) -> sqlite3.Connection:
    """Open a connection with the pragmas this workload needs.

    Same WAL/synchronous/busy_timeout choices, same
    directory-creation-on-first-write behaviour as the legacy ``v.db``
    connector this replaced. Kept as its own function (rather than a shared
    helper) so this module has no import-time dependency on anything else.
    """
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=60.0, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=60000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


#: Default threshold for :func:`wal_monitor`'s first warning. SQLite's own
#: automatic checkpoint fires every ~4 MB (`wal_autocheckpoint`'s default of
#: 1000 pages) and normally keeps `-wal` near that size, so anything reaching
#: this size means checkpointing has stalled, not just fallen behind briefly.
WAL_WARN_BYTES = 256 * 1024 * 1024  # 256 MiB


def wal_monitor(
    path: Path | str,
    *,
    threshold_bytes: int = WAL_WARN_BYTES,
    logger: logging.Logger | None = None,
) -> Callable[[], int]:
    """Build a callable that logs loudly the first time (and again each time
    it doubles again past) `<path>-wal` crosses `threshold_bytes`.

    A `-wal` file that keeps growing well past SQLite's own automatic
    checkpoint threshold almost always means a checkpoint has stopped making
    progress entirely -- most likely a long-lived reader (an open cursor, or
    an idle connection from e.g. a notebook) pinning the oldest snapshot the
    WAL cannot be trimmed past. See `reroll_convert.convert`/
    `crawl.crawl`/`db2_backfill`, which used to hold exactly such a cursor
    open for an entire multi-hour run before this was fixed.

    Callers are expected to invoke the returned callable periodically (e.g.
    once per progress report, not once per row) -- the doubling threshold
    keeps this from spamming the log even if called often while the WAL
    stays oversized for a long time.

    Schema-agnostic: takes a bare database path, so it works the same for
    `main.db`, `pypi.db`, or (during migration) the legacy `v.db`.
    """
    log = logger or logging.getLogger(__name__)
    wal_path = Path(str(path) + "-wal")
    next_warn = threshold_bytes

    def check() -> int:
        nonlocal next_warn
        try:
            size = wal_path.stat().st_size
        except FileNotFoundError:
            return 0
        if size >= next_warn:
            log.warning(
                "%s has grown to %.0f MiB (>= %.0f MiB threshold) -- "
                "checkpointing looks stuck. Likely cause: a long-lived "
                "reader (an open cursor or idle connection) pinning the WAL "
                "snapshot -- check `lsof %s*` for other open connections. "
                "Once nothing else has the db open: "
                "sqlite3 %s \"PRAGMA wal_checkpoint(TRUNCATE);\"",
                wal_path,
                size / (1024 * 1024),
                next_warn / (1024 * 1024),
                path,
                path,
            )
            while size >= next_warn:
                next_warn *= 2
        return size

    return check


def connect_main(
    data_dir: Path | str = DEFAULT_DATA_DIR, *, read_only: bool = False
) -> sqlite3.Connection:
    """Open `<data_dir>/main.db`."""
    return _connect(Path(data_dir) / MAIN_DB_FILENAME, read_only=read_only)


def connect_pypi(
    data_dir: Path | str = DEFAULT_DATA_DIR, *, read_only: bool = False
) -> sqlite3.Connection:
    """Open `<data_dir>/pypi.db`."""
    return _connect(Path(data_dir) / PYPI_DB_FILENAME, read_only=read_only)


class SchemaMismatch(RuntimeError):
    """Raised when an existing database's tables do not match its schema.

    `CREATE TABLE IF NOT EXISTS` is a no-op against a table that already
    exists under a *different* definition -- it neither creates the missing
    CHECK/STRICT-ness nor complains that they are missing. Since
    `ALTER TABLE` cannot add a CHECK constraint or make an existing table
    STRICT, there is no safe automatic fix once a mismatched table exists;
    per this project's data-preservation rules, `init_main`/`init_pypi`
    refuse to silently proceed and raise this instead so a human decides how
    to migrate.
    """


def _check_table_shape(
    db: sqlite3.Connection,
    *,
    expected_strict: dict[str, bool],
    expected_columns: dict[str, set[str]],
) -> list[str]:
    """Return a list of human-readable problems, empty if everything matches.

    Compares only the properties `ALTER TABLE` cannot retrofit -- STRICT-ness
    and column presence -- since those are what would make an existing table
    silently diverge from its schema after a plain `CREATE TABLE IF NOT
    EXISTS`. This is not a full DDL diff (it does not, for example,
    re-derive each CHECK expression from `sqlite_master`); it is the
    practical subset worth automating. Shared by both `init_main` and
    `init_pypi`.
    """
    problems: list[str] = []
    tables = {
        row[0]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    for table, want_strict in expected_strict.items():
        if table not in tables:
            continue  # init_*() will CREATE TABLE IF NOT EXISTS this one.
        info = db.execute(f"PRAGMA table_list({table})").fetchall()
        # PRAGMA table_list columns: (schema, name, type, ncol, wr, strict)
        is_strict = bool(info[0][5]) if info else False
        if is_strict != want_strict:
            problems.append(
                f"table {table!r} exists but strict={is_strict} "
                f"(expected {want_strict}); ALTER TABLE cannot fix this -- "
                "a rebuild (CREATE new table, INSERT...SELECT, swap) is "
                "required"
            )
    for table, want_cols in expected_columns.items():
        if table not in tables:
            continue
        have_cols = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        missing = want_cols - have_cols
        if missing:
            problems.append(
                f"table {table!r} is missing column(s) {sorted(missing)}; "
                "CHECK/STRICT constraints on a new column cannot be added "
                "via ALTER TABLE -- a rebuild is required, not a plain "
                "ADD COLUMN"
            )
    return problems


_MAIN_EXPECTED_STRICT = {
    "meta": True,
    # WITHOUT ROWID is its own storage mode -- SQLite does not allow a table
    # to be both WITHOUT ROWID and STRICT.
    "pypi_conda_names": False,
    "wheel": True,
    "reroll_errors": True,
}

_MAIN_EXPECTED_COLUMNS = {
    "meta": {"key", "value"},
    "pypi_conda_names": {"pypi_name", "conda_name", "updated_at"},
    "wheel": {
        "id", "filename", "project", "yanked", "requires_prerelease",
        "reroll_version", "reroll_data", "resolutions", "updated_at",
    },
    "reroll_errors": {
        "wheel_id", "category", "sub_category", "description", "updated_at",
    },
}

_PYPI_EXPECTED_STRICT = {
    "meta": True,
    "project": True,
    "pypi_index": True,
    "wheel_metadata": True,
    "metadata_blob": True,
}

_PYPI_EXPECTED_COLUMNS = {
    "meta": {"key", "value"},
    "project": {
        "name", "index_serial", "crawled_serial", "status", "n_wheels",
        "error", "fetched_at",
    },
    "pypi_index": {
        "filename", "project", "yanked", "metadata_sha256", "pypi_metadata",
    },
    "wheel_metadata": {
        "filename", "project", "state", "blob_sha256", "lease_until",
        "attempts", "error", "parser_version", "updated_at",
    },
    "metadata_blob": {
        "id", "sha256", "n_bytes", "codec", "z_body", "parsed_json", "stored_at",
    },
}


def init_main(db: sqlite3.Connection) -> None:
    """Create any missing `main.db` tables/indexes, verifying existing ones.

    Safe to call against a brand-new file (everything is created fresh) or
    against a `main.db` already on this exact schema (every statement is a
    no-op). Raises :class:`SchemaMismatch` if an existing table's STRICT-ness
    or column set has drifted from `MAIN_SCHEMA` -- deliberately not
    attempting an automatic rebuild; see `SchemaMismatch`'s docstring.
    """
    problems = _check_table_shape(
        db, expected_strict=_MAIN_EXPECTED_STRICT, expected_columns=_MAIN_EXPECTED_COLUMNS
    )
    if problems:
        raise SchemaMismatch(
            "existing main.db does not match reroll_data.db2.MAIN_SCHEMA:\n"
            + "\n".join(f"  - {p}" for p in problems)
        )
    db.executescript(MAIN_SCHEMA)


def init_pypi(db: sqlite3.Connection) -> None:
    """Create any missing `pypi.db` tables/indexes, verifying existing ones.

    Same contract as :func:`init_main`, against `PYPI_SCHEMA` instead.
    """
    problems = _check_table_shape(
        db, expected_strict=_PYPI_EXPECTED_STRICT, expected_columns=_PYPI_EXPECTED_COLUMNS
    )
    if problems:
        raise SchemaMismatch(
            "existing pypi.db does not match reroll_data.db2.PYPI_SCHEMA:\n"
            + "\n".join(f"  - {p}" for p in problems)
        )
    db.executescript(PYPI_SCHEMA)


def init_all(data_dir: Path | str = DEFAULT_DATA_DIR) -> None:
    """Connect to and initialize both `main.db` and `pypi.db` under `data_dir`.

    Convenience for a fresh setup; each database still gets its own
    connection (and its own writer thread, in the crawler/conversion jobs
    themselves) -- this just avoids the two-call boilerplate of `connect_*`
    + `init_*` for each file when both are being created together.
    """
    main_db = connect_main(data_dir)
    try:
        init_main(main_db)
    finally:
        main_db.close()

    pypi_db = connect_pypi(data_dir)
    try:
        init_pypi(pypi_db)
    finally:
        pypi_db.close()


def get_meta(db: sqlite3.Connection, key: str) -> str | None:
    """Read from `<db>.meta`. Each of `main.db`/`pypi.db` has its own."""
    row = db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return None if row is None else row[0]


def set_meta(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def stats_main(db: sqlite3.Connection) -> dict[str, int]:
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    by_category = dict(
        db.execute("SELECT category, count(*) FROM reroll_errors GROUP BY 1")
    )
    return {
        "wheels": q("SELECT count(*) FROM wheel"),
        "yanked": q("SELECT count(*) FROM wheel WHERE yanked = 1"),
        # A `runtime`-only row does not settle a wheel's failure (see
        # `reroll_errors`'s module docstring), so it is still outstanding.
        "outstanding": q(f"SELECT count(*) FROM wheel w WHERE {OUTSTANDING_WHEEL}"),
        "ok": q("SELECT count(*) FROM wheel WHERE reroll_data IS NOT NULL"),
        "scope": by_category.get("scope", 0),
        "invalid": by_category.get("invalid", 0),
        "unconvertable": by_category.get("unconvertable", 0),
        "unavailable": by_category.get("unavailable", 0),
        "unexpected": by_category.get("unexpected", 0),
        # Accounting only -- see `reroll_errors`'s module docstring for why
        # this is never a settled failure and is already folded into
        # "outstanding" above rather than subtracted from it.
        "runtime": by_category.get("runtime", 0),
        "mapped_names": q(
            "SELECT count(*) FROM pypi_conda_names WHERE conda_name IS NOT NULL"
        ),
        "unmappable_names": q(
            "SELECT count(*) FROM pypi_conda_names "
            "WHERE conda_name IS NULL AND updated_at IS NOT NULL"
        ),
        "unchecked_names": q(
            "SELECT count(*) FROM pypi_conda_names WHERE updated_at IS NULL"
        ),
    }


def stats_pypi(db: sqlite3.Connection) -> dict[str, int]:
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    by_state = dict(
        db.execute("SELECT state, count(*) FROM wheel_metadata GROUP BY state")
    )
    return {
        "projects": q("SELECT count(*) FROM project"),
        "pending_projects": q(
            "SELECT count(*) FROM project "
            "WHERE crawled_serial IS NULL OR index_serial > crawled_serial"
        ),
        "files": q("SELECT count(*) FROM pypi_index"),
        "yanked": q("SELECT count(*) FROM pypi_index WHERE yanked = 1"),
        "metadata_todo": by_state.get("todo", 0),
        "metadata_lease": by_state.get("lease", 0),
        "metadata_done": by_state.get("done", 0),
        "metadata_missing": by_state.get("missing", 0),
        "metadata_error": by_state.get("error", 0),
        "blobs": q("SELECT count(*) FROM metadata_blob"),
    }
