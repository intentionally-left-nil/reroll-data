"""SQLite storage for the PyPI wheel index.

Concurrency model
-----------------
SQLite supports exactly **one** writer at a time. WAL mode permits many
concurrent readers alongside that single writer, but two simultaneous write
transactions will always cause one to fail with ``database is locked``
(``busy_timeout`` only converts that failure into a blocking retry, which
serialises them anyway). The crawler therefore routes every mutation through a
single dedicated writer thread; see :mod:`reroll_data.crawl`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# The corpus the crawl actually populates, so bare `reroll-data` and
# `reroll-investigate` invocations agree with the Makefile instead of pointing
# at a path that has never existed.
DEFAULT_DB = Path("data/v.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- One row per project as listed in the root /simple/ index.
CREATE TABLE IF NOT EXISTS project (
    -- Display name exactly as returned by the root index (not normalised).
    name           TEXT PRIMARY KEY,
    -- _last-serial from the root index: the state we want to reach.
    index_serial   INTEGER NOT NULL,
    -- Serial of the data actually stored. NULL means never crawled.
    -- Work to do == crawled_serial IS NULL OR index_serial > crawled_serial.
    crawled_serial INTEGER,
    -- NULL/'pending' | 'done' | 'gone' (404) | 'error'
    status         TEXT,
    n_wheels       INTEGER,
    error          TEXT,
    fetched_at     INTEGER
);

-- Partial index over just the outstanding work, so building the worklist stays
-- fast once the vast majority of the 860k+ projects are done.
CREATE INDEX IF NOT EXISTS project_pending
    ON project(name)
    WHERE crawled_serial IS NULL OR index_serial > crawled_serial;

-- One row per .whl file. Every field of the PEP 691 file record is stored;
-- `extra_json` captures any keys this version does not model explicitly.
CREATE TABLE IF NOT EXISTS wheel (
    project         TEXT NOT NULL,
    filename        TEXT NOT NULL,
    url             TEXT,
    size            INTEGER,
    upload_time     TEXT,
    requires_python TEXT,
    sha256          TEXT,
    -- Only populated when hashes carries algorithms besides sha256.
    hashes_json     TEXT,
    yanked          INTEGER NOT NULL DEFAULT 0,
    yanked_reason   TEXT,
    -- PEP 658: a .metadata sidecar is served next to the wheel.
    has_metadata    INTEGER,
    metadata_sha256 TEXT,
    provenance_url  TEXT,
    extra_json      TEXT,
    first_seen      INTEGER,
    last_seen       INTEGER,
    PRIMARY KEY (project, filename)
) WITHOUT ROWID;

-- ------------------------------------------------------------------------- --
-- PEP 658 core-metadata bodies. See :mod:`reroll_data.metadata`.
-- ------------------------------------------------------------------------- --

-- Content-addressed store: one row per *distinct* metadata body, not per
-- wheel. Measured on a full 11.1M-wheel crawl, 11,135,055 wheels carry only
-- 7,402,585 distinct metadata_sha256 values (1.50x). Since the digest is known
-- from the index *before* downloading, a body we already hold needs no request
-- at all -- that dedup is worth ~3.7M fetches, not just disk.
--
-- Deliberately a rowid table with a UNIQUE index rather than
-- `WITHOUT ROWID`: the ~11M existence probes ("do we already have this
-- digest?") then touch only the slim index (~0.5 GB, which stays cached)
-- instead of descending a B-tree whose leaves are padded with body bytes.
CREATE TABLE IF NOT EXISTS metadata_blob (
    id      INTEGER PRIMARY KEY,
    -- Hex sha256 of the *uncompressed* body, matching wheel.metadata_sha256.
    sha256  TEXT NOT NULL UNIQUE,
    -- Uncompressed length, so consumers can size a buffer without inflating.
    n_bytes INTEGER NOT NULL,
    -- zlib-compressed body (level 6; ~2.81x on real bodies).
    --
    -- BLOB, never TEXT. METADATA is not reliably valid UTF-8 and may carry
    -- embedded NULs, and TEXT breaks both ways: binding a
    -- surrogateescape-decoded str raises UnicodeEncodeError (which would kill
    -- the writer thread mid-batch), and SQLite's string functions silently
    -- truncate at the first NUL -- `length()` under-reports and `LIKE` fails
    -- to match past it, so any future SQL-side search would quietly miss data.
    z_body  BLOB NOT NULL,
    stored_at INTEGER,
    -- Structured fields pulled out of the body (name, version, license,
    -- requires_dist, ...) via reroll.wheel_metadata.parse_metadata, serialised
    -- as JSON text. Parsing the raw body on every read would mean re-running
    -- packaging's email parser plus pydantic validation on every one of the
    -- ~11M wheels every time; storing the result once per distinct body
    -- (naturally deduplicated the same way z_body is) makes it a plain
    -- column read instead. NULL means either not parsed yet (this row
    -- predates the column -- see `_add_missing_columns`) or parsing failed;
    -- `reroll_data.metadata` populates it for bodies fetched from here on.
    -- Existing rows are backfilled separately.
    parsed_json TEXT
);

-- Per-wheel fetch state machine. Narrow rows only; bodies live in
-- metadata_blob. Kept separate from `wheel` so that (a) crawl.py needs no
-- changes and its UPSERT cannot clobber fetch state, and (b) the high-churn
-- lease updates hit small pages here instead of rewriting 443-byte rows in the
-- 5.5 GB `wheel` B-tree.
CREATE TABLE IF NOT EXISTS wheel_metadata (
    project     TEXT NOT NULL,
    filename    TEXT NOT NULL,
    -- 'todo'    -- not looked at yet
    -- 'lease'   -- in flight; reclaimable once lease_until has passed
    -- 'done'    -- body stored, blob_sha256 points at it
    -- 'missing' -- index says there is no sidecar (verified: PyPI 404s)
    -- 'error'   -- gave up after repeated failures; see `error`
    state       TEXT NOT NULL,
    -- The digest we actually stored. Compared against wheel.metadata_sha256 to
    -- detect upstream changes, mirroring crawled_serial vs index_serial.
    blob_sha256 TEXT,
    -- Unix time this lease expires. A killed worker's rows are reclaimed then,
    -- so progress is never permanently stuck on an in-flight row.
    lease_until INTEGER,
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    updated_at  INTEGER,
    PRIMARY KEY (project, filename)
) WITHOUT ROWID;

-- Partial indexes over just the open work, so claiming a batch stays a seek
-- rather than a scan once most of the 11M rows are 'done'. They shrink to
-- nothing as the corpus completes.
--
-- The predicate is a *single equality* deliberately. SQLite will only use a
-- partial index when it can prove the query implies the index's WHERE clause,
-- and its prover is limited: measured at 800k rows, predicates of the form
-- `state IN ('todo','lease','error')` and `state <> 'done'` were both ignored
-- entirely (full scan), while `state = 'todo'` is matched and gives a covering
-- index seek -- 0.04 ms versus 3.5 ms, for 0.02 MB instead of the ~825 MB a
-- plain (state, attempts) index would cost at 11M rows.
CREATE INDEX IF NOT EXISTS wheel_metadata_todo
    ON wheel_metadata(attempts)
    WHERE state = 'todo';

CREATE INDEX IF NOT EXISTS wheel_metadata_lease
    ON wheel_metadata(lease_until)
    WHERE state = 'lease';

-- ------------------------------------------------------------------------- --
-- Repodata conversion comparison: reroll's own translator vs upstream
-- conda-pypi, run over every wheel. See :mod:`reroll_data.repodata_sync`.
-- ------------------------------------------------------------------------- --

-- One row per wheel. `repodata_sync.sync()` is the only writer that inserts
-- rows and the only one that ever touches `conda_pypi_compatible`; everything
-- else here (the two `_data`/`_error` pairs, and `reroll_compatible`) is left
-- for whatever job actually runs each converter to fill in later -- sync()
-- only ensures a row exists and that its filename-derived compatibility flag
-- is set, the same division of labour as `wheel_metadata`'s `sync` vs `fetch`.
CREATE TABLE IF NOT EXISTS repodata_conversion (
    project               TEXT NOT NULL,
    filename              TEXT NOT NULL,
    -- JSON text of the repodata entry reroll's own translator produced, or
    -- NULL if not yet attempted or the attempt errored.
    reroll_data           TEXT,
    -- Error message from reroll's attempt (type + message), or NULL if it
    -- succeeded or has not been attempted yet.
    reroll_error          TEXT,
    -- JSON text of the repodata["v3"]["whl"] entry conda-pypi produced (see
    -- reroll_data.conda_pypi_index_demo), or NULL if not yet attempted / errored.
    conda_pypi_data       TEXT,
    conda_pypi_error      TEXT,
    -- 0/1, computed purely from the filename's (interpreter, abi, platform)
    -- tag triple: interpreter GLOB 'py3*', abi = 'none', platform = 'any' --
    -- see reroll_data.repodata_sync.is_conda_pypi_compatible. Set for every
    -- row by sync(); never NULL once synced, regardless of whether the
    -- conda-pypi conversion itself has run yet.
    conda_pypi_compatible INTEGER NOT NULL,
    -- 0/1/NULL. Unknown ("NULL") until a separate probe run against reroll
    -- decides it -- sync() always inserts NULL here and never overwrites it.
    reroll_compatible     INTEGER,
    updated_at            INTEGER,
    PRIMARY KEY (project, filename)
) WITHOUT ROWID;

-- Partial index over just the conda-pypi work still outstanding, so
-- `repodata_convert.convert()`'s claim scan stays a seek once most rows have
-- a conda_pypi_data or conda_pypi_error. Two IS NULL terms ANDed together are
-- exactly the query's own WHERE clause (see `repodata_convert`), which is the
-- straightforward conjunctive case SQLite's partial-index prover handles --
-- unlike the IN/<> forms noted in `wheel_metadata_todo` above, which it does
-- not. `conda_pypi_compatible` is the indexed column so `= 1` inside that
-- narrowed set is a cheap seek rather than a second scan.
CREATE INDEX IF NOT EXISTS repodata_conversion_todo
    ON repodata_conversion(conda_pypi_compatible)
    WHERE conda_pypi_data IS NULL AND conda_pypi_error IS NULL;
"""


def connect(path: Path | str, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a connection with the pragmas this workload needs."""
    path = Path(path)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=60.0, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=60000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


# Columns added to a table after its first release. `CREATE TABLE IF NOT
# EXISTS` in `SCHEMA` above only creates a table that does not exist yet -- on
# a database that already has the table (e.g. the multi-day `v.db` corpus),
# it is a no-op and never adds a column a later version of `SCHEMA` introduced.
# `_add_missing_columns` closes that gap with `ALTER TABLE ... ADD COLUMN`,
# which SQLite performs in place (existing rows read back NULL for it; nothing
# is rewritten or rebuilt), so it is safe to run against a database that
# already holds data. A brand new database gets the column straight from
# `SCHEMA` instead, so each entry here only ever fires once per database, the
# first time `init()` runs after upgrading.
_ADDED_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "metadata_blob": [("parsed_json", "TEXT")],
}


def _add_missing_columns(db: sqlite3.Connection) -> None:
    for table, columns in _ADDED_COLUMNS.items():
        existing = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        for name, coltype in columns:
            if name not in existing:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {coltype}")


def init(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    _add_missing_columns(db)


def get_meta(db: sqlite3.Connection, key: str) -> str | None:
    row = db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return None if row is None else row[0]


def set_meta(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def stats(db: sqlite3.Connection) -> dict[str, int]:
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    return {
        "projects": q("SELECT count(*) FROM project"),
        "pending": q(
            "SELECT count(*) FROM project "
            "WHERE crawled_serial IS NULL OR index_serial > crawled_serial"
        ),
        "done": q("SELECT count(*) FROM project WHERE status = 'done'"),
        "gone": q("SELECT count(*) FROM project WHERE status = 'gone'"),
        "error": q("SELECT count(*) FROM project WHERE status = 'error'"),
        # Crawled, but the index still reports a newer serial -- normally 0.
        # A persistently non-zero value means project pages are being served
        # staler than the root index, and those projects re-crawl every run.
        "stale": q(
            "SELECT count(*) FROM project "
            "WHERE status = 'done' AND index_serial > crawled_serial"
        ),
        "wheels": q("SELECT count(*) FROM wheel"),
        "yanked": q("SELECT count(*) FROM wheel WHERE yanked = 1"),
    }


def metadata_stats(
    db: sqlite3.Connection, *, include_bytes: bool = False
) -> dict[str, int]:
    """Counts for the metadata state machine.

    Deliberately *not* folded into :func:`stats`, which ``refresh_index`` calls
    twice per run -- these add several more full scans of an 11M-row table.

    `include_bytes` is opt-in because summing over `metadata_blob` has to walk
    every leaf page of a table that grows to ~20 GB, which takes minutes. The
    state counts come from a much smaller index and stay quick, so progress can
    be checked cheaply during a multi-day run.
    """
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    by_state = dict(
        db.execute("SELECT state, count(*) FROM wheel_metadata GROUP BY state")
    )
    out = {
        "tracked": sum(by_state.values()),
        "todo": by_state.get("todo", 0),
        "lease": by_state.get("lease", 0),
        "done": by_state.get("done", 0),
        "missing": by_state.get("missing", 0),
        "error": by_state.get("error", 0),
        "blobs": q("SELECT count(*) FROM metadata_blob"),
    }
    if include_bytes:
        out["blob_bytes_z"] = q(
            "SELECT coalesce(sum(length(z_body)), 0) FROM metadata_blob"
        )
        out["blob_bytes_raw"] = q("SELECT coalesce(sum(n_bytes), 0) FROM metadata_blob")
    return out


def repodata_conversion_stats(db: sqlite3.Connection) -> dict[str, int]:
    """Counts for the reroll-vs-conda-pypi repodata comparison.

    Not folded into :func:`stats` for the same reason as :func:`metadata_stats`
    -- this is its own full scan of a many-million-row table, so callers only
    pay for it when they ask.
    """
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    return {
        "tracked": q("SELECT count(*) FROM repodata_conversion"),
        "wheels": q("SELECT count(*) FROM wheel"),
        "conda_pypi_ok": q(
            "SELECT count(*) FROM repodata_conversion WHERE conda_pypi_compatible = 1"
        ),
        "conda_pypi_data": q(
            "SELECT count(*) FROM repodata_conversion WHERE conda_pypi_data IS NOT NULL"
        ),
        "conda_pypi_error": q(
            "SELECT count(*) FROM repodata_conversion WHERE conda_pypi_error IS NOT NULL"
        ),
        "reroll_data": q(
            "SELECT count(*) FROM repodata_conversion WHERE reroll_data IS NOT NULL"
        ),
        "reroll_error": q(
            "SELECT count(*) FROM repodata_conversion WHERE reroll_error IS NOT NULL"
        ),
        "reroll_ok": q(
            "SELECT count(*) FROM repodata_conversion WHERE reroll_compatible = 1"
        ),
        "reroll_bad": q(
            "SELECT count(*) FROM repodata_conversion WHERE reroll_compatible = 0"
        ),
        "reroll_unknown": q(
            "SELECT count(*) FROM repodata_conversion WHERE reroll_compatible IS NULL"
        ),
    }
