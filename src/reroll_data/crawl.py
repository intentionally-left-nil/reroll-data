"""Incremental crawl of every .whl filename on PyPI, into the ``db2`` schema.

Targets ``pypi.db``/``main.db`` (:mod:`reroll_data.db2`) exclusively -- this
module no longer reads or writes the legacy ``v.db`` (:mod:`reroll_data.db`;
that migration is done, see :mod:`reroll_data.db2_backfill`). ``wal_monitor``
is the one thing still borrowed from :mod:`reroll_data.db`, since it has no
dependency on either schema.

How the incremental logic works
--------------------------------
The root ``/simple/`` index reports a ``_last-serial`` for every project, which
increments whenever anything about that project changes. We store it as
``pypi.db.project.index_serial`` (the state we want) alongside
``crawled_serial`` (the state we have). Outstanding work is therefore just::

    crawled_serial IS NULL OR index_serial > crawled_serial

So a refresh costs a single request, and only genuinely-changed projects get
re-fetched.

Note that ``pypi_simple.IndexPage`` discards the per-project serials -- its
``projects`` field is a ``list[str]`` -- so the root index is parsed from the
raw JSON body rather than through that class.

Deletions, not a 'gone' status
-------------------------------
The old ``v.db`` design marked a project ``status = 'gone'`` and left its rows
in place forever -- both when the root index stopped listing it, and when its
own ``/simple/<name>/`` page 404s mid-crawl. That undercounts what "gone"
should mean: a project rename to a *different raw display spelling* that
happens to still be there also makes the old raw name vanish from the index,
so ``gone`` picked up renames as false positives (see
:mod:`reroll_data.db2_backfill`'s module docstring for the corpus evidence);
and more fundamentally, once PyPI stops listing a project at all, none of its
wheels can be downloaded any more either -- so leaving its rows behind serves
no purpose beyond bloat.

This module deletes instead, driven by ``pypi.db`` alone (never a full scan --
see :func:`sync_consistency` for that):

1. Fetch every project currently in the root index.
2. Diff that against ``pypi.db.project`` to find names the index no longer
   reports.
3. For each, look up every filename it owns in ``pypi.db.pypi_index``.
4. Delete those filenames from ``main.db.wheel`` first.
5. Delete the project's rows from ``pypi.db.project``/``pypi_index``/
   ``wheel_metadata``.

Deleting ``main.db`` first, and the ``pypi.db.project`` row itself last,
means an interrupted deletion is safely retried: as long as
``pypi.db.project`` still has the row, :func:`_delete_project` has not
finished, and re-running it (e.g. on the next `refresh`) is a correct,
idempotent no-op over whatever already got deleted.

A rename to a raw spelling that normalizes to the same PEP 503 name is *not*
special-cased -- it goes through the same delete, then gets re-added as a
"new" project on the very next crawl. Conservative rather than clever: see
the module's own design discussion for why detecting the rename precisely
was judged not worth it.

The exact same deletion happens if a project's own page 404s mid-crawl
(deleted between `refresh` and now) -- see the ``"deleted"`` `Result` status
below, applied by the writer thread rather than the worker that discovers it
(the writer is the sole thread allowed to mutate either database).
"""

from __future__ import annotations

import json
import queue
import re
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pypi_simple import (
    ACCEPT_JSON_ONLY,
    PYPI_SIMPLE_ENDPOINT,
    DistributionPackage,
    NoSuchProjectError,
    PyPISimple,
)

from . import db as _db
from . import db2 as _db2
from .ratelimit import TokenBucket

USER_AGENT = (
    "reroll-data/0.1 (+https://github.com/anaconda/reroll-data; akulkarni@anaconda.com)"
)

#: File-record keys we model explicitly (either as a `pypi_index` column, or
#: folded into `pypi_metadata` by :func:`_build_pypi_metadata`). Anything else
#: lands in `pypi_metadata` too, under its own raw key, so future PEP 691
#: additions are not silently dropped.
KNOWN_FILE_KEYS = frozenset(
    {
        "filename",
        "url",
        "size",
        "upload-time",
        "requires-python",
        "hashes",
        "yanked",
        "core-metadata",
        # Deprecated aliases of core-metadata; intentionally not stored twice.
        "data-dist-info-metadata",
        "dist-info-metadata",
        "provenance",
        "gpg-sig",
    }
)

# PEP 503 normalization: lowercase, and every run of `-`/`_`/`.` collapsed to
# one `-`. Matches `reroll_data.db2._NORMALIZED_NAME_CHECK` exactly (see that
# module's docstring) -- and matches `reroll_data.db2_backfill.normalize`,
# duplicated rather than imported since that module is a one-off migration
# script this one has no business depending on.
_NAME_RUNS = re.compile(r"[-_.]+")


def normalize(name: str) -> str:
    """PEP 503 normalize a project display name for `main.wheel.project`."""
    return _NAME_RUNS.sub("-", name).lower()


_SENTINEL = object()


@dataclass
class WheelRow:
    """One `.whl` file, as observed on its project's `/simple/<project>/` page.

    Carries everything `pypi_index` and `main.wheel` need between them --
    `project` is PyPI's raw, as-observed spelling (not normalized; the writer
    normalizes it itself only when building the `main.wheel` row, per
    `reroll_data.db2`'s module docstring).
    """

    filename: str
    project: str
    yanked: bool
    metadata_sha256: bytes | None
    pypi_metadata: str  # JSON text; the writer wraps it in jsonb(?)


@dataclass
class Result:
    project: str
    status: str  # done | deleted | error
    serial: int | None = None
    wheels: list[WheelRow] = field(default_factory=list)
    error: str | None = None


# --------------------------------------------------------------------------- #
# index refresh
# --------------------------------------------------------------------------- #


def _session(user_agent: str, pool: int) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = user_agent
    # 429 is deliberately excluded so we see it and can slow the whole crawler
    # down, rather than letting urllib3 quietly retry one worker.
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.6,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _delete_project(
    main_db: sqlite3.Connection, pypi_db: sqlite3.Connection, name: str
) -> int:
    """Delete `name` (PyPI's raw display spelling) and everything under it.

    `main.db.wheel` is keyed by `filename` alone, and `main.wheel.project` is
    PEP 503 normalized -- neither is safe to match against `name` directly
    (a differently-spelled, still-live project could normalize the same way).
    So the filenames to remove from `main.db` are read from `pypi.db.pypi_index`
    (`project` there is the exact raw spelling PyPI reported, unnormalized)
    *before* that row is deleted, not derived from `main.db` itself.

    Order matters for resumability: `main.db` is touched first, and
    `pypi.db.project`'s own row is deleted *last*. A run interrupted midway
    always leaves the `project` row standing until every dependent row -- in
    both databases -- is actually gone, so simply calling this again (as
    `refresh_index` does, every time this project still shows up as "no
    longer in the index") finishes the job without double-deleting anything
    or leaving `main.db` orphaned.

    Returns the number of `main.db.wheel` filenames removed.
    """
    filenames = [
        row[0]
        for row in pypi_db.execute(
            "SELECT filename FROM pypi_index WHERE project = ?", (name,)
        )
    ]

    if filenames:
        main_db.execute("BEGIN IMMEDIATE")
        try:
            main_db.executemany(
                "DELETE FROM wheel WHERE filename = ?", ((f,) for f in filenames)
            )
            main_db.execute("COMMIT")
        except BaseException:
            main_db.execute("ROLLBACK")
            raise

    pypi_db.execute("BEGIN IMMEDIATE")
    try:
        pypi_db.execute("DELETE FROM pypi_index WHERE project = ?", (name,))
        pypi_db.execute("DELETE FROM wheel_metadata WHERE project = ?", (name,))
        pypi_db.execute("DELETE FROM project WHERE name = ?", (name,))
        pypi_db.execute("COMMIT")
    except BaseException:
        pypi_db.execute("ROLLBACK")
        raise

    return len(filenames)


def refresh_index(
    pypi_db: sqlite3.Connection,
    main_db: sqlite3.Connection,
    *,
    endpoint: str = PYPI_SIMPLE_ENDPOINT,
    user_agent: str = USER_AGENT,
) -> dict[str, int]:
    """Fetch the root index, delete whatever it no longer reports, then
    reconcile `pypi.db.project`'s serials against what is left.

    Deletion runs first and unconditionally (see the module docstring) --
    there is no `mark_removed`/skip flag any more; leaving a stale project's
    rows in place was the bug being fixed, not a feature to keep optional.
    """
    sess = _session(user_agent, pool=4)
    resp = sess.get(endpoint, headers={"Accept": ACCEPT_JSON_ONLY}, timeout=180)
    resp.raise_for_status()
    payload = resp.json()

    global_serial = int(payload.get("meta", {}).get("_last-serial") or 0)
    entries = payload["projects"]

    rows: list[tuple[str, int]] = []
    for entry in entries:
        name = entry["name"]
        serial = entry.get("_last-serial")
        # Without a serial we cannot diff, so fall back to the global serial,
        # which always advances and thus forces a re-crawl. Rare in practice.
        rows.append((name, int(serial) if serial is not None else global_serial))

    existing = {r[0] for r in pypi_db.execute("SELECT name FROM project")}
    incoming = {r[0] for r in rows}
    removed_names = existing - incoming

    removed = 0
    removed_wheels = 0
    for name in removed_names:
        removed_wheels += _delete_project(main_db, pypi_db, name)
        removed += 1

    pypi_db.execute("BEGIN IMMEDIATE")
    try:
        pypi_db.executemany(
            "INSERT INTO project(name, index_serial) VALUES(?, ?) "
            "ON CONFLICT(name) DO UPDATE SET index_serial = excluded.index_serial",
            rows,
        )
        _db2.set_meta(pypi_db, "index_serial", str(global_serial))
        _db2.set_meta(pypi_db, "index_refreshed_at", str(int(time.time())))
        _db2.set_meta(pypi_db, "index_project_count", str(len(rows)))
        pypi_db.execute("COMMIT")
    except BaseException:
        pypi_db.execute("ROLLBACK")
        raise

    after = _db2.stats_pypi(pypi_db)
    return {
        "index_serial": global_serial,
        "in_index": len(rows),
        "new_projects": len(incoming - existing),
        "pending": after["pending_projects"],
        "removed": removed,
        "removed_wheels": removed_wheels,
    }


# --------------------------------------------------------------------------- #
# fetching
# --------------------------------------------------------------------------- #


def _build_pypi_metadata(pkg: DistributionPackage, extra: dict) -> str:
    """Fold every non-promoted field of one PEP 691 file record into JSON.

    Mirrors `reroll_data.db2_backfill._build_pypi_metadata`'s field set and
    dashed-key spelling (matching PEP 691's own spelling), just built
    straight from a `pypi_simple.DistributionPackage` plus its page's raw
    per-file dict, rather than from already-column-shaped `v.db` data.
    `extra` (any raw key `KNOWN_FILE_KEYS` does not model) is merged in
    first, so every explicitly-typed field below wins on any collision.
    """
    merged: dict = dict(extra)

    if pkg.url is not None:
        merged["url"] = pkg.url
    if pkg.size is not None:
        merged["size"] = pkg.size
    if pkg.upload_time is not None:
        merged["upload-time"] = pkg.upload_time.isoformat()
    if pkg.requires_python is not None:
        merged["requires-python"] = pkg.requires_python

    if pkg.digests:
        merged["hashes"] = dict(pkg.digests)

    if pkg.yanked_reason is not None:
        merged["yanked-reason"] = pkg.yanked_reason
    if pkg.has_metadata is not None:
        merged["has-metadata"] = bool(pkg.has_metadata)
    if pkg.provenance_url is not None:
        merged["provenance-url"] = pkg.provenance_url

    return json.dumps(merged, separators=(",", ":"), sort_keys=True)


class Fetcher:
    """Per-thread pypi-simple client that also exposes the raw response.

    pypi-simple returns parsed objects and drops the response body, but we want
    the raw JSON to detect unmapped fields. A response hook captures it, along
    with the status code -- which we must check, because a 304 makes pypi-simple
    return a ProjectPage with zero packages instead of raising.
    """

    def __init__(self, endpoint: str, user_agent: str, pool: int) -> None:
        self._last: dict = {}
        sess = _session(user_agent, pool)
        sess.hooks["response"].append(self._capture)
        self.client = PyPISimple(
            endpoint=endpoint, session=sess, accept=ACCEPT_JSON_ONLY
        )

    def _capture(self, resp: requests.Response, *a, **kw) -> None:
        self._last = {
            "status": resp.status_code,
            "serial": resp.headers.get("X-PyPI-Last-Serial"),
            "content_type": resp.headers.get("Content-Type", ""),
            "body": resp.content,
        }

    def wheels(self, project: str) -> tuple[list[WheelRow], int | None]:
        """Return (wheel rows, serial) for `project`.

        Raises NoSuchProjectError for a deleted project, or requests.HTTPError.
        """
        page = self.client.get_project_page(project, timeout=(10, 60))
        meta = self._last
        status = meta.get("status")
        if status != 200:
            # Guard the silent-empty-page failure mode described above.
            raise RuntimeError(f"unexpected HTTP {status} for {project!r}")

        raw_by_name: dict[str, dict] = {}
        if "json" in (meta.get("content_type") or ""):
            try:
                for rec in json.loads(meta["body"]).get("files", []):
                    raw_by_name[rec.get("filename", "")] = rec
            except (ValueError, AttributeError):
                pass

        rows: list[WheelRow] = []
        for pkg in page.packages:
            # Match on the extension rather than pypi-simple's package_type:
            # parse_filename() raises on oddities like "package-0.0.0.whl" and
            # from_file() turns that into package_type=None, which would drop a
            # file that is nonetheless downloadable.
            if not pkg.filename.endswith(".whl"):
                continue

            raw = raw_by_name.get(pkg.filename, {})
            extra = {k: v for k, v in raw.items() if k not in KNOWN_FILE_KEYS}

            md = pkg.metadata_digests or {}
            md_sha256_hex = md.get("sha256")

            rows.append(
                WheelRow(
                    filename=pkg.filename,
                    project=project,
                    yanked=bool(pkg.is_yanked),
                    metadata_sha256=(
                        bytes.fromhex(md_sha256_hex) if md_sha256_hex else None
                    ),
                    pypi_metadata=_build_pypi_metadata(pkg, extra),
                )
            )

        serial = page.last_serial or meta.get("serial")
        return rows, int(serial) if serial is not None else None


# --------------------------------------------------------------------------- #
# writer
# --------------------------------------------------------------------------- #

# `yanked` is the one field PyPI genuinely changes after initial publish (PEP
# 592) -- see `reroll_data.db2`'s module docstring. Everything else about a
# given filename is immutable once served, so both upserts below are
# conditioned on `yanked` actually differing: a re-crawl of an already-known,
# never-yanked wheel touches neither table, rather than rewriting an
# identical row on every pass.
_INSERT_PYPI_INDEX = """
INSERT INTO pypi_index(filename, project, yanked, metadata_sha256, pypi_metadata)
VALUES (?, ?, ?, ?, jsonb(?))
ON CONFLICT(filename) DO UPDATE SET
    yanked          = excluded.yanked,
    metadata_sha256 = excluded.metadata_sha256,
    pypi_metadata   = excluded.pypi_metadata
WHERE pypi_index.yanked <> excluded.yanked
"""

_INSERT_MAIN_WHEEL = """
INSERT INTO wheel(filename, project, yanked, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(filename) DO UPDATE SET
    yanked     = excluded.yanked,
    updated_at = excluded.updated_at
WHERE wheel.yanked <> excluded.yanked
"""


def _writer(
    data_dir: Path | str,
    results: queue.Queue,
    counters: dict,
    lock: threading.Lock,
    batch_size: int,
    flush_interval: float,
) -> None:
    """Sole writer. SQLite permits one writer *per file*, and this owns both
    `main.db` and `pypi.db` connections so a project's wheels can be mirrored
    into `main.db` before `pypi.db` ever considers that project done -- see
    `flush`'s comment for why that ordering, not batching efficiency, is the
    reason `main.db` is written first.
    """
    main_db = _db2.connect_main(data_dir)
    pypi_db = _db2.connect_pypi(data_dir)
    _db2.init_main(main_db)
    _db2.init_pypi(pypi_db)
    pending: list[Result] = []
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if not pending:
            last_flush = time.monotonic()
            return
        now = int(time.time())

        deleted_wheels = 0
        to_write: list[Result] = []
        for res in pending:
            if res.status == "deleted":
                # Discovered mid-crawl (the project's own page 404d) -- the
                # exact same deletion `refresh_index` runs in bulk, just for
                # one project. Done here, not in the worker that raised, since
                # this thread is the only one allowed to mutate either file.
                deleted_wheels += _delete_project(main_db, pypi_db, res.project)
            else:
                to_write.append(res)

        # main.db first, and only for results that will go on to update
        # pypi.db.project below in *this same* flush -- so a crash between
        # the two leaves that project's crawled_serial stale (still
        # "pending"), and the next run's worklist query simply re-fetches
        # and re-applies it, rather than silently leaving main.db behind.
        main_rows = [
            (w.filename, normalize(w.project), int(w.yanked), now)
            for res in to_write
            for w in res.wheels
        ]
        if main_rows:
            main_db.execute("BEGIN IMMEDIATE")
            try:
                main_db.executemany(_INSERT_MAIN_WHEEL, main_rows)
                main_db.execute("COMMIT")
            except BaseException:
                main_db.execute("ROLLBACK")
                raise

        if to_write:
            pypi_db.execute("BEGIN IMMEDIATE")
            try:
                for res in to_write:
                    if res.wheels:
                        pypi_db.executemany(
                            _INSERT_PYPI_INDEX,
                            [
                                (
                                    w.filename,
                                    w.project,
                                    int(w.yanked),
                                    w.metadata_sha256,
                                    w.pypi_metadata,
                                )
                                for w in res.wheels
                            ],
                        )
                    # Wheels and the project's new serial commit together, so
                    # a project is never left half-recorded.
                    #
                    # The serial comes from the project page itself, not from
                    # the index. If a cached page reports an older serial than
                    # the index did, the project simply stays pending and is
                    # re-fetched next run -- preferable to marking it
                    # reconciled with stale data. Falling back to
                    # index_serial keeps endpoints that omit
                    # X-PyPI-Last-Serial from looping forever.
                    pypi_db.execute(
                        "UPDATE project SET "
                        "crawled_serial = coalesce(?, index_serial, crawled_serial), "
                        "status = ?, n_wheels = ?, error = ?, fetched_at = ? "
                        "WHERE name = ?",
                        (
                            res.serial,
                            res.status,
                            len(res.wheels),
                            res.error,
                            now,
                            res.project,
                        ),
                    )
                pypi_db.execute("COMMIT")
            except BaseException:
                pypi_db.execute("ROLLBACK")
                raise

        with lock:
            counters["written"] += len(pending)
            counters["wheels"] += sum(len(r.wheels) for r in to_write)
            counters["deleted_wheels"] += deleted_wheels
        pending.clear()
        last_flush = time.monotonic()

    try:
        while True:
            timeout = max(0.05, flush_interval - (time.monotonic() - last_flush))
            try:
                item = results.get(timeout=timeout)
            except queue.Empty:
                flush()
                continue
            if item is _SENTINEL:
                flush()
                return
            pending.append(item)
            if len(pending) >= batch_size:
                flush()
    finally:
        try:
            flush()
        finally:
            main_db.close()
            pypi_db.close()


# --------------------------------------------------------------------------- #
# orchestration
# --------------------------------------------------------------------------- #


def crawl(
    data_dir: Path | str,
    *,
    workers: int = 8,
    rate_per_minute: float = 900.0,
    limit: int | None = None,
    retry_errors: bool = False,
    endpoint: str = PYPI_SIMPLE_ENDPOINT,
    user_agent: str = USER_AGENT,
    batch_size: int = 500,
    flush_interval: float = 5.0,
    max_attempts: int = 3,
    progress_every: float = 10.0,
) -> dict:
    pypi_db = _db2.connect_pypi(data_dir)
    _db2.init_pypi(pypi_db)

    sql = (
        "SELECT name FROM project "
        "WHERE (crawled_serial IS NULL OR index_serial > crawled_serial)"
    )
    if not retry_errors:
        sql += " AND coalesce(status, '') <> 'error'"
    sql += " ORDER BY name"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    todo = [r[0] for r in pypi_db.execute(sql)]
    pypi_db.close()

    total = len(todo)
    if total == 0:
        return {"total": 0, "done": 0, "deleted": 0, "error": 0, "wheels": 0}

    bucket = TokenBucket(rate_per_minute)
    work: queue.Queue = queue.Queue(maxsize=workers * 4)
    results: queue.Queue = queue.Queue(maxsize=workers * 8)
    counters = {
        "done": 0,
        "deleted": 0,
        "error": 0,
        "written": 0,
        "wheels": 0,
        "deleted_wheels": 0,
        "throttled": 0,
    }
    lock = threading.Lock()
    stop = threading.Event()

    writer = threading.Thread(
        target=_writer,
        args=(data_dir, results, counters, lock, batch_size, flush_interval),
        name="writer",
        daemon=True,
    )
    writer.start()

    def feeder() -> None:
        for name in todo:
            if stop.is_set():
                break
            work.put((name, 1))
        for _ in range(workers):
            work.put(_SENTINEL)

    def worker() -> None:
        fetcher = Fetcher(endpoint, user_agent, pool=2)
        while not stop.is_set():
            item = work.get()
            if item is _SENTINEL:
                return
            name, attempt = item
            bucket.acquire()
            try:
                rows, serial = fetcher.wheels(name)
            except NoSuchProjectError:
                # Deleted between index refresh and now -- delete it exactly
                # as refresh_index would, applied by the writer thread.
                results.put(Result(name, "deleted"))
                with lock:
                    counters["deleted"] += 1
            except requests.HTTPError as exc:
                code = getattr(exc.response, "status_code", None)
                if code in (429, 503) and attempt < max_attempts:
                    new_rate = bucket.penalise()
                    with lock:
                        counters["throttled"] += 1
                    print(
                        f"  ! HTTP {code} on {name!r}; global rate -> "
                        f"{new_rate:.0f}/min",
                        file=sys.stderr,
                    )
                    time.sleep(min(30.0, 2.0**attempt))
                    work.put((name, attempt + 1))
                    continue
                results.put(Result(name, "error", error=f"HTTP {code}: {exc}"[:500]))
                with lock:
                    counters["error"] += 1
            except Exception as exc:  # noqa: BLE001
                if attempt < max_attempts:
                    time.sleep(min(15.0, 1.5**attempt))
                    work.put((name, attempt + 1))
                    continue
                results.put(
                    Result(name, "error", error=f"{type(exc).__name__}: {exc}"[:500])
                )
                with lock:
                    counters["error"] += 1
            else:
                results.put(Result(name, "done", serial=serial, wheels=rows))
                with lock:
                    counters["done"] += 1

    feed = threading.Thread(target=feeder, name="feeder", daemon=True)
    feed.start()
    pool = [
        threading.Thread(target=worker, name=f"worker-{i}", daemon=True)
        for i in range(workers)
    ]
    for t in pool:
        t.start()

    check_main_wal = _db.wal_monitor(Path(data_dir) / _db2.MAIN_DB_FILENAME)
    check_pypi_wal = _db.wal_monitor(Path(data_dir) / _db2.PYPI_DB_FILENAME)

    started = time.monotonic()
    next_report = started + progress_every
    interrupted = False
    try:
        while any(t.is_alive() for t in pool):
            time.sleep(0.5)
            now = time.monotonic()
            if now < next_report:
                continue
            next_report = now + progress_every
            with lock:
                seen = counters["done"] + counters["deleted"] + counters["error"]
                wheels = counters["wheels"]
            elapsed = now - started
            rpm = seen / elapsed * 60 if elapsed else 0.0
            eta = (total - seen) * elapsed / seen if seen else 0.0
            check_main_wal()
            check_pypi_wal()
            print(
                f"  {seen:>7}/{total} projects ({seen / total * 100:5.1f}%)  "
                f"{wheels:>9} wheels  {rpm:6.0f} req/min  eta {eta / 3600:5.2f}h",
                file=sys.stderr,
            )
    except KeyboardInterrupt:
        interrupted = True
        print("\n  interrupted -- flushing pending writes...", file=sys.stderr)
        stop.set()
        # Unblock any worker parked on an empty work queue.
        for _ in range(workers):
            try:
                work.put_nowait(_SENTINEL)
            except queue.Full:
                pass

    for t in pool:
        t.join(timeout=90)
    results.put(_SENTINEL)
    writer.join(timeout=300)

    with lock:
        out = dict(counters)
    out["total"] = total
    out["interrupted"] = interrupted
    return out


# --------------------------------------------------------------------------- #
# consistency check: full reconciliation of main.db against pypi.db
# --------------------------------------------------------------------------- #


def sync_consistency(data_dir: Path | str, *, batch_size: int = 5000) -> dict:
    """Reconcile every `main.db.wheel` row against `pypi.db.pypi_index`.

    Deliberately the *only* place in this module that scans either table in
    full -- the incremental path (`refresh_index`/`crawl`) never needs to,
    because it always knows exactly which filenames it just touched. This is
    the "once in a blue moon" (or "after an error") safety net for whatever
    drift that incremental path cannot rule out by construction -- e.g. a
    crash between `main.db`'s write and `pypi.db`'s in the writer's `flush`.

    Two anti-joins, both driven off one ATTACHed connection so they can be
    expressed as ordinary SQL rather than pulling either table into Python:

    * `pypi_index` rows with no matching `wheel.filename` -> insert into
      `main.db` (a project's wheels that were fetched but never mirrored).
    * `wheel` rows with no matching `pypi_index.filename` -> delete from
      `main.db` (a leftover `main.db` only should never have on its own --
      `pypi.db` is the sole source of truth for which filenames exist).

    Yanked-flag drift between the two tables is not checked here -- the
    incremental path's own conditional upsert (`WHERE ... yanked <>
    excluded.yanked`) is what keeps that in sync during normal operation;
    this only restores rows one side is simply missing.
    """
    data_dir = Path(data_dir)
    pypi_db_path = data_dir / _db2.PYPI_DB_FILENAME

    main_db = _db2.connect_main(data_dir)
    _db2.init_main(main_db)
    main_db.execute("ATTACH DATABASE ? AS pypidb", (str(pypi_db_path),))
    try:
        missing = main_db.execute(
            "SELECT p.filename, p.project, p.yanked FROM pypidb.pypi_index p "
            "LEFT JOIN wheel w ON w.filename = p.filename "
            "WHERE w.filename IS NULL"
        ).fetchall()
        extra = main_db.execute(
            "SELECT w.filename FROM wheel w "
            "LEFT JOIN pypidb.pypi_index p ON p.filename = w.filename "
            "WHERE p.filename IS NULL"
        ).fetchall()

        now = int(time.time())
        for start in range(0, len(missing), batch_size):
            batch = missing[start : start + batch_size]
            main_db.execute("BEGIN IMMEDIATE")
            try:
                main_db.executemany(
                    "INSERT INTO wheel(filename, project, yanked, updated_at) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(filename) DO NOTHING",
                    [(f, normalize(p), int(y), now) for f, p, y in batch],
                )
                main_db.execute("COMMIT")
            except BaseException:
                main_db.execute("ROLLBACK")
                raise

        for start in range(0, len(extra), batch_size):
            batch = extra[start : start + batch_size]
            main_db.execute("BEGIN IMMEDIATE")
            try:
                main_db.executemany(
                    "DELETE FROM wheel WHERE filename = ?", batch
                )
                main_db.execute("COMMIT")
            except BaseException:
                main_db.execute("ROLLBACK")
                raise
    finally:
        main_db.execute("DETACH DATABASE pypidb")
        main_db.close()

    return {"added": len(missing), "removed": len(extra)}
