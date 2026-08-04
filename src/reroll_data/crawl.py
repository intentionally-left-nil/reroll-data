"""Incremental crawl of every .whl filename on PyPI.

How the incremental logic works
-------------------------------
The root ``/simple/`` index reports a ``_last-serial`` for every project, which
increments whenever anything about that project changes. We store it as
``project.index_serial`` (the state we want) alongside ``project.crawled_serial``
(the state we have). Outstanding work is therefore just::

    crawled_serial IS NULL OR index_serial > crawled_serial

So a refresh costs a single request, and only genuinely-changed projects get
re-fetched.

Note that ``pypi_simple.IndexPage`` discards the per-project serials -- its
``projects`` field is a ``list[str]`` -- so the root index is parsed from the
raw JSON body rather than through that class.
"""

from __future__ import annotations

import json
import queue
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
    NoSuchProjectError,
    PyPISimple,
)

from . import db as _db
from .ratelimit import TokenBucket

USER_AGENT = (
    "reroll-data/0.1 (+https://github.com/anaconda/reroll-data; akulkarni@anaconda.com)"
)

#: File-record keys we model as columns. Anything else lands in `extra_json` so
#: future PEP 691 additions are not silently dropped.
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

_SENTINEL = object()


@dataclass
class Result:
    project: str
    status: str  # done | gone | error
    serial: int | None = None
    wheels: list[tuple] = field(default_factory=list)
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


def refresh_index(
    db: sqlite3.Connection,
    *,
    endpoint: str = PYPI_SIMPLE_ENDPOINT,
    user_agent: str = USER_AGENT,
    mark_removed: bool = True,
) -> dict[str, int]:
    """Fetch the root index and reconcile the `project` table against it."""
    sess = _session(user_agent, pool=4)
    resp = sess.get(endpoint, headers={"Accept": ACCEPT_JSON_ONLY}, timeout=180)
    resp.raise_for_status()
    payload = resp.json()

    global_serial = int(payload.get("meta", {}).get("_last-serial") or 0)
    entries = payload["projects"]

    rows = []
    for entry in entries:
        name = entry["name"]
        serial = entry.get("_last-serial")
        # Without a serial we cannot diff, so fall back to the global serial,
        # which always advances and thus forces a re-crawl. Rare in practice.
        rows.append((name, int(serial) if serial is not None else global_serial))

    before = _db.stats(db)
    db.execute("BEGIN IMMEDIATE")
    try:
        db.executemany(
            "INSERT INTO project(name, index_serial) VALUES(?, ?) "
            "ON CONFLICT(name) DO UPDATE SET index_serial = excluded.index_serial",
            rows,
        )

        removed = 0
        if mark_removed:
            db.execute("CREATE TEMP TABLE IF NOT EXISTS _seen(name TEXT PRIMARY KEY)")
            db.execute("DELETE FROM _seen")
            db.executemany(
                "INSERT OR IGNORE INTO _seen(name) VALUES(?)", ((r[0],) for r in rows)
            )
            cur = db.execute(
                "UPDATE project SET status = 'gone' "
                "WHERE name NOT IN (SELECT name FROM _seen) "
                "AND coalesce(status, '') <> 'gone'"
            )
            removed = cur.rowcount or 0
            db.execute("DROP TABLE _seen")

        _db.set_meta(db, "index_serial", str(global_serial))
        _db.set_meta(db, "index_refreshed_at", str(int(time.time())))
        _db.set_meta(db, "index_project_count", str(len(rows)))
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise

    after = _db.stats(db)
    return {
        "index_serial": global_serial,
        "in_index": len(rows),
        "new_projects": after["projects"] - before["projects"],
        "pending": after["pending"],
        "removed": removed,
    }


# --------------------------------------------------------------------------- #
# fetching
# --------------------------------------------------------------------------- #


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

    def wheels(self, project: str) -> tuple[list[tuple], int | None]:
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

        now = int(time.time())
        rows: list[tuple] = []
        for pkg in page.packages:
            # Match on the extension rather than pypi-simple's package_type:
            # parse_filename() raises on oddities like "package-0.0.0.whl" and
            # from_file() turns that into package_type=None, which would drop a
            # file that is nonetheless downloadable.
            if not pkg.filename.endswith(".whl"):
                continue

            digests = dict(pkg.digests or {})
            sha256 = digests.get("sha256")
            others = {k: v for k, v in digests.items() if k != "sha256"}

            md = pkg.metadata_digests or {}
            raw = raw_by_name.get(pkg.filename, {})
            extra = {k: v for k, v in raw.items() if k not in KNOWN_FILE_KEYS}

            rows.append(
                (
                    project,
                    pkg.filename,
                    pkg.url,
                    pkg.size,
                    pkg.upload_time.isoformat() if pkg.upload_time else None,
                    pkg.requires_python,
                    sha256,
                    json.dumps(others, sort_keys=True) if others else None,
                    1 if pkg.is_yanked else 0,
                    pkg.yanked_reason,
                    None if pkg.has_metadata is None else int(pkg.has_metadata),
                    md.get("sha256"),
                    pkg.provenance_url,
                    json.dumps(extra, sort_keys=True) if extra else None,
                    now,
                    now,
                )
            )

        serial = page.last_serial or meta.get("serial")
        return rows, int(serial) if serial is not None else None


# --------------------------------------------------------------------------- #
# writer
# --------------------------------------------------------------------------- #

_INSERT_WHEEL = """
INSERT INTO wheel(
    project, filename, url, size, upload_time, requires_python, sha256,
    hashes_json, yanked, yanked_reason, has_metadata, metadata_sha256,
    provenance_url, extra_json, first_seen, last_seen
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(project, filename) DO UPDATE SET
    url             = excluded.url,
    size            = excluded.size,
    upload_time     = excluded.upload_time,
    requires_python = excluded.requires_python,
    sha256          = excluded.sha256,
    hashes_json     = excluded.hashes_json,
    -- yanked status genuinely changes over time, so refresh it rather than
    -- ignoring the conflict.
    yanked          = excluded.yanked,
    yanked_reason   = excluded.yanked_reason,
    has_metadata    = excluded.has_metadata,
    metadata_sha256 = excluded.metadata_sha256,
    provenance_url  = excluded.provenance_url,
    extra_json      = excluded.extra_json,
    last_seen       = excluded.last_seen
"""


def _writer(
    db_path: Path,
    results: queue.Queue,
    counters: dict,
    lock: threading.Lock,
    batch_size: int,
    flush_interval: float,
) -> None:
    """Sole writer. SQLite permits one writer, so all mutations funnel here."""
    db = _db.connect(db_path)
    pending: list[Result] = []
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if not pending:
            last_flush = time.monotonic()
            return
        now = int(time.time())
        db.execute("BEGIN IMMEDIATE")
        try:
            for res in pending:
                if res.wheels:
                    db.executemany(_INSERT_WHEEL, res.wheels)
                # Wheels and the project's new serial commit together, so a
                # project is never left half-recorded.
                #
                # The serial comes from the project page itself, not from the
                # index. If a cached page reports an older serial than the index
                # did, the project simply stays pending and is re-fetched next
                # run -- preferable to marking it reconciled with stale data.
                # Falling back to index_serial keeps endpoints that omit
                # X-PyPI-Last-Serial from looping forever.
                db.execute(
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
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        with lock:
            counters["written"] += len(pending)
            counters["wheels"] += sum(len(r.wheels) for r in pending)
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
            db.close()


# --------------------------------------------------------------------------- #
# orchestration
# --------------------------------------------------------------------------- #


def crawl(
    db_path: Path,
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
    db = _db.connect(db_path)
    _db.init(db)

    sql = (
        "SELECT name FROM project "
        "WHERE (crawled_serial IS NULL OR index_serial > crawled_serial)"
    )
    if not retry_errors:
        sql += " AND coalesce(status, '') NOT IN ('gone', 'error')"
    else:
        sql += " AND coalesce(status, '') <> 'gone'"
    sql += " ORDER BY name"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    todo = [r[0] for r in db.execute(sql)]
    db.close()

    total = len(todo)
    if total == 0:
        return {"total": 0, "done": 0, "gone": 0, "error": 0, "wheels": 0}

    bucket = TokenBucket(rate_per_minute)
    work: queue.Queue = queue.Queue(maxsize=workers * 4)
    results: queue.Queue = queue.Queue(maxsize=workers * 8)
    counters = {
        "done": 0,
        "gone": 0,
        "error": 0,
        "written": 0,
        "wheels": 0,
        "throttled": 0,
    }
    lock = threading.Lock()
    stop = threading.Event()

    writer = threading.Thread(
        target=_writer,
        args=(db_path, results, counters, lock, batch_size, flush_interval),
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
                # Deleted between index refresh and now.
                results.put(Result(name, "gone", serial=None))
                with lock:
                    counters["gone"] += 1
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
                seen = counters["done"] + counters["gone"] + counters["error"]
                wheels = counters["wheels"]
            elapsed = now - started
            rpm = seen / elapsed * 60 if elapsed else 0.0
            eta = (total - seen) * elapsed / seen if seen else 0.0
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
