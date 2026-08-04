"""Incremental download of PEP 658 core-metadata sidecars.

Every wheel on PyPI is served alongside a ``<wheel-url>.metadata`` file holding
its ``METADATA``, and the root index publishes that file's sha256 *before* we
download it. This module stores those bodies, resumably.

Why content addressing
----------------------
The published digest is the key to the whole design. Measured over a full crawl,
11,135,055 wheels carry only 7,402,585 distinct ``metadata_sha256`` values --
the platform and interpreter variants of one release almost always ship byte
identical metadata. Because the digest is known up front, a body we already hold
requires **no request at all**, so that 1.50x is a saving in fetches (~3.7M of
them) as much as in disk. Bodies therefore live in :table:`metadata_blob`, keyed
by digest, and :table:`wheel_metadata` merely points at them.

The state machine
-----------------
``wheel_metadata.state`` is one of::

    todo    -> not looked at yet
    lease   -> in flight, until `lease_until`
    done    -> stored; blob_sha256 identifies the body
    missing -> the index reports no sidecar (PyPI serves 404 for these)
    error   -> gave up after `max_attempts`; `error` says why

Naively selecting outstanding work as ``blob_sha256 IS NULL`` would hand the
same rows to every pass while the first pass still had them open. Claiming
instead flips rows to ``lease`` with an expiry inside a single write
transaction, so a row is either clearly finished, clearly untouched, or held by
someone with a deadline. A worker killed mid-flight loses its lease rather than
stranding its rows, and since claiming also increments ``attempts``, a row that
reliably kills its worker is eventually retired to ``error`` instead of being
retried forever.

Idempotency
-----------
:func:`sync` reconciles ``wheel`` into ``wheel_metadata`` and is a no-op once
converged, so follow-up runs cost one pass rather than re-fetching anything.
Staleness is detected the same way :mod:`reroll_data.crawl` does it -- by
comparing what we stored against what the index now advertises::

    wheel_metadata.blob_sha256 <> wheel.metadata_sha256

This tolerates ``crawl`` running concurrently: new wheel rows simply appear as
``todo`` on the next sync, and a wheel whose metadata was replaced upstream
returns to ``todo`` automatically.

Rate limiting
-------------
A 429 or 503 halves the global rate via :meth:`TokenBucket.penalise`, and the
progress loop eases it back up once the server has been quiet for
``recover_after`` seconds -- over a multi-day run a single transient 429 must not
leave the crawler permanently crippled.

Note that throttling still consumes one of a row's ``max_attempts``, because
attempts are counted at claim time so that a row which kills its worker cannot
be retried forever. Sustained throttling can therefore retire rows to ``error``;
``--retry-errors`` re-arms them (see :func:`reset_errors`).
"""

from __future__ import annotations

import hashlib
import queue
import sqlite3
import sys
import threading
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

import requests

from . import crawl as _crawl
from . import db as _db
from .ratelimit import TokenBucket

#: zlib level. 6 measured 2.81x on real bodies; 9 gained 0.1% for ~4x the CPU.
ZLIB_LEVEL = 6

_SENTINEL = object()


@dataclass
class Result:
    project: str
    filename: str
    #: done | missing | error | todo  ('todo' releases the lease for a retry)
    status: str
    sha256: str | None = None
    z_body: bytes | None = None
    n_bytes: int = 0
    error: str | None = None
    #: True when the body was already held, so no request was made.
    deduped: bool = False


# --------------------------------------------------------------------------- #
# sync
# --------------------------------------------------------------------------- #

# Note the `INSERT OR IGNORE ... SELECT` rather than `ON CONFLICT DO NOTHING`:
# SQLite cannot parse a bare ON CONFLICT after INSERT..SELECT (it is ambiguous
# with the SELECT's own clauses). The only constraint on the table that OR
# IGNORE could mask is the primary key, which is exactly what we want skipped.
#
# Both tables are keyed (project, filename) and stored in that order, so this
# walks the two B-trees in lockstep instead of probing randomly.
_SYNC_INSERT = """
INSERT OR IGNORE INTO wheel_metadata(project, filename, state, updated_at)
SELECT project,
       filename,
       -- A null digest means the index advertises no sidecar; verified against
       -- PyPI, those URLs 404. Recording 'missing' up front spends no request.
       CASE WHEN metadata_sha256 IS NULL THEN 'missing' ELSE 'todo' END,
       ?
FROM wheel
"""

# Re-open rows the index now disagrees with. coalesce() makes one statement
# cover both directions: a 'done' row whose digest changed upstream, and a
# 'missing' row that has since gained a sidecar.
_SYNC_STALE = """
UPDATE wheel_metadata AS wm
   SET state = 'todo', blob_sha256 = NULL, attempts = 0, error = NULL,
       lease_until = NULL, updated_at = ?
  FROM wheel AS w
 WHERE w.project = wm.project
   AND w.filename = wm.filename
   AND wm.state IN ('done', 'missing')
   AND coalesce(wm.blob_sha256, '') <> coalesce(w.metadata_sha256, '')
"""

# The payoff: anything whose digest we already hold is finished without a
# request. On a converged database this matches nothing and costs one pass.
_SYNC_LINK = """
UPDATE wheel_metadata AS wm
   SET state = 'done', blob_sha256 = w.metadata_sha256, error = NULL,
       lease_until = NULL, updated_at = ?
  FROM wheel AS w
 WHERE w.project = wm.project
   AND w.filename = wm.filename
   AND wm.state = 'todo'
   AND EXISTS (SELECT 1 FROM metadata_blob b WHERE b.sha256 = w.metadata_sha256)
"""


def sync(db: sqlite3.Connection) -> dict[str, int]:
    """Reconcile `wheel` into `wheel_metadata`. Safe to re-run; converges."""
    now = int(time.time())
    out: dict[str, int] = {}
    # Each statement commits separately so the WAL can checkpoint in between --
    # these touch millions of rows and one giant transaction would balloon it.
    for key, sql in (
        ("added", _SYNC_INSERT),
        ("reopened", _SYNC_STALE),
        ("linked", _SYNC_LINK),
    ):
        db.execute("BEGIN IMMEDIATE")
        try:
            out[key] = db.execute(sql, (now,)).rowcount or 0
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    return out


def release_leases(db: sqlite3.Connection, *, all_leases: bool = False) -> int:
    """Return leased rows to 'todo'.

    Only one instance is expected to run at a time, so on a clean start any
    surviving lease belongs to a dead process and can be released immediately
    rather than waiting out its expiry.
    """
    now = int(time.time())
    sql = (
        "UPDATE wheel_metadata SET state = 'todo', updated_at = ? WHERE state = 'lease'"
    )
    params: tuple = (now,)
    if not all_leases:
        sql += " AND coalesce(lease_until, 0) <= ?"
        params = (now, now)
    db.execute("BEGIN IMMEDIATE")
    try:
        n = db.execute(sql, params).rowcount or 0
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return n


def reset_errors(db: sqlite3.Connection) -> int:
    """Re-arm 'error' rows for another go, clearing their attempt count.

    Retiring a row to 'error' means its attempts are spent, so the claim filter
    (`attempts < max_attempts`) would otherwise skip it forever. Re-attempting
    is therefore an explicit act that resets the counter, rather than a
    predicate the claim query could express.
    """
    now = int(time.time())
    db.execute("BEGIN IMMEDIATE")
    try:
        n = (
            db.execute(
                "UPDATE wheel_metadata SET state = 'todo', attempts = 0, "
                "error = NULL, lease_until = NULL, updated_at = ? "
                "WHERE state = 'error'",
                (now,),
            ).rowcount
            or 0
        )
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return n


# --------------------------------------------------------------------------- #
# claiming
# --------------------------------------------------------------------------- #

# Only 'todo' is claimed. That single equality is what lets SQLite use the
# `wheel_metadata_todo` partial index (its implication prover rejects the
# IN/<> forms -- see the schema comment), turning each claim into a covering
# index seek instead of a scan of 11M mostly-'done' rows. Expired leases are
# recovered by :func:`release_leases` when a claim comes back empty, rather
# than by widening this predicate.
#
# `wheel` is joined in for the URL and the expected digest.
_CLAIM_SELECT = """
SELECT wm.project, wm.filename, w.url, w.metadata_sha256, wm.attempts
  FROM wheel_metadata AS wm
  JOIN wheel AS w
    ON w.project = wm.project AND w.filename = wm.filename
 WHERE wm.state = 'todo'
   AND wm.attempts < ?
 LIMIT ?
"""


def _claim(
    db: sqlite3.Connection,
    *,
    n: int,
    lease_seconds: int,
    max_attempts: int,
) -> list[tuple]:
    """Atomically lease up to `n` rows and return them."""
    now = int(time.time())
    db.execute("BEGIN IMMEDIATE")
    try:
        rows = db.execute(_CLAIM_SELECT, (max_attempts, n)).fetchall()
        if rows:
            # attempts is incremented at claim time, not on failure: a worker
            # that dies without reporting still burns an attempt, so a row that
            # crashes its worker cannot be retried forever.
            db.executemany(
                "UPDATE wheel_metadata SET state = 'lease', lease_until = ?, "
                "attempts = attempts + 1, updated_at = ? "
                "WHERE project = ? AND filename = ?",
                [(now + lease_seconds, now, r[0], r[1]) for r in rows],
            )
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return rows


# --------------------------------------------------------------------------- #
# fetching
# --------------------------------------------------------------------------- #


def fetch_one(
    sess: requests.Session, url: str, want_sha: str | None
) -> tuple[str, bytes | None, str | None]:
    """Download one sidecar. Returns (status, body, error)."""
    resp = sess.get(url + ".metadata", timeout=(10, 60))
    if resp.status_code == 404:
        # The index said there should be one, but there is not. Terminal, and
        # not an error worth retrying.
        return "missing", None, None
    resp.raise_for_status()
    body = resp.content
    got = hashlib.sha256(body).hexdigest()
    if want_sha is not None and got != want_sha:
        # Refuse to store a body under a digest that does not describe it --
        # that would silently corrupt the content-addressed store.
        return "error", None, f"sha256 mismatch: want {want_sha} got {got}"
    return "done", body, None


# --------------------------------------------------------------------------- #
# writer
# --------------------------------------------------------------------------- #


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
                if res.status == "done" and res.z_body is not None:
                    # Body and pointer land in one transaction, so blob_sha256
                    # can never reference a row that was not written. Two
                    # results in this batch sharing a digest collapse here.
                    db.execute(
                        "INSERT INTO metadata_blob(sha256, n_bytes, z_body, stored_at) "
                        "VALUES(?,?,?,?) ON CONFLICT(sha256) DO NOTHING",
                        (res.sha256, res.n_bytes, res.z_body, now),
                    )
                db.execute(
                    "UPDATE wheel_metadata SET state = ?, blob_sha256 = ?, "
                    "error = ?, lease_until = NULL, updated_at = ? "
                    "WHERE project = ? AND filename = ?",
                    (
                        res.status,
                        res.sha256 if res.status == "done" else None,
                        res.error,
                        now,
                        res.project,
                        res.filename,
                    ),
                )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        with lock:
            for res in pending:
                counters[res.status] = counters.get(res.status, 0) + 1
                if res.deduped:
                    counters["deduped"] += 1
                if res.z_body:
                    counters["z_bytes"] += len(res.z_body)
                    counters["raw_bytes"] += res.n_bytes
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


def fetch(
    db_path: Path,
    *,
    workers: int = 8,
    rate_per_minute: float = 900.0,
    limit: int | None = None,
    lease_seconds: int = 900,
    claim_batch: int = 2000,
    max_attempts: int = 3,
    user_agent: str = _crawl.USER_AGENT,
    batch_size: int = 500,
    flush_interval: float = 5.0,
    progress_every: float = 10.0,
    recover_after: float = 120.0,
) -> dict:
    """Download outstanding metadata sidecars. Resumable and rate limited."""
    bucket = TokenBucket(rate_per_minute)
    work: queue.Queue = queue.Queue(maxsize=workers * 4)
    results: queue.Queue = queue.Queue(maxsize=workers * 8)
    counters = {
        "done": 0,
        "missing": 0,
        "error": 0,
        "todo": 0,
        "deduped": 0,
        "z_bytes": 0,
        "raw_bytes": 0,
        "throttled": 0,
        "claimed": 0,
    }
    lock = threading.Lock()
    stop = threading.Event()
    # Monotonic timestamp of the last 429/503, so the rate can be eased back up
    # once PyPI stops pushing back. Without this a single transient 429 in hour
    # two would leave the rate halved for the remaining days of the run.
    throttled_at = [0.0]

    writer = threading.Thread(
        target=_writer,
        args=(db_path, results, counters, lock, batch_size, flush_interval),
        name="writer",
        daemon=True,
    )
    writer.start()

    def feeder() -> None:
        """Claim batches and refill the work queue until nothing is left."""
        db = _db.connect(db_path)
        claimed = 0
        try:
            while not stop.is_set():
                n = claim_batch if limit is None else min(claim_batch, limit - claimed)
                if n <= 0:
                    break
                rows = _claim(
                    db,
                    n=n,
                    lease_seconds=lease_seconds,
                    max_attempts=max_attempts,
                )
                if not rows:
                    # Nothing claimable. Before concluding we are finished, look
                    # for leases that expired while this run was in progress --
                    # a batch that outlived `lease_seconds`, say. Only if that
                    # frees nothing is the work genuinely exhausted.
                    if release_leases(db, all_leases=False):
                        continue
                    break
                claimed += len(rows)
                with lock:
                    counters["claimed"] += len(rows)
                for row in rows:
                    if stop.is_set():
                        break
                    work.put(row)
        finally:
            db.close()
            for _ in range(workers):
                work.put(_SENTINEL)

    def worker() -> None:
        sess = _crawl._session(user_agent, pool=2)
        # A read-only connection purely to answer "do we already hold this
        # digest?". sync() links pre-existing bodies, but on a first pass the
        # store starts empty and the 1.50x duplication is only discovered as we
        # go -- without this probe the first run would make ~3.7M needless
        # requests. An indexed read is many orders of magnitude cheaper, and it
        # only ever sees committed rows, so a 'done' pointer is never dangling.
        probe = _db.connect(db_path, read_only=True)
        try:
            while not stop.is_set():
                item = work.get()
                if item is _SENTINEL:
                    return
                project, filename, url, want_sha, attempt = item

                if want_sha is not None:
                    hit = probe.execute(
                        "SELECT 1 FROM metadata_blob WHERE sha256 = ?", (want_sha,)
                    ).fetchone()
                    if hit is not None:
                        results.put(
                            Result(
                                project,
                                filename,
                                "done",
                                sha256=want_sha,
                                deduped=True,
                            )
                        )
                        continue

                # Only a real request consumes rate budget.
                bucket.acquire()
                try:
                    status, body, err = fetch_one(sess, url, want_sha)
                except requests.HTTPError as exc:
                    code = getattr(exc.response, "status_code", None)
                    if code in (429, 503):
                        new_rate = bucket.penalise()
                        with lock:
                            counters["throttled"] += 1
                            throttled_at[0] = time.monotonic()
                        print(
                            f"  ! HTTP {code} on {filename!r}; global rate -> "
                            f"{new_rate:.0f}/min",
                            file=sys.stderr,
                        )
                        time.sleep(min(30.0, 2.0**attempt))
                    results.put(
                        _retry_or_fail(
                            project, filename, attempt, max_attempts,
                            f"HTTP {code}: {exc}"[:500],
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    results.put(
                        _retry_or_fail(
                            project, filename, attempt, max_attempts,
                            f"{type(exc).__name__}: {exc}"[:500],
                        )
                    )
                else:
                    if status == "done":
                        results.put(
                            Result(
                                project,
                                filename,
                                "done",
                                sha256=hashlib.sha256(body).hexdigest(),
                                z_body=zlib.compress(body, ZLIB_LEVEL),
                                n_bytes=len(body),
                            )
                        )
                    elif status == "missing":
                        results.put(Result(project, filename, "missing"))
                    else:
                        # Digest mismatch: terminal, retrying will not help.
                        results.put(Result(project, filename, "error", error=err))
        finally:
            probe.close()

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
    writer_died = False
    try:
        while any(t.is_alive() for t in pool):
            time.sleep(0.5)
            # A run lasts days, so a writer that dies on an unexpected database
            # error must not leave every worker parked forever on results.put().
            if not writer.is_alive():
                writer_died = True
                print(
                    "  ! writer thread died -- aborting run", file=sys.stderr
                )
                stop.set()
                break
            now = time.monotonic()
            if now < next_report:
                continue
            next_report = now + progress_every
            with lock:
                seen = (
                    counters["done"]
                    + counters["missing"]
                    + counters["error"]
                    + counters["todo"]
                )
                dedup = counters["deduped"]
                mb = counters["z_bytes"] / 1e6
                last_throttle = throttled_at[0]
            # Ease back toward the configured rate once the server has been
            # quiet for a while. penalise() halves on pushback; without this
            # counterpart the rate only ever ratchets downward.
            if (
                last_throttle
                and now - last_throttle > recover_after
                and bucket.rate_per_minute < rate_per_minute
            ):
                bucket.recover()
            elapsed = now - started
            rpm = (seen - dedup) / elapsed * 60 if elapsed else 0.0
            print(
                f"  {seen:>8} wheels  {dedup:>8} deduped  {mb:9.1f} MB stored  "
                f"{rpm:6.0f} req/min",
                file=sys.stderr,
            )
    except KeyboardInterrupt:
        interrupted = True
        print("\n  interrupted -- flushing pending writes...", file=sys.stderr)
        stop.set()

    if interrupted or writer_died:
        # Unblock anyone parked on a full/empty queue so the joins below can
        # actually complete.
        for _ in range(workers):
            try:
                work.put_nowait(_SENTINEL)
            except queue.Full:
                pass
        while writer_died:
            # Nothing is draining `results` any more; empty it so the workers
            # can notice `stop` and exit.
            try:
                results.get_nowait()
            except queue.Empty:
                break

    for t in pool:
        t.join(timeout=90)
    results.put(_SENTINEL)
    writer.join(timeout=300)

    with lock:
        out = dict(counters)
    out["interrupted"] = interrupted
    out["writer_died"] = writer_died
    return out


def _retry_or_fail(
    project: str, filename: str, attempt: int, max_attempts: int, error: str
) -> Result:
    """Release the lease for another try, or retire the row to 'error'.

    `attempt` is the value of `attempts` *before* this claim incremented it, so
    the row has now been tried `attempt + 1` times.
    """
    if attempt + 1 < max_attempts:
        return Result(project, filename, "todo", error=error)
    return Result(project, filename, "error", error=error)
