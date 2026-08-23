"""Command line interface."""

from __future__ import annotations

import argparse
import sys
import zlib
from pathlib import Path

from . import crawl as _crawl
from . import db2 as _db2
from . import metadata as _metadata
from . import refresh_names as _refresh_names
from . import reroll_convert as _reroll_convert


def _fmt(stats: dict) -> str:
    return "  " + "\n  ".join(f"{k:<16} {v:>12,}" for k, v in stats.items())


def cmd_db_init(args: argparse.Namespace) -> int:
    """Create `main.db`/`pypi.db` (the per-database schema) if missing.

    Non-destructive: `db2.init_main`/`init_pypi` only ever run
    `CREATE TABLE/INDEX IF NOT EXISTS` against an existing file, and raise
    `SchemaMismatch` instead of altering a table whose shape has drifted.
    """
    try:
        _db2.init_all(args.data_dir)
    except _db2.SchemaMismatch as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    data_dir = Path(args.data_dir)
    print(
        f"initialized {data_dir / _db2.MAIN_DB_FILENAME} and "
        f"{data_dir / _db2.PYPI_DB_FILENAME}",
        file=sys.stderr,
    )
    return 0


def cmd_refresh(args: argparse.Namespace) -> int:
    """Fetch the root index into `pypi.db`/`main.db` (see `reroll_data.db2`)."""
    pypi_db = _db2.connect_pypi(args.data_dir)
    main_db = _db2.connect_main(args.data_dir)
    _db2.init_pypi(pypi_db)
    _db2.init_main(main_db)
    print(f"fetching root index from {args.endpoint} ...", file=sys.stderr)
    info = _crawl.refresh_index(
        pypi_db, main_db, endpoint=args.endpoint, user_agent=args.user_agent
    )
    print("index refreshed:", file=sys.stderr)
    print(_fmt(info), file=sys.stderr)
    pypi_db.close()
    main_db.close()
    return 0


def cmd_crawl(args: argparse.Namespace) -> int:
    pypi_db = _db2.connect_pypi(args.data_dir)
    _db2.init_pypi(pypi_db)
    if _db2.get_meta(pypi_db, "index_serial") is None:
        pypi_db.close()
        print(
            "no index snapshot yet -- run `reroll-data refresh` first.", file=sys.stderr
        )
        return 2
    pypi_db.close()

    print(
        f"crawling at {args.rate:.0f} req/min with {args.workers} workers ...",
        file=sys.stderr,
    )
    out = _crawl.crawl(
        args.data_dir,
        workers=args.workers,
        rate_per_minute=args.rate,
        limit=args.limit,
        retry_errors=args.retry_errors,
        endpoint=args.endpoint,
        user_agent=args.user_agent,
        batch_size=args.batch_size,
    )
    print("crawl finished:", file=sys.stderr)
    print(_fmt(out), file=sys.stderr)
    return 1 if out.get("interrupted") else 0


def cmd_sync_consistency(args: argparse.Namespace) -> int:
    """Full reconciliation of `main.db.wheel` against `pypi.db.pypi_index`.

    Rare by design (see `reroll_data.crawl.sync_consistency`'s docstring) --
    a full scan of both tables, meant for occasional hygiene or after an
    error, not for every regular `sync-filenames` run.
    """
    print(
        "reconciling main.db.wheel against pypi.db.pypi_index (full scan) ...",
        file=sys.stderr,
    )
    out = _crawl.sync_consistency(args.data_dir)
    print("sync-consistency finished:", file=sys.stderr)
    print(_fmt(out), file=sys.stderr)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show crawl + metadata counts, sourced from `pypi.db` (`reroll_data.db2`).

    See `_db2.stats_pypi` for the full breakdown (projects/pending files/
    yanked/metadata state/blobs).
    """
    db = _db2.connect_pypi(args.data_dir, read_only=True)
    _db2.init_pypi(db)
    s = _db2.stats_pypi(db)
    s["index_serial"] = int(_db2.get_meta(db, "index_serial") or 0)
    print(_fmt(s))
    db.close()
    return 0


def cmd_metadata_sync(args: argparse.Namespace) -> int:
    db = _db2.connect_pypi(args.data_dir)
    _db2.init_pypi(db)
    print("reconciling pypi_index -> wheel_metadata ...", file=sys.stderr)
    info = _metadata.sync(db)
    if args.release_leases:
        info["released"] = _metadata.release_leases(db, all_leases=True)
    print("sync finished:", file=sys.stderr)
    print(_fmt(info), file=sys.stderr)
    print(_fmt(_metadata.stats(db)), file=sys.stderr)
    db.close()
    return 0


def cmd_metadata_fetch(args: argparse.Namespace) -> int:
    db = _db2.connect_pypi(args.data_dir)
    _db2.init_pypi(db)
    tracked = db.execute("SELECT count(*) FROM wheel_metadata").fetchone()[0]
    if tracked == 0:
        db.close()
        print(
            "nothing tracked yet -- run `reroll-data metadata sync` first.",
            file=sys.stderr,
        )
        return 2
    # Only one instance runs at a time, so any lease still standing at startup
    # belongs to a process that died; reclaim it now instead of waiting it out.
    released = _metadata.release_leases(db, all_leases=True)
    if released:
        print(f"released {released:,} stale leases", file=sys.stderr)
    if args.retry_errors:
        rearmed = _metadata.reset_errors(db)
        print(f"re-armed {rearmed:,} previously-failed wheels", file=sys.stderr)
    db.close()

    print(
        f"fetching at {args.rate:.0f} req/min with {args.workers} workers ...",
        file=sys.stderr,
    )
    out = _metadata.fetch(
        args.data_dir,
        workers=args.workers,
        rate_per_minute=args.rate,
        limit=args.limit,
        lease_seconds=args.lease_seconds,
        claim_batch=args.claim_batch,
        user_agent=args.user_agent,
        batch_size=args.batch_size,
    )
    print("fetch finished:", file=sys.stderr)
    print(_fmt(out), file=sys.stderr)
    return 1 if out.get("interrupted") or out.get("writer_died") else 0


def cmd_metadata_status(args: argparse.Namespace) -> int:
    db = _db2.connect_pypi(args.data_dir, read_only=True)
    _db2.init_pypi(db)
    print(_fmt(_metadata.stats(db, include_bytes=args.bytes)))
    db.close()
    return 0


def cmd_metadata_show(args: argparse.Namespace) -> int:
    """Print one stored body, so the store can be spot-checked by hand."""
    db = _db2.connect_pypi(args.data_dir, read_only=True)
    row = db.execute(
        "SELECT b.z_body FROM wheel_metadata wm "
        "JOIN metadata_blob b ON b.sha256 = wm.blob_sha256 "
        "WHERE wm.filename = ?",
        (args.filename,),
    ).fetchone()
    db.close()
    if row is None:
        print(f"no stored metadata for {args.filename!r}", file=sys.stderr)
        return 2
    # Written straight to the byte stream: the body is not guaranteed to be
    # valid UTF-8, which is why it is stored as a BLOB in the first place.
    sys.stdout.buffer.write(zlib.decompress(row[0]))
    return 0


def cmd_reroll_status(args: argparse.Namespace) -> int:
    """Show reroll's own conversion counts, sourced from `main.db.wheel`
    plus `main.db.reroll_errors` (`reroll_data.db2`). See `_db2.stats_main`
    for the category breakdown
    (outstanding/ok/scope/invalid/unconvertable/unavailable/unexpected/runtime
    -- `runtime` is accounting-only, already folded into `outstanding` since
    it never settles a wheel's failure).
    """
    db = _db2.connect_main(args.data_dir, read_only=True)
    _db2.init_main(db)
    s = _db2.stats_main(db)
    db.close()
    print(_fmt(s))
    ok, unconvertable, wheels = s["ok"], s["unconvertable"], s["wheels"]
    coverage_pct = ok / wheels * 100 if wheels else 0.0
    unconvertable_pct = (
        unconvertable / (unconvertable + ok) * 100 if (unconvertable + ok) else 0.0
    )
    print(f"  {'coverage':<16} {coverage_pct:>11.1f}%  (ok / wheels)")
    print(f"  {'unconvertable':<16} {unconvertable_pct:>11.1f}%  (unconvertable / (unconvertable + ok))")
    return 0


def cmd_convert(args: argparse.Namespace) -> int:
    """Run reroll's own translator over every outstanding `main.db.wheel`
    row. See `reroll_data.reroll_convert`.
    """
    main_db = _db2.connect_main(args.data_dir)
    _db2.init_main(main_db)
    if args.retry_errors:
        rearmed = _reroll_convert.reset_errors(main_db)
        print(f"re-armed {rearmed:,} previously-failed wheels", file=sys.stderr)
    if args.retry_stale_version:
        rearmed = _reroll_convert.reset_stale_version(main_db)
        print(
            f"re-armed {rearmed:,} wheels converted by a different reroll_version",
            file=sys.stderr,
        )
    main_db.close()

    out = _reroll_convert.convert(
        args.data_dir,
        workers=args.workers,
        limit=args.limit,
        read_batch=args.read_batch,
        chunksize=args.chunksize,
        write_batch=args.write_batch,
        allow_pre=args.allow_pre,
    )
    print("convert finished:", file=sys.stderr)
    for key, value in out.items():
        print(f"  {key:<16} {value}", file=sys.stderr)
    return 1 if out.get("interrupted") else 0


def cmd_names_refresh(args: argparse.Namespace) -> int:
    """Refresh `main.db.pypi_conda_names` against reroll's default mapper
    chain, then re-arm any `main.db.wheel` row it affects. See
    `reroll_data.refresh_names`.
    """
    out = _refresh_names.refresh(args.data_dir, limit=args.limit)
    print("names refresh finished:", file=sys.stderr)
    for key, value in out.items():
        print(f"  {key:<16} {value}", file=sys.stderr)
    return 1 if out.get("interrupted") else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="reroll-data",
        description="Incrementally scrape every .whl filename on PyPI into SQLite.",
    )
    p.add_argument(
        "--endpoint",
        default=_crawl.PYPI_SIMPLE_ENDPOINT,
        help="simple-index endpoint (default: %(default)s)",
    )
    p.add_argument(
        "--user-agent",
        default=_crawl.USER_AGENT,
        help="User-Agent to identify this crawler to PyPI",
    )
    p.add_argument(
        "--data-dir",
        default=str(_db2.DEFAULT_DATA_DIR),
        help=(
            "directory main.db/pypi.db live under -- used by every "
            "subcommand here "
            f"(default: {_db2.DEFAULT_DATA_DIR})"
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser(
        "db",
        help="manage main.db/pypi.db (reroll_data.db2)",
    )
    dsub = d.add_subparsers(dest="db_command", required=True)

    di = dsub.add_parser(
        "init",
        help="create main.db and pypi.db if missing (non-destructive)",
    )
    di.set_defaults(func=cmd_db_init)

    r = sub.add_parser(
        "refresh",
        help=(
            "fetch the root index into pypi.db/main.db, deleting any project "
            "the index no longer reports (see reroll_data.crawl)"
        ),
    )
    r.set_defaults(func=cmd_refresh)

    c = sub.add_parser("crawl", help="fetch outstanding projects (resumable)")
    c.add_argument("--workers", type=int, default=8)
    c.add_argument(
        "--rate",
        type=float,
        default=900.0,
        help="global cap in requests/minute (default: %(default)s)",
    )
    c.add_argument("--limit", type=int, default=None, help="only crawl N projects")
    c.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt projects previously marked 'error'",
    )
    c.add_argument("--batch-size", type=int, default=500, help="projects per commit")
    c.set_defaults(func=cmd_crawl)

    sc = sub.add_parser(
        "sync-consistency",
        help=(
            "full reconciliation of main.db.wheel against pypi.db.pypi_index "
            "-- rare (full scan); see reroll_data.crawl.sync_consistency"
        ),
    )
    sc.set_defaults(func=cmd_sync_consistency)

    s = sub.add_parser("status", help="show crawl + metadata counts (pypi.db)")
    s.set_defaults(func=cmd_status)

    m = sub.add_parser("metadata", help="download PEP 658 core-metadata bodies")
    msub = m.add_subparsers(dest="metadata_command", required=True)

    ms = msub.add_parser(
        "sync", help="reconcile pypi_index -> wheel_metadata (idempotent, resumable)"
    )
    ms.add_argument(
        "--release-leases",
        action="store_true",
        help="also return every leased row to 'todo'",
    )
    ms.set_defaults(func=cmd_metadata_sync)

    mf = msub.add_parser("fetch", help="download outstanding metadata (resumable)")
    mf.add_argument("--workers", type=int, default=8)
    mf.add_argument(
        "--rate",
        type=float,
        default=900.0,
        help="global cap in requests/minute (default: %(default)s)",
    )
    mf.add_argument("--limit", type=int, default=None, help="only fetch N wheels")
    mf.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt wheels previously marked 'error'",
    )
    mf.add_argument(
        "--lease-seconds",
        type=int,
        default=900,
        help="how long a claimed wheel stays leased (default: %(default)s)",
    )
    mf.add_argument(
        "--claim-batch",
        type=int,
        default=2000,
        help="wheels leased per claim (default: %(default)s)",
    )
    mf.add_argument("--batch-size", type=int, default=500, help="wheels per commit")
    mf.set_defaults(func=cmd_metadata_fetch)

    mt = msub.add_parser("status", help="show metadata counts and stored size")
    mt.add_argument(
        "--bytes",
        action="store_true",
        help="also total the stored bytes (scans the whole blob table; slow)",
    )
    mt.set_defaults(func=cmd_metadata_status)

    mw = msub.add_parser("show", help="print one stored METADATA body to stdout")
    mw.add_argument("filename", help="wheel filename")
    mw.set_defaults(func=cmd_metadata_show)

    rs = sub.add_parser(
        "reroll-status",
        help=(
            "show reroll's own conversion counts by category "
            "(scope/invalid/unconvertable/unavailable/unexpected/runtime/ok/outstanding), "
            "plus coverage and unconvertable percentages, from main.db.wheel/reroll_errors"
        ),
    )
    rs.set_defaults(func=cmd_reroll_status)

    cv = sub.add_parser(
        "convert",
        help=(
            "run reroll's own translator over every outstanding main.db.wheel "
            "row (main.db/pypi.db, db2); idempotent, resumable"
        ),
    )
    cv.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes (default: all cores, via os.process_cpu_count())",
    )
    cv.add_argument("--limit", type=int, default=None, help="only convert N wheels")
    cv.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt wheels previously marked with a settled (non-runtime) error",
    )
    cv.add_argument(
        "--retry-stale-version",
        action="store_true",
        help="also re-attempt wheels last converted by a different reroll_version",
    )
    cv.add_argument(
        "--allow-pre",
        action="store_true",
        help="accept a pre-release wheel version or dependency version on the first attempt",
    )
    cv.add_argument(
        "--read-batch",
        type=int,
        default=_reroll_convert.READ_BATCH,
        help="rows read per round trip (default: %(default)s)",
    )
    cv.add_argument(
        "--chunksize",
        type=int,
        default=_reroll_convert.CHUNKSIZE,
        help="wheels handed to a worker process per task (default: %(default)s)",
    )
    cv.add_argument(
        "--write-batch",
        type=int,
        default=_reroll_convert.WRITE_BATCH,
        help="rows committed per write transaction (default: %(default)s)",
    )
    cv.set_defaults(func=cmd_convert)

    n = sub.add_parser(
        "names",
        help="curate main.db.pypi_conda_names against reroll's own mapper chain",
    )
    nsub = n.add_subparsers(dest="names_command", required=True)

    nr = nsub.add_parser(
        "refresh",
        help=(
            "re-run reroll's default mapper chain over every "
            "pypi_conda_names row, overwriting conda_name in place wherever "
            "it disagrees, then re-arm any main.db.wheel row it affects"
        ),
    )
    nr.add_argument(
        "--limit", type=int, default=None, help="only check N pypi_conda_names rows"
    )
    nr.set_defaults(func=cmd_names_refresh)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
