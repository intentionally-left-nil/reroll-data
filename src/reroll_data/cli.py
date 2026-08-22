"""Command line interface."""

from __future__ import annotations

import argparse
import json
import sys
import zlib
from pathlib import Path

from . import backfill as _backfill
from . import crawl as _crawl
from . import db as _db
from . import db2 as _db2
from . import metadata as _metadata
from . import repodata_convert as _repodata_convert
from . import repodata_sync as _repodata_sync
from . import reroll_convert as _reroll_convert
from . import retry_metadata_conversion as _retry_metadata_conversion


def _fmt(stats: dict) -> str:
    return "  " + "\n  ".join(f"{k:<16} {v:>12,}" for k, v in stats.items())


def cmd_db_init(args: argparse.Namespace) -> int:
    """Create `main.db`/`pypi.db` (the new per-database schema) if missing.

    Deliberately separate from the legacy `--db`/`reroll_data.db` (`v.db`)
    machinery every other command uses -- see `reroll_data.db2`'s module
    docstring. Non-destructive: `db2.init_main`/`init_pypi` only ever run
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
    """Fetch the root index into `pypi.db`/`main.db` (see `reroll_data.db2`).

    Targets the new per-database schema exclusively -- `refresh`/`crawl`
    no longer touch the legacy `--db`/`v.db` at all.
    """
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
    db = _db.connect(args.db, read_only=True)
    _db.init(db)
    s = _db.stats(db)
    s["index_serial"] = int(_db.get_meta(db, "index_serial") or 0)
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


def cmd_metadata_backfill(args: argparse.Namespace) -> int:
    out = _backfill.backfill_parsed(
        Path(args.db),
        workers=args.workers,
        limit=args.limit,
        read_batch=args.batch_size,
    )
    print("backfill finished:", file=sys.stderr)
    print(_fmt(out), file=sys.stderr)
    return 1 if out.get("interrupted") else 0


def cmd_metadata_retry_conversion(args: argparse.Namespace) -> int:
    out = _retry_metadata_conversion.retry_metadata_conversion(
        Path(args.db), args.sha256
    )
    print(
        f"blob {out['sha256']} (id {out['id']}): "
        f"{'parsed' if out['parsed'] else 'failed to parse'}",
        file=sys.stderr,
    )
    return 0 if out["parsed"] else 1


def cmd_repodata_sync(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
    print("reconciling wheel -> repodata_conversion ...", file=sys.stderr)
    info = _repodata_sync.sync(db)
    print("sync finished:", file=sys.stderr)
    print(_fmt(info), file=sys.stderr)
    print(_fmt(_db.repodata_conversion_stats(db)), file=sys.stderr)
    db.close()
    return 0


def cmd_repodata_status(args: argparse.Namespace) -> int:
    db = _db.connect(args.db, read_only=True)
    _db.init(db)
    print(_fmt(_db.repodata_conversion_stats(db)))
    db.close()
    return 0


def cmd_reroll_status(args: argparse.Namespace) -> int:
    db = _db.connect(args.db, read_only=True)
    _db.init(db)
    s = _db.reroll_status_stats(db)
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


def cmd_repodata_convert(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
    tracked = db.execute(
        "SELECT count(*) FROM repodata_conversion WHERE conda_pypi_compatible = 1"
    ).fetchone()[0]
    if tracked == 0:
        db.close()
        print(
            "nothing conda-pypi-compatible tracked yet -- run "
            "`reroll-data repodata sync` first.",
            file=sys.stderr,
        )
        return 2
    if args.retry_errors:
        rearmed = _repodata_convert.reset_errors(db)
        print(f"re-armed {rearmed:,} previously-failed wheels", file=sys.stderr)
    db.close()

    out = _repodata_convert.convert(
        Path(args.db),
        workers=args.workers,
        limit=args.limit,
        read_batch=args.read_batch,
        chunksize=args.chunksize,
        write_batch=args.write_batch,
    )
    print("convert finished:", file=sys.stderr)
    print(_fmt(out), file=sys.stderr)
    return 1 if out.get("interrupted") else 0


def cmd_convert(args: argparse.Namespace) -> int:
    """Run reroll's own translator over every outstanding `main.db.wheel`
    row -- the `main.db`/`pypi.db` (db2) replacement for the old,
    `v.db`-based `repodata reroll-convert`. See `reroll_data.reroll_convert`.
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


def cmd_export(args: argparse.Namespace) -> int:
    db = _db.connect(args.db, read_only=True)
    sql = "SELECT project, filename, yanked FROM wheel"
    if not args.include_yanked:
        sql += " WHERE yanked = 0"
    sql += " ORDER BY project, filename"

    out = sys.stdout if args.output == "-" else open(args.output, "w", encoding="utf-8")
    n = 0
    try:
        if args.format == "jsonl":
            for project, filename, yanked in db.execute(sql):
                out.write(
                    json.dumps(
                        {
                            "project": project,
                            "filename": filename,
                            "yanked": bool(yanked),
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                n += 1
        else:  # tsv
            for project, filename, _ in db.execute(sql):
                out.write(f"{project}\t{filename}\n")
                n += 1
    finally:
        if out is not sys.stdout:
            out.close()
        db.close()
    print(f"exported {n:,} wheels to {args.output}", file=sys.stderr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="reroll-data",
        description="Incrementally scrape every .whl filename on PyPI into SQLite.",
    )
    p.add_argument(
        "--db",
        default=str(_db.DEFAULT_DB),
        help=f"SQLite database path (default: {_db.DEFAULT_DB})",
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
            "directory main.db/pypi.db live under -- used by `db init`, "
            "`refresh`, `crawl`, `sync-consistency`, `convert`, and "
            "`metadata sync`/`fetch`/`status`/`show` "
            f"(default: {_db2.DEFAULT_DATA_DIR})"
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser(
        "db",
        help="manage main.db/pypi.db -- the new per-database schema (reroll_data.db2)",
    )
    dsub = d.add_subparsers(dest="db_command", required=True)

    di = dsub.add_parser(
        "init",
        help="create main.db and pypi.db if missing (non-destructive; leaves v.db alone)",
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

    s = sub.add_parser("status", help="show counts")
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

    mb = msub.add_parser(
        "backfill",
        help="one-off: parse stored bodies into parsed_json (local, no network)",
    )
    mb.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes (default: all cores, via os.process_cpu_count())",
    )
    mb.add_argument(
        "--limit", type=int, default=None, help="only backfill N blobs"
    )
    mb.add_argument(
        "--batch-size",
        type=int,
        default=_backfill.READ_BATCH,
        help="blobs read/committed per round trip (default: %(default)s)",
    )
    mb.set_defaults(func=cmd_metadata_backfill)

    mr = msub.add_parser(
        "retry-conversion",
        help="one-off: force re-parse one blob's parsed_json (local, no network)",
    )
    mr.add_argument("sha256", help="metadata_blob.sha256 digest to re-parse")
    mr.set_defaults(func=cmd_metadata_retry_conversion)

    e = sub.add_parser("export", help="dump wheel filenames")
    e.add_argument("-o", "--output", default="-", help="output path, or - for stdout")
    e.add_argument("--format", choices=("tsv", "jsonl"), default="tsv")
    e.add_argument("--include-yanked", action="store_true")
    e.set_defaults(func=cmd_export)

    rp = sub.add_parser(
        "repodata", help="compare reroll's vs conda-pypi's repodata conversion"
    )
    rpsub = rp.add_subparsers(dest="repodata_command", required=True)

    rps = rpsub.add_parser(
        "sync",
        help="reconcile wheel -> repodata_conversion (idempotent, resumable)",
    )
    rps.set_defaults(func=cmd_repodata_sync)

    rpt = rpsub.add_parser("status", help="show repodata_conversion counts")
    rpt.set_defaults(func=cmd_repodata_status)

    rrs = rpsub.add_parser(
        "reroll-status",
        help=(
            "show reroll's own conversion counts by error category "
            "(scope/invalid/unconvertable/runtime/unavailable/unexpected/ok), "
            "plus coverage and unconvertable percentages"
        ),
    )
    rrs.set_defaults(func=cmd_reroll_status)

    rpc = rpsub.add_parser(
        "convert",
        help=(
            "run conda-pypi's translator over compatible wheels -- must run "
            "inside conda-pypi's own pixi env, see `make repodata-convert` "
            "(idempotent, resumable)"
        ),
    )
    rpc.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes (default: all cores, via os.process_cpu_count())",
    )
    rpc.add_argument("--limit", type=int, default=None, help="only convert N wheels")
    rpc.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt wheels previously marked conda_pypi_error",
    )
    rpc.add_argument(
        "--read-batch",
        type=int,
        default=_repodata_convert.READ_BATCH,
        help="rows read per round trip (default: %(default)s)",
    )
    rpc.add_argument(
        "--chunksize",
        type=int,
        default=_repodata_convert.CHUNKSIZE,
        help="wheels handed to a worker process per task (default: %(default)s)",
    )
    rpc.add_argument(
        "--write-batch",
        type=int,
        default=_repodata_convert.WRITE_BATCH,
        help="rows committed per write transaction (default: %(default)s)",
    )
    rpc.set_defaults(func=cmd_repodata_convert)

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
        help="also re-attempt wheels previously marked with a non-ok conversion_status",
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

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
