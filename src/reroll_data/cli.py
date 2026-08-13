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
from . import metadata as _metadata
from . import repodata_convert as _repodata_convert
from . import repodata_sync as _repodata_sync
from . import reroll_convert as _reroll_convert
from . import retry_metadata_conversion as _retry_metadata_conversion


def _fmt(stats: dict) -> str:
    return "  " + "\n  ".join(f"{k:<16} {v:>12,}" for k, v in stats.items())


def cmd_refresh(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
    print(f"fetching root index from {args.endpoint} ...", file=sys.stderr)
    info = _crawl.refresh_index(
        db,
        endpoint=args.endpoint,
        user_agent=args.user_agent,
        mark_removed=not args.no_mark_removed,
    )
    print("index refreshed:", file=sys.stderr)
    print(_fmt(info), file=sys.stderr)
    db.close()
    return 0


def cmd_crawl(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
    if _db.get_meta(db, "index_serial") is None:
        db.close()
        print(
            "no index snapshot yet -- run `reroll-data refresh` first.", file=sys.stderr
        )
        return 2
    db.close()

    print(
        f"crawling at {args.rate:.0f} req/min with {args.workers} workers ...",
        file=sys.stderr,
    )
    out = _crawl.crawl(
        Path(args.db),
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


def cmd_status(args: argparse.Namespace) -> int:
    db = _db.connect(args.db, read_only=True)
    _db.init(db)
    s = _db.stats(db)
    s["index_serial"] = int(_db.get_meta(db, "index_serial") or 0)
    print(_fmt(s))
    db.close()
    return 0


def cmd_metadata_sync(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
    print("reconciling wheel -> wheel_metadata ...", file=sys.stderr)
    info = _metadata.sync(db)
    if args.release_leases:
        info["released"] = _metadata.release_leases(db, all_leases=True)
    print("sync finished:", file=sys.stderr)
    print(_fmt(info), file=sys.stderr)
    print(_fmt(_db.metadata_stats(db)), file=sys.stderr)
    db.close()
    return 0


def cmd_metadata_fetch(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
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
        Path(args.db),
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
    db = _db.connect(args.db, read_only=True)
    _db.init(db)
    print(_fmt(_db.metadata_stats(db, include_bytes=args.bytes)))
    db.close()
    return 0


def cmd_metadata_show(args: argparse.Namespace) -> int:
    """Print one stored body, so the store can be spot-checked by hand."""
    db = _db.connect(args.db, read_only=True)
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


def cmd_repodata_reroll_convert(args: argparse.Namespace) -> int:
    db = _db.connect(args.db)
    _db.init(db)
    if args.retry_errors:
        rearmed = _reroll_convert.reset_errors(db)
        print(f"re-armed {rearmed:,} previously-failed wheels", file=sys.stderr)
    db.close()

    out = _reroll_convert.convert(
        Path(args.db),
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
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser(
        "refresh",
        help="fetch the root index and queue projects whose serial advanced",
    )
    r.add_argument(
        "--no-mark-removed",
        action="store_true",
        help="do not mark projects missing from the index as 'gone'",
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

    s = sub.add_parser("status", help="show counts")
    s.set_defaults(func=cmd_status)

    m = sub.add_parser("metadata", help="download PEP 658 core-metadata bodies")
    msub = m.add_subparsers(dest="metadata_command", required=True)

    ms = msub.add_parser(
        "sync", help="reconcile wheel -> wheel_metadata (idempotent, resumable)"
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

    rrc = rpsub.add_parser(
        "reroll-convert",
        help=(
            "run reroll's own translator over every corpus wheel -- no "
            "compatibility pre-filter, runs in the ordinary uv env (needs "
            "`uv sync --group probe`); idempotent, resumable"
        ),
    )
    rrc.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes (default: all cores, via os.process_cpu_count())",
    )
    rrc.add_argument("--limit", type=int, default=None, help="only convert N wheels")
    rrc.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt wheels previously marked reroll_error",
    )
    rrc.add_argument(
        "--read-batch",
        type=int,
        default=_reroll_convert.READ_BATCH,
        help="rows read per round trip (default: %(default)s)",
    )
    rrc.add_argument(
        "--chunksize",
        type=int,
        default=_reroll_convert.CHUNKSIZE,
        help="wheels handed to a worker process per task (default: %(default)s)",
    )
    rrc.add_argument(
        "--write-batch",
        type=int,
        default=_reroll_convert.WRITE_BATCH,
        help="rows committed per write transaction (default: %(default)s)",
    )
    rrc.set_defaults(func=cmd_repodata_reroll_convert)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
