"""One-off: force re-parse of a single `metadata_blob` row.

Unlike :func:`reroll_data.backfill.backfill_parsed`, which only ever selects
rows with `parsed_json IS NULL` (a row that already parsed successfully is
never touched again), this targets exactly one blob by its `sha256` digest and
overwrites `parsed_json` unconditionally -- even if it is already non-NULL.
That is the point: after a parser bug fix, this lets a specific already-"done"
row be re-converted without re-running the backfill over the whole table (or
having to first NULL the column out by hand).

Looked up by `sha256` rather than the integer `id` primary key because that is
the identifier a blob is actually known by everywhere else -- it is the
content-addressing key (`metadata_blob.sha256`, `wheel.metadata_sha256`,
`wheel_metadata.blob_sha256`), and it is what `backfill.py`'s own error
logging prints (`_parse_blob`'s `context=sha256`), so it is what shows up in
output identifying a blob worth retrying.

A single row is cheap enough that none of `backfill.py`'s machinery --
`ProcessPoolExecutor`, batching, progress reporting, resumability -- is
warranted here; this just does the one row inline.
"""

from __future__ import annotations

import zlib
from pathlib import Path

from . import db as _db
from . import metadata as _metadata


def retry_metadata_conversion(db_path: Path, sha256: str) -> dict:
    """Re-parse `metadata_blob.sha256 == sha256` and overwrite its `parsed_json`.

    Raises `ValueError` if no row has that digest. Returns a small dict
    describing what happened (`parsed_json` is None when the row's body
    failed to decompress, decode, or validate -- the same failure modes
    `_metadata._parse_metadata_json` already logs to stdout).
    """
    if _metadata.parse_metadata is None:
        raise RuntimeError(
            "reroll is not importable in this environment -- install the "
            "'probe' dependency group (`uv sync --group probe`) before "
            "running this."
        )

    db = _db.connect(db_path)
    try:
        row = db.execute(
            "SELECT id, z_body FROM metadata_blob WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if row is None:
            raise ValueError(f"no metadata_blob row with sha256={sha256!r}")
        blob_id, z_body = row

        raw = zlib.decompress(z_body)
        parsed_json = _metadata._parse_metadata_json(raw, context=sha256)

        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute(
                "UPDATE metadata_blob SET parsed_json = ? WHERE id = ?",
                (parsed_json, blob_id),
            )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    finally:
        db.close()

    return {"id": blob_id, "sha256": sha256, "parsed": parsed_json is not None}
