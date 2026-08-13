"""Idempotent sync of `wheel` -> `repodata_conversion`.

`repodata_conversion` is where reroll's own repodata translator and upstream
conda-pypi (see :mod:`reroll_data.conda_pypi_index_demo`) get compared, one row per
wheel. This module only does the bookkeeping *around* that comparison -- it
never itself runs either converter:

- ensures every wheel has a row here;
- computes `conda_pypi_compatible` for each newly-inserted row, purely from
  its filename (see :func:`is_conda_pypi_compatible`);
- leaves `reroll_compatible` and both `_data`/`_error` pairs NULL. Those are
  for whatever job actually runs each converter to fill in later, mirroring
  how `wheel_metadata.sync` only ever sets `state`/`blob_sha256` and leaves
  fetching the body itself to `metadata.fetch`.

One statement, not a batched loop
----------------------------------
The first draft of this batched inserts using ``LEFT JOIN repodata_conversion
... WHERE rc.project IS NULL`` to find untracked wheels, matching
:func:`reroll_data.metadata.sync`'s look. That degrades badly here: measured
on the real corpus, throughput fell from ~117k rows/s to ~5k rows/s as the
table filled, because SQLite has no way to *skip* the already-tracked prefix
of `wheel` -- ``EXPLAIN QUERY PLAN`` shows a full ``SCAN w`` on every call, so
each successive batch re-walks a longer and longer stretch of already-matched
rows before finding `batch_size` new ones (quadratic overall).

`metadata.py` never hits this because its `_SYNC_INSERT` is a single
``INSERT OR IGNORE ... SELECT ... FROM wheel`` with no JOIN at all: one
sequential scan of `wheel`, and letting `OR IGNORE` skip existing rows via
`repodata_conversion`'s own primary-key uniqueness check (a cheap per-row
btree probe) rather than via a correlated anti-join. This module now does the
same, via a scalar function (`sqlite3.Connection.create_function`) so
`conda_pypi_compatible` -- which needs real per-row logic, unlike
`metadata.py`'s CASE-expressible flag -- can still be computed inline. One
pass over the full 12M-row corpus measures at a few seconds to ~1 minute
depending on how much of it is new.

Purely additive: this only creates a new table and inserts into it. `wheel`
and every other existing table are never written to, and nothing is ever
deleted -- this corpus took many days to crawl and is not to be put at risk
for a comparison run.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

from . import db as _db


def is_conda_pypi_compatible(filename: str) -> bool:
    """True if conda-pypi would treat this wheel as a convertible pure wheel.

    conda-pypi's own filter (`pypi_to_repodata`) only checks the filename ends
    in ``-none-any.whl``; it does not care what the interpreter tag says. This
    project's own scope is narrower -- Python 3.4+ pure-Python wheels -- so
    this additionally requires the interpreter tag to read as Python 3
    (``py3``, ``py38``, ``py312``, ...; *not* ``py2``, ``cp311``, or a
    compressed multi-tag like ``py2.py3``).

    Reuses the same right-anchored split as
    ``reroll_data.investigate.shape_of``: a wheel filename is
    ``{name}-{version}(-{build})?-{interpreter}-{abi}-{platform}.whl``, and
    wheel-spec name/version fields have their own ``-`` characters replaced
    with ``_``, so the last 3 ``-``-separated fields are always the tag triple
    for any well-formed filename. Fewer than 5 fields means there is no tag
    triple to read at all (a malformed name), which is not compatible either.
    """
    parts = filename.removesuffix(".whl").split("-")
    if len(parts) < 5:
        return False
    interpreter, abi, platform = parts[-3:]
    return abi == "none" and platform == "any" and interpreter.startswith("py3")


# Name registered with `sqlite3.Connection.create_function` -- kept distinct
# from the Python-level `is_conda_pypi_compatible` so a stack trace inside the
# SQL function is unambiguous about which layer it is in.
_SQL_FUNC_NAME = "repodata_sync_is_conda_pypi_compatible"

# One sequential scan of `wheel`, no JOIN -- see the module docstring for why
# that matters. `OR IGNORE` makes an already-tracked row a no-op collision on
# the primary key rather than something this query has to notice up front.
_SYNC_INSERT = f"""
INSERT OR IGNORE INTO repodata_conversion(project, filename, conda_pypi_compatible, updated_at)
SELECT project, filename, {_SQL_FUNC_NAME}(filename), ?
FROM wheel
"""


def sync(db: sqlite3.Connection) -> dict[str, int]:
    """Insert one `repodata_conversion` row per wheel not yet tracked.

    Idempotent and resumable: safe to re-run after a fresh crawl (only the new
    wheels cause an insert; everything else collides harmlessly on the primary
    key) or after a Ctrl-C partway through -- the whole call is one
    transaction, so an interruption simply rolls back to before the run
    rather than leaving a half-tagged table.
    """
    db.create_function(
        _SQL_FUNC_NAME, 1, lambda fn: int(is_conda_pypi_compatible(fn))
    )
    before = db.execute("SELECT count(*) FROM repodata_conversion").fetchone()[0]
    now = int(time.time())
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(_SYNC_INSERT, (now,))
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    after, compatible = db.execute(
        "SELECT count(*), sum(conda_pypi_compatible) FROM repodata_conversion"
    ).fetchone()
    return {
        "inserted": after - before,
        "tracked": after,
        "conda_pypi_ok": compatible or 0,
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="reroll-data-repodata-sync",
        description="Reconcile wheel -> repodata_conversion (idempotent, resumable).",
    )
    parser.add_argument(
        "--db",
        default=str(_db.DEFAULT_DB),
        type=Path,
        help=f"SQLite database path (default: {_db.DEFAULT_DB})",
    )
    args = parser.parse_args(argv)

    db = _db.connect(args.db)
    _db.init(db)
    print("reconciling wheel -> repodata_conversion ...", file=sys.stderr)
    started = time.perf_counter()
    info = sync(db)
    elapsed = time.perf_counter() - started
    print(f"sync finished in {elapsed:.1f}s:", file=sys.stderr)
    for key, value in info.items():
        print(f"  {key:<24} {value:>12,}", file=sys.stderr)
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
