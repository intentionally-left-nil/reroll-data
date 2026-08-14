"""One-off demo: run a single corpus wheel through conda-pypi's own ``conda
pypi index`` conversion internals and return the ``repodata.json`` v3
``"whl"`` subsection entry it would produce -- without ever needing the
actual ``.whl`` file on disk.

Why this works without the wheel file
--------------------------------------
``conda pypi index`` (see ``conda_pypi/cli/index.py``) builds a small PyPI-API
-shaped dict per wheel::

    {
      "info": {"name", "version", "requires_dist", "requires_python"},
      "urls": [{"packagetype": "bdist_wheel", "filename", "url", "size",
                 "digests": {"sha256"}}],
    }

and feeds it to :func:`conda_pypi.pypi_metadata.pypi_to_repodata`, which is
pure data transformation -- it never touches the filesystem. The CLI happens
to source that dict's fields from an on-disk wheel (``wheel.stat().st_size``,
a fresh sha256, and the parsed ``METADATA`` body), but every one of those
fields is already sitting in this corpus: ``wheel.size``, ``wheel.sha256`` and
``wheel.url`` come straight from the PEP 691 file record, and the ``METADATA``
body is the PEP 658 sidecar already stored in ``metadata_blob`` (see
:mod:`reroll_data.metadata`). So the whole pipeline can be driven from the
database alone.

The one piece of real parsing needed is turning a raw ``METADATA`` body into
the ``name``/``version``/``requires_dist``/``requires_python`` fields
``pypi_to_repodata`` expects. conda-pypi already has that in
:func:`conda_pypi.license_files.package_metadata_from_metadata_body`, built
for exactly this ("a single ``.metadata`` fetched from PyPI instead of a
``*.dist-info`` folder") -- see ``conda_pypi/translate.py``'s
``FileDistribution``.

The output shape mirrors ``conda_index.index.ChannelIndex`` verbatim: in
``_extract_indexed_packages_v3``, a wheel record is keyed by
``f"{name}-{version}-{build}"`` under ``repodata["v3"]["whl"]``. That is
exactly what :func:`wheel_to_v3_whl_entry` returns -- a one-entry ``dict``
suitable for ``json.dumps``.

Cross-environment note
-----------------------
``conda_pypi`` is a published conda package (also a conda plugin): it
(transitively) needs the real ``conda``, which is not pip-installable and is
therefore *not* part of this repo's ordinary uv environment. Run this module
from *this repo's own* pixi environment instead -- see ``pyproject.toml``'s
``[tool.pixi.*]`` tables, which install ``conda-pypi`` straight from
conda-forge alongside this whole project (editable, so source edits need no
reinstall)::

    pixi run --manifest-path pyproject.toml \\
      python src/reroll_data/conda_pypi_index_demo.py \\
      six-1.16.0-py2.py3-none-any.whl

Run as a plain script rather than ``-m``: :mod:`reroll_data.repodata_convert`
(eagerly imported by ``reroll_data/__init__.py`` via ``cli.py``) already
imports this module by name, so ``python -m reroll_data.conda_pypi_index_demo``
finds it already sitting in ``sys.modules`` and re-executes it anyway, with a
``RuntimeWarning`` about doing so -- harmless, but avoided entirely by running
it as a script instead.

For running this over *every* compatible wheel rather than one at a time, see
:mod:`reroll_data.repodata_convert` instead, which runs inside that same pixi
environment via the ordinary ``reroll-data`` console script
(``reroll-data repodata convert``), with no cross-interpreter calls at all.
This module's demo CLI stays useful alongside it, for spot-checking one wheel
by hand; :mod:`reroll_data.repodata_convert` reuses its core conversion
function directly (see :func:`_entry_from_db`) rather than shelling out to it.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import zlib
from pathlib import Path
from typing import Any

try:
    from . import db as _db
except ImportError:  # pragma: no cover - running as a standalone script
    import db as _db  # type: ignore[no-redef]

# conda_pypi is only importable from its own (conda-based) environment -- see
# the module docstring. Optional, like `reroll` in metadata.py: importing this
# module (e.g. just to reuse the SQL helpers) must not require it.
try:
    from conda_pypi.license_files import package_metadata_from_metadata_body
    from conda_pypi.pypi_metadata import pypi_to_repodata
except ImportError:  # pragma: no cover - exercised outside conda-pypi's env
    package_metadata_from_metadata_body = None  # type: ignore[assignment]
    pypi_to_repodata = None  # type: ignore[assignment]


class WheelNotFound(LookupError):
    """No (unambiguous) ``wheel`` row matched the given filename."""


class MetadataUnavailable(LookupError):
    """The wheel has no stored PEP 658 ``METADATA`` body to convert."""


class NotPureWheel(ValueError):
    """conda-pypi only converts pure-Python (``*-none-any.whl``) wheels."""


_WHEEL_COLUMNS = "project, filename, url, size, upload_time, sha256, metadata_sha256"


def _find_wheel(db: sqlite3.Connection, filename: str, project: str | None) -> tuple:
    """Return one ``wheel`` row for ``filename``, disambiguating by ``project``.

    ``filename`` alone is not a database key (``wheel``'s primary key is
    ``(project, filename)``) -- measured on the full corpus, 2,020 filenames
    are shared by more than one project. Most callers will never hit that, so
    ``project`` is optional and this only raises once a real collision shows
    up, naming the candidates so the caller can pass ``project=`` to pick one.
    """
    if project is not None:
        row = db.execute(
            f"SELECT {_WHEEL_COLUMNS} FROM wheel WHERE project = ? AND filename = ?",
            (project, filename),
        ).fetchone()
        rows = [row] if row is not None else []
    else:
        rows = db.execute(
            f"SELECT {_WHEEL_COLUMNS} FROM wheel WHERE filename = ?", (filename,)
        ).fetchall()

    if not rows:
        where = f"filename={filename!r}" + (f" project={project!r}" if project else "")
        raise WheelNotFound(f"no wheel row for {where}")
    if len(rows) > 1:
        projects = ", ".join(sorted({row[0] for row in rows}))
        raise WheelNotFound(
            f"filename={filename!r} is ambiguous across projects: {projects} "
            "-- pass project= to disambiguate"
        )
    return rows[0]


def _metadata_body(db: sqlite3.Connection, metadata_sha256: str | None) -> str:
    """Decompress and decode the stored METADATA body for a wheel's digest.

    Decodes as UTF-8, falling back to cp1252 -- which accepts any byte
    sequence, so this never itself raises -- for the rare pre-2010s upload
    whose METADATA is not valid UTF-8, typically a non-ASCII
    Author/Maintainer header encoded in Latin-1/cp1252.
    """
    if metadata_sha256 is None:
        raise MetadataUnavailable(
            "wheel has no PEP 658 metadata sidecar recorded (metadata_sha256 IS NULL)"
        )
    row = db.execute(
        "SELECT z_body FROM metadata_blob WHERE sha256 = ?", (metadata_sha256,)
    ).fetchone()
    if row is None:
        raise MetadataUnavailable(
            f"metadata_blob has no body for sha256={metadata_sha256!r} "
            "-- run `make sync-metadata` first"
        )
    (z_body,) = row
    raw = zlib.decompress(z_body)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


def _entry_from_db(
    db: sqlite3.Connection, filename: str, *, project: str | None = None
) -> dict[str, Any]:
    """Core of :func:`wheel_to_v3_whl_entry`, operating on an open connection.

    Split out so a caller that already holds a long-lived read-only
    connection across many wheels -- :mod:`reroll_data.repodata_convert`'s
    batch job, one per worker process -- reuses it instead of paying a fresh
    ``sqlite3.connect()``/``close()`` per wheel. :func:`wheel_to_v3_whl_entry`
    itself is unchanged and remains the one-off, single-connection entry
    point.
    """
    if package_metadata_from_metadata_body is None or pypi_to_repodata is None:
        raise RuntimeError(
            "conda_pypi is not importable in this environment -- see the "
            f"{__name__} module docstring for how to run this against "
            "conda-pypi's own (conda-based) environment."
        )

    (
        _project,
        fn,
        url,
        size,
        upload_time,
        wheel_sha256,
        metadata_sha256,
    ) = _find_wheel(db, filename, project)
    body = _metadata_body(db, metadata_sha256)

    if not fn.endswith("-none-any.whl"):
        raise NotPureWheel(f"{fn!r} is not a pure-python (*-none-any.whl) wheel")
    if not wheel_sha256:
        raise ValueError(f"{fn!r} has no recorded sha256 in the wheel table")

    wheel_metadata = package_metadata_from_metadata_body(body)
    info = wheel_metadata.json

    pypi_data = {
        "info": {
            "name": info.get("name"),
            "version": info.get("version"),
            "requires_dist": info.get("requires_dist", []),
            "requires_python": info.get("requires_python"),
        },
        "urls": [
            {
                "packagetype": "bdist_wheel",
                "filename": fn,
                "url": url or "",
                "size": size,
                "upload_time": upload_time,
                "digests": {"sha256": wheel_sha256},
            }
        ],
    }

    entry = pypi_to_repodata(pypi_data)
    if entry is None:
        # pypi_to_repodata itself only returns None when it finds no
        # "*-none-any.whl" bdist_wheel url -- unreachable given the check
        # above, but kept so a future change to that filter fails loudly here
        # instead of raising a confusing KeyError below.
        raise NotPureWheel(f"conda-pypi declined to convert {fn!r}")

    key = f"{entry['name']}-{entry['version']}-{entry['build']}"
    return {key: entry}


def wheel_to_v3_whl_entry(
    db_path: Path | str, filename: str, *, project: str | None = None
) -> dict[str, Any]:
    """Build the ``repodata["v3"]["whl"]`` entry for one corpus wheel.

    Looks up ``filename`` (optionally scoped to ``project``) in the ``wheel``
    table, fetches its stored ``METADATA`` body from ``metadata_blob``, and
    runs both through conda-pypi's own ``pypi_to_repodata`` -- the same
    function ``conda pypi index`` uses. Returns a one-entry dict keyed by
    ``f"{name}-{version}-{build}"``, matching
    ``ChannelIndex._extract_indexed_packages_v3``'s output exactly, so this is
    a drop-in preview of what a real index run would produce for this wheel.

    Raises :class:`WheelNotFound`, :class:`MetadataUnavailable`, or
    :class:`NotPureWheel` for the obvious reasons; :class:`RuntimeError` if
    conda_pypi is not importable in this interpreter (see the module
    docstring for how to run this).
    """
    db = _db.connect(db_path, read_only=True)
    try:
        return _entry_from_db(db, filename, project=project)
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="reroll-data-conda-pypi-index-demo",
        description=(
            "Generate the repodata.json v3 'whl' subsection entry conda-pypi "
            "would produce for one wheel already in the corpus."
        ),
    )
    parser.add_argument(
        "filename", help="wheel filename, e.g. six-1.16.0-py2.py3-none-any.whl"
    )
    parser.add_argument(
        "--project", default=None, help="disambiguate if filename is not unique"
    )
    parser.add_argument(
        "--db",
        default=str(_db.DEFAULT_DB),
        type=Path,
        help=f"SQLite corpus (default: {_db.DEFAULT_DB})",
    )
    args = parser.parse_args(argv)

    try:
        section = wheel_to_v3_whl_entry(args.db, args.filename, project=args.project)
    except (WheelNotFound, MetadataUnavailable, NotPureWheel, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover - argparse.error() exits already
    print(json.dumps(section, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
