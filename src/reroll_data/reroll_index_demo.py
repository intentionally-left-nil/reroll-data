"""One-off demo: run a single corpus wheel through reroll's own wheel-to-
repodata pipeline and return the ``WheelRecord`` (s) it would produce --
without ever needing the actual ``.whl`` file on disk.

Why this works without the wheel file
--------------------------------------
``reroll()`` (``reroll.__init__``) is just three stages run in sequence --
``reroll.stages.extract_metadata_file`` -> ``reroll.stages.parse_metadata`` ->
``reroll.stages.get_wheel_records`` -- and the first of those is the *only*
one that ever touches the wheel archive itself (it unzips
``*.dist-info/METADATA`` out of the ``.whl``). This corpus already stores
that exact body -- the PEP 658 sidecar, see :mod:`reroll_data.metadata` --
so this module hooks into :mod:`reroll.stages` to skip straight past
``extract_metadata_file`` to ``parse_metadata`` on the stored text, then
``get_wheel_records`` on the result plus the filename: precisely the two
stages :mod:`reroll_data.reroll_convert`'s batch job also calls, and exactly
what ``reroll()`` itself would have done with the ``METADATA`` that
extraction stage would otherwise have produced.

``sha256``/``size``/``url`` are never derived from the wheel itself --
``reroll()`` leaves them ``None`` unless a caller supplies them (see
``reroll.wheel_record``'s docstring) -- so this passes the wheel's own
recorded ``sha256``, ``size``, and ``url`` straight from the PEP 691 file
record, the same way :mod:`reroll_data.conda_pypi_index_demo` does for
conda-pypi's PyPI-API dict. The wheel lookup and METADATA-body plumbing is
shared with that module (:func:`~reroll_data.conda_pypi_index_demo._find_wheel`,
:func:`~reroll_data.conda_pypi_index_demo._metadata_body`) rather than
duplicated here.

Environment
-----------
Unlike conda-pypi, ``reroll`` is an ordinary (if optional) dependency of this
project -- see ``pyproject.toml``'s ``probe`` dependency group -- so this runs
from this repo's regular uv environment, no cross-interpreter dance required::

    uv run --group probe python src/reroll_data/reroll_index_demo.py \\
      six-1.16.0-py2.py3-none-any.whl

(``--group probe`` is redundant if it is already part of your default sync --
see ``pyproject.toml``'s ``[tool.uv] default-groups``.)

For running this over *every* wheel rather than one at a time, see
:mod:`reroll_data.reroll_convert`, which reuses this module's
:func:`_entry_from_db` and :func:`categorize_error` directly.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

try:
    from . import db as _db
    from .conda_pypi_index_demo import (
        MetadataUnavailable,
        WheelNotFound,
        _find_wheel,
        _metadata_body,
    )
except ImportError:  # pragma: no cover - running as a standalone script
    import db as _db  # type: ignore[no-redef]
    from conda_pypi_index_demo import (  # type: ignore[no-redef]
        MetadataUnavailable,
        WheelNotFound,
        _find_wheel,
        _metadata_body,
    )

# `reroll` is an optional dependency (see pyproject.toml's `probe` group) --
# importing this module (e.g. just to reuse the SQL helpers, or to categorize
# an error caught elsewhere) must not require it, mirroring
# `reroll_data.metadata`'s own `parse_metadata` probe.
try:
    from reroll.default_mappers import default_mappers
    from reroll.errors import (
        RerollInvalidWheelError,
        RerollRuntimeError,
        RerollScopeError,
        RerollUnconvertableError,
    )
    from reroll.stages import get_wheel_records, parse_metadata
except ImportError:  # pragma: no cover - exercised without the `probe` group
    RerollScopeError = None  # type: ignore[assignment,misc]
    RerollInvalidWheelError = None  # type: ignore[assignment,misc]
    RerollUnconvertableError = None  # type: ignore[assignment,misc]
    RerollRuntimeError = None  # type: ignore[assignment,misc]
    get_wheel_records = None  # type: ignore[assignment]
    parse_metadata = None  # type: ignore[assignment]
    default_mappers = None  # type: ignore[assignment]

#: Every category :func:`categorize_error` can return. Kept in one place so
#: `reroll_convert.py`'s progress reporting and this module's own CLI print
#: the same vocabulary. Matches ``docs/errors_and_logging.md``'s four reroll
#: categories, plus two buckets of our own for failures reroll never gets a
#: chance to raise: "unavailable" (this corpus has no METADATA to feed it
#: yet) and "unexpected" (anything reroll did not wrap in a `RerollError` --
#: see `reroll.errors.UnexpectedError`'s own docstring for why that leaf
#: exists and should be rare).
CATEGORIES = (
    "scope",
    "invalid",
    "unconvertable",
    "runtime",
    "unavailable",
    "unexpected",
)

#: Categories carried over verbatim from `docs/errors_and_logging.md`:
#: "Runtime issues ... should generally stop batch processing of jobs until
#: the underlying host environment is stable" -- unlike the other three,
#: which say something about the *wheel*, this says something about *this
#: process's* environment (network, cache, local sqlite). Exposed so
#: `reroll_convert.py` can tell the two apart without re-encoding the
#: category name as a string in two places.
STOP_THE_WORLD_CATEGORIES = frozenset({"runtime"})


def categorize_error(exc: BaseException) -> str:
    """One of :data:`CATEGORIES` for `exc`.

    `"unavailable"` covers this module's own `WheelNotFound`/
    `MetadataUnavailable` -- a wheel this corpus cannot even hand to reroll
    yet (no wheel row, or no PEP 658 METADATA body stored), not something
    reroll itself rejected. Every real `RerollError` leaf reroll raises maps
    to exactly one of its four documented categories; anything else --
    including `RerollError` itself being unimportable, i.e. this environment
    lacks the `probe` dependency group -- falls to `"unexpected"`.
    """
    if isinstance(exc, (WheelNotFound, MetadataUnavailable)):
        return "unavailable"
    if RerollScopeError is not None and isinstance(exc, RerollScopeError):
        return "scope"
    if RerollInvalidWheelError is not None and isinstance(exc, RerollInvalidWheelError):
        return "invalid"
    if RerollUnconvertableError is not None and isinstance(exc, RerollUnconvertableError):
        return "unconvertable"
    if RerollRuntimeError is not None and isinstance(exc, RerollRuntimeError):
        return "runtime"
    return "unexpected"


def format_error(exc: BaseException, *, max_len: int = 1000) -> str:
    """`"<category>: <ExceptionType>: <message>"`, truncated to `max_len`.

    The category comes first (and is always one short lowercase word) so a
    `LIKE 'scope:%'`/`LIKE 'runtime:%'` query against `reroll_error` can slice
    the failure taxonomy without re-parsing exception names.
    """
    category = categorize_error(exc)
    return f"{category}: {type(exc).__name__}: {exc}"[:max_len]


def _entry_from_db(
    db: sqlite3.Connection,
    filename: str,
    *,
    project: str | None = None,
    mappers: Any = None,
) -> list[dict[str, Any]]:
    """Core of :func:`wheel_to_records`, operating on an open connection.

    Split out the same way :mod:`reroll_data.conda_pypi_index_demo`'s
    `_entry_from_db` is, so a caller already holding a long-lived read-only
    connection across many wheels -- :mod:`reroll_data.reroll_convert`'s batch
    job, one per worker process -- reuses it instead of paying a fresh
    `sqlite3.connect()`/`close()` per wheel.

    `mappers` is passed straight through to `get_wheel_records` as its own
    `mappers` argument -- a `reroll.name_mapping.NameMappers` chain, or
    `None` to let `get_wheel_records` build a fresh `default_mappers()`
    itself. Building that chain is not free (`parselmouth_mapper` opens,
    and may refresh over the network, a local sqlite evidence cache -- see
    `reroll.parselmouth_mapper.parselmouth_mapper`), so a caller converting
    many wheels should build it once and pass it in here every time rather
    than leaving `mappers=None` and paying that cost per wheel;
    :mod:`reroll_data.reroll_convert`'s `_init_worker` does exactly that,
    once per worker process.
    """
    if get_wheel_records is None or parse_metadata is None:
        raise RuntimeError(
            "reroll is not importable in this environment -- install the "
            "'probe' dependency group (`uv sync --group probe`) before "
            f"running this. See the {__name__} module docstring."
        )

    (
        _project,
        fn,
        url,
        size,
        _upload_time,
        wheel_sha256,
        metadata_sha256,
    ) = _find_wheel(db, filename, project)
    body = _metadata_body(db, metadata_sha256)

    # The one stage `reroll.stages` skips here: no `extract_metadata_file`,
    # since `body` already *is* the METADATA text it would have produced.
    metadata = parse_metadata(body)
    records = get_wheel_records(
        metadata, fn, mappers=mappers, sha256=wheel_sha256, size=size, url=url
    )
    return [record.model_dump(mode="json", exclude_none=True) for record in records]


def wheel_to_records(
    db_path: Path | str, filename: str, *, project: str | None = None
) -> list[dict[str, Any]]:
    """`WheelRecord`(s) reroll would produce for one corpus wheel, as plain
    dicts ready for `json.dumps`.

    Looks up `filename` (optionally scoped to `project`) in the `wheel`
    table, fetches its stored `METADATA` body from `metadata_blob`, and runs
    both through `reroll.stages.parse_metadata` + `get_wheel_records` --
    skipping `reroll.stages.extract_metadata_file`, the one stage that needs
    a real wheel archive on disk (see the module docstring).

    Raises whatever `get_wheel_records` itself raises -- a `RerollError`
    leaf, see `reroll.errors` -- for a wheel reroll cannot convert;
    `WheelNotFound`/`MetadataUnavailable` for the obvious reasons; or
    `RuntimeError` if `reroll` is not importable in this interpreter. Pass
    any of these through :func:`format_error` for the same
    `"category: Type: message"` string the batch job stores.
    """
    db = _db.connect(db_path, read_only=True)
    try:
        return _entry_from_db(db, filename, project=project)
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="reroll-data-reroll-index-demo",
        description=(
            "Show the WheelRecord(s) reroll would produce for one wheel "
            "already in the corpus."
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
        records = wheel_to_records(args.db, args.filename, project=args.project)
    except RuntimeError as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover - argparse.error() exits already
    except Exception as exc:  # noqa: BLE001 - the whole point is to show this
        print(format_error(exc))
        return 1
    print(json.dumps(records, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
