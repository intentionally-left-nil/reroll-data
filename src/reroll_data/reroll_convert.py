"""Run reroll's own wheel-to-repodata translator over every `main.db.wheel`
row, against the ``main.db``/``pypi.db`` schema (:mod:`reroll_data.db2`).

This is a from-scratch replacement of the earlier `v.db`/`repodata_conversion`
job of the same name: everything here reads its METADATA input from
``pypi.db`` and writes its result to ``main.db.wheel`` -- never the other way
around, and never `v.db` at all.

Where the METADATA comes from
------------------------------
:mod:`reroll_data.metadata` already downloads every wheel's PEP 658 sidecar
into ``pypi.db`` (``wheel_metadata`` points at a deduplicated body in
``metadata_blob``, keyed by content hash) and, on a normal fetch, parses it
once into ``metadata_blob.parsed_json`` (JSONB, itself just
``WheelMetadata.model_dump_json()``). This module reuses that cached parse
whenever it is present -- `parse_metadata` and `model_dump_json` round-trip
through the exact same pydantic model, so re-running the parser on a body
already parsed by it is pure duplicated work, not a correctness hedge.
`parsed_json` is only ever missing for a body that predates the parser
column or whose parse failed; only then does this fall back to decompressing
``z_body`` and calling `parse_metadata` fresh. Either way, nothing here ever
writes back into ``pypi.db`` -- this job is a `main.db`-only writer, per
`reroll_data.db2`'s module docstring on why the two files are split.

The new mapper: a curated static table, ahead of reroll's own chain
---------------------------------------------------------------------
`reroll.default_mappers.default_mappers()` (grayskull, conda-lock, the
hand-maintained overrides table, parselmouth, aggregated, with a passthrough
fallback) is still used -- but only as the *second* half of a chain built
fresh per worker process by :func:`_build_mappers`, and always with
`use_existing_cache=True`: every worker, in every process this run spawns,
reads `conda_lock_mapper`'s and `parselmouth_mapper`'s already-warmed
on-disk cache rather than making its own network call, so the whole run
resolves names against one consistent snapshot regardless of how many
processes or when each one happens to run. That cache is warmed by
`reroll_data.refresh_names.refresh` (`use_existing_cache=False`, run on its
own schedule, ahead of this job) -- see its module docstring's "Priming the
on-disk cache" section; `_build_mappers` propagates
`reroll.errors.MissingCacheError` uncaught if `refresh_names.refresh` has
never actually run. Ahead of the default chain sits a `reroll.static_mapper`
built from every `main.db.pypi_conda_names` row with a
non-NULL `conda_name` -- i.e. a name a human has actually curated, not a
mapper's live guess. A hit there ends the chain immediately, exactly like any
other `static_mapper`; a miss (no row, or a row with `conda_name IS NULL` --
either tri-state, see `reroll_data.db2`'s module docstring) defers to the
normal chain unchanged.

After a wheel converts, :func:`_convert_one` inspects every resulting
`WheelRecord.resolutions` (unioned across every record one wheel can expand
into -- noarch plus any per-arch splits, all sharing one `main.db.wheel` row)
and checks each one's `winner.mapper`. A resolution this job trusts enough to
persist is one that came from `passthrough_mapper` (nobody had an opinion; a
low-confidence guess we already accept everywhere else) or from this
module's own static mapper (a human already curated it). Anything else --
grayskull, conda-lock, `overrides_mapper`, parselmouth, or the aggregator's
own consensus vote -- means reroll's live chain found something we have not
curated ourselves yet. Rather than silently trust that live guess, this:

1. Seeds `main.db.pypi_conda_names` with `(pypi_name, NULL, NULL)` for every
   such name -- deliberately the *never-checked* tri-state (not
   *checked-and-unmappable*), since this is a placeholder for a human to
   later promote to a real `conda_name`, not a claim that no mapping exists.
   `ON CONFLICT DO NOTHING` so an existing curated or already-decided row is
   never clobbered.
2. Rejects the whole wheel with :class:`MissingPypiCondaStaticMapping`, a
   `RerollUnconvertableError` subclass -- so it falls into the ordinary
   `unconvertable` bucket of `main.db.reroll_errors.category` without a
   schema change (that column's CHECK constraint is a fixed enum; adding a
   value would need a full table rebuild, per `reroll_data.db2`'s "STRICT
   and CHECK from the start" design note).

A wheel that resolved *every* name through the static table or passthrough
gets its `resolutions` column written as `{pypi_name: conda_name}` for
exactly those names (see `reroll_data.db2`'s module docstring for what reads
that column later); a rejected wheel gets `resolutions` left NULL, since it
never had a resolution set this job is willing to stand behind.

Pre-release retry, but only for an actual pre-release error
-------------------------------------------------------------
Every wheel is first converted with `allow_pre=False`. Two, and only two,
reroll errors can mean "this needed a pre-release":

* `UnsupportedPrereleaseError` -- unambiguous: the wheel's own version is a
  pre-release.
* `UnconvertableRequirementError` -- ambiguous in general (it also covers a
  local version label, an over-long extra name, a marker referring to
  `extra`, or a matchspec that fails validation), but exactly one of its
  raise sites is a dependency's pre-release version, and its message is the
  only one containing the substring `"pre-release"` (checked against every
  `raise UnconvertableRequirementError` in reroll's own source).

Only those two are retried, and only once, with `allow_pre=True` -- any other
error (including every other `UnconvertableRequirementError` message) is
never retried, so a wheel that is going to fail regardless is not converted
twice. `main.db.wheel.requires_prerelease` records the outcome: `0` if the
first attempt already succeeded, `1` if only the retry did, and left `NULL`
(its "not yet determined" state) if neither attempt produced a record at
all -- the column describes a wheel that *converted*, not a failure.

`reroll_version`, for cheap re-runs after a reroll upgrade
--------------------------------------------------------------
Every write to `main.db.wheel` -- a success, any settled failure category,
*and* now a `runtime` one too (see `reroll_data.db2`'s module docstring for
why `runtime` is recorded at all despite never settling a wheel) -- stamps
`reroll_version = reroll.__version__` -- the installed `py-reroll`
distribution version, the same provenance value
`reroll_data.metadata.PARSER_VERSION` already uses, not this repo's own
`pyproject.toml` version. Because every attempt stamps it, `reroll_version`
is NULL *exactly* for a wheel that has never been attempted -- a future run
can cheaply find every row a newer reroll might resolve differently with
`WHERE reroll_version IS NOT NULL AND reroll_version <> '<current>'`
(`reset_stale_version` below does exactly that).

Runtime errors stop the batch, but are now recorded
-----------------------------------------------------
A `RerollRuntimeError` says nothing about the wheel it happened to be
converting, so it must never *settle* that wheel's failure -- unlike the
previous version of this module (which left the row completely untouched),
this one still writes a `main.db.reroll_errors` row with `category =
'runtime'`, purely for accounting/visibility (see `reroll_data.db2`'s module
docstring). Every reader that decides "is this wheel done" (the worklist
query below, `stats_main`) explicitly excludes `category = 'runtime'` when
checking for a settled failure, so the row does not stop the wheel being
retried. The batch itself still stops after flushing whatever it already
decided -- the error says nothing about *this* wheel, but every remaining
wheel in the batch is likely to hit the same unstable environment.

Idempotency and resumability
-----------------------------
Work is selected with `main.db.wheel`'s own `wheel_todo` partial index
(`WHERE reroll_data IS NULL AND yanked = 0`) narrowed by an anti-join
against `main.db.reroll_errors` (excluding `category = 'runtime'`, which
never settles a wheel) -- see :data:`reroll_data.db2.OUTSTANDING_WHEEL`. A
row that already succeeded or settled on a failure simply stops matching --
no lease/claim machinery needed, the same reasoning as the `v.db`-based
predecessor. `reset_errors` re-arms every wheel with a settled
(non-`runtime`) error; `reset_stale_version` re-arms rows an older reroll
produced.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import time
import zlib
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import reroll
from reroll.default_mappers import default_mappers
from reroll.errors import (
    RerollInvalidWheelError,
    RerollRuntimeError,
    RerollScopeError,
    RerollUnconvertableError,
    UnconvertableRequirementError,
    UnsupportedPrereleaseError,
)
from reroll.name_mapping import NameMappers, NameResolution, is_passthrough, static_mapper
from reroll.stages import get_wheel_records, parse_metadata
from reroll.wheel_metadata import WheelMetadata
from reroll.wheel_record import WheelRecord

from . import db2 as _db2

#: Attributed `Winner.mapper` for a hit against `main.db.pypi_conda_names`.
#: Distinct from `reroll`'s own mapper names (`"passthrough_mapper"`,
#: `"grayskull_mapper"`, ...) so `_is_trusted_resolution` can tell "we
#: curated this" apart from "reroll's live chain guessed this" by name alone.
STATIC_MAPPER_NAME = "pypi_conda_names_mapper"

#: Every value :func:`_categorize` can return that gets persisted to
#: `main.db.reroll_errors.category` -- unlike the legacy `conversion_status`
#: column this table replaced, `"runtime"` *is* included: a
#: `RerollRuntimeError` is now recorded for accounting instead of silently
#: leaving the row untouched (see module docstring's "Runtime errors stop
#: the batch, but are now recorded"). `"ok"` is deliberately not a member --
#: a successful wheel gets no `reroll_errors` row at all, see
#: `reroll_data.db2`'s module docstring. Matches
#: `reroll_data.db2.MAIN_SCHEMA`'s `reroll_errors.category` CHECK constraint
#: exactly.
CATEGORIES = ("scope", "invalid", "unconvertable", "unavailable", "unexpected", "runtime")

#: Rows pulled from `main.db.wheel` per round trip / progress report.
READ_BATCH = 2000

#: Wheels handed to a single worker-process call at a time.
CHUNKSIZE = 16

#: Rows committed per write transaction.
WRITE_BATCH = 500

# Set once per worker process by `_init_worker`.
_PYPI_DB: sqlite3.Connection | None = None
_MAPPERS: NameMappers | None = None
_ALLOW_PRE: bool = False


class MetadataUnavailable(Exception):
    """`pypi.db` has no usable METADATA body yet for this wheel -- not a
    reroll error (reroll is never even invoked), and not a `runtime` issue
    either: it just means `reroll_data.metadata.fetch` has not reached this
    wheel yet (or `pypi.db`/`main.db` have briefly drifted). Categorized as
    `"unavailable"`, the same bucket the legacy job used for the same
    condition against `v.db`.
    """


class MissingPypiCondaStaticMapping(RerollUnconvertableError):
    """A wheel (or one of its dependencies) resolved to a conda name only
    reroll's own live mapper chain agreed on -- not `main.db.pypi_conda_names`
    (a curated, human-reviewed mapping) and not `passthrough_mapper` (a
    deliberately low-confidence, opt-in fallback). Raised only after
    seeding `pypi_conda_names` with a `(pypi_name, NULL, NULL)` placeholder
    for every such name -- see the module docstring's "The new mapper"
    section. A `RerollUnconvertableError` subclass so it falls into the
    ordinary `unconvertable` category everywhere that already switches on
    that base class, with no separate case needed.
    """

    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names
        super().__init__(
            "no curated main.db.pypi_conda_names mapping (and no passthrough) for: "
            + ", ".join(names)
        )


def _is_prerelease_error(exc: Exception) -> bool:
    """Whether `exc` means "this wheel needed `allow_pre=True`".

    `UnsupportedPrereleaseError` (the wheel's own version) is unambiguous.
    `UnconvertableRequirementError` (a dependency's version) is not -- it
    also covers a local version label, an over-long extra, a marker
    referring to `extra`, and a matchspec that fails validation -- so it
    only counts here when its own message is reroll's one pre-release
    variant (`reroll.dependencies.matchspec_specifier._reject_unsupported_version`),
    checked by substring since that is the only raise site of this
    exception whose text contains "pre-release" anywhere in reroll's source.
    """
    if isinstance(exc, UnsupportedPrereleaseError):
        return True
    return isinstance(exc, UnconvertableRequirementError) and "pre-release" in str(exc)


def _categorize(exc: BaseException) -> str:
    """One of :data:`CATEGORIES` -- including `"runtime"`, now persisted
    (as a non-settling row) rather than refused, see module docstring.
    """
    if isinstance(exc, MetadataUnavailable):
        return "unavailable"
    if isinstance(exc, RerollScopeError):
        return "scope"
    if isinstance(exc, RerollInvalidWheelError):
        return "invalid"
    if isinstance(exc, RerollUnconvertableError):
        return "unconvertable"
    if isinstance(exc, RerollRuntimeError):
        return "runtime"
    return "unexpected"


def _build_mappers(main_db: sqlite3.Connection) -> NameMappers:
    """The curated `pypi_conda_names` static mapper, then reroll's own
    default chain -- see the module docstring's "The new mapper" section.

    Reads `pypi_conda_names` once (only rows with a non-NULL `conda_name` --
    a curated, confirmed mapping); a row with `conda_name IS NULL`, in
    either tri-state, is excluded from the table so a miss here always
    defers to `default_mappers()` rather than being mistaken for "checked,
    no mapping" by `reroll.static_mapper` (which cannot tell "no row" from
    "row present but NULL" apart -- both look like a dict miss).

    `default_mappers(use_existing_cache=True)`: this worker process must
    never make its own network call to warm `conda_lock_mapper`'s or
    `parselmouth_mapper`'s on-disk cache -- `reroll_data.refresh_names.refresh`
    is the one job that fetches live upstream data and warms that cache
    (see its module docstring's "Priming the on-disk cache" section); every
    `reroll_convert` worker, across every process this run spawns, must
    instead read that already-warmed cache so the whole run resolves names
    against one consistent snapshot, not whatever each worker happened to
    fetch at its own random moment. Raises `reroll.errors.MissingCacheError`
    (propagated, uncaught) if `refresh_names.refresh` has never run -- there
    is nothing to fall back to, since fetching live data here would defeat
    the whole point of pinning every worker to the same snapshot.

    The returned chain is not yet open -- `_init_worker` calls `.open()` on
    every mapper before `_convert_one` ever uses it.
    """
    table = dict(
        main_db.execute(
            "SELECT pypi_name, conda_name FROM pypi_conda_names WHERE conda_name IS NOT NULL"
        )
    )
    pypi_conda_names_mapper = static_mapper(table, mapper_name=STATIC_MAPPER_NAME)
    return (pypi_conda_names_mapper, *default_mappers(use_existing_cache=True))


def _init_worker(data_dir: str, allow_pre: bool) -> None:
    """`ProcessPoolExecutor` initializer: this worker's own `pypi.db`
    connection and its own mapper chain, built once and reused by every
    `_convert_one` call this process ever makes -- mirrors the previous
    version of this module's `_init_worker`, now against `pypi.db`/`main.db`
    instead of `v.db`. The `main.db` connection used to seed the mapper
    chain is opened, read, and closed immediately -- this worker never
    writes to `main.db` itself; see `convert`'s `flush` for why every write
    (including a rejected wheel's `pypi_conda_names` seed rows) funnels
    through the single writer connection in the main process instead.

    Calls `.open()` on every mapper in the chain `_build_mappers` returns --
    since py-reroll 0.5.0, `reroll.stages.get_wheel_records` only opens/closes
    a mapper chain it builds for itself, never one passed in explicitly the
    way `_convert_one` always does (see `_convert_with_prerelease_retry`), so
    this worker owns that lifecycle instead. No matching `.close()` runs at
    worker-process exit: a `ProcessPoolExecutor` worker has no teardown hook
    to run one from, so this mirrors how `_PYPI_DB`'s sqlite connection is
    already left for process death to reclaim rather than explicitly closed.
    """
    global _PYPI_DB, _MAPPERS, _ALLOW_PRE
    _PYPI_DB = _db2.connect_pypi(data_dir, read_only=True)
    main_db = _db2.connect_main(data_dir, read_only=True)
    try:
        _MAPPERS = _build_mappers(main_db)
    finally:
        main_db.close()
    for mapper in _MAPPERS:
        mapper.open()
    _ALLOW_PRE = allow_pre
    logging.getLogger("reroll").setLevel(logging.ERROR)


# Both digests are BLOBs in `pypi.db` (see `reroll_data.db2`); `filename`
# alone is the join key across `wheel_metadata`/`pypi_index`/`metadata_blob`,
# same reasoning as everywhere else in this schema (PyPI's filename
# namespace is already global). `json(m.parsed_json)` converts the JSONB
# blob back to JSON text SQLite-side, since `WheelMetadata.model_validate_json`
# wants text, not the raw JSONB encoding.
_METADATA_SELECT = """
SELECT wm.state, wm.blob_sha256, p.pypi_metadata ->> 'url',
       p.pypi_metadata ->> 'size', p.pypi_metadata ->> 'sha256',
       m.z_body, m.codec, json(m.parsed_json)
  FROM wheel_metadata AS wm
  JOIN pypi_index AS p ON p.filename = wm.filename
  LEFT JOIN metadata_blob AS m ON m.sha256 = wm.blob_sha256
 WHERE wm.filename = ?
"""


def _load_metadata(
    pypi_db: sqlite3.Connection, filename: str
) -> tuple[WheelMetadata, str | None, int | None, str | None]:
    """`(metadata, sha256_hex, size, url)` for `filename`, sourced from
    `pypi.db`.

    Prefers the already-parsed `metadata_blob.parsed_json` -- itself just a
    `WheelMetadata.model_dump_json()` from whichever `reroll_data.metadata.fetch`
    run first produced it -- over
    re-decompressing and re-parsing `z_body`: both paths run the identical
    `parse_metadata` code over the identical bytes, so preferring the cached
    result only skips duplicated work, never a different code path. Falls
    back to `z_body` only when `parsed_json` is NULL (not yet backfilled, or
    a body whose parse previously failed and is worth retrying against the
    currently-installed reroll).

    Raises :class:`MetadataUnavailable` if `pypi.db` has no `wheel_metadata`
    row, no linked `metadata_blob` row, or the state machine has not reached
    `'done'` yet for this filename.
    """
    row = pypi_db.execute(_METADATA_SELECT, (filename,)).fetchone()
    if row is None:
        raise MetadataUnavailable(f"no wheel_metadata/pypi_index row for {filename!r}")
    state, blob_sha256, url, size, sha256_hex, z_body, codec, parsed_json = row
    if state != "done" or blob_sha256 is None or z_body is None:
        raise MetadataUnavailable(
            f"{filename!r} metadata not fetched yet (wheel_metadata.state={state!r})"
        )
    if parsed_json is not None:
        metadata = WheelMetadata.model_validate_json(parsed_json)
    else:
        if codec != "zlib6":
            raise MetadataUnavailable(f"{filename!r} metadata_blob has unknown codec {codec!r}")
        raw = zlib.decompress(z_body)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("cp1252")
        metadata = parse_metadata(text)
    return metadata, sha256_hex, (int(size) if size is not None else None), url


def _merge_resolutions(records: tuple[WheelRecord, ...]) -> tuple[NameResolution, ...]:
    """One `NameResolution` per unique `pypi_name`, across every record one
    wheel expands into (a noarch record plus any per-arch splits) -- all of
    which share the single `main.db.wheel` row this is written back to.
    """
    seen: dict[str, NameResolution] = {}
    for record in records:
        for resolution in record.resolutions:
            seen.setdefault(resolution.pypi_name, resolution)
    return tuple(seen[name] for name in sorted(seen))


def _is_trusted_resolution(resolution: NameResolution) -> bool:
    """Whether this job accepts `resolution.winner` as-is: a curated
    `main.db.pypi_conda_names` hit, or `passthrough_mapper`'s deliberately
    low-confidence "nobody had an opinion" fallback -- see the module
    docstring's "The new mapper" section for why every other mapper is
    rejected instead of trusted silently.
    """
    winner = resolution.winner
    return is_passthrough(winner) or winner.mapper == STATIC_MAPPER_NAME


def _convert_with_prerelease_retry(
    metadata: WheelMetadata,
    filename: str,
    *,
    mappers: NameMappers,
    sha256: str | None,
    size: int | None,
    url: str | None,
) -> tuple[tuple[WheelRecord, ...], bool]:
    """`(records, requires_prerelease)`. Tries `allow_pre=False` first;
    retries once, with `allow_pre=True`, only if that attempt's error is a
    genuine pre-release rejection (:func:`_is_prerelease_error`) -- any
    other error propagates immediately, un-retried. See the module
    docstring's "Pre-release retry" section.
    """
    try:
        records = get_wheel_records(
            metadata, filename, mappers=mappers, allow_pre=False, sha256=sha256, size=size, url=url
        )
    except Exception as exc:  # noqa: BLE001 - only a pre-release error is special-cased
        if not _is_prerelease_error(exc):
            raise
        records = get_wheel_records(
            metadata, filename, mappers=mappers, allow_pre=True, sha256=sha256, size=size, url=url
        )
        return records, True
    return records, False


@dataclass
class _Result:
    wheel_id: int
    category: str
    requires_prerelease: bool | None = None
    reroll_data_json: str | None = None
    resolutions_json: str | None = None
    #: `main.db.reroll_errors.sub_category`/`.description` for a failure --
    #: the raising exception's class name and `str(exception)`, respectively.
    #: Always `None` for `category == "ok"`.
    sub_category: str | None = None
    description: str | None = None
    #: pypi_names to seed into `pypi_conda_names` as `(name, NULL, NULL)` --
    #: only set when `category == "unconvertable"` via
    #: `MissingPypiCondaStaticMapping`. Written by the single writer
    #: connection in `convert`, never by this worker itself.
    seed_names: tuple[str, ...] = ()


def _convert_one(item: tuple[int, str]) -> _Result:
    """Run in a worker process: convert one wheel. Every failure is caught
    here (never propagated) except one bad wheel must not take down the
    pool or the rest of the batch -- whether a `"runtime"` category should
    stop the whole run is decided by the caller (`convert`), which needs to
    see across the whole batch, not just one wheel.
    """
    wheel_id, filename = item
    assert _PYPI_DB is not None and _MAPPERS is not None, "worker process not initialized"
    try:
        metadata, sha256, size, url = _load_metadata(_PYPI_DB, filename)
        records, requires_prerelease = _convert_with_prerelease_retry(
            metadata, filename, mappers=_MAPPERS, sha256=sha256, size=size, url=url
        )
        resolutions = _merge_resolutions(records)
        offending = tuple(
            sorted({r.pypi_name for r in resolutions if not _is_trusted_resolution(r)})
        )
        if offending:
            raise MissingPypiCondaStaticMapping(offending)
    except Exception as exc:  # noqa: BLE001 - any conversion failure is data
        category = _categorize(exc)
        seed_names = exc.names if isinstance(exc, MissingPypiCondaStaticMapping) else ()
        return _Result(
            wheel_id,
            category,
            sub_category=type(exc).__name__,
            description=str(exc),
            seed_names=seed_names,
        )
    return _Result(
        wheel_id,
        "ok",
        requires_prerelease=requires_prerelease,
        reroll_data_json=json.dumps(
            [record.model_dump(mode="json", exclude_none=True) for record in records],
            separators=(",", ":"),
        ),
        resolutions_json=json.dumps(
            {r.pypi_name: r.winner.conda_name for r in resolutions}, separators=(",", ":")
        ),
    )


def reset_errors(main_db: sqlite3.Connection) -> int:
    """Re-arm every wheel with a settled (non-`runtime`) error for another
    attempt.

    Mirrors the previous version of this module's `reset_errors`: `convert`'s
    selection predicate excludes any wheel with a settled `reroll_errors`
    row, so a wheel that failed once is otherwise skipped forever. Only
    relevant after a reroll fix or a `pypi_conda_names` curation pass --
    re-running `convert()` unmodified would just reproduce the same
    rejections. Deliberately leaves `runtime`-only rows alone -- those never
    excluded the wheel from the worklist in the first place (see module
    docstring), so there is nothing to re-arm.

    The `wheel` `UPDATE` runs first, while its `reroll_errors` subquery still
    sees the about-to-be-cleared rows; the `DELETE` that actually clears them
    runs after, using the same `category <> 'runtime'` predicate (unaffected
    by the `UPDATE`, which never touches `reroll_errors`) -- so the two
    statements can't disagree about which rows they mean.
    """
    now = int(time.time())
    main_db.execute("BEGIN IMMEDIATE")
    try:
        n = (
            main_db.execute(
                "UPDATE wheel SET reroll_data = NULL, resolutions = NULL, "
                "requires_prerelease = NULL, reroll_version = NULL, updated_at = ? "
                "WHERE id IN ("
                "SELECT wheel_id FROM reroll_errors WHERE category <> 'runtime')",
                (now,),
            ).rowcount
            or 0
        )
        main_db.execute("DELETE FROM reroll_errors WHERE category <> 'runtime'")
        main_db.execute("COMMIT")
    except BaseException:
        main_db.execute("ROLLBACK")
        raise
    return n


def reset_unconvertable(main_db: sqlite3.Connection) -> int:
    """Re-arm every wheel whose most recent attempt was rejected specifically
    by :class:`MissingPypiCondaStaticMapping` -- the "no curated conda name"
    error -- for another attempt. The narrow slice of :func:`reset_errors`
    relevant right after a `reroll_data.refresh_names.refresh` run, rather
    than a full reroll upgrade.

    Narrowed via `reroll_errors.sub_category` (the raising exception's class
    name) to this one exception specifically, not the whole `unconvertable`
    category: `unconvertable` is reroll's own catch-all for
    `RerollUnconvertableError`, and :class:`MissingPypiCondaStaticMapping` is
    only one of its raise sites -- the one this repo raises itself,
    specifically when a dependency resolved to a name not yet curated in
    `main.db.pypi_conda_names` (module docstring's "The new mapper" section).
    It is also the only `unconvertable` cause a `pypi_conda_names` curation
    pass can plausibly fix; every other `unconvertable` rejection (a bad
    matchspec, an over-long extra, a marker referring to `extra`, ...) would
    just reproduce the identical rejection on the very next `convert` pass
    regardless of what this run curated, so leaving those rows settled
    (rather than re-arming them too) avoids wasted re-attempts at scale.

    A `refresh_names.refresh` run is exactly what fills in those curated
    rows; without re-arming here, a wheel rejected for that reason stays
    rejected forever, since `convert`'s selection predicate never looks at a
    settled `reroll_errors` row again.
    """
    sub_category = MissingPypiCondaStaticMapping.__name__
    now = int(time.time())
    main_db.execute("BEGIN IMMEDIATE")
    try:
        n = (
            main_db.execute(
                "UPDATE wheel SET reroll_data = NULL, resolutions = NULL, "
                "requires_prerelease = NULL, reroll_version = NULL, updated_at = ? "
                "WHERE id IN ("
                "SELECT wheel_id FROM reroll_errors "
                "WHERE category = 'unconvertable' AND sub_category = ?)",
                (now, sub_category),
            ).rowcount
            or 0
        )
        main_db.execute(
            "DELETE FROM reroll_errors WHERE category = 'unconvertable' AND sub_category = ?",
            (sub_category,),
        )
        main_db.execute("COMMIT")
    except BaseException:
        main_db.execute("ROLLBACK")
        raise
    return n


def reset_stale_version(main_db: sqlite3.Connection, *, current_version: str | None = None) -> int:
    """Re-arm every row (`ok` and any settled or `runtime` error included)
    whose `reroll_version` disagrees with `current_version` (defaults to the
    installed `reroll.__version__`) -- see the module docstring's
    "`reroll_version`, for cheap re-runs" section. A row that has never been
    attempted (`reroll_version IS NULL`) is left alone; it is already
    outstanding work, not something to re-arm. Since every attempt --
    success, settled failure, or `runtime` -- stamps `reroll_version`,
    `reroll_version IS NOT NULL` alone is exactly "has been attempted",
    replacing the old `conversion_status IS NOT NULL` guard with no change
    in meaning.

    The `DELETE` runs first here, deliberately the opposite order from
    :func:`reset_errors`/:func:`reset_unconvertable`: its subquery reads
    `wheel.reroll_version`, which the `UPDATE` below is about to null out,
    so it must see the pre-`UPDATE` value. The `UPDATE`'s own predicate reads
    the same, still-untouched `wheel.reroll_version` column, so the two
    statements agree on the same row set regardless of ordering between
    themselves -- only the dependency on `wheel.reroll_version` staying
    intact until both have read it is real, and `DELETE`-then-`UPDATE`
    satisfies that.
    """
    version = current_version or reroll.__version__
    main_db.execute("BEGIN IMMEDIATE")
    try:
        main_db.execute(
            "DELETE FROM reroll_errors WHERE wheel_id IN ("
            "SELECT id FROM wheel WHERE reroll_version IS NOT NULL AND reroll_version <> ?)",
            (version,),
        )
        now = int(time.time())
        n = (
            main_db.execute(
                "UPDATE wheel SET reroll_data = NULL, resolutions = NULL, "
                "requires_prerelease = NULL, reroll_version = NULL, updated_at = ? "
                "WHERE reroll_version IS NOT NULL AND reroll_version <> ?",
                (now, version),
            ).rowcount
            or 0
        )
        main_db.execute("COMMIT")
    except BaseException:
        main_db.execute("ROLLBACK")
        raise
    return n


def convert(
    data_dir: Path | str,
    *,
    workers: int | None = None,
    read_batch: int = READ_BATCH,
    chunksize: int = CHUNKSIZE,
    write_batch: int = WRITE_BATCH,
    limit: int | None = None,
    progress_every: float = 5.0,
    allow_pre: bool = False,
) -> dict:
    """Populate `main.db.wheel`'s conversion-facing columns, plus
    `main.db.reroll_errors`, for every outstanding row (see
    :data:`reroll_data.db2.OUTSTANDING_WHEEL`: never converted ok, not
    yanked, and not already settled on a non-`runtime` failure).

    `allow_pre` here gates only the *first* attempt: `True` skips the
    pre-release retry entirely (every wheel is tried once, with pre-releases
    already allowed) -- `False` (the default) is the normal two-attempt
    policy described in the module docstring.

    Stops early (`interrupted=True`) the first time any wheel fails with
    reroll's `"runtime"` category -- that row's `reroll_errors` entry is
    still written (for accounting), but a `runtime`-only row never settles a
    wheel (see module docstring), so it is retried once the run is retried.
    """
    workers = workers or os.process_cpu_count() or os.cpu_count() or 1
    data_dir = Path(data_dir)

    # `main.db` is the only file this job ever writes to; `pypi.db` is
    # read-only from here, always through each worker's own connection
    # (`_init_worker`), never through this main-process connection.
    read_db = _db2.connect_main(data_dir, read_only=True)
    write_db = _db2.connect_main(data_dir)

    table_total = read_db.execute("SELECT count(*) FROM wheel").fetchone()[0]
    outstanding_before = read_db.execute(
        f"SELECT count(*) FROM wheel w WHERE {_db2.OUTSTANDING_WHEEL}"
    ).fetchone()[0]
    total = outstanding_before if limit is None else min(outstanding_before, limit)
    coverage_before = (
        (table_total - outstanding_before) / table_total * 100 if table_total else 0.0
    )

    print(
        f"converting {total:,} wheel(s) with {workers} worker process(es) "
        f"({coverage_before:.1f}% of {table_total:,} corpus wheels already "
        "attempted) ...",
        file=sys.stderr,
    )

    counters: Counter[str] = Counter()
    started = time.monotonic()
    next_report = started + progress_every
    interrupted = False
    runtime_error: tuple[int, str] | None = None
    remaining_limit = limit
    check_wal = _db2.wal_monitor(data_dir / _db2.MAIN_DB_FILENAME)

    #: (wheel_id, category, reroll_data_json, resolutions_json,
    #: requires_prerelease, sub_category, description) -- one entry per
    #: wheel this batch decided, `category == "ok"` included, so `flush`
    #: can both update `wheel` and upsert/clear `reroll_errors` from the
    #: same list.
    pending_writes: list[
        tuple[int, str, str | None, str | None, int | None, str | None, str | None]
    ] = []
    #: pypi_names to seed as `(name, NULL, NULL)`, deduped across the whole
    #: batch before the single writer connection inserts them.
    pending_seeds: set[str] = set()
    version = reroll.__version__

    def flush() -> None:
        if not pending_writes and not pending_seeds:
            return
        now = int(time.time())
        write_db.execute("BEGIN IMMEDIATE")
        try:
            if pending_seeds:
                write_db.executemany(
                    "INSERT INTO pypi_conda_names(pypi_name, conda_name, updated_at) "
                    "VALUES (?, NULL, NULL) ON CONFLICT(pypi_name) DO NOTHING",
                    [(name,) for name in pending_seeds],
                )
            if pending_writes:
                write_db.executemany(
                    "UPDATE wheel SET reroll_data = jsonb(?), resolutions = jsonb(?), "
                    "requires_prerelease = ?, reroll_version = ?, updated_at = ? "
                    "WHERE id = ?",
                    [
                        (data, res, req_pre, version, now, wid)
                        for (wid, _cat, data, res, req_pre, _sub, _desc) in pending_writes
                    ],
                )
                # `ok` clears any stale `reroll_errors` row (the one case
                # this matters: a `runtime`-only row from a previous,
                # unstable run, now superseded by success) -- see module
                # docstring's "Runtime errors stop the batch, but are now
                # recorded". Every other category upserts its row in place,
                # `ON CONFLICT` covering the same "retried after a
                # `runtime` failure, failed again differently" case.
                ok_ids = [wid for (wid, cat, *_rest) in pending_writes if cat == "ok"]
                error_rows = [
                    (wid, cat, sub, desc, now)
                    for (wid, cat, _data, _res, _req, sub, desc) in pending_writes
                    if cat != "ok"
                ]
                if ok_ids:
                    write_db.executemany(
                        "DELETE FROM reroll_errors WHERE wheel_id = ?",
                        [(wid,) for wid in ok_ids],
                    )
                if error_rows:
                    write_db.executemany(
                        "INSERT INTO reroll_errors"
                        "(wheel_id, category, sub_category, description, updated_at) "
                        "VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT(wheel_id) DO UPDATE SET "
                        "category = excluded.category, "
                        "sub_category = excluded.sub_category, "
                        "description = excluded.description, "
                        "updated_at = excluded.updated_at",
                        error_rows,
                    )
            write_db.execute("COMMIT")
        except BaseException:
            write_db.execute("ROLLBACK")
            raise
        pending_writes.clear()
        pending_seeds.clear()

    def report(force: bool = False) -> None:
        nonlocal next_report
        now = time.monotonic()
        if not force and now < next_report:
            return
        next_report = now + progress_every
        check_wal()
        done = sum(counters.values())
        errors = done - counters["ok"]
        remaining = max(total - done, 0)
        elapsed = now - started
        rpm = done / elapsed * 60 if elapsed else 0.0
        attempted = table_total - outstanding_before + done
        coverage = attempted / table_total * 100 if table_total else 0.0
        breakdown = "  ".join(
            f"{cat}={counters[cat]:,}" for cat in CATEGORIES if counters[cat]
        )
        print(
            f"  {done:>9,}/{total:,} converted  {remaining:>9,} remaining  "
            f"{errors:>7,} errors  {rpm:8.0f} wheels/min  "
            f"{coverage:5.1f}% attempted"
            + (f"  ({breakdown})" if breakdown else ""),
            file=sys.stderr,
        )

    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(str(data_dir), allow_pre),
        ) as pool:
            while runtime_error is None:
                n = read_batch if remaining_limit is None else min(read_batch, remaining_limit)
                if n <= 0:
                    break
                # Re-issued fresh every batch, fully drained by `fetchall()`
                # -- see `reroll_data.db2.wal_monitor`'s docstring on why an
                # unfinished SELECT left open across a whole run pins the
                # WAL. `_db2.OUTSTANDING_WHEEL` covers this exactly.
                rows = read_db.execute(
                    f"SELECT w.id, w.filename FROM wheel w WHERE {_db2.OUTSTANDING_WHEEL} "
                    "LIMIT ?",
                    (n,),
                ).fetchall()
                if not rows:
                    break
                if remaining_limit is not None:
                    remaining_limit -= len(rows)

                for result in pool.map(_convert_one, rows, chunksize=chunksize):
                    counters[result.category] += 1
                    pending_seeds.update(result.seed_names)
                    pending_writes.append(
                        (
                            result.wheel_id,
                            result.category,
                            result.reroll_data_json,
                            result.resolutions_json,
                            (
                                None
                                if result.requires_prerelease is None
                                else int(result.requires_prerelease)
                            ),
                            result.sub_category,
                            result.description,
                        )
                    )
                    if len(pending_writes) >= write_batch:
                        flush()
                    if result.category == "runtime":
                        # Says nothing about this wheel, and every remaining
                        # wheel in this batch is likely to hit the same
                        # unstable environment -- stop rather than burn
                        # through the rest of the corpus reproducing it. The
                        # row above is still flushed below (recorded for
                        # accounting), but a `runtime`-only row never
                        # settles a wheel, so it stays eligible for a
                        # genuine retry -- see module docstring.
                        runtime_error = (result.wheel_id, result.description or "runtime error")
                        break
                flush()
                report()
                if runtime_error is not None:
                    break
    except KeyboardInterrupt:
        interrupted = True
        print(
            "\n  interrupted -- already-committed rows stay done; re-run to "
            "pick up where this left off.",
            file=sys.stderr,
        )
    finally:
        flush()
        report(force=True)
        read_db.close()
        write_db.close()

    if runtime_error is not None:
        wheel_id, error = runtime_error
        print(
            f"\n  stopping: runtime error converting wheel id={wheel_id}: "
            f"{error}\n  this indicates the host environment (network, "
            "local cache, sqlite) is unstable, not a bad wheel -- fix that, "
            "then re-run to pick up where this left off.",
            file=sys.stderr,
        )

    out = dict(counters)
    out["interrupted"] = interrupted or runtime_error is not None
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="reroll-data-reroll-convert",
        description=(
            "Run reroll's own translator over every main.db.wheel row "
            "(idempotent, resumable)."
        ),
    )
    parser.add_argument(
        "--data-dir",
        default=str(_db2.DEFAULT_DATA_DIR),
        type=Path,
        help=f"directory main.db/pypi.db live under (default: {_db2.DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes (default: all cores, via os.process_cpu_count())",
    )
    parser.add_argument("--limit", type=int, default=None, help="only convert N wheels")
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="also re-attempt wheels previously marked with a settled (non-runtime) error",
    )
    parser.add_argument(
        "--retry-stale-version",
        action="store_true",
        help="also re-attempt wheels last converted by a different reroll_version",
    )
    parser.add_argument(
        "--allow-pre",
        action="store_true",
        help="accept a pre-release wheel version or dependency version on the first attempt",
    )
    parser.add_argument("--read-batch", type=int, default=READ_BATCH)
    parser.add_argument("--chunksize", type=int, default=CHUNKSIZE)
    parser.add_argument("--write-batch", type=int, default=WRITE_BATCH)
    args = parser.parse_args(argv)

    main_db = _db2.connect_main(args.data_dir)
    _db2.init_main(main_db)
    if args.retry_errors:
        rearmed = reset_errors(main_db)
        print(f"re-armed {rearmed:,} previously-failed wheels", file=sys.stderr)
    if args.retry_stale_version:
        rearmed = reset_stale_version(main_db)
        print(f"re-armed {rearmed:,} wheels converted by a different reroll_version", file=sys.stderr)
    main_db.close()

    out = convert(
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


if __name__ == "__main__":
    raise SystemExit(main())
