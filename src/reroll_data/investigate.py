"""Run a probe over the wheel corpus and report what it cannot handle.

Reading the corpus is deliberately dumb, because measurement says it is free:
a full scan of ``(project, filename)`` for all 12M wheels takes ~4s, so there
is no export cache, no covering index and no streaming trick here. The probe is
the only expensive part, and how expensive depends entirely on what it does --
a filename-only probe runs at ~170k wheels/s/core (the whole corpus in ~90s on
one core), while a probe that pulls each wheel's stored METADATA body runs at
~1k/s/core because it pays a random btree seek into the 21GB blob table. So
``--workers`` defaults to 1 and exists for the latter case.

A probe signals "reroll cannot handle this wheel" by raising; the exception's
type and message are the failure taxonomy. See ``probes/filename.py``.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import os
import re
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from . import db as _db

# Columns of the failure CSV. No "succeeded" column: every row here is a
# failure, so it would be a constant.
CSV_HEADER = ("project", "version", "filename", "exc_type", "exc_message")

# Rows between progress lines. Large enough that the check is free at 170k/s.
_PROGRESS_EVERY = 500_000

# Quoted literals are the only part of a validation message that varies per
# wheel ("invalid abi tag: 'cp36m'" vs "'cp37m'"), so squashing them turns
# thousands of distinct messages into a handful of reasons worth reading.
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")

_NORMALIZE = re.compile(r"[-_.]+")


def normalize_message(msg: str) -> str:
    """Collapse per-wheel literals so messages group into reasons."""
    return _QUOTED.sub("'?'", msg.strip().replace("\n", " "))


def canonical_name(name: str) -> str:
    """PEP 503 normalization, so ``Foo.Bar`` and ``foo-bar`` compare equal."""
    return _NORMALIZE.sub("-", name).lower()


def version_of(filename: str) -> str:
    """Best-effort version from a wheel filename.

    Cannot use ``packaging`` here: this has to produce something for exactly
    the filenames that are too malformed for ``packaging`` to parse, which are
    the interesting rows in the failure CSV. A wheel is
    ``name-version(-build)?-interp-abi-platform.whl``, so field 1 is the
    version whenever enough fields are present at all.
    """
    parts = filename.removesuffix(".whl").split("-")
    return parts[1] if len(parts) >= 5 else ""


def connect_ro(path: str | Path) -> sqlite3.Connection:
    """Open the corpus genuinely read-only.

    ``db.connect(read_only=True)`` only skips a mkdir; it still opens for
    writing. A metadata fetch may be running against this file, so a diagnostic
    tool must not be able to touch it.
    """
    resolved = Path(path).resolve()
    # Checked up front because `mode=ro` will not create a missing file, and
    # sqlite's own "unable to open database file" does not say which one.
    if not resolved.is_file():
        raise SystemExit(
            f"corpus not found: {resolved}\n"
            f"pass --db (the crawl writes data/v.db), or run `make sync-filenames` first."
        )
    db = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    db.execute("PRAGMA busy_timeout=60000")
    return db


# --------------------------------------------------------------------------- #
# probe loading
# --------------------------------------------------------------------------- #


_PROBE_CACHE: dict[Path, Callable[[str], object]] = {}


def probe_dirs() -> list[Path]:
    """Directories searched for a probe given by bare name.

    The working directory comes first so a checkout can shadow the installed
    copy, then the repo the package was installed from -- which is the same
    place for an editable install, and lets ``--probe filename`` work from
    anywhere rather than only from the repo root.
    """
    dirs = [Path("probes").resolve(), Path(__file__).resolve().parents[2] / "probes"]
    seen: set[Path] = set()
    return [d for d in dirs if not (d in seen or seen.add(d))]


def available_probes() -> list[str]:
    """Names of the checked-in probes, for help and error messages."""
    names: set[str] = set()
    for directory in probe_dirs():
        if directory.is_dir():
            names.update(
                path.stem
                for path in directory.glob("*.py")
                if not path.name.startswith("_")
            )
    return sorted(names)


def resolve_probe(spec: str) -> Path:
    """Resolve ``--probe`` from either a bare name or a path.

    A bare name (``filename``) is looked up in ``probes/``, so a checked-in
    probe can be re-run by name without repeating its path. Anything carrying a
    directory separator or a ``.py`` suffix is treated as an explicit path, and
    is never silently redirected into ``probes/``.
    """
    candidate = Path(spec)
    if candidate.suffix == ".py" or os.sep in spec or "/" in spec:
        if candidate.is_file():
            return candidate.resolve()
        raise SystemExit(f"probe not found: {candidate}")

    for directory in probe_dirs():
        path = directory / f"{spec}.py"
        if path.is_file():
            return path.resolve()

    known = ", ".join(available_probes()) or "none found"
    raise SystemExit(
        f"unknown probe {spec!r}\n"
        f"available probes: {known}\n"
        f"(or pass a path ending in .py)"
    )


def load_probe(path: str | Path) -> Callable[[str], object]:
    """Import a probe file and return its ``probe`` callable.

    Imported once per process, so module-level setup in the probe (installing a
    log handler, building lookup tables) is paid once rather than per wheel.

    Cached per path because loading twice in one process would be actively
    harmful, not merely wasteful: re-exec'ing the module rebinds the name in
    ``sys.modules`` but leaves anything the first copy registered externally --
    such as a logging handler on ``reroll.filename`` -- still attached and now
    unreachable, so it would accumulate records for the whole run with nothing
    ever draining it.
    """
    path = Path(path).resolve()
    cached = _PROBE_CACHE.get(path)
    if cached is not None:
        return cached
    if not path.is_file():
        raise SystemExit(f"probe not found: {path}")
    spec = importlib.util.spec_from_file_location(f"_probe_{path.stem}", path)
    if spec is None or spec.loader is None:  # pragma: no cover - unimportable path
        raise SystemExit(f"cannot import probe: {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so a probe that imports itself, or pickles
    # anything defined in it, resolves the module by name.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    probe = getattr(module, "probe", None)
    if not callable(probe):
        raise SystemExit(f"probe {path} does not define a callable named 'probe'")
    _PROBE_CACHE[path] = probe
    return probe


def probe_revision() -> str:
    """Describe the revision of reroll the probe just loaded, if any.

    Reported so a results file can be tied back to a reroll commit -- an
    editable install means the code under test changes without any reinstall,
    which is convenient but leaves nothing else to identify it by.
    """
    module = sys.modules.get("reroll")
    if module is None or not getattr(module, "__file__", None):
        return "unknown (probe did not import reroll)"
    root = Path(module.__file__).resolve().parents[2]

    def git(*args: str) -> str | None:
        try:
            out = subprocess.run(
                ["git", "-C", str(root), *args],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,  # returncode is inspected; a failure is not fatal here
            )
        except (OSError, subprocess.SubprocessError):  # pragma: no cover
            return None
        return out.stdout.strip() if out.returncode == 0 else None

    sha = git("rev-parse", "--short", "HEAD")
    if sha is None:
        return f"{root} (not a git checkout)"
    branch = git("rev-parse", "--abbrev-ref", "HEAD") or "?"
    dirty = " +dirty" if git("status", "--porcelain") else ""
    return f"{sha} ({branch}){dirty}"


# --------------------------------------------------------------------------- #
# corpus selection
# --------------------------------------------------------------------------- #


def resolve_packages(
    db: sqlite3.Connection, packages: Sequence[str]
) -> tuple[list, list]:
    """Split ``--package`` values into exact names and GLOB patterns.

    ``wheel.project`` holds the unnormalized display name, so ``--package
    requests`` should still find ``Requests``. An exact hit is a PK seek and
    costs nothing, so the normalized lookup only runs when that misses.
    """
    names: list[str] = []
    globs: list[str] = []
    unresolved: list[str] = []

    for want in packages:
        if any(ch in want for ch in "*?["):
            globs.append(want)
            continue
        row = db.execute(
            "SELECT 1 FROM wheel WHERE project = ? LIMIT 1", (want,)
        ).fetchone()
        if row is not None:
            names.append(want)
        else:
            unresolved.append(want)

    if unresolved:
        wanted = {canonical_name(u): u for u in unresolved}
        # 861k rows, ~1s, and only on the miss path.
        for (name,) in db.execute("SELECT name FROM project"):
            if canonical_name(name) in wanted:
                names.append(name)
                wanted.pop(canonical_name(name), None)
        for missing in wanted.values():
            print(f"warning: no wheels for package {missing!r}", file=sys.stderr)

    return names, globs


def build_query(
    names: Sequence[str],
    globs: Sequence[str],
    lo: str | None,
    hi: str | None,
    limit: int | None,
) -> tuple[str, list]:
    """Build the wheel selection query.

    ``ORDER BY project, filename`` is free: ``wheel`` is WITHOUT ROWID keyed on
    exactly that, so the PK btree already yields this order and no sort is
    added. It matters because distinct projects are then counted by watching
    for changes instead of holding 862k names in a set.
    """
    where: list[str] = []
    params: list = []
    if lo is not None:
        where.append("project >= ?")
        params.append(lo)
    if hi is not None:
        where.append("project < ?")
        params.append(hi)
    if names or globs:
        alts = ["project = ?"] * len(names) + ["project GLOB ?"] * len(globs)
        where.append("(" + " OR ".join(alts) + ")")
        params.extend([*names, *globs])

    sql = "SELECT project, filename FROM wheel"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY project, filename"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return sql, params


def shard_bounds(
    db: sqlite3.Connection, workers: int
) -> list[tuple[str | None, str | None]]:
    """Split the project keyspace into ranges of roughly equal wheel count.

    Balanced on ``project.n_wheels`` rather than on project count, because the
    distribution is extremely skewed -- splitting 862k projects evenly would
    hand one worker a shard many times heavier than another's.
    """
    if workers <= 1:
        return [(None, None)]
    rows = db.execute(
        "SELECT name, coalesce(n_wheels, 0) FROM project ORDER BY name"
    ).fetchall()
    total = sum(n for _, n in rows)
    if total == 0:  # pragma: no cover - empty corpus
        return [(None, None)]

    per = total / workers
    bounds: list[str] = []
    seen = 0
    for name, n in rows:
        # Cut before adding this project, so a boundary always lands on a
        # project edge and shards never overlap or drop a project.
        if seen >= per * (len(bounds) + 1) and len(bounds) < workers - 1:
            bounds.append(name)
        seen += n

    edges: list[str | None] = [None, *bounds, None]
    return [(edges[i], edges[i + 1]) for i in range(len(edges) - 1)]


# --------------------------------------------------------------------------- #
# results
# --------------------------------------------------------------------------- #


@dataclass
class Stats:
    """Aggregate outcome of a run. Mergeable so shards can be combined."""

    wheels: int = 0
    projects: int = 0
    successes: int = 0
    failures: int = 0
    by_type: Counter = field(default_factory=Counter)
    by_reason: Counter = field(default_factory=Counter)
    # Per (interpreter, abi, platform) triple. Both totals and failures are
    # tracked so a shape can be told apart from a wheel: a shape whose every
    # wheel fails is a gap in tag support, whereas a shape that fails for only
    # some of its wheels is a name/version problem in an otherwise fine shape.
    # ~8.1k distinct triples corpus-wide, so this costs nothing to carry.
    shape_total: Counter = field(default_factory=Counter)
    shape_failed: Counter = field(default_factory=Counter)
    shape_reason: dict = field(default_factory=dict)
    # Filenames with too few fields to have a tag triple at all.
    malformed: int = 0

    def record_failure(self, exc: BaseException) -> tuple[str, str]:
        self.failures += 1
        exc_type = type(exc).__qualname__
        message = str(exc)
        self.by_type[exc_type] += 1
        # Grouped on the normalized message so per-wheel literals do not shatter
        # the histogram into thousands of one-row buckets; the raw message is
        # what gets written to the CSV and shown per tag shape.
        self.by_reason[(exc_type, normalize_message(message))] += 1
        return exc_type, message

    def record_shape_failure(self, shape: tuple, exc_type: str, message: str) -> None:
        self.shape_failed[shape] += 1
        # The raw message, not the normalized one: a shape is printed alongside
        # its reason, so the offending tag is already pinned down and collapsing
        # it to '?' would only throw detail away. Normalization exists to group
        # reasons *across* tags, which is the reason histogram's job, not this.
        #
        # Smallest wins rather than first, so the representative does not depend
        # on which wheel of a shape happened to be scanned first.
        reason = (exc_type, message)
        current = self.shape_reason.get(shape)
        if current is None or reason < current:
            self.shape_reason[shape] = reason

    def merge(self, other: Stats) -> None:
        self.wheels += other.wheels
        self.projects += other.projects
        self.successes += other.successes
        self.failures += other.failures
        self.malformed += other.malformed
        self.by_type.update(other.by_type)
        self.by_reason.update(other.by_reason)
        self.shape_total.update(other.shape_total)
        self.shape_failed.update(other.shape_failed)
        for shape, reason in other.shape_reason.items():
            current = self.shape_reason.get(shape)
            if current is None or reason < current:
                self.shape_reason[shape] = reason

    def unsupported_shapes(self) -> list[tuple]:
        """Shapes where *every* wheel failed -- i.e. the tag itself is the gap."""
        return [s for s, n in self.shape_failed.items() if n == self.shape_total[s]]

    def partial_shapes(self) -> list[tuple]:
        """Shapes where only some wheels failed -- not the tag's fault."""
        return [s for s, n in self.shape_failed.items() if n < self.shape_total[s]]


def shape_of(filename: str) -> tuple | None:
    """The ``(interpreter, abi, platform)`` triple, or None if there isn't one."""
    parts = filename.removesuffix(".whl").split("-")
    return tuple(parts[-3:]) if len(parts) >= 5 else None


def run_shard(
    db_path: str,
    probe_path: str,
    out_path: str,
    names: Sequence[str],
    globs: Sequence[str],
    lo: str | None,
    hi: str | None,
    limit: int | None,
    progress: bool,
) -> Stats:
    """Probe every wheel in one keyspace range, writing failures to ``out_path``.

    Each shard writes its own CSV rather than returning rows: failures are a
    large minority of the corpus (~19% for filename parsing, so ~2.3M rows),
    and pickling those back to the parent would cost more than the probe.
    """
    probe = load_probe(probe_path)
    db = connect_ro(db_path)
    sql, params = build_query(names, globs, lo, hi, limit)
    stats = Stats()
    started = time.perf_counter()
    last_project = None

    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        try:
            for project, filename in db.execute(sql, params):
                stats.wheels += 1
                if project != last_project:
                    stats.projects += 1
                    last_project = project

                shape = shape_of(filename)
                if shape is None:
                    stats.malformed += 1
                else:
                    stats.shape_total[shape] += 1

                try:
                    probe(filename)
                except Exception as exc:  # noqa: BLE001 - the probe's whole job
                    exc_type, message = stats.record_failure(exc)
                    if shape is not None:
                        stats.record_shape_failure(shape, exc_type, message)
                    writer.writerow(
                        (project, version_of(filename), filename, exc_type, message)
                    )
                else:
                    stats.successes += 1

                if progress and stats.wheels % _PROGRESS_EVERY == 0:
                    rate = stats.wheels / (time.perf_counter() - started)
                    print(
                        f"  {stats.wheels:>12,} wheels  "
                        f"{stats.failures:>12,} failures  {rate:>9,.0f}/s",
                        file=sys.stderr,
                    )
        finally:
            db.close()
    return stats


def merge_parts(parts: Sequence[Path], destination: Path) -> None:
    """Concatenate shard CSVs into one file with a single header."""
    with open(destination, "w", encoding="utf-8", newline="") as out:
        csv.writer(out).writerow(CSV_HEADER)
        for part in parts:
            with open(part, encoding="utf-8", newline="") as fh:
                # Copied by block rather than parsed and re-serialised; the
                # rows were written by csv.writer and need no rewriting.
                while chunk := fh.read(1 << 20):
                    out.write(chunk)
            part.unlink()


# --------------------------------------------------------------------------- #
# reporting
# --------------------------------------------------------------------------- #


def _pct(part: int, whole: int) -> str:
    return f"{100.0 * part / whole:5.1f}%" if whole else "    - "


def report(stats: Stats, elapsed: float, revision: str, top: int) -> None:
    """Print overall statistics and the failure breakdown to stdout."""
    rate = stats.wheels / elapsed if elapsed else 0.0
    print()
    print(f"reroll revision   {revision}")
    print(f"packages          {stats.projects:>12,}")
    print(f"wheels            {stats.wheels:>12,}")
    print(
        f"successes         {stats.successes:>12,}  {_pct(stats.successes, stats.wheels)}"
    )
    print(
        f"failures          {stats.failures:>12,}  {_pct(stats.failures, stats.wheels)}"
    )
    print(f"elapsed           {elapsed:>12.1f}s  {rate:,.0f} wheels/s")

    if not stats.failures:
        return

    print()
    print("failures by exception type")
    width = max(len(t) for t in stats.by_type)
    for exc_type, count in stats.by_type.most_common():
        share = _pct(count, stats.failures)
        print(f"  {exc_type:<{width}}  {count:>12,}  {share}")

    print()
    print(f"top {top} failure reasons (varying literals shown as '?')")
    for (exc_type, reason), count in stats.by_reason.most_common(top):
        print(f"  {count:>12,}  {_pct(count, stats.failures)}  {exc_type}: {reason}")

    report_shapes(stats, top)


def report_shapes(stats: Stats, top: int) -> None:
    """Print tag-space coverage: which ``interpreter-abi-platform`` shapes fail.

    Counted over real filenames during the same scan, so this costs one string
    split per wheel and needs no second pass. It answers a different question
    from the failure counts above -- those say how many wheels fail, this says
    how much of the tag space is unsupported and which gaps are widest.

    The split between fully and partly failing shapes is the useful part: a
    shape whose every wheel fails is a genuine gap in tag support, while a shape
    that fails for only some of its wheels is a name or version problem in a
    shape reroll otherwise handles.
    """
    shapes = stats.shape_total
    if not shapes:
        return
    unsupported = stats.unsupported_shapes()
    partial = stats.partial_shapes()
    unsupported_wheels = sum(stats.shape_failed[s] for s in unsupported)
    partial_wheels = sum(stats.shape_failed[s] for s in partial)

    print()
    print("tag shapes (interpreter-abi-platform)")
    print(
        f"  distinct            {len(shapes):>12,}"
        f"  ({stats.wheels / len(shapes):,.0f}x redundancy)"
    )
    print(
        f"  fully unsupported   {len(unsupported):>12,}  {_pct(len(unsupported), len(shapes))}"
        f"  {unsupported_wheels:>12,} wheels  {_pct(unsupported_wheels, stats.wheels)}"
    )
    print(
        f"  partly failing      {len(partial):>12,}  {_pct(len(partial), len(shapes))}"
        f"  {partial_wheels:>12,} wheels  {_pct(partial_wheels, stats.wheels)}"
    )
    if stats.malformed:
        print(
            f"  malformed names     {stats.malformed:>12,}"
            "                (too few fields to have a tag)"
        )

    if not unsupported:
        return
    print()
    print(f"top {top} unsupported tag shapes by wheel count")
    # Sorted on the tag as a tiebreak so equal counts do not reorder run to run.
    ranked = sorted(unsupported, key=lambda s: (-stats.shape_failed[s], s))
    for shape in ranked[:top]:
        count = stats.shape_failed[shape]
        exc_type, reason = stats.shape_reason[shape]
        tag = "-".join(shape)
        print(
            f"  {count:>12,}  {_pct(count, stats.wheels)}  {tag:<44}  {exc_type}: {reason}"
        )


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #


def cmd_run(args: argparse.Namespace) -> int:
    probe_path = resolve_probe(args.probe)
    db = connect_ro(args.db)
    names, globs = resolve_packages(db, args.package)
    if args.package and not names and not globs:
        db.close()
        return 2

    # --limit has no meaning per shard (each would take its own N), so it forces
    # a single process. A --package filter does not: shards just add a keyspace
    # predicate alongside it, which also makes sharding testable on a subset
    # instead of only on a full 12M-wheel run.
    workers = 1 if args.limit is not None else max(1, args.workers)
    bounds = shard_bounds(db, workers) if workers > 1 else [(None, None)]
    db.close()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    revision = probe_revision_for(probe_path)

    print(
        f"probing with {probe_path.name} "
        f"({workers} worker{'s' if workers > 1 else ''}) ...",
        file=sys.stderr,
    )
    started = time.perf_counter()
    stats = Stats()
    # Suffix appended rather than substituted: with_suffix would turn
    # "failures.csv" into "failures.part0" and could collide with a real file.
    parts = [Path(f"{output}.part{i}") for i in range(len(bounds))]

    try:
        if workers == 1:
            stats = run_shard(
                str(args.db),
                str(probe_path),
                str(parts[0]),
                names,
                globs,
                None,
                None,
                args.limit,
                True,
            )
        else:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(
                        run_shard,
                        str(args.db),
                        str(probe_path),
                        str(part),
                        names,
                        globs,
                        lo,
                        hi,
                        None,
                        False,
                    )
                    for part, (lo, hi) in zip(parts, bounds)
                ]
                for done, future in enumerate(futures, 1):
                    stats.merge(future.result())
                    print(
                        f"  shard {done}/{len(futures)} done  "
                        f"{stats.wheels:,} wheels  {stats.failures:,} failures",
                        file=sys.stderr,
                    )
        merge_parts(parts, output)
    finally:
        # Leave no litter if a shard died partway through.
        for part in parts:
            part.unlink(missing_ok=True)

    report(stats, time.perf_counter() - started, revision, args.top)
    print()
    print(f"wrote {stats.failures:,} failures to {output}")
    return 0


def probe_revision_for(probe_path: Path) -> str:
    """Load the probe in this process just to read the revision it pulls in."""
    load_probe(probe_path)
    return probe_revision()


def build_parser() -> argparse.ArgumentParser:
    known = ", ".join(available_probes()) or "none found"
    p = argparse.ArgumentParser(
        prog="reroll-investigate",
        description="Run a probe over the wheel corpus to find what reroll cannot handle.",
    )
    p.add_argument(
        "--db",
        default=str(_db.DEFAULT_DB),
        help=f"SQLite corpus, opened read-only (default: {_db.DEFAULT_DB})",
    )
    p.add_argument(
        "--probe",
        required=True,
        metavar="NAME",
        help=f"checked-in probe to run, by name -- one of: {known} "
        "(or a path to a .py file defining probe(filename), which raises to fail)",
    )
    p.add_argument(
        "--package",
        action="append",
        default=[],
        metavar="NAME",
        help="restrict to a package; repeatable, matched case/separator-"
        "insensitively, and treated as a GLOB if it contains * ? or [",
    )
    p.add_argument("--limit", type=int, default=None, help="stop after N wheels")
    p.add_argument(
        "--top",
        type=int,
        default=25,
        help="reasons and tag shapes to list (default: %(default)s)",
    )
    p.add_argument(
        "-o",
        "--output",
        default="failures.csv",
        help="failure CSV (default: %(default)s)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="processes, each scanning its own slice of the project keyspace. "
        "Only worth raising for probes that do per-wheel I/O; a filename-only "
        "probe finishes the whole corpus in ~90s on one core (default: %(default)s)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    return cmd_run(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
