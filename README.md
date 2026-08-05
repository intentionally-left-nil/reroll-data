# reroll-data
Static pypi data for determining reroll compatibility percentages with real packages

Two halves:

- **`reroll-data`** scrapes every `.whl` filename on PyPI, plus its PEP 658
  `METADATA` body, into a SQLite corpus. See the `Makefile`.
- **`reroll-investigate`** runs a *probe* over that corpus and reports what
  [`reroll`](../reroll) cannot handle.

## Diagnostics

```sh
make investigate                       # probe every wheel -> failures.csv
make investigate PACKAGE=requests      # one package
make investigate LIMIT=10000           # first N wheels
make investigate PROBE=filename        # pick a probe from probes/
```

or directly:

```sh
uv run reroll-investigate --probe filename --package 'numpy*' -o failures.csv
uv run reroll-investigate --probe filename --limit 10 --package requests
```

`--probe` takes the *name* of a checked-in probe from `probes/`, so anything can
be re-run by name; `--help` lists what's available. A path ending in `.py` is
also accepted and is never redirected into `probes/`.

Output is overall statistics, a failure breakdown by exception type and reason,
and a tag-space coverage section — plus `failures.csv` with `project, version,
filename, exc_type, exc_message`. Failures only, since a "succeeded" column
would be constant.

Only the **failure reasons** histogram shows literals as `'?'`
(`invalid abi tag: '?'`). That section aggregates across the whole tag space, so
collapsing the varying literal is what makes `invalid abi tag` a single row
instead of one row per ABI tag. Everywhere the tag is already pinned down — the
tag-shape section and `failures.csv` — the real literal is shown
(`invalid abi tag: 'cp35m'`).

### Reading the tag-shape section

Alongside the per-wheel counts, each run reports the distinct
`(interpreter, abi, platform)` triples it saw. The 12M wheels collapse to ~8.1k
triples, so this says how much of the *tag space* is unsupported and which gaps
are widest, rather than just how many wheels failed. It is computed from real
filenames during the same scan, at the cost of one string split per wheel.

The split between the two buckets is the useful part:

- **fully unsupported** — every wheel with this shape failed, so the tag itself
  is the gap. This is the queue of things to teach `reroll`.
- **partly failing** — only some wheels with this shape failed, so the shape is
  fine and the name or version is at fault (`ManagerTk-0.1(r)`,
  `InPynamoDB-4.1.0_2`). `malformed names` counts filenames with too few fields
  to have a tag at all (`OpenPS_1.0.0-py3-none-any.whl`, which has no version
  field).

One caveat: a shape whose *only* wheel fails for a name/version reason is
counted as fully unsupported, because nothing distinguishes it from a tag gap by
count alone. The per-shape reason makes it visible — an `UnparseableFilename`
there is a bad version, not a missing tag. It did not occur in any selection
measured so far.

### Writing a probe

A probe is a file in `probes/` defining `probe(filename)`. **It signals "reroll
cannot handle this wheel" by raising**; the exception's type and message become
the failure taxonomy. Returning normally means success.

```python
from reroll.filename import parse_filename

class Unsupported(Exception): pass

def probe(filename: str) -> None:
    if not parse_filename(filename):
        raise Unsupported("no configs")
```

The file is imported once per process, so module-level setup is paid once rather
than per wheel. Anything the probe raises is caught and counted, so a genuine
bug in `reroll` shows up as its own exception type rather than aborting the run.

`probes/filename.py` is the real probe for `parse_filename`, and is more
involved than the sketch above for one reason: `parse_filename` is documented
never to raise. An unparseable filename and a filename whose every tag is
unsupported both just return `()`, with the reason logged at `DEBUG`. So a bare
empty-tuple check puts every failure in a single bucket called "returned
empty". The probe instead attaches a handler to the `reroll.filename` logger and
turns those records back into typed exceptions (`UnsupportedInterpreter`,
`UnsupportedAbi`, `UnsupportedPlatform`, ...). If `parse_filename` ever grows a
real exception contract, that machinery can be deleted and the exception left to
propagate.

### Reproducibility

Output is byte-identical across `--workers` counts and `PYTHONHASHSEED` values,
so two result files can be diffed to see what a change to `reroll` actually
moved. This takes care: `parse_filename` iterates a *frozenset* of tags, so
anything derived from "the first reason logged" varies per process. `probes/`
and `investigate.py` both normalize and sort rather than relying on arrival
order.

Each run prints the `reroll` git revision it loaded, including a `+dirty` marker
— with an editable install the code under test changes with no reinstall, so
there is otherwise nothing to identify a result file by.

### Performance

Measured on the full 12M-wheel corpus:

| stage | throughput | full corpus |
| --- | --- | --- |
| SQLite scan of `(project, filename)` | 3.1M rows/s | ~4 s |
| `parse_filename` probe | ~170k/s/core | ~90 s, one core |
| probe reading each stored `METADATA` body | ~1k/s/core | ~3.4 h, one core |
Reading is free, which is why there is no export cache, no covering index and no
streaming trick here — a plain `SELECT` outruns any probe. `--workers` therefore
defaults to 1: parallelising a 90-second job is not worth the complexity.

Raise it for probes in the third row. Those are bound by a random btree seek into
the 21 GB blob table, not by CPU, so workers each open their own read-only
connection and scan **their own slice of the `project` keyspace**, balanced on
`project.n_wheels` (the distribution is skewed enough that splitting projects
evenly would not split work evenly). Nothing but counts crosses a process
boundary; each shard writes its own CSV and the parent concatenates, because
pickling ~2.3M failure rows back would cost more than the probe.

## Development

`reroll` is installed editable from `../reroll` via a `probe` dependency group
that `uv sync` picks up by default, so edits to `reroll` are live with no
reinstall. Run `make sync-probe` only if `reroll`'s own dependencies change.
