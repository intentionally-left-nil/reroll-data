"""Default probe: does ``reroll.filename.parse_filename`` support this wheel?

``parse_filename`` is documented never to raise -- an unparseable filename and
a filename whose every tag is unsupported both just return ``()``, and the
reason is logged at DEBUG. That makes the bare return value useless as a
diagnostic: every failure would land in one bucket called "returned empty".

So this probe attaches a handler to the ``reroll.filename`` logger and turns
those DEBUG records back into typed exceptions. It reads ``record.args``
rather than the formatted text, because the args still hold the structured
``ValidationError.errors()`` list that the message only renders.

If ``parse_filename`` ever grows a real exception contract, delete the capture
machinery and let the exception propagate; the harness already reports whatever
type comes out.
"""

from __future__ import annotations

import logging
from typing import Any

from reroll.filename import parse_filename

from reroll_data.investigate import normalize_message

_LOGGER_NAME = "reroll.filename"

# At most this many reasons are spelled out in one message; a wheel with a
# compressed tag set can accumulate dozens of near-identical ones.
_MAX_REASONS = 4


class Unsupported(Exception):
    """reroll cannot produce any config for this wheel."""


class UnparseableFilename(Unsupported):
    """Not a wheel filename at all (``packaging`` raised InvalidWheelFilename)."""


class UnsupportedInterpreter(Unsupported):
    """The interpreter tag is out of scope (py2, pypy, an unknown prefix)."""


class UnsupportedAbi(Unsupported):
    """The ABI tag is out of scope (an old ``cp36m``-style tag, say)."""


class IncompatibleInterpreterAbi(Unsupported):
    """Interpreter and ABI are each fine but not valid together."""


class UnsupportedPlatform(Unsupported):
    """The platform tag is out of scope (musllinux, win32, i686, ...)."""


class UnsupportedArch(Unsupported):
    """The architecture is not one reroll targets for this platform."""


class UnsupportedConfig(Unsupported):
    """Rejected for a reason this probe does not recognise."""


class NoReasonGiven(Unsupported):
    """Returned empty but logged nothing -- means this probe is out of date."""


# pydantic reports the offending field in `loc`, so a field error maps straight
# to a class.
_BY_FIELD = {
    "interpreter": UnsupportedInterpreter,
    "abi": UnsupportedAbi,
    "platform": UnsupportedPlatform,
    "arch": UnsupportedArch,
}

# Which reason to blame when a wheel fails several ways at once, ordered most
# to least fundamental: "we do not support py2" is a more useful headline than
# "we do not support win32" for a py2 win32 wheel. Alphabetical order would
# pick arbitrarily; this is both deterministic and meaningful.
_RANK = {
    "<filename>": 0,
    "interpreter": 1,
    "abi": 2,
    "interpreter/abi": 3,
    "platform": 4,
    "arch": 5,
    "name": 6,
    "version": 7,
}


class _Capture(logging.Handler):
    """Collect records without formatting them."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


_capture = _Capture()
_logger = logging.getLogger(_LOGGER_NAME)
_logger.addHandler(_capture)
_logger.setLevel(logging.DEBUG)
# The reasons are this probe's payload, not console noise; keep them off any
# root handler the harness or a library may have installed.
_logger.propagate = False


def _tidy(msg: str) -> str:
    """Drop pydantic's ``Value error,`` prefix, which adds nothing here."""
    return msg.removeprefix("Value error, ")


def _classify(loc: tuple, msg: str) -> tuple[str, type[Unsupported]]:
    """Attribute one pydantic error to a field and a class.

    Whole-model validators report an empty ``loc``, which would otherwise dump
    the single largest group of real failures -- unsupported platform tags --
    into an uninformative catch-all. They are recognised by message prefix
    instead. Prefix, not substring: the arch message also contains the word
    "platform" ("arch ... unsupported for platform ...").
    """
    if loc:
        field = str(loc[0])
        return field, _BY_FIELD.get(field, UnsupportedConfig)
    if msg.startswith("unsupported platform tag"):
        return "platform", UnsupportedPlatform
    if msg.startswith("arch "):
        return "arch", UnsupportedArch
    # The remaining model validators all police interpreter/ABI agreement:
    # versioned-abi minor mismatch, generic tag needing a specific ABI, and
    # the abi3/abi3t minimum-CPython checks.
    return "interpreter/abi", IncompatibleInterpreterAbi


def _reasons(records: list[logging.LogRecord]) -> list[tuple[int, str, str, str, type]]:
    """Extract deduplicated reasons as ``(rank, field, normalized, raw, class)``.

    Deduplication is on the *normalized* message, not the raw one. A single
    compressed-tag filename is retried across every tag and arch, so it can log
    the same complaint forty times with only the quoted tag differing; keying on
    the raw text would leave all forty in place and shatter the histogram.

    Where several raw messages collapse to one reason, the lexicographically
    smallest is kept as the representative. Taking whichever arrived first would
    look equivalent but is not reproducible: ``parse_filename`` iterates a
    frozenset of tags, so arrival order follows string hash randomization and
    differs from process to process. That would make a wheel tagged
    ``manylinux_2_17_i686.manylinux2014_i686`` report either literal at random,
    and two identical runs would produce a diff.
    """
    seen: dict[tuple[str, str], tuple[int, str, str, str, type]] = {}

    def keep(field: str, raw: str, exc_class: type) -> None:
        key = (field, normalize_message(raw))
        candidate = (_RANK.get(field, 99), field, key[1], raw, exc_class)
        current = seen.get(key)
        if current is None or raw < current[3]:
            seen[key] = candidate

    for record in records:
        args: Any = record.args
        if not isinstance(args, tuple):  # pragma: no cover - defensive
            continue
        if record.msg.startswith("unparseable"):
            raw = str(args[1]) if len(args) > 1 else "unparseable wheel filename"
            keep("<filename>", raw, UnparseableFilename)
        elif record.msg.startswith("rejected") and len(args) >= 4:
            errors = args[3]
            if not isinstance(errors, list):  # pragma: no cover - defensive
                continue
            for error in errors:
                raw = _tidy(str(error.get("msg", "invalid")))
                field, exc_class = _classify(tuple(error.get("loc") or ()), raw)
                keep(field, raw, exc_class)

    # Sorted on (rank, field, normalized, raw); the class in the final position
    # is never reached as a tiebreak, since those four are already unique.
    return sorted(seen.values(), key=lambda r: r[:4])


def probe(filename: str) -> None:
    """Raise if reroll cannot turn ``filename`` into at least one config."""
    _capture.records.clear()
    if parse_filename(filename):
        return

    reasons = _reasons(_capture.records)
    if not reasons:
        raise NoReasonGiven(f"parse_filename returned no configs for {filename!r}")

    _, _, _, raw, exc_class = reasons[0]
    if len(reasons) == 1:
        # One reason: keep the raw text, so the specific offending tag survives
        # into the CSV where it is worth having.
        raise exc_class(raw)

    # Several reasons: the first picks the class, but all of them go in the
    # message so a wheel failing many ways is not misread as failing one. Raw
    # text, not normalized: deduplication already guarantees one entry per
    # distinct reason, so the repetition that motivated normalizing is gone and
    # keeping the literals costs nothing.
    listed = "; ".join(
        f"{field}: {raw}" for _, field, _, raw, _ in reasons[:_MAX_REASONS]
    )
    if len(reasons) > _MAX_REASONS:
        listed += f" (+{len(reasons) - _MAX_REASONS} more)"
    raise exc_class(listed)
