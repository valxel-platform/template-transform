"""Items with a feature on against items with it off, while the SDLC tool releases it gradually.

A job's unit of work is an item (a reminder, a record to reconcile). Wrap each in `run.item()`:
every flag the work reads for it (`flags.enabled`) counts the item once on the side it got, as
failed when the item raised, and as slow when it took longer than `JOB_COHORTS_SLOW_MS` (5 s).
At the end of the run each flag's counts are logged as one line, which the tool sums and compares:
items that fail more with the feature on stop its release.

    valxel.flag_cohorts {"flag": "emr.emr-feat-3.sms-reminders", "on": {"requests": 40, "errors": 0, "slow": 1},
                         "off": {"requests": 760, "errors": 2, "slow": 9}}

(The tool reads `requests` as units of work, whatever they are.) The product events an item
records count on the same side. Only counts leave the job.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

MARKER = "valxel.flag_cohorts"
log = logging.getLogger("job.cohorts")
_KEY = re.compile(r"[A-Za-z0-9._-]{1,200}")
_EVENT = re.compile(r"[a-z][a-z0-9_.]{0,99}")
_current: ContextVar[dict[str, bool] | None] = ContextVar("flag_cohorts", default=None)
_events: ContextVar[dict[str, int] | None] = ContextVar("flag_cohort_events", default=None)
_counts: dict[str, list[int]] = {}  # flag -> [on items, on errors, on slow, off items, off errors, off slow]
_event_counts: dict[str, dict[str, list[int]]] = {}  # flag -> event -> [on, off]
SLOW_MS = float(os.environ.get("JOB_COHORTS_SLOW_MS", "5000"))
_lock = threading.Lock()


def seen(key: str, on: bool) -> None:
    """A flag was read for the current item. Outside an item it isn't counted."""
    current = _current.get()
    if current is not None and _KEY.fullmatch(key or ""):
        current.setdefault(key, bool(on))


def event(name: str) -> None:
    """A product event happened for the current item (`events.record`)."""
    current = _events.get()
    if current is not None and _EVENT.fullmatch(name or ""):
        current[name] = current.get(name, 0) + 1


@contextmanager
def item() -> Iterator[None]:
    """One unit of work: the flags read and events recorded inside count for it."""
    tokens = (_current.set({}), _events.set({}))
    started, failed = time.monotonic(), False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        flags, happened = _current.get() or {}, _events.get() or {}
        _current.reset(tokens[0])
        _events.reset(tokens[1])
        _count(flags, happened, failed, (time.monotonic() - started) * 1000)


def _count(flags: dict[str, bool], happened: dict[str, int], failed: bool, elapsed_ms: float) -> None:
    with _lock:
        for key, on in flags.items():
            c = _counts.setdefault(key, [0, 0, 0, 0, 0, 0])
            side = 0 if on else 3
            c[side] += 1
            c[side + 1] += int(failed)
            c[side + 2] += int(elapsed_ms > SLOW_MS)
            for name, n in happened.items():
                _event_counts.setdefault(key, {}).setdefault(name, [0, 0])[0 if on else 1] += n


def drain() -> list[str]:
    """One line per flag since the last call, and the counts start again."""
    with _lock:
        counts, events = dict(_counts), dict(_event_counts)
        _counts.clear()
        _event_counts.clear()

    def side(c: list[int], i: int, ev: dict[str, list[int]]) -> dict[str, Any]:
        out: dict[str, Any] = {"requests": c[i * 3], "errors": c[i * 3 + 1], "slow": c[i * 3 + 2]}
        if ev:
            out["events"] = {name: n[i] for name, n in sorted(ev.items())}
        return out

    return [
        f"{MARKER} "
        + json.dumps(
            {"flag": k, "on": side(c, 0, events.get(k, {})), "off": side(c, 1, events.get(k, {}))},
            separators=(",", ":"),
        )
        for k, c in sorted(counts.items())
    ]


def report() -> None:
    for line in drain():
        log.info(line)
