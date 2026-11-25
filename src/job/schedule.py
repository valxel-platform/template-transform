"""Five-field cron schedules: when the job is due, and how many runs fit between two times.

The platform runs the job as a Kubernetes CronJob from job.yaml's schedule and time zone. The job
reads the same schedule to know which period a run covers and whether runs were missed. Fields:
minute hour day-of-month month day-of-week, each '*', a number, a range 'a-b', a step '*/n' or
'a-b/n', or a comma list of those. Day-of-week is 0-6 from Sunday (7 is Sunday too). As in cron,
when both day fields are restricted, a day matches either. Names (MON, JAN) and '@daily' are not
accepted: write the numbers, so the tool's check and Kubernetes read the same thing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_BOUNDS = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
_NAMES = ("minute", "hour", "day of month", "month", "day of week")
# Looking further back than this for a previous run means the schedule can never match.
_HORIZON = timedelta(days=366 * 4)


class ScheduleError(ValueError):
    pass


def _field(text: str, lo: int, hi: int, name: str) -> frozenset[int]:
    out: set[int] = set()
    for part in text.split(","):
        body, _, step_text = part.partition("/")
        if step_text and not step_text.isdigit():
            raise ScheduleError(f"{name}: step '{step_text}' isn't a number")
        step = int(step_text) if step_text else 1
        if step < 1:
            raise ScheduleError(f"{name}: a step must be 1 or more")
        if body == "*":
            start, end = lo, hi
        elif "-" in body:
            a, _, b = body.partition("-")
            if not (a.isdigit() and b.isdigit()):
                raise ScheduleError(f"{name}: '{body}' isn't a range of numbers")
            start, end = int(a), int(b)
        elif body.isdigit():
            start = int(body)
            end = hi if step_text else start
        else:
            raise ScheduleError(f"{name}: '{body}' isn't a number, a range or '*'")
        if not (lo <= start <= hi and lo <= end <= hi) or start > end:
            raise ScheduleError(f"{name}: '{part}' is outside {lo}-{hi}")
        out.update(range(start, end + 1, step))
    return frozenset(out)


@dataclass(frozen=True)
class Schedule:
    text: str
    zone: ZoneInfo
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    any_day: bool
    any_weekday: bool

    @classmethod
    def parse(cls, text: str, timezone: str = "UTC") -> Schedule:
        parts = (text or "").split()
        if len(parts) != 5:
            raise ScheduleError(f"'{text}' needs five fields: minute hour day-of-month month day-of-week")
        try:
            zone = ZoneInfo(timezone or "UTC")
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ScheduleError(f"'{timezone}' isn't a time zone (use an IANA name, e.g. America/Chicago)") from exc
        fields = [_field(p, lo, hi, n) for p, (lo, hi), n in zip(parts, _BOUNDS, _NAMES, strict=True)]
        weekdays = frozenset(d % 7 for d in fields[4])
        return cls(
            text=" ".join(parts),
            zone=zone,
            minutes=fields[0],
            hours=fields[1],
            days=fields[2],
            months=fields[3],
            weekdays=weekdays,
            any_day=parts[2] == "*",
            any_weekday=parts[4] == "*",
        )

    def _day_ok(self, local: datetime) -> bool:
        if local.month not in self.months:
            return False
        dom = local.day in self.days
        dow = (local.isoweekday() % 7) in self.weekdays
        if self.any_day and self.any_weekday:
            return True
        if self.any_day:
            return dow
        if self.any_weekday:
            return dom
        return dom or dow

    def previous(self, at: datetime) -> datetime:
        """The latest time at or before `at` the schedule fires (in UTC)."""
        local = at.astimezone(self.zone).replace(second=0, microsecond=0)
        limit = local - _HORIZON
        while local > limit:
            if not self._day_ok(local):
                local = local.replace(hour=23, minute=59) - timedelta(days=1)
                continue
            if local.hour not in self.hours:
                local = local.replace(minute=59) - timedelta(hours=1)
                continue
            if local.minute not in self.minutes:
                local -= timedelta(minutes=1)
                continue
            return local.astimezone(UTC)
        raise ScheduleError(f"'{self.text}' never fires")

    def ticks_between(self, start: datetime, end: datetime, *, cap: int = 10_000) -> int:
        """How many times the schedule fires after `start` and at or before `end` (at most `cap`)."""
        n, t = 0, end
        while n < cap:
            t = self.previous(t)
            if t <= start:
                return n
            n += 1
            t -= timedelta(minutes=1)
        return n
