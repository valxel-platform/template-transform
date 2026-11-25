from datetime import UTC, datetime

import pytest

from job.schedule import Schedule, ScheduleError


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=UTC)


def test_daily_at_a_local_time_follows_daylight_saving():
    s = Schedule.parse("15 3 * * *", "America/Chicago")
    assert s.previous(utc(2026, 7, 2, 12, 0)) == utc(2026, 7, 2, 8, 15)  # CDT, UTC-5
    assert s.previous(utc(2026, 12, 2, 12, 0)) == utc(2026, 12, 2, 9, 15)  # CST, UTC-6


def test_steps_ranges_lists_and_the_exact_minute():
    s = Schedule.parse("*/15 9-17 * * 1-5", "UTC")
    assert s.previous(utc(2026, 11, 16, 12, 7)) == utc(2026, 11, 16, 12, 0)  # a Monday
    assert s.previous(utc(2026, 11, 16, 12, 15)) == utc(2026, 11, 16, 12, 15)  # at the minute itself
    assert s.previous(utc(2026, 11, 15, 12, 0)) == utc(2026, 11, 13, 17, 45)  # Sunday -> Friday's last
    assert Schedule.parse("0 0 1,15 * *").previous(utc(2026, 11, 14)) == utc(2026, 11, 1)


def test_both_day_fields_match_either_day_as_in_cron():
    s = Schedule.parse("0 6 1 * 0", "UTC")  # the 1st, or any Sunday
    assert s.previous(utc(2026, 11, 3, 7)) == utc(2026, 11, 1, 6)  # a Sunday and the 1st
    assert s.previous(utc(2026, 11, 10, 7)) == utc(2026, 11, 8, 6)  # Sunday the 8th
    assert Schedule.parse("0 6 * * 7").previous(utc(2026, 11, 10, 7)) == utc(2026, 11, 8, 6)  # 7 is Sunday too


def test_counting_runs_between_two_times():
    s = Schedule.parse("0 * * * *")
    assert s.ticks_between(utc(2026, 1, 1, 0), utc(2026, 1, 1, 5)) == 5
    assert s.ticks_between(utc(2026, 1, 1, 5), utc(2026, 1, 1, 5)) == 0
    assert s.ticks_between(utc(2026, 1, 1, 0), utc(2027, 1, 1, 0), cap=10) == 10


@pytest.mark.parametrize(
    "text,zone",
    [
        ("* * * *", "UTC"),
        ("60 * * * *", "UTC"),
        ("0 0 31 2 *", "Mars/Base"),
        ("0 0 * JAN *", "UTC"),
        ("*/0 * * * *", "UTC"),
        ("5-1 * * * *", "UTC"),
        ("@daily", "UTC"),
        ("0 0 1/x * *", "UTC"),
        ("1-x * * * *", "UTC"),
    ],
)
def test_bad_schedules_are_refused(text, zone):
    with pytest.raises(ScheduleError):
        Schedule.parse(text, zone)


def test_a_schedule_that_never_fires_says_so():
    with pytest.raises(ScheduleError, match="never fires"):
        Schedule.parse("0 0 31 2 *").previous(utc(2026, 3, 1))
