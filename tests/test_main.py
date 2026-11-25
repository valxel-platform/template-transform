import json
import logging
import os
import signal
from datetime import UTC, datetime, timedelta

import pytest

from job import main as m
from job import work
from job.config import parse
from job.context import Stopped
from job.runs import LastRun, MemoryRunLog
from job.settings import Settings

CFG = parse('name: reminders\nschedule: "0 6 * * *"\ntimezone: UTC\n')


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=UTC)


def test_the_first_run_covers_one_period_and_later_ones_start_at_the_last_success():
    at = utc(2026, 11, 16, 6, 3)
    assert m.plan(CFG, None, at) == (utc(2026, 11, 15, 6), utc(2026, 11, 16, 6), 0)
    last = LastRun(started_at=utc(2026, 11, 15, 6, 1), window_end=utc(2026, 11, 15, 6))
    assert m.plan(CFG, last, at) == (utc(2026, 11, 15, 6), utc(2026, 11, 16, 6), 0)


def test_after_an_outage_the_window_reaches_back_and_the_missed_runs_are_counted():
    last = LastRun(started_at=utc(2026, 11, 12, 6, 1), window_end=utc(2026, 11, 12, 6))
    assert m.plan(CFG, last, utc(2026, 11, 16, 7)) == (utc(2026, 11, 12, 6), utc(2026, 11, 16, 6), 3)
    latest = parse('name: r\nschedule: "0 6 * * *"\ncatch_up: latest\n')
    assert m.plan(latest, last, utc(2026, 11, 16, 7)) == (utc(2026, 11, 15, 6), utc(2026, 11, 16, 6), 3)


def run(monkeypatch, log=None, **kw):
    monkeypatch.setattr(m, "now", lambda: utc(2026, 11, 16, 6, 1))
    log = log or MemoryRunLog()
    return m.execute(CFG, Settings(), log, **kw), log


def test_a_run_records_its_window_and_outcome(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    code, log = run(monkeypatch)
    assert code == 0 and log.rows[0]["status"] == "succeeded" and log.rows[0]["window_end"] == utc(2026, 11, 16, 6)
    assert "valxel.job finished" in caplog.text


def test_missed_runs_are_logged_for_the_alert(monkeypatch, caplog):
    log = MemoryRunLog()
    log._next = 1
    log.rows.append(
        {
            "id": 1,
            "job": "reminders",
            "started_at": utc(2026, 11, 10, 6),
            "finished_at": utc(2026, 11, 10, 6),
            "window_start": utc(2026, 11, 9, 6),
            "window_end": utc(2026, 11, 10, 6),
            "status": "succeeded",
            "items": 0,
            "error": "",
            "manual": False,
        }
    )
    code, _ = run(monkeypatch, log)
    assert code == 0 and "valxel.job.missed: 5 scheduled runs" in caplog.text
    assert log.rows[-1]["window_start"] == utc(2026, 11, 10, 6)


def test_a_second_run_while_one_holds_the_lock_does_nothing(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    log = MemoryRunLog()
    held = log.lock("reminders")
    code, _ = run(monkeypatch, log)
    assert code == 0 and log.rows == [] and "valxel.job skipped" in caplog.text
    held.release()


def test_a_failing_run_exits_1_and_logs_the_exception(monkeypatch, caplog):
    def boom(_run):
        raise RuntimeError("the API said no")

    monkeypatch.setattr(work, "run", boom)
    code, log = run(monkeypatch)
    assert code == 1 and log.rows[0]["status"] == "failed" and "the API said no" in log.rows[0]["error"]
    assert "valxel.job failed" in caplog.text and caplog.records[-1].exc_info
    assert log.lock("reminders") is not None  # the lock was let go


def test_the_platform_asking_to_stop_ends_the_run_cleanly(monkeypatch, caplog):
    def slow(run):
        os.kill(os.getpid(), signal.SIGTERM)  # Kubernetes stopping the pod
        run.check()
        return 1

    monkeypatch.setattr(work, "run", slow)
    code, log = run(monkeypatch)
    assert code == 0 and log.rows[0]["status"] == "stopped" and "valxel.job stopped" in caplog.text
    assert log.last_success("reminders") is None  # so the next run covers this window again


def test_the_time_limit_stops_a_run(monkeypatch):
    def forever(run):
        run.deadline = 0  # as if the limit has passed
        run.check()
        return 1

    monkeypatch.setattr(work, "run", forever)
    code, log = run(monkeypatch)
    assert code == 0 and log.rows[0]["status"] == "stopped" and "time limit" in log.rows[0]["error"]


def test_a_dry_run_changes_nothing_and_isnt_a_success(monkeypatch):
    log = MemoryRunLog()
    log.start("reminders", utc(2020, 1, 1), utc(2020, 1, 2), manual=False)
    log.finish(1, "succeeded")
    log.rows[0]["finished_at"] = utc(2020, 1, 2)
    code, _ = run(monkeypatch, log, dry_run=True)
    assert code == 0 and len(log.rows) == 2 and log.rows[1]["status"] == "stopped"


def test_a_backfill_window_is_recorded_as_manual(monkeypatch):
    code, log = run(monkeypatch, window=(utc(2026, 10, 1), utc(2026, 10, 2)))
    assert code == 0 and log.rows[0]["manual"] and log.rows[0]["window_start"] == utc(2026, 10, 1)


def test_the_command_line(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "job.yaml").write_text('name: r\nschedule: "0 6 * * *"\n')
    assert m.main(["--check"]) == 0 and "'0 6 * * *'" in capsys.readouterr().out
    assert m.main(["--from", "2026-11-02T00:00:00Z"]) == 2
    assert m.main(["--dry-run"]) == 0
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["service.name"] == "r" and line["outcome"] == "dry_run" and line["dry_run"] is True
    assert m.main(["--from", "2026-11-01T00:00:00Z", "--to", "2026-11-02T00:00:00Z"]) == 0
    (tmp_path / "job.yaml").write_text("name: r\n")
    assert m.main([]) == 2
    with pytest.raises(SystemExit):
        m.main(["--from", "2026-11-01"])  # no time zone


def test_stopped_is_not_an_exception_people_see_as_an_error():
    assert not issubclass(Stopped, RuntimeError)
    assert timedelta(seconds=m.MARGIN_S) < timedelta(minutes=1)
