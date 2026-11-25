from datetime import UTC, datetime, timedelta

import psycopg

from job.runs import PostgresRunLog, lock_key


def test_the_lock_keeps_a_second_run_out_until_the_first_ends(db):
    a, b = PostgresRunLog(db), PostgresRunLog(db)
    held = a.lock("reminders")
    assert held is not None and b.lock("reminders") is None and b.lock("other") is not None
    held.release()
    again = b.lock("reminders")
    assert again is not None
    again.release()


def test_history_and_the_last_success(db):
    log = PostgresRunLog(db)
    t = datetime(2026, 11, 1, 6, tzinfo=UTC)
    ok = log.start("reminders", t - timedelta(days=1), t, manual=False)
    log.finish(ok, "succeeded", items=4)
    bad = log.start("reminders", t, t + timedelta(days=1), manual=False)
    log.finish(bad, "failed", error="x" * 900)
    last = log.last_success("reminders")
    assert last is not None and last.window_end == t and log.last_success("nobody") is None
    with psycopg.connect(db) as conn:
        assert conn.execute("select length(error) from job_runs where id = %s", (bad,)).fetchone()[0] == 500
        conn.execute("update job_runs set finished_at = now() - interval '100 days' where id = %s", (ok,))
    assert log.purge("reminders", datetime.now(UTC) - timedelta(days=90)) == 1


def test_lock_keys_are_stable_and_distinct():
    assert lock_key("a") == lock_key("a") != lock_key("b")
