"""Run history and the run lock, in the job's own Postgres (or in memory without one).

Every run records when it started, the period it covered (window_start, window_end), how it ended
and how many items it handled. The next run reads the last success to know where to start and
whether runs were missed. The lock is a Postgres advisory lock held on its own connection for the
whole run: a run started by hand while the scheduled one is going (or a second CronJob pod after a
node failure) finds it taken and stops without doing anything.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class LastRun:
    started_at: datetime
    window_end: datetime


class Lock(Protocol):
    def release(self) -> None: ...


class RunLog(Protocol):
    def lock(self, job: str) -> Lock | None: ...
    def last_success(self, job: str) -> LastRun | None: ...
    def start(self, job: str, window_start: datetime, window_end: datetime, *, manual: bool) -> int: ...
    def finish(self, run_id: int, status: str, items: int = 0, error: str = "") -> None: ...
    def purge(self, job: str, before: datetime) -> int: ...


def lock_key(job: str) -> int:
    """A job's advisory lock: 64 bits of SHA-256, so two jobs never share one."""
    return int.from_bytes(hashlib.sha256(f"valxel-job\x00{job}".encode()).digest()[:8], "big", signed=True)


def now() -> datetime:
    return datetime.now(UTC)


class MemoryRunLog:
    """For tests, and a job run without a database: no history survives the process."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self._held: set[str] = set()
        self._next = 0
        self._guard = threading.Lock()

    def lock(self, job: str) -> Lock | None:
        with self._guard:
            if job in self._held:
                return None
            self._held.add(job)
        log = self

        class _Held:
            def release(self) -> None:
                with log._guard:
                    log._held.discard(job)

        return _Held()

    def last_success(self, job: str) -> LastRun | None:
        ok = [r for r in self.rows if r["job"] == job and r["status"] == "succeeded"]
        if not ok:
            return None
        last = max(ok, key=lambda r: r["window_end"])
        return LastRun(started_at=last["started_at"], window_end=last["window_end"])

    def start(self, job: str, window_start: datetime, window_end: datetime, *, manual: bool) -> int:
        self._next += 1
        self.rows.append(
            {
                "id": self._next,
                "job": job,
                "started_at": now(),
                "finished_at": None,
                "window_start": window_start,
                "window_end": window_end,
                "status": "running",
                "items": 0,
                "error": "",
                "manual": manual,
            }
        )
        return self._next

    def finish(self, run_id: int, status: str, items: int = 0, error: str = "") -> None:
        row = next(r for r in self.rows if r["id"] == run_id)
        row.update(status=status, items=items, error=error, finished_at=now())

    def purge(self, job: str, before: datetime) -> int:
        keep = [r for r in self.rows if r["job"] != job or (r["finished_at"] or now()) >= before]
        gone = len(self.rows) - len(keep)
        self.rows[:] = keep
        return gone


SCHEMA = """
create table if not exists job_runs (
  id bigserial primary key,
  job text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  window_start timestamptz not null,
  window_end timestamptz not null,
  status text not null check (status in ('running', 'succeeded', 'failed', 'stopped')),
  items bigint not null default 0,
  error text not null default '',
  manual boolean not null default false
);
create index if not exists job_runs_job_status on job_runs (job, status, window_end desc);
"""


class PostgresRunLog:
    def __init__(self, dsn: str, connect: Any = None) -> None:
        import psycopg

        self.dsn = dsn
        self.connect = connect or (lambda: psycopg.connect(dsn, autocommit=True, connect_timeout=10))
        with self.connect() as conn:
            conn.execute(SCHEMA)

    def lock(self, job: str) -> Lock | None:
        conn = self.connect()
        try:
            row = conn.execute("select pg_try_advisory_lock(%s::bigint)", (lock_key(job),)).fetchone()
        except Exception:
            conn.close()
            raise
        if not row or not row[0]:
            conn.close()
            return None

        class _Held:
            def release(self) -> None:
                conn.close()  # ending the session drops its advisory locks

        return _Held()

    def last_success(self, job: str) -> LastRun | None:
        with self.connect() as conn:
            row = conn.execute(
                "select started_at, window_end from job_runs where job = %s and status = 'succeeded'"
                " order by window_end desc limit 1",
                (job,),
            ).fetchone()
        # Back in UTC: a local time in the fall-back hour doesn't compare equal to its UTC instant.
        return LastRun(started_at=row[0].astimezone(UTC), window_end=row[1].astimezone(UTC)) if row else None

    def start(self, job: str, window_start: datetime, window_end: datetime, *, manual: bool) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "insert into job_runs (job, window_start, window_end, status, manual)"
                " values (%s, %s, %s, 'running', %s) returning id",
                (job, window_start, window_end, manual),
            ).fetchone()
        assert row is not None
        return int(row[0])

    def finish(self, run_id: int, status: str, items: int = 0, error: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                "update job_runs set status = %s, items = %s, error = %s, finished_at = now() where id = %s",
                (status, items, error[:500], run_id),
            )

    def purge(self, job: str, before: datetime) -> int:
        with self.connect() as conn:
            cur = conn.execute("delete from job_runs where job = %s and finished_at < %s", (job, before))
            return int(cur.rowcount or 0)
