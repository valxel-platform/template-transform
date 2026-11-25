"""`job-run`: one run of the job, start to finish, then exit.

1. Take the run lock; if another run holds it, log and exit 0.
2. Work out the window: from the last success (catch_up: window) or the previous scheduled time
   (catch_up: latest), to this run's scheduled time. Log valxel.job.missed when job.yaml's
   missed_after scheduled runs passed with no success.
3. Run work.run with the time limit and stop request it checks, and record how it ended:
     succeeded  log "valxel.job finished", exit 0
     stopped    log "valxel.job stopped" (warning), exit 0: the next run covers the window again
     failed     log "valxel.job failed" with the exception, exit 1

job-run --check                     check job.yaml and exit
job-run --dry-run                   count what would change, change nothing
job-run --from <ISO> --to <ISO>     run once over a window you choose (backfill), recorded as manual
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from types import FrameType

from opentelemetry.trace import Status, StatusCode

from . import cohorts, errors, events, flags, logs, telemetry, work
from .audit import Auditor, make_sink
from .component import COMPONENT
from .config import ConfigError, JobConfig, for_environment, load
from .context import Run, Stopped
from .runs import LastRun, MemoryRunLog, PostgresRunLog, RunLog, now
from .settings import Settings

log = logging.getLogger("job")
# A run stops this long before its time limit, so it records how it ended before Kubernetes kills it.
MARGIN_S = 30


def plan(cfg: JobConfig, last: LastRun | None, at: datetime) -> tuple[datetime, datetime, int]:
    """(window_start, window_end, scheduled runs missed) for a run at `at`."""
    this_tick = cfg.schedule.previous(at)
    prior_tick = cfg.schedule.previous(this_tick - timedelta(minutes=1))
    if last is None:
        return prior_tick, this_tick, 0
    missed = max(0, cfg.schedule.ticks_between(last.window_end, this_tick) - 1)
    if cfg.catch_up == "latest":
        return max(prior_tick, min(last.window_end, this_tick)), this_tick, missed
    return min(last.window_end, this_tick), this_tick, missed


def _parse_time(text: str) -> datetime:
    t = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if t.tzinfo is None:
        raise argparse.ArgumentTypeError(f"'{text}' needs a time zone (e.g. 2026-11-01T00:00:00Z)")
    return t


def _runlog(settings: Settings) -> RunLog:
    if settings.database_url:
        return PostgresRunLog(settings.database_url)
    log.warning("no JOB_DATABASE_URL: runs keep no history, so missed and overlapping runs can't be detected")
    return MemoryRunLog()


def _auditor(settings: Settings) -> Auditor:
    """The audit trail: required (a run that can't publish a record fails) for payments and PHI."""
    required = settings.audit_required
    if required is None:
        required = settings.data_class in ("payments", "phi")
    sink = make_sink(
        settings.audit_sink,
        bootstrap=settings.audit_kafka_bootstrap,
        topic=settings.audit_topic,
        config=dict(settings.audit_kafka_config),
        data_class=settings.data_class,
    )
    return Auditor(
        sink,
        service=COMPONENT,
        product=settings.product,
        environment=settings.environment,
        data_class=settings.data_class,
        required=required,
    )


def execute(
    cfg: JobConfig,
    settings: Settings,
    runlog: RunLog,
    *,
    dry_run: bool = False,
    window: tuple[datetime, datetime] | None = None,
    auditor: Auditor | None = None,
) -> int:
    """One run; returns the process exit code."""
    lock = runlog.lock(cfg.name)
    if lock is None:
        log.info(
            "valxel.job skipped: another run of %s is in progress",
            cfg.name,
            extra={"fields": {"job": cfg.name, "outcome": "skipped"}},
        )
        return 0
    try:
        at = now()
        if window:
            start, end, missed = window[0], window[1], 0
        else:
            start, end, missed = plan(cfg, runlog.last_success(cfg.name), at)
        if missed >= cfg.missed_after:
            log.warning(
                "valxel.job.missed: %d scheduled runs of %s passed without a success; this run covers from %s",
                missed,
                cfg.name,
                start.isoformat(),
                extra={"fields": {"job": cfg.name, "missed": missed}},
            )
        limit = cfg.time_limit_minutes * 60
        run = Run(
            job=cfg.name,
            window_start=start,
            window_end=end,
            settings=settings,
            runlog=runlog,
            deadline=time.monotonic() + max(limit - MARGIN_S, limit // 2),
            dry_run=dry_run,
            manual=window is not None,
            auditor=auditor or _auditor(settings),
        )

        def stop(_sig: int, _frame: FrameType | None) -> None:
            run.stop_requested = True

        previous = signal.signal(signal.SIGTERM, stop)
        run_id = runlog.start(cfg.name, start, end, manual=run.manual)
        began = time.monotonic()
        fields: dict[str, str | bool] = {
            "job": cfg.name,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "dry_run": dry_run,
            "manual": run.manual,
        }
        # The run is one trace: its log lines carry the trace id, and calls it makes carry the trace.
        with telemetry.tracer().start_as_current_span(
            "job.run", attributes={f"job.{k}": v for k, v in fields.items()}
        ) as span:
            try:
                items = work.run(run)
            except Stopped as exc:
                runlog.finish(run_id, "stopped", error=str(exc))
                span.set_attribute("job.outcome", "stopped")
                log.warning(
                    "valxel.job stopped: %s; the next run covers this window again",
                    exc,
                    extra={"fields": {**fields, "outcome": "stopped"}},
                )
                return 0
            except Exception as exc:
                runlog.finish(run_id, "failed", error=f"{type(exc).__name__}: {exc}")
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
                span.set_attribute("job.outcome", "failed")
                log.exception("valxel.job failed", extra={"fields": {**fields, "outcome": "failed"}})
                return 1
            finally:
                signal.signal(signal.SIGTERM, previous)
                events.report()  # the run's product events and flag cohorts, whatever its outcome
                cohorts.report()
            # A dry run changes nothing, so it isn't a success the next run may start from.
            runlog.finish(
                run_id, "stopped" if dry_run else "succeeded", items=items, error="dry run" if dry_run else ""
            )
            outcome = "dry_run" if dry_run else "succeeded"
            span.set_attributes({"job.outcome": outcome, "job.items": items})
            log.info(
                "valxel.job finished: %d items in %.1fs",
                items,
                time.monotonic() - began,
                extra={"fields": {**fields, "outcome": outcome, "items": items}},
            )
            return 0
    finally:
        lock.release()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="job-run", description="Run the job once.")
    p.add_argument("--check", action="store_true", help="check job.yaml and exit")
    p.add_argument("--dry-run", action="store_true", help="count what would change, change nothing")
    p.add_argument("--from", dest="start", type=_parse_time, help="window start (ISO time with zone)")
    p.add_argument("--to", dest="end", type=_parse_time, help="window end (ISO time with zone)")
    args = p.parse_args(argv)
    settings = Settings()
    try:
        cfg = for_environment(load(settings.config_path), settings.environment)
    except (ConfigError, OSError) as exc:
        print(f"job.yaml: {exc}", file=sys.stderr)
        return 2
    if args.check:
        print(
            f"{cfg.name}: '{cfg.schedule.text}' ({cfg.schedule.zone.key}) in {settings.environment}; "
            f"time limit {cfg.time_limit_minutes} min; catch_up {cfg.catch_up}"
        )
        return 0
    if (args.start is None) != (args.end is None) or (args.start and args.end and args.start >= args.end):
        print("--from and --to go together, with --from before --to", file=sys.stderr)
        return 2
    logs.setup(cfg.name, settings.log_level)
    telemetry.setup(cfg.name)
    errors.start_errors()
    flags.configure(settings)
    window = (args.start, args.end) if args.start and args.end else None
    try:
        return execute(cfg, settings, _runlog(settings), dry_run=args.dry_run, window=window)
    finally:  # the process ends with the run: send what's queued
        telemetry.shutdown()
        errors.flush()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
