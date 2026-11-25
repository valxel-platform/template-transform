"""The job's work: one transform run (transform/run.py) over the run's window.

The job harness around it is template-job's, unchanged: the run lock, the window and catch-up after
missed runs (job.yaml), the time limit (`run.check()` between steps), run history, and the
job-missed and job-failed alerts. A run that fails publishes nothing (readers keep the last good
marts) and is recorded as failed; the next run builds again from the raw tables.

After a run, the job's own run history is kept to JOB_KEEP_RUNS_DAYS days.

Returns the rows in the published models, which the harness logs and keeps in the run's record.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from .context import Run

SEED_ENVIRONMENTS = ("ci", "dev")


def run(run: Run) -> int:
    from transform.run import Settings, transform

    settings = Settings()
    if settings.seed_synthetic:
        _seed(settings, run.settings.environment)

    def audit(action: str, *, resource: tuple[str, list[str]], details: dict[str, Any]) -> None:
        run.audit(action, resource=resource, details=details)

    summary = transform(
        settings, window=(run.window_start, run.window_end), dry_run=run.dry_run, audit=audit, check=run.check
    )
    if not run.dry_run:
        run.runlog.purge(run.job, run.window_end - timedelta(days=run.settings.keep_runs_days))
    return sum((summary.get("rows") or {}).values())


def _seed(settings: Any, environment: str) -> None:
    """CI and local runs only: the warehouse set up and synthetic raw tables (transform/seed.py)."""
    from pathlib import Path

    import psycopg

    from transform import pg, seed
    from transform.config import load
    from transform.run import TransformError, _secret

    if environment not in SEED_ENVIRONMENTS:
        raise TransformError(f"TRANSFORM_SEED_SYNTHETIC replaces raw tables: refused in {environment}")
    cfg = load(Path(settings.config_path))
    url = _secret(settings.database_url, settings.database_url_file, "database URL")
    with psycopg.connect(url, autocommit=True) as conn:
        pg.setup(conn)
        seed.seed(conn, Path(settings.project_dir), Path("contracts"), cfg.source_schema)
