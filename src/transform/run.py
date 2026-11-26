"""One transform run, called by the job harness (job/work.py) inside its run lock, window and time limit.

1. Revoke break-glass grants past their time (audited).
2. A fresh build schema, with each incremental mart's live table copied in (pg.prepare).
3. `dbt build`: models and tests together, in dependency order, into the build schema. Every test
   is blocking, control totals included (dbt/macros/valxel.sql).
4. The data-class check on what was built (classes.py): CI runs it on every PR; here it's the
   last line, so a model that slipped past can't publish.
5. Retention in the build schema (transform.yaml marts).
6. Publish: the build schema swapped in, in one transaction, with readers' grants and tenant row
   security (pg.publish). A failure anywhere before this leaves what readers see untouched; the run
   fails, and the job harness records it and alerts (job-failed).
7. Evidence: dbt's manifest.json and run_results.json, the data-class report and a summary (code
   version, window, rows per model, tests, control totals, retention) under TRANSFORM_EVIDENCE_DIR,
   and the summary as a `valxel.transform.run` line the tool reads. An audit record names what was
   published (ids and counts only).

A dry run builds and checks but publishes nothing.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import tempfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import classes, pg
from .config import Config, load

log = logging.getLogger("transform")
SUMMARY = "valxel.transform.run"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TRANSFORM_")

    config_path: str = "transform.yaml"
    project_dir: str = "dbt"
    # The warehouse connection, as the transform's own login (platform-resources postgres-login). In
    # environments the login's outputs reach the pod as TRANSFORM_PG_HOST, _PORT, _DATABASE, _USER
    # and _PASSWORD (workload.yaml binds them; the password from its Secret). Otherwise a URL: the
    # variable for local runs (transform-dev), or a mounted file.
    pg_host: str = ""
    pg_port: int = 5432
    pg_database: str = ""
    pg_user: str = ""
    pg_password: str = ""
    pg_sslmode: str = "require"
    database_url: str = ""
    database_url_file: str = "/var/run/secrets/valxel/transform-database-url"
    # The tokenize key (at least 32 characters): the login's own, bound from its Secret as
    # TRANSFORM_TOKENIZE_KEY, or a mounted file.
    tokenize_key: str = ""
    tokenize_key_file: str = "/var/run/secrets/valxel/transform-tokenize-key"
    evidence_dir: str = ""  # where each run's evidence is kept (a mounted volume); unset: not kept
    version: str = ""  # the deployed commit, set by the Valxel SDLC
    # CI and local only: set up the warehouse and fill the raw tables with synthetic rows before the
    # run (seed.py). Refused unless JOB_ENVIRONMENT is ci or dev: it replaces raw tables.
    seed_synthetic: bool = False


class TransformError(RuntimeError):
    """The build, a test, a control total or the data-class check failed: nothing was published."""


def _secret(value: str, path: str, what: str) -> str:
    if value:
        return value
    try:
        return Path(path).read_text().strip()
    except OSError as e:
        raise TransformError(f"no {what}: mount it at {path}") from e


def database_url(settings: Settings) -> str:
    """The transform's own login: the bound parts when there are any, else a URL."""
    if settings.pg_host:
        if not (settings.pg_user and settings.pg_password and settings.pg_database):
            raise TransformError("TRANSFORM_PG_HOST is set: TRANSFORM_PG_USER, _PASSWORD and _DATABASE are needed too")
        return make_conninfo(
            host=settings.pg_host,
            port=settings.pg_port,
            dbname=settings.pg_database,
            user=settings.pg_user,
            password=settings.pg_password,
            sslmode=settings.pg_sslmode,
        )
    return _secret(settings.database_url, settings.database_url_file, "database URL")


def dbt_env(url: str, cfg: Config, key: str) -> dict[str, str]:
    """What dbt's profile reads (dbt/profiles/profiles.yml). The password and the key reach dbt only
    through DBT_ENV_SECRET_ and libpq's PGOPTIONS, which dbt keeps out of its logs and artifacts."""
    info = conninfo_to_dict(url)
    if len(key) < pg.MIN_KEY:
        raise TransformError(f"the tokenize key must be at least {pg.MIN_KEY} characters")
    return {
        "TRANSFORM_WAREHOUSE": cfg.warehouse,
        "TRANSFORM_PG_HOST": str(info.get("host") or "localhost"),
        "TRANSFORM_PG_PORT": str(info.get("port") or "5432"),
        "TRANSFORM_PG_USER": str(info.get("user") or ""),
        "TRANSFORM_PG_DATABASE": str(info.get("dbname") or ""),
        "TRANSFORM_PG_SSLMODE": str(info.get("sslmode") or "prefer"),
        "DBT_ENV_SECRET_PG_PASSWORD": str(info.get("password") or ""),
        "TRANSFORM_BUILD_SCHEMA": cfg.build_schema,
        "TRANSFORM_SOURCE_SCHEMA": cfg.source_schema,
        "TRANSFORM_HELPER_SCHEMA": cfg.helper_schema,
        "PGOPTIONS": f"-c valxel.tokenize_key={key}",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",  # nothing leaves for dbt Labs; nothing written beside the profile
    }


@contextlib.contextmanager
def _environ(env: dict[str, str]) -> Iterator[None]:
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def dbt(project: Path, target: Path, args: list[str], env: dict[str, str]) -> bool:
    """Run a dbt command in-process; True when it succeeded."""
    from dbt.cli.main import dbtRunner

    with _environ(env):
        result = dbtRunner().invoke(
            [
                *args,
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(project / "profiles"),
                "--target-path",
                str(target),
                "--log-path",
                str(target / "logs"),
            ]
        )
    return bool(result.success)


def _results(path: Path) -> dict[str, Any]:
    """Tests and control totals from run_results.json: counts and the names that failed."""
    data = json.loads(path.read_text()) if path.exists() else {"results": []}
    out: dict[str, Any] = {"passed": 0, "failed": [], "control_totals": [], "errors": []}
    for r in data.get("results", []):
        uid, status = str(r.get("unique_id", "")), str(r.get("status", ""))
        if uid.startswith("test."):
            if status == "pass":
                out["passed"] += 1
            else:
                out["failed"].append(uid)
            if ".valxel_control_total_" in uid:
                out["control_totals"].append({"test": uid, "status": status, "failures": r.get("failures")})
        elif status not in ("success", "pass"):
            out["errors"].append(uid)
    return out


def transform(
    settings: Settings,
    *,
    window: tuple[datetime, datetime],
    dry_run: bool = False,
    audit: Any = None,
    check: Any = None,
) -> dict[str, Any]:
    """One run; returns the summary (also logged and kept as evidence). Raises TransformError."""
    cfg = load(Path(settings.config_path))
    url = database_url(settings)
    key = _secret(settings.tokenize_key, settings.tokenize_key_file, "tokenize key")
    env = dbt_env(url, cfg, key)
    project = Path(settings.project_dir).resolve()
    stop = check or (lambda: None)
    summary: dict[str, Any] = {
        "component": cfg.name,
        "version": settings.version,
        "warehouse": cfg.warehouse,
        "window": [window[0].isoformat(), window[1].isoformat()],
        "dry_run": dry_run,
        "published": False,
    }
    with tempfile.TemporaryDirectory(prefix="transform-") as tmp, psycopg.connect(url, autocommit=True) as conn:
        target = Path(tmp) / "target"
        pg.helpers(conn, cfg)
        break_glass = pg.break_glass_role(pg.login(conn))
        for login in pg.break_glass_expire(conn, cfg):
            log.warning("break-glass access for %s expired and was revoked", login)
            if audit:
                audit("transform.break_glass.revoked", resource=("db_login", [login]), details={})
        if not dbt(project, target, ["parse"], env):
            raise TransformError("the dbt project doesn't parse")
        manifest = json.loads((target / "manifest.json").read_text())
        incremental = [m.name for m in pg.models(manifest) if m.materialized == "incremental"]
        summary["copied"] = pg.prepare(conn, cfg, incremental)
        stop()
        vars_ = json.dumps({"window_start": summary["window"][0], "window_end": summary["window"][1]})
        built_ok = dbt(project, target, ["build", "--vars", vars_], env)
        manifest = json.loads((target / "manifest.json").read_text())
        summary["tests"] = _results(target / "run_results.json")
        report = classes.check(manifest, cfg.helper_schema)
        summary["classes"] = {"ok": report.ok, "problems": report.problems}
        (target / "classes.json").write_text(json.dumps(report.as_dict(), indent=2))
        built = pg.models(manifest)
        try:
            if not built_ok or summary["tests"]["failed"] or summary["tests"]["errors"]:
                raise TransformError(
                    "dbt build failed: " + ", ".join(summary["tests"]["errors"] + summary["tests"]["failed"])
                )
            if not report.ok:
                raise TransformError("data-class check: " + "; ".join(report.problems))
            stop()
            summary["expired"] = pg.expire(conn, cfg, [m.name for m in built])
            summary["rows"] = pg.counts(conn, cfg.build_schema, built)
            if not dry_run:
                pg.publish(conn, cfg, built, break_glass)
                summary["published"] = True
                if audit:
                    audit(
                        "transform.published",
                        resource=("transform_model", [m.name for m in built]),
                        details={"rows": summary["rows"], "tests_passed": summary["tests"]["passed"]},
                    )
        finally:
            _evidence(settings, target, summary)
    return summary


def _evidence(settings: Settings, target: Path, summary: dict[str, Any]) -> None:
    line = {k: summary.get(k) for k in ("component", "version", "window", "published", "rows", "expired")}
    tests = summary.get("tests") or {}
    line["tests"] = {"passed": tests.get("passed", 0), "failed": len(tests.get("failed", []))}
    line["control_totals"] = len(tests.get("control_totals", []))
    line["classes_ok"] = (summary.get("classes") or {}).get("ok")
    log.info("%s %s", SUMMARY, json.dumps(line, separators=(",", ":"), default=str))
    if not settings.evidence_dir:
        return
    stamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    out = Path(settings.evidence_dir) / f"{stamp}-{settings.version or 'local'}"
    out.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "run_results.json", "classes.json"):
        if (target / name).exists():
            shutil.copy2(target / name, out / name)
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
