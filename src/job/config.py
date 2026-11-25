"""job.yaml: when the job runs, how long a run may take, and what a run covers after a gap.

The Valxel SDLC reads the same file when it deploys: it sets the CronJob's schedule, time zone and
time limit for each environment from it, so a schedule changes through a pull request like code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .schedule import Schedule, ScheduleError

CATCH_UP = ("window", "latest")
ENVIRONMENTS = ("dev", "staging", "production")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class EnvOverride:
    schedule: str = ""
    suspend: bool = False


@dataclass(frozen=True)
class JobConfig:
    name: str
    schedule: Schedule
    time_limit_minutes: int = 30
    # window: a run covers everything since the last successful run, so runs missed during an outage
    #         are made up by the next one. latest: a run covers only its own period.
    catch_up: str = "window"
    # Log valxel.job.missed when this many scheduled runs passed without a success.
    missed_after: int = 2
    environments: dict[str, EnvOverride] = field(default_factory=dict)


def parse(text: str) -> JobConfig:
    try:
        raw = yaml.safe_load(text or "") or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"job.yaml isn't YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("job.yaml must be a mapping")
    name = str(raw.get("name") or "").strip()
    if not name:
        raise ConfigError("job.yaml: name is required")
    try:
        schedule = Schedule.parse(str(raw.get("schedule") or ""), str(raw.get("timezone") or "UTC"))
    except ScheduleError as exc:
        raise ConfigError(f"job.yaml schedule: {exc}") from exc
    limit = raw.get("time_limit_minutes", 30)
    if not isinstance(limit, int) or not 1 <= limit <= 24 * 60:
        raise ConfigError("job.yaml: time_limit_minutes must be a whole number from 1 to 1440")
    catch_up = str(raw.get("catch_up") or "window")
    if catch_up not in CATCH_UP:
        raise ConfigError(f"job.yaml: catch_up must be one of {', '.join(CATCH_UP)}")
    missed = raw.get("missed_after", 2)
    if not isinstance(missed, int) or missed < 1:
        raise ConfigError("job.yaml: missed_after must be a whole number, 1 or more")
    envs: dict[str, EnvOverride] = {}
    for env, o in (raw.get("environments") or {}).items():
        if env not in ENVIRONMENTS:
            raise ConfigError(f"job.yaml environments: '{env}' isn't one of {', '.join(ENVIRONMENTS)}")
        o = o or {}
        if o.get("schedule"):
            try:
                Schedule.parse(str(o["schedule"]), str(raw.get("timezone") or "UTC"))
            except ScheduleError as exc:
                raise ConfigError(f"job.yaml environments.{env}.schedule: {exc}") from exc
        envs[env] = EnvOverride(schedule=str(o.get("schedule") or ""), suspend=bool(o.get("suspend", False)))
    return JobConfig(
        name=name,
        schedule=schedule,
        time_limit_minutes=limit,
        catch_up=catch_up,
        missed_after=missed,
        environments=envs,
    )


def load(path: str = "job.yaml") -> JobConfig:
    return parse(Path(path).read_text(encoding="utf-8"))


def for_environment(cfg: JobConfig, env: str) -> JobConfig:
    """The config a run in `env` uses: its own schedule there, when job.yaml gives one."""
    o = cfg.environments.get(env)
    if not o or not o.schedule:
        return cfg
    return JobConfig(
        name=cfg.name,
        schedule=Schedule.parse(o.schedule, cfg.schedule.zone.key),
        time_limit_minutes=cfg.time_limit_minutes,
        catch_up=cfg.catch_up,
        missed_after=cfg.missed_after,
        environments=cfg.environments,
    )
