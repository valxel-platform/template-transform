"""Commands around the transform.

transform-check                        build every model on the database TRANSFORM_DATABASE_URL names
                                       (a local or CI Postgres) and run the data-class check; prints the
                                       report and exits 1 on a problem. The PR check runs it.
transform-admin break-glass --login L --minutes N --reason "..."
                                       write access to the published marts for a person, for at most
                                       N minutes (up to 240); recorded, revoked by the next run after
                                       it ends. Runs as the transform's login, which administers
                                       <login>_break_glass.
transform-dev                          a local run end to end: the roles environments have, raw tables
                                       seeded from the contracts, a run, the data-class report

The transform's login and what it may read are the platform's (platform-resources postgres-login,
made when the transform deploys, from the contracts it reads); the login makes its own schemas.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg

from . import classes, pg, seed
from .config import load
from .run import Settings, TransformError, _secret, database_url, dbt, dbt_env, transform

MAX_BREAK_GLASS_MINUTES = 240


def _url(settings: Settings) -> str:
    return database_url(settings)


def check_main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(prog="transform-check", description=__doc__).parse_args(argv)
    settings = Settings()
    cfg = load(Path(settings.config_path))
    url = _url(settings)
    key = (
        "" if settings.tokenize_function else _secret(settings.tokenize_key, settings.tokenize_key_file, "tokenize key")
    )
    env = dbt_env(url, cfg, key)
    with tempfile.TemporaryDirectory(prefix="transform-check-") as tmp:
        target = Path(tmp)
        with psycopg.connect(url, autocommit=True) as conn:
            pg.helpers(conn, cfg)
            pg.prepare(conn, cfg, [])
        if not dbt(Path(settings.project_dir).resolve(), target, ["compile"], env):
            print("the dbt project doesn't compile", file=sys.stderr)
            return 1
        report = classes.check(
            json.loads((target / "manifest.json").read_text()),
            cfg.helper_schema,
            tokens_deidentify=bool(cfg.expert_determination),
        )
    print(classes.render(report))
    return 0 if report.ok else 1


def admin_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transform-admin", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("break-glass")
    b.add_argument("--login", required=True)
    b.add_argument("--minutes", type=int, required=True)
    b.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    settings = Settings()
    cfg = load(Path(settings.config_path))
    with psycopg.connect(_url(settings), autocommit=True) as conn:
        if not 1 <= args.minutes <= MAX_BREAK_GLASS_MINUTES or len(args.reason.strip()) < 10:
            print(f"break-glass: 1-{MAX_BREAK_GLASS_MINUTES} minutes and a reason of a few words", file=sys.stderr)
            return 2
        until = datetime.now(UTC) + timedelta(minutes=args.minutes)
        pg.break_glass_grant(conn, cfg, args.login, until, args.reason.strip(), settings.break_glass_role)
        print(f"{args.login} may write the published marts until {until.isoformat()}; recorded")
    return 0


def dev_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transform-dev", description=__doc__)
    parser.add_argument("--rows", type=int, default=60, help="synthetic rows per raw table")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    settings = Settings()
    cfg = load(Path(settings.config_path))
    url = _url(settings)
    with psycopg.connect(url, autocommit=True) as conn:
        pg.dev_setup(conn)
        seeded = seed.seed(conn, Path(settings.project_dir), Path("contracts"), cfg.source_schema, rows=args.rows)
    print(f"seeded {', '.join(f'{t} ({n} rows)' for t, n in seeded.items())}")
    end = datetime.now(UTC)
    try:
        summary = transform(settings, window=(end - timedelta(days=1), end))
    except TransformError as e:
        print(f"the run failed, nothing published: {e}", file=sys.stderr)
        return 1
    print(json.dumps({k: summary[k] for k in ("published", "rows", "tests", "classes")}, indent=2, default=str))
    return 0
