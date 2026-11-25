"""Commands around the transform.

transform-check                        build every model on the database TRANSFORM_DATABASE_URL names
                                       (a local or CI Postgres) and run the data-class check; prints the
                                       report and exits 1 on a problem. The PR check runs it.
transform-admin setup                  the valxel schema (tokenize, generalize_year), the break-glass
                                       table and roles: run once by the platform as the database owner
transform-admin grants --login L --database D
                                       print the grants for the transform's login, from the approved
                                       sources: the platform applies them
transform-admin break-glass --login L --minutes N --reason "..."
                                       write access to the published marts for a person, for at most
                                       N minutes (up to 240); recorded, revoked by the next run after
                                       it ends
transform-dev                          a local run end to end: setup, raw tables seeded from the
                                       contracts, a run, the data-class report
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
from .run import Settings, TransformError, _secret, dbt, dbt_env, transform

MAX_BREAK_GLASS_MINUTES = 240


def _url(settings: Settings) -> str:
    return _secret(settings.database_url, settings.database_url_file, "database URL")


def check_main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(prog="transform-check", description=__doc__).parse_args(argv)
    settings = Settings()
    cfg = load(Path(settings.config_path))
    url = _url(settings)
    env = dbt_env(url, cfg, _secret(settings.tokenize_key, settings.tokenize_key_file, "tokenize key"))
    with tempfile.TemporaryDirectory(prefix="transform-check-") as tmp:
        target = Path(tmp)
        with psycopg.connect(url, autocommit=True) as conn:
            pg.prepare(conn, cfg, [])
        if not dbt(Path(settings.project_dir).resolve(), target, ["compile"], env):
            print("the dbt project doesn't compile", file=sys.stderr)
            return 1
        report = classes.check(json.loads((target / "manifest.json").read_text()))
    print(classes.render(report))
    return 0 if report.ok else 1


def admin_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transform-admin", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup")
    g = sub.add_parser("grants")
    g.add_argument("--login", required=True)
    g.add_argument("--database", required=True)
    b = sub.add_parser("break-glass")
    b.add_argument("--login", required=True)
    b.add_argument("--minutes", type=int, required=True)
    b.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    settings = Settings()
    if args.command == "grants":
        cfg = load(Path(settings.config_path))
        with tempfile.TemporaryDirectory(prefix="transform-grants-") as tmp:
            env = {"TRANSFORM_SOURCE_SCHEMA": cfg.source_schema, "TRANSFORM_BUILD_SCHEMA": cfg.build_schema}
            if not dbt(Path(settings.project_dir).resolve(), Path(tmp), ["parse"], env):
                print("the dbt project doesn't parse", file=sys.stderr)
                return 1
            manifest = json.loads((Path(tmp) / "manifest.json").read_text())
        for stmt in pg.grants(cfg, manifest, args.login, args.database):
            print(f"{stmt};")
        return 0
    with psycopg.connect(_url(settings), autocommit=True) as conn:
        if args.command == "setup":
            pg.setup(conn)
            print("valxel schema, break-glass table and roles in place")
            return 0
        if not 1 <= args.minutes <= MAX_BREAK_GLASS_MINUTES or len(args.reason.strip()) < 10:
            print(f"break-glass: 1-{MAX_BREAK_GLASS_MINUTES} minutes and a reason of a few words", file=sys.stderr)
            return 2
        until = datetime.now(UTC) + timedelta(minutes=args.minutes)
        pg.break_glass_grant(conn, args.login, until, args.reason.strip())
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
        pg.setup(conn)
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
