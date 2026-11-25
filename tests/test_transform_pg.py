"""The transform end to end on a real Postgres: set TRANSFORM_TEST_DATABASE_URL (a database these
tests may reset; the login must be able to create roles). CI runs them against its Postgres service.

The deletion test is generic: for every source with a key, it deletes one row from the raw table,
runs again and proves the key is gone from every mart that carries it (traced by the data-class
report's lineage), whatever models the product has written.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from transform import pg, seed
from transform.classes import check
from transform.config import load
from transform.run import SUMMARY, Settings, TransformError, transform

URL = os.environ.get("TRANSFORM_TEST_DATABASE_URL", "")
KEY = "k" * 40
ROOT = Path(__file__).resolve().parents[1]
END = datetime(2026, 11, 25, tzinfo=UTC)
WINDOW = (END - timedelta(days=1), END)

pytestmark = pytest.mark.skipif(not URL, reason="set TRANSFORM_TEST_DATABASE_URL to run the Postgres tests")


def sql(*statements: str, url: str = URL) -> list[tuple]:
    import psycopg

    rows: list[tuple] = []
    with psycopg.connect(url, autocommit=True) as conn:
        for s in statements:
            cur = conn.execute(s)
            rows = cur.fetchall() if cur.description else []
    return rows


@pytest.fixture
def warehouse(tmp_path):
    import psycopg

    sql(
        "drop schema if exists transform cascade",
        "drop schema if exists transform_build cascade",
        "drop schema if exists transform_old cascade",
        "drop table if exists public.notes",
        "create table if not exists public._reporting_logins (login text, tenant text, primary key (login, tenant))",
        "create or replace function public.reporting_tenants() returns setof text language sql stable"
        " security definer set search_path = pg_catalog, public as"
        " $$ select tenant from public._reporting_logins where login = session_user $$",
    )
    with psycopg.connect(URL, autocommit=True) as conn:
        pg.setup(conn)
        seed.seed(conn, ROOT / "dbt", ROOT / "contracts", "public", rows=60)
    sql("truncate valxel.break_glass", "truncate public._reporting_logins")
    for role in ("clinic_reader", "oncall_person"):
        sql(
            f"do $$ begin if exists (select 1 from pg_roles where rolname = '{role}') then"
            f" drop owned by {role}; drop role {role}; end if; end $$"
        )
    project = tmp_path / "dbt"
    shutil.copytree(ROOT / "dbt", project, ignore=shutil.ignore_patterns("target", "logs"))
    evidence = tmp_path / "evidence"
    settings = Settings(
        config_path=str(ROOT / "transform.yaml"),
        project_dir=str(project),
        database_url=URL,
        tokenize_key=KEY,
        evidence_dir=str(evidence),
        version="abc123",
    )
    return settings


def run(settings: Settings, **kw):
    return transform(settings, window=WINDOW, **kw)


def published(table: str) -> int:
    return sql(f"select count(*) from transform.{table}")[0][0]


def test_a_run_publishes_every_mart_with_its_tests_control_totals_and_evidence(warehouse, caplog):
    audits = []
    with caplog.at_level(logging.INFO, logger="transform"):
        summary = run(warehouse, audit=lambda a, **kw: audits.append((a, kw)))
    assert summary["published"] and summary["rows"] == {"notes_by_clinic_year": 3, "notes_signed": 60}
    assert summary["tests"]["failed"] == [] and len(summary["tests"]["control_totals"]) == 3
    assert published("notes_signed") == 60 and not sql("select 1 from pg_namespace where nspname = 'transform_build'")
    assert audits == [
        (
            "transform.published",
            {
                "resource": ("transform_model", ["notes_by_clinic_year", "notes_signed", "stg_notes"]),
                "details": {"rows": summary["rows"], "tests_passed": 9},
            },
        )
    ]
    [line] = [r.getMessage() for r in caplog.records if r.getMessage().startswith(SUMMARY)]
    assert json.loads(line.split(" ", 1)[1])["classes_ok"] is True
    [kept] = list(Path(warehouse.evidence_dir).iterdir())
    assert kept.name.endswith("-abc123")
    assert sorted(p.name for p in kept.iterdir()) == [
        "classes.json",
        "manifest.json",
        "run_results.json",
        "summary.json",
    ]
    for p in kept.iterdir():
        assert KEY not in p.read_text(), p.name  # the tokenize key never reaches evidence


def test_a_report_login_sees_only_its_tenants_marts_and_nothing_raw(warehouse):
    run(warehouse)
    sql(
        "create role clinic_reader login password 'reader-pw-123'",
        "grant reporting_reader to clinic_reader",
        "grant usage on schema valxel to clinic_reader",
        "insert into public._reporting_logins values ('clinic_reader', 'clinic_id-1') on conflict do nothing",
    )
    from psycopg.conninfo import make_conninfo

    reader = make_conninfo(URL, user="clinic_reader", password="reader-pw-123")
    tenants = sql("select distinct clinic_id from transform.notes_signed", url=reader)
    assert tenants == [("clinic_id-1",)]
    with pytest.raises(Exception, match="permission denied"):
        sql("select count(*) from transform.stg_notes", url=reader)
    tokens = sql("select patient_token, signed_year from transform.notes_signed limit 1", url=reader)[0]
    assert len(tokens[0]) == 64 and not tokens[0].startswith("patient") and tokens[1] == 2025


def test_a_deleted_record_is_gone_from_every_mart_that_carries_it(warehouse):
    first = run(warehouse)
    report = check(json.loads(next(Path(warehouse.evidence_dir).iterdir()).joinpath("manifest.json").read_text()))
    sources = [t for t in seed.sources(Path(warehouse.project_dir)) if (t.get("meta") or {}).get("key")]
    assert sources, "a source with a key"
    for table in sources:
        key = table["meta"]["key"]
        [(victim,)] = sql(f"select {key} from public.{table['name']} order by {key} limit 1")
        carriers = {
            (c.model, c.name)
            for c in report.columns
            if any(i.split(" (")[0].endswith(f".{key}") and "de-identified" not in i for i in c.inputs)
        }
        marts = {(m, col) for m, col in carriers if m in first["rows"]}
        assert marts, f"some mart carries {table['name']}.{key}"
        for model, col in marts:
            assert sql(f"select count(*) from transform.{model} where {col} = '{victim}'")[0][0] == 1
        sql(f"delete from public.{table['name']} where {key} = '{victim}'")
        run(warehouse)
        for model, col in marts:
            assert sql(f"select count(*) from transform.{model} where {col} = '{victim}'")[0][0] == 0, (model, col)


def test_a_failing_test_publishes_nothing_and_readers_keep_the_last_good_marts(warehouse):
    run(warehouse)
    sql(
        "insert into public.notes (note_id, kind, clinic_id, patient_id, signed_at)"
        " values ('00000000-0000-0000-0000-0000000000ff', 'other', 'clinic_id-1', 'p', now())"
    )
    with pytest.raises(TransformError, match="accepted_values_stg_notes_kind"):
        run(warehouse)
    assert published("notes_signed") == 60  # the last good set, untouched
    assert sql("select 1 from pg_namespace where nspname = 'transform_build'")  # kept to look into


def test_a_downgraded_column_publishes_nothing(warehouse):
    run(warehouse)
    project = Path(warehouse.project_dir)
    (project / "models/marts/leak.sql").write_text("select note_id, patient_id from {{ ref('stg_notes') }}\n")
    (project / "models/marts/leak.yml").write_text(
        "version: 2\nmodels:\n  - name: leak\n    columns:\n"
        "      - { name: note_id, meta: { data_class: none } }\n"
        "      - { name: patient_id, meta: { data_class: none } }\n"
    )
    with pytest.raises(TransformError, match=r"leak\.patient_id is declared none but is made from phi data"):
        run(warehouse)
    assert not sql("select 1 from pg_tables where schemaname = 'transform' and tablename = 'leak'")


def test_a_dry_run_builds_and_checks_but_publishes_nothing(warehouse):
    summary = run(warehouse, dry_run=True)
    assert summary["published"] is False and summary["rows"]["notes_signed"] == 60
    assert not sql("select 1 from pg_namespace where nspname = 'transform'")


def test_retention_ages_out_old_rows_of_an_incremental_mart(warehouse):
    run(warehouse)
    sql(
        "update transform.notes_signed set _updated_at = now() - interval '3000 days'"
        " where note_id = (select min(note_id) from transform.notes_signed)"
    )
    summary = run(warehouse)
    assert summary["expired"] == {"notes_signed": 1} and summary["copied"] == ["notes_signed"]


def test_break_glass_is_time_boxed_and_revoked_by_the_next_run(warehouse):
    import psycopg

    run(warehouse)
    sql("create role oncall_person login")
    with psycopg.connect(URL, autocommit=True) as conn:
        pg.break_glass_grant(conn, "oncall_person", datetime.now(UTC) - timedelta(minutes=1), "fix a bad row in a mart")
    assert sql("select pg_has_role('oncall_person', 'transform_break_glass', 'member')")[0][0]
    audits = []
    run(warehouse, audit=lambda a, **kw: audits.append((a, kw)))
    assert not sql("select pg_has_role('oncall_person', 'transform_break_glass', 'member')")[0][0]
    assert ("transform.break_glass.revoked", {"resource": ("db_login", ["oncall_person"]), "details": {}}) in audits
    assert sql("select revoked_at is not null from valxel.break_glass where login = 'oncall_person'") == [(True,)]


def test_tokens_need_the_sessions_key(warehouse):
    with pytest.raises(Exception, match="no key of at least 32 characters"):
        sql("select valxel.tokenize('x')")


def test_the_grants_name_only_the_declared_sources():
    cfg = load(ROOT / "transform.yaml")
    manifest = {"sources": {"s": {"schema": "public", "identifier": "notes"}}}
    stmts = pg.grants(cfg, manifest, "emr_transform", "emr")
    assert 'grant select on "public"."notes" to "emr_transform"' in stmts
    assert 'grant create on database "emr" to "emr_transform"' in stmts
    assert not [s for s in stmts if "grant select on all" in s or ("insert" in s and "notes" in s)]
