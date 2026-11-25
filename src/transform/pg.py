"""The transform on Postgres: the reporting store's database, a schema beside its raw tables.

A run builds every model in <schema>_build. Only when every model and test passed does `publish`
swap it in, in one transaction: readers see the old marts or the new ones, never a half-built or
failing set. The swap re-applies what readers may do (`reader_grants`): reporting_reader (the
report logins) reads the marts (models under models/marts/) and nothing else, so staging and
intermediate models, which still carry raw columns, stay with the transform. A mart table that
declares `meta: {tenant: <column>}` gets row-level security by tenant, with the same
reporting_tenants() the raw tables use; a mart view runs with the reader's own rights
(security_invoker), so the row security under it applies to the reader, not to the view's owner. Only the
transform's login owns and writes the schema; people read through reporting_reader, and write only
through break-glass, which is time-boxed and audited.

Every statement is composed with psycopg's sql module: names come from the approved transform.yaml
and dbt's manifest and are quoted as identifiers; values are bound parameters.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql

from .config import Config

READER = "reporting_reader"  # the report logins' role, from template-reporting
BREAK_GLASS = "transform_break_glass"
MIN_KEY = 32

SETUP = [
    "create extension if not exists pgcrypto",
    "create schema if not exists valxel",
    # A keyed token. The key is the session's (PGOPTIONS, from the transform's mounted secret): a
    # session without one, or with a short one, fails instead of producing guessable tokens.
    f"""create or replace function valxel.tokenize(v text) returns text language plpgsql stable
        set search_path = pg_catalog, public as $$
        declare k text := current_setting('valxel.tokenize_key', true);
        begin
          if v is null then return null; end if;
          if k is null or length(k) < {MIN_KEY} then
            raise exception 'valxel.tokenize: no key of at least {MIN_KEY} characters for this session';
          end if;
          return encode(hmac(v, k, 'sha256'), 'hex');
        end $$""",
    """create or replace function valxel.generalize_year(t timestamptz) returns int language sql immutable
        set search_path = pg_catalog as $$ select extract(year from t)::int $$""",
    "grant usage on schema valxel to public",
    """create table if not exists valxel.break_glass (
        login text not null, granted_at timestamptz not null default now(), until timestamptz not null,
        reason text not null, revoked_at timestamptz)""",
    "revoke all on valxel.break_glass from public",
    f"do $$ begin create role {BREAK_GLASS} nologin; exception when duplicate_object then null; end $$",
    f"do $$ begin create role {READER} nologin; exception when duplicate_object then null; end $$",
]


@dataclass(frozen=True)
class Model:
    name: str
    materialized: str
    tenant: str = ""
    layer: str = ""  # staging, intermediate or marts: the folder under models/

    @property
    def readable(self) -> bool:
        return self.layer == "marts"


def models(manifest: dict[str, Any]) -> list[Model]:
    out = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        cfg, meta = node.get("config") or {}, node.get("meta") or {}
        fqn = node.get("fqn") or []
        layer = str(fqn[1]) if len(fqn) > 2 else ""
        out.append(Model(node["name"], str(cfg.get("materialized") or "view"), str(meta.get("tenant") or ""), layer))
    return sorted(out, key=lambda m: m.name)


def sources(manifest: dict[str, Any]) -> list[tuple[str, str]]:
    """(schema, table) of every source the models may read."""
    return sorted({(s["schema"], s["identifier"]) for s in manifest.get("sources", {}).values()})


def _exists(cur: psycopg.Cursor[Any], schema: str, table: str | None = None) -> bool:
    if table is None:
        cur.execute("select 1 from pg_namespace where nspname = %s", [schema])
    else:
        cur.execute("select 1 from pg_tables where schemaname = %s and tablename = %s", [schema, table])
    return cur.fetchone() is not None


def prepare(conn: psycopg.Connection[Any], cfg: Config, incremental: Iterable[str]) -> list[str]:
    """A fresh build schema, with a copy of each incremental model's live table so the run adds to
    it rather than rebuilding it. Returns the tables copied."""
    copied = []
    with conn.transaction(), conn.cursor() as cur:
        build, live = sql.Identifier(cfg.build_schema), sql.Identifier(cfg.schema)
        cur.execute(sql.SQL("drop schema if exists {} cascade").format(build))
        cur.execute(sql.SQL("create schema {}").format(build))
        for name in incremental:
            if _exists(cur, cfg.schema, name):
                t = sql.Identifier(name)
                cur.execute(sql.SQL("create table {}.{} (like {}.{} including all)").format(build, t, live, t))
                cur.execute(sql.SQL("insert into {}.{} select * from {}.{}").format(build, t, live, t))
                copied.append(name)
    return copied


def expire(conn: psycopg.Connection[Any], cfg: Config, existing: Iterable[str]) -> dict[str, int]:
    """Retention, in the build schema before it's published: rows older than each mart's
    retention_days go. Returns rows removed per mart."""
    out: dict[str, int] = {}
    names = set(existing)
    with conn.transaction(), conn.cursor() as cur:
        for mart in cfg.marts.values():
            if mart.name not in names:
                continue
            cur.execute(
                sql.SQL("delete from {}.{} where {} < now() - make_interval(days => %s)").format(
                    sql.Identifier(cfg.build_schema), sql.Identifier(mart.name), sql.Identifier(mart.retention_column)
                ),
                [mart.retention_days],
            )
            out[mart.name] = cur.rowcount
    return out


def reader_grants(cfg: Config, built: list[Model]) -> list[sql.Composed]:
    """What readers and break-glass may do in the published schema: re-applied at every swap."""
    live = sql.Identifier(cfg.schema)
    out: list[sql.Composed] = [
        sql.SQL("revoke all on schema {} from public").format(live),
        sql.SQL("grant usage on schema {} to {}, {}").format(live, sql.Identifier(READER), sql.Identifier(BREAK_GLASS)),
    ]
    for m in built:
        t = sql.SQL("{}.{}").format(live, sql.Identifier(m.name))
        if m.materialized == "view":
            # Every view runs with its reader's rights: row security below it applies to them.
            out.append(sql.SQL("alter view {} set (security_invoker = true)").format(t))
        if not m.readable:
            continue
        out.append(sql.SQL("grant select on {} to {}").format(t, sql.Identifier(READER)))
        if m.materialized != "view":
            out.append(
                sql.SQL("grant select, insert, update, delete on {} to {}").format(t, sql.Identifier(BREAK_GLASS))
            )
        if m.tenant and m.materialized != "view":
            out += [
                sql.SQL("alter table {} enable row level security").format(t),
                sql.SQL("drop policy if exists tenant_rows on {}").format(t),
                sql.SQL(
                    "create policy tenant_rows on {} for select to {} using ({} in (select reporting_tenants()))"
                ).format(t, sql.Identifier(READER), sql.Identifier(m.tenant)),
                sql.SQL("drop policy if exists break_glass on {}").format(t),
                sql.SQL("create policy break_glass on {} for all to {} using (true)").format(
                    t, sql.Identifier(BREAK_GLASS)
                ),
            ]
    return out


def publish(conn: psycopg.Connection[Any], cfg: Config, built: list[Model]) -> None:
    """Swap the build schema in, in one transaction, with readers' grants re-applied."""
    with conn.transaction(), conn.cursor() as cur:
        live, build, old = (sql.Identifier(s) for s in (cfg.schema, cfg.build_schema, cfg.old_schema))
        cur.execute(sql.SQL("drop schema if exists {} cascade").format(old))
        if _exists(cur, cfg.schema):
            cur.execute(sql.SQL("alter schema {} rename to {}").format(live, old))
        cur.execute(sql.SQL("alter schema {} rename to {}").format(build, live))
        for stmt in reader_grants(cfg, built):
            cur.execute(stmt)
        cur.execute(sql.SQL("drop schema if exists {} cascade").format(old))


def counts(conn: psycopg.Connection[Any], schema: str, built: list[Model]) -> dict[str, int]:
    out = {}
    with conn.cursor() as cur:
        for m in built:
            if m.materialized == "view":
                continue
            cur.execute(sql.SQL("select count(*) from {}.{}").format(sql.Identifier(schema), sql.Identifier(m.name)))
            row = cur.fetchone()
            out[m.name] = int(row[0]) if row else 0
    return out


def grants(cfg: Config, manifest: dict[str, Any], login: str, database: str) -> list[str]:
    """`transform-admin grants`: what the platform applies for this transform's login, rendered
    from the approved sources: read only the raw tables it declares, own and write only its schemas."""
    who = sql.Identifier(login)
    stmts: list[sql.Composable] = [sql.SQL(s) for s in SETUP]
    stmts += [
        # create: the run makes its build schema; it owns that schema and the published one, nothing else.
        sql.SQL("grant create on database {} to {}").format(sql.Identifier(database), who),
        sql.SQL("grant usage on schema valxel to {}").format(who),
    ]
    for schema in sorted({s for s, _ in sources(manifest)}):
        stmts.append(sql.SQL("grant usage on schema {} to {}").format(sql.Identifier(schema), who))
    for schema, table in sources(manifest):
        stmts.append(sql.SQL("grant select on {}.{} to {}").format(sql.Identifier(schema), sql.Identifier(table), who))
    stmts += [
        sql.SQL("grant {} to {} with admin option").format(sql.Identifier(BREAK_GLASS), who),
        sql.SQL("grant select, insert, update on valxel.break_glass to {}").format(who),
    ]
    return [s.as_string(None) for s in stmts]


def setup(conn: psycopg.Connection[Any]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        for stmt in SETUP:
            cur.execute(stmt)


def break_glass_grant(conn: psycopg.Connection[Any], login: str, until: datetime, reason: str) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("insert into valxel.break_glass (login, until, reason) values (%s, %s, %s)", [login, until, reason])
        cur.execute(sql.SQL("grant {} to {}").format(sql.Identifier(BREAK_GLASS), sql.Identifier(login)))


def break_glass_expire(conn: psycopg.Connection[Any]) -> list[str]:
    """Revoke every break-glass grant past its time; returns the logins revoked."""
    revoked = []
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "update valxel.break_glass set revoked_at = now() where revoked_at is null and until <= now()"
            " returning login"
        )
        for (login,) in cur.fetchall():
            cur.execute("select 1 from pg_roles where rolname = %s", [login])
            if cur.fetchone():
                cur.execute(sql.SQL("revoke {} from {}").format(sql.Identifier(BREAK_GLASS), sql.Identifier(login)))
            revoked.append(str(login))
    return revoked
