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

The transform signs in with a login of its own (platform-resources postgres-login): SELECT on the
source columns its contracts give it, CREATE on the database, and ADMIN on <login>_break_glass. It
makes everything else itself, with no owner rights: its helper schema (helpers), the build
schema and the published one.

Every statement is composed with psycopg's sql module: names come from the approved transform.yaml
and dbt's manifest and are quoted as identifiers; values are bound parameters.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql

from .config import Config

READER = "reporting_reader"  # the report logins' role, made by template-reporting (deployed first)
MIN_KEY = 32
_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
FUNCTIONS = ("tokenize", "generalize_year")  # in the transform's helper schema (Config.helper_schema)


def break_glass_role(login: str) -> str:
    """The NOLOGIN role the platform makes beside the transform's login (platform-resources
    postgres-login, breakGlass): the login administers it, so it grants and revokes it itself."""
    return f"{login}_break_glass"


def helper_sql(helpers: str, tokenize_function: str = "") -> list[sql.Composed]:
    """The transform's helper schema, made by its own login (it has CREATE on the database, nothing
    more): the de-identification functions, the break-glass record and the run state. Idempotent.

    In environments `tokenize` calls the database's own tokenize function (tokenize_function, a
    postgres-login output): HMAC-SHA256 with a versioned key that never leaves the database, tokens
    reading "v2:...". Without one (local runs and CI), it is the same HMAC with the session's key
    (PGOPTIONS), tokens reading "v0:...", and a session without a key of 32+ characters fails."""
    h = sql.Identifier(helpers)
    if tokenize_function:
        tokenize = sql.SQL(
            """create or replace function {}.tokenize(v text) returns text language sql stable
        set search_path = pg_catalog as $$ select {}(v) $$"""
        ).format(h, _qualified(tokenize_function))
    else:
        tokenize = sql.SQL(
            """create or replace function {}.tokenize(v text) returns text language plpgsql stable
        set search_path = pg_catalog as $$
        declare
          ks text := current_setting('valxel.tokenize_key', true);
          k bytea; ipad bytea; opad bytea;
        begin
          if v is null then return null; end if;
          if ks is null or length(ks) < {} then
            raise exception 'tokenize: no key of at least {} characters for this session';
          end if;
          k := convert_to(ks, 'UTF8');
          if length(k) > 64 then k := sha256(k); end if;
          k := k || decode(repeat('00', 64 - length(k)), 'hex');
          ipad := k; opad := k;
          for i in 0..63 loop
            ipad := set_byte(ipad, i, get_byte(k, i) # 54);
            opad := set_byte(opad, i, get_byte(k, i) # 92);
          end loop;
          return 'v0:' || encode(sha256(opad || sha256(ipad || convert_to(v, 'UTF8'))), 'hex');
        end $$"""
        ).format(h, sql.SQL(str(MIN_KEY)), sql.SQL(str(MIN_KEY)))
    return [
        sql.SQL("create schema if not exists {}").format(h),
        tokenize,
        sql.SQL(
            """create or replace function {}.generalize_year(t timestamptz) returns int language sql immutable
        set search_path = pg_catalog as $$ select extract(year from t)::int $$"""
        ).format(h),
        sql.SQL(
            """create table if not exists {}.break_glass (
        login text not null, granted_at timestamptz not null default now(), until timestamptz not null,
        reason text not null, revoked_at timestamptz)"""
        ).format(h),
        sql.SQL("create table if not exists {}.state (key_version int)").format(h),
        sql.SQL("revoke all on schema {} from public").format(h),
        sql.SQL("revoke all on {}.break_glass from public").format(h),
        sql.SQL("revoke all on {}.state from public").format(h),
    ]


def _qualified(name: str) -> sql.Composable:
    """A schema-qualified function name from a login's output (<login>_key.tokenize)."""
    parts = name.split(".")
    if len(parts) != 2 or not all(_NAME.match(p) for p in parts):
        raise ValueError(f"not a schema-qualified function: {name!r}")
    return sql.SQL(".").join(sql.Identifier(p) for p in parts)


def key_version(conn: psycopg.Connection[Any], tokenize_function: str) -> int | None:
    """The database's active tokenize key version (<login>_key.key_version()); None without one."""
    if not tokenize_function:
        return None
    schema = tokenize_function.split(".")[0]
    with conn.cursor() as cur:
        execute(cur, sql.SQL("select {}()").format(_qualified(f"{schema}.key_version")))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def last_key_version(conn: psycopg.Connection[Any], cfg: Config) -> int | None:
    with conn.cursor() as cur:
        execute(cur, sql.SQL("select max(key_version) from {}.state").format(sql.Identifier(cfg.helper_schema)))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def record_key_version(conn: psycopg.Connection[Any], cfg: Config, version: int) -> None:
    with conn.transaction(), conn.cursor() as cur:
        h = sql.Identifier(cfg.helper_schema)
        execute(cur, sql.SQL("delete from {}.state").format(h))
        execute(cur, sql.SQL("insert into {}.state (key_version) values (%s)").format(h), [version])


def execute(cur: psycopg.Cursor[Any], stmt: sql.SQL | sql.Composed | str, params: list[Any] | None = None) -> None:
    """Every statement this package runs. A statement is fixed SQL text, or composed with psycopg's
    sql module: names as quoted identifiers, values as bound parameters, never pasted in."""
    if not isinstance(stmt, (str, sql.SQL, sql.Composed)):
        raise TypeError("a statement is fixed SQL text or psycopg-composed SQL")
    query = sql.SQL(stmt) if isinstance(stmt, str) else stmt
    # Semgrep's SQLAlchemy rule can't tell psycopg composition from string concatenation:
    # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
    cur.execute(query, params)


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
        execute(cur, "select 1 from pg_namespace where nspname = %s", [schema])
    else:
        execute(cur, "select 1 from pg_tables where schemaname = %s and tablename = %s", [schema, table])
    return cur.fetchone() is not None


def prepare(conn: psycopg.Connection[Any], cfg: Config, incremental: Iterable[str]) -> list[str]:
    """A fresh build schema, with a copy of each incremental model's live table so the run adds to
    it rather than rebuilding it. Returns the tables copied."""
    copied = []
    with conn.transaction(), conn.cursor() as cur:
        build, live = sql.Identifier(cfg.build_schema), sql.Identifier(cfg.schema)
        execute(cur, sql.SQL("drop schema if exists {} cascade").format(build))
        execute(cur, sql.SQL("create schema {}").format(build))
        for name in incremental:
            if _exists(cur, cfg.schema, name):
                t = sql.Identifier(name)
                execute(cur, sql.SQL("create table {}.{} (like {}.{} including all)").format(build, t, live, t))
                execute(cur, sql.SQL("insert into {}.{} select * from {}.{}").format(build, t, live, t))
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
            execute(
                cur,
                sql.SQL("delete from {}.{} where {} < now() - make_interval(days => %s)").format(
                    sql.Identifier(cfg.build_schema), sql.Identifier(mart.name), sql.Identifier(mart.retention_column)
                ),
                [mart.retention_days],
            )
            out[mart.name] = cur.rowcount
    return out


def reader_grants(cfg: Config, built: list[Model], break_glass: str) -> list[sql.Composed]:
    """What readers and break-glass may do in the published schema: re-applied at every swap. Readers
    also use the helper schema: a mart view that generalizes a year runs with their rights."""
    live, helpers = sql.Identifier(cfg.schema), sql.Identifier(cfg.helper_schema)
    who = sql.SQL("{}, {}").format(sql.Identifier(READER), sql.Identifier(break_glass))
    out: list[sql.Composed] = [
        sql.SQL("revoke all on schema {} from public").format(live),
        sql.SQL("grant usage on schema {} to {}").format(live, who),
        sql.SQL("grant usage on schema {} to {}").format(helpers, who),
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
                sql.SQL("grant select, insert, update, delete on {} to {}").format(t, sql.Identifier(break_glass))
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
                    t, sql.Identifier(break_glass)
                ),
            ]
    return out


def publish(conn: psycopg.Connection[Any], cfg: Config, built: list[Model], break_glass: str) -> None:
    """Swap the build schema in, in one transaction, with readers' grants re-applied."""
    with conn.transaction(), conn.cursor() as cur:
        live, build, old = (sql.Identifier(s) for s in (cfg.schema, cfg.build_schema, cfg.old_schema))
        execute(cur, sql.SQL("drop schema if exists {} cascade").format(old))
        if _exists(cur, cfg.schema):
            execute(cur, sql.SQL("alter schema {} rename to {}").format(live, old))
        execute(cur, sql.SQL("alter schema {} rename to {}").format(build, live))
        for stmt in reader_grants(cfg, built, break_glass):
            execute(cur, stmt)
        execute(cur, sql.SQL("drop schema if exists {} cascade").format(old))


def counts(conn: psycopg.Connection[Any], schema: str, built: list[Model]) -> dict[str, int]:
    out = {}
    with conn.cursor() as cur:
        for m in built:
            if m.materialized == "view":
                continue
            execute(cur, sql.SQL("select count(*) from {}.{}").format(sql.Identifier(schema), sql.Identifier(m.name)))
            row = cur.fetchone()
            out[m.name] = int(row[0]) if row else 0
    return out


def login(conn: psycopg.Connection[Any]) -> str:
    with conn.cursor() as cur:
        execute(cur, "select current_user")
        row = cur.fetchone()
    return str(row[0]) if row else ""


def helpers(conn: psycopg.Connection[Any], cfg: Config, tokenize_function: str = "") -> None:
    """The helper schema, as the transform's own login: at the start of every run."""
    with conn.transaction(), conn.cursor() as cur:
        for stmt in helper_sql(cfg.helper_schema, tokenize_function):
            execute(cur, stmt)


def dev_setup(conn: psycopg.Connection[Any]) -> None:
    """CI and local only, on a database whose login may create roles: the roles the platform and
    template-reporting make in environments (reporting_reader, <login>_break_glass)."""
    me = login(conn)
    with conn.transaction(), conn.cursor() as cur:
        for role in (READER, break_glass_role(me)):
            execute(cur, "select 1 from pg_roles where rolname = %s", [role])
            if not cur.fetchone():
                execute(cur, sql.SQL("create role {} nologin").format(sql.Identifier(role)))


def break_glass_grant(
    conn: psycopg.Connection[Any], cfg: Config, person: str, until: datetime, reason: str, role: str = ""
) -> None:
    """`role`: the login's break-glass role (a postgres-login output); by default <login>_break_glass."""
    helpers(conn, cfg)
    role = role or break_glass_role(login(conn))
    with conn.transaction(), conn.cursor() as cur:
        execute(
            cur,
            sql.SQL("insert into {}.break_glass (login, until, reason) values (%s, %s, %s)").format(
                sql.Identifier(cfg.helper_schema)
            ),
            [person, until, reason],
        )
        execute(cur, sql.SQL("grant {} to {}").format(sql.Identifier(role), sql.Identifier(person)))


def break_glass_expire(conn: psycopg.Connection[Any], cfg: Config, role: str = "") -> list[str]:
    """Revoke every break-glass grant past its time; returns the logins revoked."""
    revoked = []
    role = role or break_glass_role(login(conn))
    with conn.transaction(), conn.cursor() as cur:
        execute(
            cur,
            sql.SQL(
                "update {}.break_glass set revoked_at = now() where revoked_at is null and until <= now()"
                " returning login"
            ).format(sql.Identifier(cfg.helper_schema)),
        )
        for (person,) in cur.fetchall():
            execute(cur, "select 1 from pg_roles where rolname = %s", [person])
            if cur.fetchone():
                execute(cur, sql.SQL("revoke {} from {}").format(sql.Identifier(role), sql.Identifier(person)))
            revoked.append(str(person))
    return revoked
