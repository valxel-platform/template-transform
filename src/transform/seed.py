"""Synthetic raw tables for local runs and CI: for each source in dbt/models/sources.yml whose
`meta.dataset` names a contract under contracts/, the table reporting would load (columns typed from
the contract, plus _offset, _partition and _updated_at) filled with made-up rows: enums cycle,
dates spread over a year, ids and other text drawn from a few values per column so groups and
joins have something to find. Never real data.
"""

from __future__ import annotations

import json
import uuid
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import yaml
from psycopg import sql

BASE = datetime(2025, 1, 1, tzinfo=UTC)


def column_type(prop: dict[str, Any]) -> str:
    t, fmt = prop.get("type"), prop.get("format")
    if t == "string":
        return {"date": "date", "date-time": "timestamptz"}.get(str(fmt), "text")
    return {"integer": "bigint", "number": "double precision", "boolean": "boolean"}.get(
        str(t), "jsonb" if t in ("object", "array") else "text"
    )


def value(col: str, prop: dict[str, Any], i: int, tenant: str) -> Any:
    if "enum" in prop:
        return prop["enum"][i % len(prop["enum"])]
    t, fmt = prop.get("type"), prop.get("format")
    if fmt == "uuid":
        return str(uuid.UUID(int=(zlib.crc32(col.encode()) & 0xFFFF) << 64 | i))
    if fmt == "date-time":
        return BASE + timedelta(days=i * 7 % 365, hours=i % 24)
    if fmt == "date":
        return (BASE + timedelta(days=i * 7 % 365)).date()
    if t == "integer":
        return i
    if t == "number":
        return round(i * 1.5, 2)
    if t == "boolean":
        return i % 2 == 0
    return f"{col}-{i % (3 if col == tenant else 9)}"


def sources(project: Path) -> list[dict[str, Any]]:
    out = []
    for path in sorted((project / "models").rglob("*.yml")):
        doc = yaml.safe_load(path.read_text()) or {}
        for src in doc.get("sources") or []:
            for table in src.get("tables") or []:
                out.append(table)
    return out


def seed(conn: psycopg.Connection[Any], project: Path, contracts: Path, schema: str, rows: int = 60) -> dict[str, int]:
    out = {}
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(schema)))
        if not _has_function(cur, "reporting_tenants"):  # reporting makes the real one; locally, no tenants
            cur.execute(
                "create function public.reporting_tenants() returns setof text language sql stable"
                " as $$ select null::text where false $$"
            )
        for table in sources(project):
            meta = table.get("meta") or {}
            dataset, key, tenant = meta.get("dataset"), meta.get("key"), str(meta.get("tenant") or "")
            path = contracts / f"{dataset}.json"
            if not dataset or not path.exists():
                continue
            props = json.loads(path.read_text()).get("properties", {})
            cols = [c["name"] for c in table.get("columns") or [] if not str(c["name"]).startswith("_")]
            name = sql.Identifier(schema, table["name"])
            defs = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(column_type(props.get(c, {})))) for c in cols
            )
            cur.execute(sql.SQL("drop table if exists {}").format(name))
            cur.execute(
                sql.SQL(
                    "create table {} ({}, _offset bigint not null default 0, _partition int not null default 0,"
                    " _updated_at timestamptz not null default now(), primary key ({}))"
                ).format(name, defs, sql.Identifier(str(key or cols[0])))
            )
            ins = sql.SQL("insert into {} ({}) values ({})").format(
                name, sql.SQL(", ").join(map(sql.Identifier, cols)), sql.SQL(", ").join(sql.Placeholder() * len(cols))
            )
            for i in range(rows):
                cur.execute(ins, [value(c, props.get(c, {}), i, tenant) for c in cols])
            out[table["name"]] = rows
    return out


def _has_function(cur: psycopg.Cursor[Any], name: str) -> bool:
    cur.execute("select 1 from pg_proc where proname = %s", [name])
    return cur.fetchone() is not None
