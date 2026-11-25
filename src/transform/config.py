"""transform.yaml: the warehouse, the schema readers see, and retention per mart."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

WAREHOUSES = ("postgres",)  # snowflake follows (CP-126, second step)
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,50}$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Mart:
    name: str
    retention_days: int
    retention_column: str


@dataclass(frozen=True)
class Config:
    name: str
    warehouse: str = "postgres"
    schema: str = "transform"
    source_schema: str = "public"
    marts: dict[str, Mart] = field(default_factory=dict)

    @property
    def build_schema(self) -> str:
        return f"{self.schema}_build"

    @property
    def old_schema(self) -> str:
        return f"{self.schema}_old"


def parse(raw: Any) -> Config:
    if not isinstance(raw, dict):
        raise ConfigError("transform.yaml must be a mapping")
    problems: list[str] = []
    name = str(raw.get("name") or "")
    if not re.match(r"^[a-z][a-z0-9-]{1,62}$", name):
        problems.append("name: the component's name (lowercase, dashes)")
    warehouse = str(raw.get("warehouse") or "postgres")
    if warehouse not in WAREHOUSES:
        problems.append(f"warehouse must be one of {', '.join(WAREHOUSES)}")
    schema, source = str(raw.get("schema") or "transform"), str(raw.get("source_schema") or "public")
    for key, value in (("schema", schema), ("source_schema", source)):
        if not _IDENT.match(value):
            problems.append(f"{key}: a lowercase SQL name")
    if schema == source:
        problems.append("schema must differ from source_schema: the transform never writes the raw tables")
    marts: dict[str, Mart] = {}
    for mname, m in dict(raw.get("marts") or {}).items():
        m = m or {}
        days, column = m.get("retention_days"), str(m.get("retention_column") or "")
        if not _IDENT.match(str(mname)):
            problems.append(f"marts.{mname}: a model's name")
        if not isinstance(days, int) or days < 1:
            problems.append(f"marts.{mname}.retention_days: a whole number of days")
        if not _IDENT.match(column):
            problems.append(f"marts.{mname}.retention_column: the timestamp column rows age by")
        if not problems:
            marts[str(mname)] = Mart(str(mname), int(days), column)  # type: ignore[arg-type]
    if problems:
        raise ConfigError("; ".join(problems))
    return Config(name=name, warehouse=warehouse, schema=schema, source_schema=source, marts=marts)


def load(path: Path) -> Config:
    return parse(yaml.safe_load(path.read_text()))
