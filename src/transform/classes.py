"""The data-class check: no model column may carry less than the data it's made from.

From dbt's compiled manifest, each model's SQL is parsed (sqlglot) and every output column traced to
the source and model columns it's computed from. A column's class must be at least the highest
class among them (none < pii < financial < phi < secret), except along a path that goes through an
approved de-identification:

  valxel_tokenize(col)          a keyed token: what it wraps counts as none
  valxel_generalize_year(col)   a date cut to its year: what it wraps counts as none
  count/sum/avg in a model with valxel_min_group(n), n at least MIN_GROUP: what's aggregated counts
                                as none (group-by columns keep their class; min and max don't
                                de-identify, they return a value as it is)

`secret` never de-identifies. Every model column must declare `meta: {data_class: ...}` in its .yml,
and every column the SQL outputs must be declared: an undeclared column fails as surely as a
downgrade. Declaring a class higher than the data needs is allowed.

The report lists each model column with its declared class, the class its inputs give it and the
inputs: the reviewer reads it beside the SQL, and the tool archives it with the release.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.lineage import Node, lineage

CLASSES = ("none", "pii", "financial", "phi", "secret")
RANK = {c: i for i, c in enumerate(CLASSES)}
MIN_GROUP = 11  # the smallest group an aggregate of personal data may show (decided Nov 24 for PHI)
DEIDENTIFY = {"valxel.tokenize", "valxel.generalize_year"}
AGGREGATES = (exp.Count, exp.Sum, exp.Avg)


@dataclass
class Column:
    model: str
    name: str
    declared: str
    derived: str = "none"
    inputs: list[str] = field(default_factory=list)
    problem: str = ""


@dataclass
class Report:
    columns: list[Column] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "problems": self.problems,
            "columns": [
                {"model": c.model, "column": c.name, "declared": c.declared, "derived": c.derived, "inputs": c.inputs}
                for c in self.columns
            ],
        }


def _rel(name: str) -> str:
    """A relation as sqlglot prints it, without quotes and lower-cased: db.schema.table."""
    return ".".join(p.strip('"').lower() for p in name.split("."))


def _class_of(meta: dict[str, Any] | None) -> str:
    return str((meta or {}).get("data_class") or "")


def _min_group(select: exp.Expression) -> int:
    """The floor a HAVING count(*) >= n sets on every group, or 0."""
    having = select.args.get("having") if isinstance(select, exp.Select) else None
    if having is None:
        return 0
    for cmp in having.find_all(exp.GTE, exp.GT):
        left, right = cmp.left, cmp.right
        if isinstance(left, exp.Count) and isinstance(right, exp.Literal) and right.is_int:
            n = int(right.this)
            return n + 1 if isinstance(cmp, exp.GT) else n
    return 0


def _func_name(node: exp.Expression) -> str:
    if isinstance(node, exp.Anonymous):
        return str(node.name).lower()
    if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Anonymous):
        return f"{node.this.name}.{node.expression.name}".lower()
    return ""


def _shielded(expr: exp.Expression, aggregate_ok: bool) -> set[str]:
    """Column names in `expr` that are referenced only inside a de-identification."""
    inside: set[str] = set()
    outside: set[str] = set()
    for col in expr.find_all(exp.Column):
        safe = False
        parent = col.parent
        while isinstance(parent, exp.Expression) and parent is not expr.parent:
            if _func_name(parent) in DEIDENTIFY:
                safe = True
                break
            if aggregate_ok and isinstance(parent, AGGREGATES):
                safe = True
                break
            parent = parent.parent
        (inside if safe else outside).add(col.name.lower())
    return inside - outside


def _leaves(node: Node, shielded: bool, out: list[tuple[str, bool]], aggregate_floor: int) -> None:
    expr = node.expression
    projection = expr.this if isinstance(expr, exp.Alias) else expr
    select = expr.find_ancestor(exp.Select) if isinstance(expr, exp.Expression) else None
    agg_ok = aggregate_floor >= MIN_GROUP or (select is not None and _min_group(select) >= MIN_GROUP)
    safe_cols = _shielded(projection, agg_ok) if isinstance(projection, exp.Expression) else set()
    if not node.downstream:
        source = node.source
        table = (
            ".".join(p for p in (source.catalog, source.db, source.name) if p).lower()
            if isinstance(source, exp.Table)
            else ""
        )
        out.append((f"{table}.{node.name.rsplit('.', 1)[-1].lower()}", shielded))
        return
    for child in node.downstream:
        col = child.name.rsplit(".", 1)[-1].lower()
        _leaves(child, shielded or col in safe_cols, out, 0)


def check(manifest: dict[str, Any]) -> Report:
    """Check every model in a compiled dbt manifest."""
    report = Report()
    known: dict[str, dict[str, str]] = {}  # relation -> column -> class
    schema: dict[str, Any] = {}  # catalog -> schema -> table -> column -> type, as sqlglot takes it
    for src in manifest.get("sources", {}).values():
        rel = _rel(src["relation_name"])
        cols = {n.lower(): _class_of(c.get("meta")) for n, c in (src.get("columns") or {}).items()}
        for name, cls in cols.items():
            if cls not in RANK:
                report.problems.append(
                    f"source {src['source_name']}.{src['name']}.{name}: no data_class from its contract"
                )
        known[rel] = cols
        _put(schema, rel, cols)
    models = {k: v for k, v in manifest.get("nodes", {}).items() if v.get("resource_type") == "model"}
    order = _topological(models)
    for uid in order:
        node = models[uid]
        name, rel = node["name"], _rel(node.get("relation_name") or "")
        declared = {n.lower(): _class_of(c.get("meta")) for n, c in (node.get("columns") or {}).items()}
        sql = node.get("compiled_code") or ""
        try:
            parsed = sqlglot.parse_one(sql, dialect="postgres")
        except sqlglot.errors.ParseError as e:
            report.problems.append(f"{name}: the SQL doesn't parse for the data-class check: {e}")
            continue
        outputs = [s.alias_or_name.lower() for s in parsed.selects] if isinstance(parsed, exp.Query) else []
        _put(schema, rel, {c: "text" for c in outputs})  # an incremental model reads itself ({{ this }})
        for col in outputs:
            if col == "*":
                report.problems.append(f"{name}: select * hides what each column carries; name the columns")
                continue
            cls = declared.get(col, "")
            entry = Column(name, col, cls or "?")
            if cls not in RANK:
                entry.problem = f"{name}.{col}: declare its data_class in the model's .yml ({', '.join(CLASSES)})"
            else:
                leaves: list[tuple[str, bool]] = []
                try:
                    root = lineage(col, sql, schema=schema, dialect="postgres")
                    _leaves(root, False, leaves, _min_group(parsed) if isinstance(parsed, exp.Select) else 0)
                except Exception as e:  # noqa: BLE001 - reported as a problem, never skipped
                    entry.problem = f"{name}.{col}: couldn't trace its inputs ({type(e).__name__}: {e})"
                    leaves = []
                derived = "none"
                for leaf, shielded in leaves:
                    table, _, column = leaf.rpartition(".")
                    leaf_cls = known.get(table, {}).get(column, "")
                    if not leaf_cls and table:
                        entry.problem = entry.problem or f"{name}.{col}: reads {leaf}, which has no data_class"
                        continue
                    if not leaf_cls:
                        continue  # a literal or count(*): carries nothing
                    entry.inputs.append(f"{leaf} ({leaf_cls}{', de-identified' if shielded else ''})")
                    effective = "none" if shielded and leaf_cls != "secret" else leaf_cls
                    if RANK[effective] > RANK[derived]:
                        derived = effective
                entry.derived = derived
                if not entry.problem and RANK[cls] < RANK[derived]:
                    entry.problem = (
                        f"{name}.{col} is declared {cls} but is made from {derived} data "
                        f"({'; '.join(entry.inputs)}): declare it {derived}, or de-identify it with "
                        "valxel_tokenize, valxel_generalize_year or an aggregate under valxel_min_group"
                    )
            if entry.problem:
                report.problems.append(entry.problem)
            report.columns.append(entry)
        for extra in sorted(set(declared) - set(outputs)):
            report.problems.append(f"{name}.{extra}: declared in the .yml but not in the SQL")
        known[rel] = {c: (declared[c] if declared.get(c) in RANK else "secret") for c in outputs}
        for hook in (node.get("config") or {}).get("post-hook") or []:
            text = str(hook.get("sql") if isinstance(hook, dict) else hook)
            if text.lstrip().lower().startswith("delete from") and text.count(node.get("relation_name") or "\0") > 1:
                report.problems.append(
                    f"{name}: its deletions hook compares the model with itself, so it removes nothing: quote it, "
                    "post_hook=[\"{{ valxel_deletions(source(...), '<key>') }}\"]"
                )
    return report


def _put(schema: dict[str, Any], rel: str, cols: dict[str, str]) -> None:
    *path, table = rel.split(".")
    level = schema
    for part in path:
        level = level.setdefault(part, {})
    level[table] = {c: "text" for c in cols}


def _topological(models: dict[str, dict[str, Any]]) -> list[str]:
    done: list[str] = []
    seen: set[str] = set()

    def visit(uid: str) -> None:
        if uid in seen:
            return
        seen.add(uid)
        for dep in models[uid].get("depends_on", {}).get("nodes", []):
            if dep in models:
                visit(dep)
        done.append(uid)

    for uid in sorted(models):
        visit(uid)
    return done


def check_file(path: Path) -> Report:
    return check(json.loads(path.read_text()))


def render(report: Report) -> str:
    lines = ["model.column: declared / from its inputs"]
    for c in report.columns:
        flag = "  FAIL" if c.problem else ""
        lines.append(f"  {c.model}.{c.name}: {c.declared} / {c.derived}{flag}")
    lines += [f"problem: {p}" for p in report.problems] or ["no problems"]
    return "\n".join(lines)
