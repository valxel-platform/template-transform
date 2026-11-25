"""The data-class check (transform/classes.py) on hand-written manifests: no database, no dbt."""

from __future__ import annotations

import pytest

from transform.classes import check, render

SRC = '"db"."public"."notes"'
STG = '"db"."tx"."stg"'


def manifest(sql: str, declared: dict[str, str], *, extra: dict | None = None, src_classes: dict | None = None):
    classes = src_classes or {
        "note_id": "none",
        "clinic_id": "none",
        "patient_id": "phi",
        "signed_at": "phi",
        "ssn": "secret",
        "amount": "financial",
    }
    nodes = {
        "model.p.mart": {
            "resource_type": "model",
            "name": "mart",
            "relation_name": '"db"."tx"."mart"',
            "compiled_code": sql,
            "depends_on": {"nodes": list((extra or {}).keys())},
            "columns": {c: {"meta": {"data_class": k}} for c, k in declared.items()},
        },
        **(extra or {}),
    }
    return {
        "sources": {
            "source.p.reporting.notes": {
                "source_name": "reporting",
                "name": "notes",
                "relation_name": SRC,
                "columns": {c: {"meta": {"data_class": k}} for c, k in classes.items()},
            }
        },
        "nodes": nodes,
    }


def problems(sql: str, declared: dict[str, str], **kw) -> list[str]:
    return check(manifest(sql, declared, **kw)).problems


def test_a_column_keeps_its_sources_class():
    assert problems(f"select note_id, patient_id from {SRC}", {"note_id": "none", "patient_id": "phi"}) == []
    found = problems(f"select note_id, patient_id from {SRC}", {"note_id": "none", "patient_id": "none"})
    assert len(found) == 1 and "mart.patient_id is declared none but is made from phi data" in found[0]


def test_declaring_higher_than_the_data_is_allowed():
    assert problems(f"select note_id from {SRC}", {"note_id": "phi"}) == []


@pytest.mark.parametrize(
    "expr",
    [
        "transform_fn.tokenize((patient_id)::text)",
        "transform_fn.generalize_year((signed_at)::timestamptz)",
    ],
)
def test_the_template_de_identifications_lower_the_class(expr):
    assert problems(f"select {expr} as x from {SRC}", {"x": "none"}) == []


def test_a_finer_cut_or_another_function_does_not_de_identify():
    assert problems(f"select date_trunc('month', signed_at) as m from {SRC}", {"m": "none"})
    assert problems(f"select md5(patient_id) as h from {SRC}", {"h": "none"})


def test_only_what_the_de_identification_wraps_is_lowered():
    sql = f"select transform_fn.tokenize((patient_id)::text) || signed_at::text as x from {SRC}"
    [found] = problems(sql, {"x": "none"})
    assert "signed_at (phi)" in found and "patient_id (phi, de-identified)" in found


def test_a_column_used_both_inside_and_outside_a_de_identification_keeps_its_class():
    sql = f"select transform_fn.tokenize((patient_id)::text) || patient_id as x from {SRC}"
    assert problems(sql, {"x": "none"})


def test_secret_never_de_identifies():
    assert problems(f"select transform_fn.tokenize((ssn)::text) as t from {SRC}", {"t": "none"})
    assert problems(f"select transform_fn.tokenize((ssn)::text) as t from {SRC}", {"t": "secret"}) == []


AGG = "select clinic_id, count(distinct patient_id) as patients, sum(amount) as total from {src} group by 1 {having}"


def test_an_aggregate_de_identifies_only_with_a_group_floor_of_11():
    declared = {"clinic_id": "none", "patients": "none", "total": "none"}
    assert problems(AGG.format(src=SRC, having="having count(*) >= 11"), declared) == []
    assert problems(AGG.format(src=SRC, having="having count(*) > 10"), declared) == []
    for having in ("", "having count(*) >= 5", "having sum(amount) >= 11"):
        found = problems(AGG.format(src=SRC, having=having), declared)
        assert any("patients is declared none" in f for f in found), having


def test_min_and_max_return_a_value_as_it_is():
    sql = f"select clinic_id, max(signed_at) as last from {SRC} group by 1 having count(*) >= 11"
    assert problems(sql, {"clinic_id": "none", "last": "none"})


def test_a_group_by_column_keeps_its_class():
    sql = f"select patient_id, count(*) as n from {SRC} group by 1 having count(*) >= 11"
    assert problems(sql, {"patient_id": "none", "n": "none"})


def test_classes_flow_through_models_and_ctes():
    stg = {
        "model.p.stg": {
            "resource_type": "model",
            "name": "stg",
            "relation_name": STG,
            "depends_on": {"nodes": []},
            "compiled_code": f"with s as (select note_id, patient_id from {SRC}) select note_id, patient_id from s",
            "columns": {"note_id": {"meta": {"data_class": "none"}}, "patient_id": {"meta": {"data_class": "phi"}}},
        }
    }
    report = check(manifest(f"select note_id, patient_id as p from {STG}", {"note_id": "none", "p": "none"}, extra=stg))
    [problem] = report.problems
    assert "mart.p is declared none but is made from phi data (db.tx.stg.patient_id (phi))" in problem


def test_every_output_column_declares_a_class_and_every_declared_one_exists():
    found = problems(f"select note_id, clinic_id from {SRC}", {"note_id": "none", "gone": "none"})
    assert any("mart.clinic_id: declare its data_class" in f for f in found)
    assert any("mart.gone: declared in the .yml but not in the SQL" in f for f in found)
    assert any("declare its data_class" in f for f in problems(f"select note_id from {SRC}", {"note_id": "low"}))


def test_select_star_and_unparseable_sql_fail():
    assert any("select * hides" in f for f in problems(f"select * from {SRC}", {}))
    assert any("doesn't parse" in f for f in problems("select from where (", {}))


def test_a_source_column_without_a_class_fails():
    found = problems(f"select note_id from {SRC}", {"note_id": "none"}, src_classes={"note_id": ""})
    assert any("no data_class from its contract" in f for f in found)


def test_the_report_names_each_columns_inputs():
    report = check(manifest(f"select transform_fn.tokenize((patient_id)::text) as t from {SRC}", {"t": "none"}))
    assert report.ok and report.as_dict()["columns"] == [
        {
            "model": "mart",
            "column": "t",
            "declared": "none",
            "derived": "none",
            "inputs": ["db.public.notes.patient_id (phi, de-identified)"],
        }
    ]
    assert "mart.t: none / none" in render(report) and "no problems" in render(report)


def test_a_deletions_hook_rendered_at_parse_time_is_caught():
    m = manifest(f"select note_id from {SRC}", {"note_id": "none"})
    me = '"db"."tx"."mart"'
    m["nodes"]["model.p.mart"]["config"] = {
        "post-hook": [{"sql": f"delete from {me} as t where not exists (select 1 from {me} as s where s.k = t.k)"}]
    }
    assert any("compares the model with itself" in p for p in check(m).problems)
    m["nodes"]["model.p.mart"]["config"] = {
        "post-hook": [{"sql": f"delete from {me} as t where not exists (select 1 from {SRC} as s where s.k = t.k)"}]
    }
    assert check(m).ok


def test_only_the_transforms_own_helpers_de_identify():
    assert problems(f"select other.tokenize((patient_id)::text) as t from {SRC}", {"t": "none"})
    assert problems(f"select tokenize((patient_id)::text) as t from {SRC}", {"t": "none"})
    found = check(manifest(f"select emr_fn.tokenize((patient_id)::text) as t from {SRC}", {"t": "none"}), "emr_fn")
    assert found.problems == []
