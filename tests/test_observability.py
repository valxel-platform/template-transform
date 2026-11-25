"""Traces, error reports and the audit trail of a run (telemetry.py, errors.py, audit via context.Run)."""

import json
import logging
from datetime import UTC, datetime

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from job import errors, events, telemetry, work
from job import main as m
from job.audit import Auditor, AuditUnavailable, MemorySink
from job.config import parse
from job.runs import MemoryRunLog
from job.settings import Settings

CFG = parse('name: reminders\nschedule: "0 6 * * *"\ntimezone: UTC\n')
SPANS = InMemorySpanExporter()
telemetry.setup("reminders", exporter=SPANS)


@pytest.fixture(autouse=True)
def at_six(monkeypatch):
    monkeypatch.setattr(m, "now", lambda: datetime(2026, 11, 16, 6, 1, tzinfo=UTC))
    SPANS.clear()
    events.reset()


def test_a_run_is_one_trace_and_its_log_lines_name_it():
    import io

    from job.logs import JsonFormatter

    out = io.StringIO()
    handler = logging.StreamHandler(out)
    handler.setFormatter(JsonFormatter("reminders"))  # formats as the line is written, inside the run
    logging.getLogger("job").addHandler(handler)
    logging.getLogger("job").setLevel(logging.INFO)
    try:
        assert m.execute(CFG, Settings(), MemoryRunLog(), auditor=Auditor(MemorySink(), service="reminders")) == 0
    finally:
        logging.getLogger("job").removeHandler(handler)
    [span] = [s for s in SPANS.get_finished_spans() if s.name == "job.run"]
    assert span.attributes["job.job"] == "reminders" and span.attributes["job.outcome"] == "succeeded"
    lines = [json.loads(x) for x in out.getvalue().splitlines()]
    finished = next(x for x in lines if x["message"].startswith("valxel.job finished"))
    assert finished["trace.id"] == f"{span.context.trace_id:032x}"


def test_a_failed_run_marks_its_trace_failed(monkeypatch):
    def boom(_run):
        raise RuntimeError("the API said no")

    monkeypatch.setattr(work, "run", boom)
    assert m.execute(CFG, Settings(), MemoryRunLog(), auditor=Auditor(MemorySink(), service="reminders")) == 1
    [span] = [s for s in SPANS.get_finished_spans() if s.name == "job.run"]
    assert span.status.status_code.name == "ERROR" and span.attributes["job.outcome"] == "failed"
    assert span.events and span.events[0].name == "exception"


def test_what_the_work_does_to_records_is_audited_with_the_job_as_actor():
    sink = MemorySink()
    log = MemoryRunLog()
    assert m.execute(CFG, Settings(product="emr"), log, auditor=Auditor(sink, service="reminders", product="emr")) == 0
    [record] = sink.events
    data = record["data"]
    assert data["action"] == "transform.published" and data["actor"] == {
        "id": "reminders",
        "type": "service",
        "client": "",
    }
    assert data["details"]["rows"] == 2 and record["source"] == "emr/reminders" and data["trace_id"]
    assert m.execute(CFG, Settings(), log, dry_run=True, auditor=Auditor(sink, service="reminders")) == 0
    assert len(sink.events) == 1  # a dry run changes nothing, so it audits nothing


def test_for_phi_a_record_that_cant_be_published_fails_the_run(monkeypatch):
    sink = MemorySink(fail=True)
    log = MemoryRunLog()
    assert m.execute(CFG, Settings(data_class="phi"), log, auditor=Auditor(sink, service="r", required=True)) == 1
    assert log.rows[0]["status"] == "failed" and "AuditUnavailable" in log.rows[0]["error"]
    assert issubclass(AuditUnavailable, RuntimeError)


def test_the_audit_trail_is_required_for_payments_and_phi_and_travels_over_tls():
    assert m._auditor(Settings(data_class="phi")).required and not m._auditor(Settings()).required
    assert not m._auditor(Settings(data_class="phi", audit_required=False)).required
    with pytest.raises(RuntimeError, match="TLS"):
        m._auditor(Settings(data_class="phi", audit_sink="kafka", audit_kafka_bootstrap="k:9092"))


def test_error_reports_carry_the_component_and_trace_and_nothing_personal():
    with telemetry.tracer().start_as_current_span("x"):
        event = errors.scrub(
            {
                "user": {"id": "nurse-7"},
                "extra": {"a": 1},
                "breadcrumbs": [1],
                "request": {"url": "/x?ssn=1"},
                "exception": {
                    "values": [{"value": "patient 123 not found", "stacktrace": {"frames": [{"vars": {"x": 1}}]}}]
                },
                "logentry": {"message": "sent %s", "params": ["Jane"], "formatted": "sent Jane"},
                "message": "sent Jane",
            }
        )
    assert not {"user", "extra", "breadcrumbs", "request", "message"} & set(event)
    assert (
        "value" not in event["exception"]["values"][0]
        and "vars" not in event["exception"]["values"][0]["stacktrace"]["frames"][0]
    )
    assert event["logentry"] == {"message": "sent %s"} and len(event["tags"]["trace_id"]) == 32
    assert errors.start_errors({}) is False


def test_sentry_starts_with_a_dsn_and_tags_the_component():
    import sentry_sdk

    assert errors.start_errors({"SENTRY_DSN": "https://k@sentry.example.com/1", "JOB_ENVIRONMENT": "staging"})
    assert sentry_sdk.get_isolation_scope()._tags["component"] == "template-transform"
    errors.flush()
    sentry_sdk.init()  # off again
    telemetry.shutdown()
