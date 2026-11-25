"""Calls to other components as the job's own identity (services.py)."""

from __future__ import annotations

import httpx
import pytest

from job.services import DryRun, NotAllowed, Services
from job.settings import Settings


def services(tmp_path, handler, **over):
    secret = tmp_path / "secret"
    secret.write_text("s3cret-synthetic\n")
    base = {
        "services": {"emr-api": "http://emr-api.emr.svc.cluster.local:8080"},
        "oidc_token_url": "https://id.example.test/token",
        "client_id": "emr-reminders",
        "client_secret_file": str(secret),
    }
    settings = Settings(**{**base, **over})
    return Services(settings, transport=httpx.MockTransport(handler))


def test_calls_go_as_the_job_with_a_token_for_that_component(tmp_path):
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path == "/token":
            return httpx.Response(200, json={"access_token": f"tok-{len(seen)}", "expires_in": 300})
        return httpx.Response(201)

    s = services(tmp_path, handler)
    assert s.call("emr-api", "post", "/v1/reminders", json={"a": 1}, idempotency_key="item-1").status_code == 201
    s.call("emr-api", "GET", "/v1/reminders")
    token, call, again = seen
    assert b"grant_type=client_credentials" in token.content and b"audience=emr-api" in token.content
    assert token.headers["authorization"].startswith("Basic ")
    assert str(call.url) == "http://emr-api.emr.svc.cluster.local:8080/v1/reminders"
    assert call.headers["authorization"] == "Bearer tok-1" and call.headers["idempotency-key"] == "item-1"
    assert again.headers["authorization"] == "Bearer tok-1"  # reused


def test_a_401_fetches_a_new_token_once(tmp_path):
    answers = iter([401, 200])

    def handler(request):
        if request.url.path == "/token":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 300})
        return httpx.Response(next(answers))

    assert services(tmp_path, handler).call("emr-api", "GET", "/x").status_code == 200


def test_only_configured_components_and_paths(tmp_path):
    s = services(tmp_path, lambda r: httpx.Response(200, json={"access_token": "t"}))
    for service, path in (
        ("billing-db", "/x"),
        ("emr-api", "https://evil.test/x"),
        ("emr-api", "//evil.test/x"),
        ("emr-api", "x"),
    ):
        with pytest.raises(NotAllowed):
            s.call(service, "GET", path)
    with pytest.raises(NotAllowed, match="must be https"):
        services(tmp_path, lambda r: httpx.Response(200), services={"x": "http://x.example.test"})


def test_dry_runs_change_nothing(tmp_path):
    s = services(tmp_path, lambda r: httpx.Response(200, json={"access_token": "t"}))
    with pytest.raises(DryRun):
        s.call("emr-api", "POST", "/v1/reminders", dry_run=True)
    assert s.call("emr-api", "GET", "/v1/reminders", dry_run=True).status_code == 200


def test_no_identity_no_calls(tmp_path):
    s = services(tmp_path, lambda r: httpx.Response(200), oidc_token_url="")
    with pytest.raises(NotAllowed, match="JOB_OIDC_TOKEN_URL"):
        s.call("emr-api", "GET", "/x")
    s = services(tmp_path, lambda r: httpx.Response(401))
    with pytest.raises(NotAllowed, match="refused"):
        s.call("emr-api", "GET", "/x")
    s = services(tmp_path, lambda r: httpx.Response(200), client_secret_file=str(tmp_path / "none"))
    with pytest.raises(NotAllowed, match="mounted"):
        s.call("emr-api", "GET", "/x")


def test_the_run_calls_through_its_services(tmp_path):
    from datetime import UTC, datetime

    from job.context import Run
    from job.runs import MemoryRunLog

    s = services(tmp_path, lambda r: httpx.Response(200, json={"access_token": "t"}))
    at = datetime(2026, 11, 17, tzinfo=UTC)
    run = Run(
        job="j",
        window_start=at,
        window_end=at,
        settings=s.settings,
        runlog=MemoryRunLog(),
        deadline=1e12,
        dry_run=True,
        services=s,
    )
    with pytest.raises(DryRun):
        run.call("emr-api", "PUT", "/v1/x")
    bare = Run(job="j", window_start=at, window_end=at, settings=Settings(), runlog=MemoryRunLog(), deadline=1e12)
    with pytest.raises(NotAllowed, match="isn't in JOB_SERVICES"):
        bare.call("emr-api", "GET", "/x")
