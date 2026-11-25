"""Calls to other components, as this job's own identity.

A job reaches other components only through their APIs (never their databases), and it does so as
itself: an OAuth client of the product's IdP (client credentials), with a token whose audience is
the component it calls. Components check that token like any caller's, so a job gets exactly the
permissions its client is given, and its calls are audited under its name.

    resp = run.call("emr-api", "POST", "/v1/reminders", json={...}, idempotency_key=item_id)

- Only the components in JOB_SERVICES can be called, at their configured base URL; https, or http
  inside the cluster (*.svc.cluster.local). Redirects aren't followed.
- The token is fetched once per audience and reused until a minute before it expires; a 401 fetches
  a new one once. The client secret is read from its mounted file (JOB_CLIENT_SECRET_FILE), so a
  rotation needs no restart, and it is never logged.
- Every call carries an Idempotency-Key when given one: a repeated run doesn't do the work twice.
- On a dry run, only GET goes out; anything else is logged and skipped (DryRun).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from .settings import Settings


class NotAllowed(RuntimeError):  # noqa: N818 - reads as `raise NotAllowed(...)`
    """A component this job isn't configured to call, or a URL outside it."""


class DryRun(RuntimeError):  # noqa: N818 - what happened, not an error
    """A change skipped because the run is a dry run."""


def _allowed_base(url: str) -> bool:
    p = urlparse(url)
    host = p.hostname or ""
    return p.scheme == "https" or (p.scheme == "http" and host.endswith(".svc.cluster.local"))


class Services:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings, self.clock = settings, clock
        bad = sorted(n for n, url in settings.services.items() if not _allowed_base(url))
        if bad:
            raise NotAllowed(f"JOB_SERVICES: {', '.join(bad)} must be https, or http inside the cluster")
        kw: dict[str, Any] = {"timeout": 30.0, "follow_redirects": False, "trust_env": False}
        if transport is not None:
            kw["transport"] = transport
        self.http = httpx.Client(**kw)
        self.tokens: dict[str, tuple[str, float]] = {}
        self.lock = threading.Lock()

    def _secret(self) -> str:
        path = Path(self.settings.client_secret_file)
        if not path.is_file():
            raise NotAllowed("the job's client secret isn't mounted (JOB_CLIENT_SECRET_FILE)")
        return path.read_text(encoding="utf-8").strip()

    def token(self, audience: str, *, fresh: bool = False) -> str:
        s = self.settings
        if not (s.oidc_token_url and s.client_id):
            raise NotAllowed("set JOB_OIDC_TOKEN_URL and JOB_CLIENT_ID: the job calls others as itself")
        with self.lock:
            hit = self.tokens.get(audience)
            if hit and not fresh and self.clock() < hit[1]:
                return hit[0]
            resp = self.http.post(
                s.oidc_token_url,
                auth=(s.client_id, self._secret()),
                data={"grant_type": "client_credentials", "audience": audience},
            )
            if resp.status_code != 200:
                raise NotAllowed(f"the IdP refused the job's credentials ({resp.status_code})")
            body = resp.json()
            self.tokens[audience] = (
                str(body["access_token"]),
                self.clock() + max(30.0, float(body.get("expires_in", 300)) - 60),
            )
            return self.tokens[audience][0]

    def call(
        self, service: str, method: str, path: str, *, dry_run: bool = False, idempotency_key: str = "", **kw: Any
    ) -> httpx.Response:
        base = self.settings.services.get(service)
        if base is None:
            raise NotAllowed(f"{service!r} isn't in JOB_SERVICES")
        if not path.startswith("/") or "://" in path or path.startswith("//"):
            raise NotAllowed("call a path on the service, not a URL")
        method = method.upper()
        if dry_run and method != "GET":
            raise DryRun(f"{method} {service}{path} skipped: dry run")
        headers = dict(kw.pop("headers", {}) or {})
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        url = base.rstrip("/") + path
        for fresh in (False, True):
            headers["Authorization"] = f"Bearer {self.token(service, fresh=fresh)}"
            resp = self.http.request(method, url, headers=headers, **kw)
            if resp.status_code != 401:
                return resp
        return resp
