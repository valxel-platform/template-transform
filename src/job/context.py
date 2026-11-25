"""What a run gets: its window, its limits, and a way to stop cleanly."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import cohorts
from .audit import Actor, Auditor, LogSink
from .runs import RunLog
from .settings import Settings


class Stopped(Exception):  # noqa: N818 - a run told to stop, not an error in the work
    """The time limit is near, or the platform asked the pod to stop."""


@dataclass
class Run:
    job: str
    window_start: datetime
    window_end: datetime
    settings: Settings
    runlog: RunLog
    deadline: float  # time.monotonic() value after which the run stops
    dry_run: bool = False
    manual: bool = False
    log: logging.Logger = field(default_factory=lambda: logging.getLogger("job.work"))
    stop_requested: bool = False
    auditor: Auditor = field(default_factory=lambda: Auditor(LogSink(), service="job", required=False))

    services: Any = None  # services.Services, made on first use

    def call(self, service: str, method: str, path: str, **kw: Any) -> Any:
        """Call another component's API as this job (services.py). On a dry run only GET goes out;
        anything else raises services.DryRun, which the work should treat as done."""
        if self.services is None:
            from .services import Services

            self.services = Services(self.settings)
        return self.services.call(service, method, path, dry_run=self.dry_run, **kw)

    def item(self) -> AbstractContextManager[None]:
        """One unit of work: wrap each so its flags and events count in the release's cohorts."""
        return cohorts.item()

    def audit(
        self,
        action: str,
        *,
        resource: tuple[str, Iterable[str]] = ("", ()),
        outcome: Any = "success",
        on_behalf_of: str = "",
        reason: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        """An audit record for something the work did to records, with this job as the actor (ids only,
        never field values). For payments and PHI products, a record that can't be published raises
        audit.AuditUnavailable: let it end the item, don't report the item done."""
        if self.dry_run:
            return  # nothing changed
        self.auditor.record(
            action,
            actor=Actor(id=self.job, type="service"),
            resource=resource,
            outcome=outcome,
            on_behalf_of=on_behalf_of,
            reason=reason,
            details=details,
        )

    def check(self) -> None:
        """Raise Stopped when the run should end now; call it between items."""
        if self.stop_requested:
            raise Stopped("the platform asked the job to stop")
        if time.monotonic() >= self.deadline:
            raise Stopped("the run reached its time limit")
