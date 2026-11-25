"""JSON logs, one object per line, in the shape the platform's alert rules read.

An exception goes under error.stack_trace, so the platform's `exceptions` rule counts it once.
Fields a caller adds with `extra={"fields": {...}}` are merged into the line.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "@timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "log.level": record.levelname,
            "log.logger": record.name,
            "message": record.getMessage(),
            "service.name": self.service,
        }
        from .telemetry import trace_ids

        trace_id, span_id = trace_ids()
        if trace_id:
            out["trace.id"], out["span.id"] = trace_id, span_id
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            out.update({k: v for k, v in fields.items() if k not in out})
        if record.exc_info:
            out["error.type"] = record.exc_info[0].__name__ if record.exc_info[0] else ""
            out["error.stack_trace"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup(service: str, level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
