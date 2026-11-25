"""Product events: the things a feature's goal counts, for the SDLC tool's outcome loop.

The feature's PRD names them (its Instrumentation section) and the HLD says which repo records
each. A job records them where they happen, in its work:

    from job import events

    events.record("reminder_sent")
    events.record("reminder_sent", channel="sms")  # categories only

At the end of each run they're logged as one summary line, which the tool counts (in staging, to
prove every event fires before release; in production, for the goal's metric read from the logs):

    valxel.events {"reminder_sent":42}

and they count on the side of each flag the item had (cohorts.event), so a gradual release can
compare items with a feature on against items with it off. A job's process is too short-lived for
a Prometheus endpoint; its counts live in its logs.

Events carry counts and categories, never people: a name that isn't lowercase words, or an
attribute that names or identifies a person (a name, contact details, an id), raises ValueError.
"""

from __future__ import annotations

import json
import logging
import re
import threading

from . import cohorts

MARKER = "valxel.events"
log = logging.getLogger("job.events")
NAME = re.compile(r"[a-z][a-z0-9_]*(\.[a-z0-9_]+)*")
_ATTRIBUTE = re.compile(r"[a-z][a-z0-9_]*")
PERSONAL = frozenset(
    {
        "name",
        "first_name",
        "last_name",
        "full_name",
        "display_name",
        "username",
        "user_name",
        "email",
        "email_address",
        "phone",
        "phone_number",
        "mobile",
        "dob",
        "date_of_birth",
        "birth_date",
        "birthdate",
        "ssn",
        "social_security_number",
        "mrn",
        "address",
        "street",
        "street_address",
        "zip",
        "zipcode",
        "zip_code",
        "postcode",
        "postal_code",
        "ip",
        "ip_address",
        "card",
        "card_number",
        "pan",
        "cvv",
        "cvc",
        "iban",
        "account_number",
        "routing_number",
        "passport",
        "passport_number",
        "license_number",
        "drivers_license",
        "lat",
        "lng",
        "latitude",
        "longitude",
        "geo",
        "location",
    }
)
PERSON_IDS = (
    "patient",
    "member",
    "beneficiary",
    "subscriber",
    "cardholder",
    "user",
    "customer",
    "guest",
    "client",
    "person",
    "account",
)
_lock = threading.Lock()
_counts: dict[str, int] = {}  # since the last summary line


def personal(key: str) -> bool:
    if key in PERSONAL or key.endswith(("_email", "_phone", "_dob", "_ssn", "_address", "_card_number")):
        return True
    if key.endswith("_name") and (
        key.startswith(("first", "last", "full")) or any(key.startswith(w + "_") for w in PERSON_IDS)
    ):
        return True
    return any(key in (w, f"{w}_id", f"{w}_ids", f"{w}_number") for w in PERSON_IDS)


def check(event: str, attributes: dict[str, str]) -> None:
    if not isinstance(event, str) or len(event) > 100 or not NAME.fullmatch(event):
        raise ValueError(
            "Event names are lowercase words joined by _ or . (record_summarized), as the PRD's "
            f"Instrumentation section names them: {event!r}"
        )
    for key, value in attributes.items():
        if not _ATTRIBUTE.fullmatch(key) or key == "event":
            raise ValueError(f"{event}: attribute {key!r} must be a plain name")
        if personal(key):
            raise ValueError(
                f"{event}: attribute {key} names or identifies a person; events carry counts and "
                "categories, never people"
            )
        if not isinstance(value, str) or not value or len(value) > 64:
            raise ValueError(f"{event}: attribute {key} needs a short category value")


def record(event: str, /, **attributes: str) -> None:
    """Something happened that the feature's goal counts."""
    check(event, attributes)
    with _lock:
        _counts[event] = _counts.get(event, 0) + 1
    cohorts.event(event)


def drain() -> list[str]:
    """The summary line since the last call (none when nothing happened), and the counts start again."""
    with _lock:
        got = dict(sorted(_counts.items()))
        _counts.clear()
    return [f"{MARKER} {json.dumps(got, separators=(',', ':'))}"] if got else []


def report() -> None:
    for line in drain():
        log.info(line)


def reset() -> None:
    """Tests: forget every count."""
    with _lock:
        _counts.clear()
