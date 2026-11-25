"""Product events: counted for the SDLC tool's outcome loop, never about people."""

import json

import pytest
from openfeature.provider.in_memory_provider import InMemoryFlag, InMemoryProvider

from job import cohorts, events, flags

KEY = "emr.emr-feat-3.record-summary"


@pytest.fixture(autouse=True)
def fresh():
    events.reset()
    cohorts.drain()
    yield
    events.reset()
    flags.use_provider(InMemoryProvider({}))


def test_events_are_counted_into_one_summary_line_a_run():
    events.record("record_summarized")
    events.record("record_summarized", record_type="lab")
    events.record("record.flagged")
    [line] = events.drain()
    assert line.startswith(events.MARKER + " ")
    assert json.loads(line[len(events.MARKER) + 1 :]) == {"record.flagged": 1, "record_summarized": 2}
    assert events.drain() == []


@pytest.mark.parametrize("attribute", ["email", "patient_id", "last_name", "user_id", "mrn", "member_name"])
def test_attributes_never_reach_a_person(attribute):
    with pytest.raises(ValueError, match="person"):
        events.record("record_summarized", **{attribute: "x"})


def test_names_are_lowercase_words_and_values_short_categories():
    with pytest.raises(ValueError, match="lowercase"):
        events.record("RecordSummarized")
    with pytest.raises(ValueError, match="plain name"):
        events.record("record_summarized", **{"event": "x"})
    with pytest.raises(ValueError, match="short category"):
        events.record("record_summarized", record_type="")
    events.record("record_summarized", record_type="lab")


def test_an_items_events_count_on_the_side_of_each_flag_it_read():
    flags.use_provider(InMemoryProvider({KEY: InMemoryFlag("on", {"on": True, "off": False})}))
    for clinic in ("c-1", "c-2"):
        with cohorts.item():
            if flags.enabled(KEY, user=clinic):
                events.record("record_summarized")
    [line] = cohorts.drain()
    got = json.loads(line[len(cohorts.MARKER) + 1 :])
    assert got["on"]["requests"] == 2 and got["on"]["events"] == {"record_summarized": 2}
    assert got["off"]["events"] == {"record_summarized": 0}
    events.record("record_summarized")  # outside an item: only the summary line counts it
    assert cohorts.drain() == []


def test_a_failed_or_slow_item_counts_on_its_side(monkeypatch):
    flags.use_provider(InMemoryProvider({KEY: InMemoryFlag("off", {"on": True, "off": False})}))
    with pytest.raises(RuntimeError), cohorts.item():
        flags.enabled(KEY, user="c-1")
        raise RuntimeError("the API said no")
    monkeypatch.setattr(cohorts, "SLOW_MS", -1)
    with cohorts.item():
        flags.enabled(KEY, user="c-2")
    [line] = cohorts.drain()
    got = json.loads(line[len(cohorts.MARKER) + 1 :])
    assert got["off"] == {"requests": 2, "errors": 1, "slow": 1} and got["on"]["requests"] == 0
    cohorts.report()  # nothing left: logs nothing
