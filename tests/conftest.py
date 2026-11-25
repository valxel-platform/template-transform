import os

import pytest

DB = os.environ.get("JOB_TEST_DATABASE_URL")


@pytest.fixture
def db():
    if not DB:
        pytest.skip("set JOB_TEST_DATABASE_URL to run the Postgres tests")
    import psycopg

    with psycopg.connect(DB, autocommit=True) as conn:
        conn.execute("drop table if exists job_runs")
    return DB


@pytest.fixture(autouse=True)
def work_stand_in(request, monkeypatch):
    """The harness's tests (job/) run a stand-in for the work: the real work is a transform run on a
    warehouse, tested in test_transform_pg.py. The stand-in does what the harness cares about: it
    checks for a stop, does one item, records an audit record (not on a dry run) and returns a count."""
    if request.node.get_closest_marker("real_work"):
        return
    from job import work

    def stand_in(run):
        run.check()
        if run.dry_run:
            return 0
        with run.item():
            pass
        run.audit("transform.published", resource=("transform_model", ["notes_signed"]), details={"rows": 2})
        return 2

    monkeypatch.setattr(work, "run", stand_in)
