import pytest

from job.config import ConfigError, for_environment, load, parse

BASE = 'name: farm-reminders\nschedule: "0 6 * * *"\ntimezone: America/Chicago\n'


def test_the_templates_own_file_loads():
    cfg = load("job.yaml")
    assert cfg.name == "template-transform" and cfg.catch_up == "window" and cfg.time_limit_minutes == 30
    assert for_environment(cfg, "dev").schedule.text == "15 * * * *"
    assert for_environment(cfg, "production").schedule.text == "15 3 * * *"


def test_defaults_and_an_environment_that_is_suspended():
    cfg = parse(BASE + "environments:\n  dev: { suspend: true }\n")
    assert cfg.missed_after == 2 and cfg.environments["dev"].suspend and for_environment(cfg, "dev") is cfg


@pytest.mark.parametrize(
    "extra,why",
    [
        ("", "name"),
        ("schedule: nope\n", "schedule"),
        ("time_limit_minutes: 0\n", "time_limit"),
        ("catch_up: all\n", "catch_up"),
        ("missed_after: 0\n", "missed_after"),
        ("environments:\n  qa: {}\n", "qa"),
        ("environments:\n  dev: { schedule: '99 * * * *' }\n", "dev.schedule"),
    ],
)
def test_mistakes_are_named(extra, why):
    text = ('schedule: "0 6 * * *"\n' + extra) if why == "name" else BASE + extra
    with pytest.raises(ConfigError, match=why):
        parse(text)


def test_not_yaml_or_not_a_mapping():
    with pytest.raises(ConfigError):
        parse("a: [")
    with pytest.raises(ConfigError):
        parse("- a")


def test_the_transform_signs_in_with_its_logins_bound_outputs():
    from psycopg.conninfo import conninfo_to_dict

    from transform.run import Settings, TransformError, database_url

    url = database_url(Settings(pg_host="db.internal", pg_database="emr", pg_user="emr_transform", pg_password="pw"))
    assert conninfo_to_dict(url) == {
        "host": "db.internal",
        "port": "5432",
        "dbname": "emr",
        "user": "emr_transform",
        "password": "pw",
        "sslmode": "require",
    }
    assert database_url(Settings(database_url="postgresql://x@y/z")) == "postgresql://x@y/z"
    with pytest.raises(TransformError, match="_PASSWORD"):
        database_url(Settings(pg_host="db.internal", pg_database="emr", pg_user="emr_transform"))


def test_the_helper_schema_is_the_transforms_own():
    from pathlib import Path

    from transform.config import load

    assert load(Path(__file__).resolve().parents[1] / "transform.yaml").helper_schema == "transform_fn"
