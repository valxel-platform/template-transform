"""Settings, from the environment (prefix JOB_)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="JOB_")

    config_path: str = "job.yaml"
    # dev | staging | production: picks job.yaml's schedule for the environment. OpenChoreo sets it.
    environment: str = "dev"
    # The job's own Postgres: its run history and lock. Without one, runs keep no history, so missed
    # runs and overlapping runs can't be detected (fine for a local try, not for an environment).
    database_url: str = ""
    keep_runs_days: int = 90  # the example work: how long run records are kept
    log_level: str = "INFO"
    # The product's data class (general | payments | phi): audit records must reach Kafka over TLS,
    # and a run whose record can't be published fails, for payments and PHI.
    data_class: str = "general"
    product: str = ""  # the product's id, e.g. emr: in audit records
    # Audit trail (audit.py): "kafka" in deployed environments, "log" locally.
    audit_sink: str = "log"
    audit_kafka_bootstrap: str = ""
    audit_topic: str = "audit.v1"
    audit_kafka_config: dict[str, str] = {}  # extra librdkafka settings: security.protocol, sasl.*
    audit_required: bool | None = None  # default: required for payments and PHI
    # Feature flags (flags.py): the product's Flipt over OFREP. Unset: every flag reads its default.
    flags_url: str = ""
    flags_environment: str = ""  # Flipt environment, e.g. production (header x-flipt-environment)
    flags_namespace: str = ""  # the product's Flipt namespace, e.g. emr
    flags_token: str = ""
    flags_timeout_s: float = 0.25
    flags_cache_s: float = 30.0
    # Other components the work calls (services.py), by name -> base URL, e.g.
    # {"emr-api": "http://emr-api.emr.svc.cluster.local:8080"}. Nothing else is reachable through it.
    services: dict[str, str] = {}
    # The job's own identity for those calls: an OAuth client of the product's IdP. The secret is a
    # mounted file, never an environment variable.
    oidc_token_url: str = ""
    client_id: str = ""
    client_secret_file: str = "/var/run/secrets/valxel/job-client-secret"  # noqa: S105 - a path, not a secret
