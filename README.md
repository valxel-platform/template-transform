# template-transform

The Valxel template for a **transform**: dbt models that turn the raw tables the reporting component
loads into marts that reports and outputs read. Each run builds every model, tests it, checks that
no column carries less than the data it's made from, and publishes only when everything passed.

The Valxel SDLC creates repos from this template (role `transform`). It runs on the job harness from
template-job, unchanged: OpenChoreo runs it as a Kubernetes CronJob on `job.yaml`'s schedule, with
the run lock, catch-up after missed runs, the time limit and the `job-missed` and `job-failed` alerts.

## What the coder writes

| File | What it holds |
| --- | --- |
| `dbt/models/staging/*.sql` | One model per raw table: rename, cast, deduplicate |
| `dbt/models/intermediate/*.sql` | Joins (optional) |
| `dbt/models/marts/*.sql` | What reports and outputs read. Only marts are readable by report logins. |
| `dbt/models/**/*.yml` | Every column of every model, each with `meta: {data_class: none\|pii\|financial\|phi\|secret}`, and its tests: dbt's (`unique`, `not_null`, `relationships`, `accepted_values`) and the template's `valxel_control_total`. A mart with a tenant column says `meta: {tenant: <column>}`. |
| `transform.yaml` | The warehouse, the schema readers see, retention per incremental mart |
| `job.yaml` | When it runs: after the loads it reads |

The tool writes `dbt/models/sources.yml` from the datasets the design lets this transform read, with
each column's class from its contract: models can only `source()` those.

The example models build two marts over reporting's synthetic `notes` table:
- `notes_signed`, incremental: the patient as a token, the date as a year;
- `notes_by_clinic_year`: counts per clinic and year, groups under 11 left out.

## What a run does

1. **Revokes break-glass access past its time**, audited.
2. **Builds in a fresh schema** (`<schema>_build`). Each incremental mart's live table is copied in,
   so the run adds to it.
3. **`dbt build`**: models and tests together, in dependency order. Every test is blocking.
4. **Checks data classes** on what was built (below). CI runs the same check on every PR.
5. **Applies retention** to incremental marts (`transform.yaml`).
6. **Publishes**: the build schema is renamed to the schema readers see, in one transaction, with
   readers' grants and tenant row security re-applied. Readers see the last good marts or the new
   ones, never a half-built or failing set. A failure anywhere before this publishes nothing; the
   run fails and the harness raises `job-failed`. The build schema stays until the next run, to look
   into.
7. **Keeps evidence**: dbt's `manifest.json` and `run_results.json`, the data-class report and a
   summary (commit, window, rows per model, tests, control totals, retention) under
   `TRANSFORM_EVIDENCE_DIR`. The summary is also logged as one `valxel.transform.run {...}` line.
   An audit record `transform.published` names the models and their row counts.

## Controls

| Control | How | Where |
| --- | --- | --- |
| Data classes through SQL | Every output column is traced through the SQL to the source columns it's made from (sqlglot). Its declared class must be at least the highest among them, unless the path goes through `valxel_tokenize`, `valxel_generalize_year`, or a `count`/`sum`/`avg` in a model with `valxel_min_group(11)` or more. `secret` never de-identifies; `min`/`max` don't de-identify. An undeclared column, `select *` or a declared column that isn't in the SQL fails too. | `src/transform/classes.py`, `transform-check` |
| Control totals | `valxel_control_total`: a measure on the model against the same, or another, measure on what it's built from, within a tolerance (`compare: equal` or `at_most`). Blocking. | `dbt/macros/valxel.sql` |
| Tokens | `valxel.tokenize`: HMAC-SHA256 with the session's key, from the transform's mounted secret through libpq's `PGOPTIONS`, so the key is never in SQL, dbt's logs or artifacts. A session without a key of 32+ characters fails instead of producing guessable tokens. | `src/transform/pg.py` |
| Deletions | Reporting deletes a raw row on a `.deleted` event. Full rebuilds follow it. An incremental model removes what's gone with `post_hook=["{{ valxel_deletions(source(...), '<key>') }}"]`, quoted so it renders at run time; `transform-check` fails an unquoted one. A test deletes a source row and proves it's gone from every mart that carries its key. | `dbt/macros/valxel.sql`, `tests/test_transform_pg.py` |
| Least privilege | `transform-admin grants` renders the transform login's grants from the approved sources: select on those raw tables, create for its own schemas, nothing else. The platform applies them. | `src/transform/pg.py` |
| Only writer | The transform login owns the published schema. Report logins (`reporting_reader`) read marts only, with row security by tenant through `reporting_tenants()`. Views run with the reader's rights (`security_invoker`). Staging and intermediate models, which still hold raw columns, aren't readable. | `src/transform/pg.py` |
| Break-glass | `transform-admin break-glass --login L --minutes N --reason "..."`: write on the marts for at most 240 minutes, recorded in `valxel.break_glass`, revoked by the first run after it ends (audited). | `src/transform/admin.py` |
| Query audit | The platform's Postgres runs pgaudit on the reporting store's database; its logs go to the product's audit topic. Not in this repo. | platform |
| Evidence | Manifest, run results, data-class report and summary per run. | `src/transform/run.py` |

## What comes with every run

Every run comes with the same security, observability and audit trail as the platform's other
templates (agentic-sdlc `config/template-contract.yaml`). Where each lives is
listed in `.valxel/capabilities.yaml`.

| Area | What the job does | Where |
| --- | --- | --- |
| Tracing | Each run is one OpenTelemetry trace: a `job.run` span with the job, window and outcome, and spans for the httpx, requests and Postgres calls the work makes, which carry the trace on to other services. Every log line has `trace.id`. Spans are flushed before the process exits. | `telemetry.py` |
| Error reports | A failed run goes to Sentry or GlitchTip (`SENTRY_DSN`), tagged with the component and the trace id. Messages, variables and anything personal are scrubbed. | `errors.py` |
| Logs and alerts | JSON lines. `valxel.job failed` and `valxel.job.missed` feed the platform's `job-failed` and `job-missed` alert rules. | `logs.py`, `main.py` |
| Audit trail | `run.audit(action, resource=(type, ids))` publishes a `valxel.audit.v1` record to the product's audit topic, with the job as a `service` actor and ids only. For payments and PHI, a record that can't be published fails the run, and Kafka must use TLS. | `audit.py`, `context.py` |
| Security | Semgrep, gitleaks and SonarQube on every PR (`security.yml`), dependency review, the image's SBOM and Grype scan, Dependabot with a week's cooldown, base images pinned to digests (`supply-chain-pins.yml`), and a non-root image run read-only with no capabilities. | `.github/` |

## job.yaml

```yaml
name: template-transform        # the component's name: keys the run lock and history
schedule: "15 3 * * *"          # minute hour day-of-month month day-of-week
timezone: America/Chicago
time_limit_minutes: 30
catch_up: window                # or latest
missed_after: 2
environments:                   # optional, outside production
  dev: { schedule: "15 * * * *" }
  staging: { schedule: "45 */6 * * *" }
```

**Schedules.** Use numbers (`*`, `5`, `1-5`, `*/15`, `1,15`); names like `MON` and macros like
`@daily` aren't accepted. When both day fields are set, a day matches either, as in cron. The time
zone is an IANA name, and a daily run keeps its local time across daylight saving.

**Deploys.** The Valxel SDLC reads `job.yaml` at the commit it deploys. It sets the CronJob's
schedule, time zone, time limit and `suspend` for each environment, so a schedule changes through a
pull request like any code. The architect's design names each job's schedule under `## Schedules`,
and G4 checks it.

## Settings

| Variable | Default | What it is |
| --- | --- | --- |
| `JOB_ENVIRONMENT` | `dev` | Picks `job.yaml`'s schedule for the environment (OpenChoreo sets it) |
| `JOB_DATABASE_URL` | none | The job's own Postgres: run history and the lock |
| `JOB_LOG_LEVEL` | `INFO` | |
| `JOB_PRODUCT`, `JOB_DATA_CLASS` | none, `general` | The product's id and data class: the audit trail is required, and over TLS, for `payments` and `phi` |
| `JOB_AUDIT_SINK` | `log` | `kafka` in environments, with `JOB_AUDIT_KAFKA_BOOTSTRAP`, `JOB_AUDIT_TOPIC` (`audit.v1`), `JOB_AUDIT_KAFKA_CONFIG` (JSON: `security.protocol`, `sasl.*`) |
| `JOB_FLAGS_URL`, `JOB_FLAGS_NAMESPACE`, `JOB_FLAGS_ENVIRONMENT`, `JOB_FLAGS_TOKEN` | none | The product's Flipt (OFREP); unset, every flag reads its default |
| `JOB_COHORTS_SLOW_MS` | `5000` | An item slower than this counts as slow in its flag cohort |
| `SENTRY_DSN`, `SENTRY_RELEASE` | none | Error reports (GlitchTip takes the same DSN) |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | none | Where traces go; unset, traces are made but not exported |

| `TRANSFORM_DATABASE_URL_FILE` | `/var/run/secrets/valxel/transform-database-url` | The warehouse connection as the transform's login, a mounted file (`TRANSFORM_DATABASE_URL` for local runs only) |
| `TRANSFORM_TOKENIZE_KEY_FILE` | `/var/run/secrets/valxel/transform-tokenize-key` | `valxel.tokenize`'s key, 32+ characters, a mounted file (`TRANSFORM_TOKENIZE_KEY` for local runs only) |
| `TRANSFORM_EVIDENCE_DIR` | none | Where each run's evidence is kept |
| `TRANSFORM_VERSION` | none | The deployed commit, in the evidence (the Valxel SDLC sets it) |
| `TRANSFORM_SEED_SYNTHETIC` | `false` | CI and local only: set up the warehouse and seed synthetic raw tables first; refused unless `JOB_ENVIRONMENT` is `ci` or `dev` |

Without `JOB_DATABASE_URL`, runs keep no history, so missed and overlapping runs can't be detected.
That's fine for a local try, but not for an environment.

## Running by hand

```bash
job-run --check                                   # check job.yaml (CI runs it for every environment)
job-run --dry-run                                 # build and check, publish nothing
job-run --from 2026-11-01T00:00:00Z --to 2026-11-02T00:00:00Z   # backfill one window, recorded as manual
```

In an environment, start a run now from OpenChoreo's "Run now" on the component. Its trigger
creates a Job from the CronJob, so it takes the same lock as the scheduled runs.

## Develop

```bash
uv sync
uv run ruff check . && uv run ruff format --check . && uv run mypy
export TRANSFORM_DATABASE_URL=postgresql://postgres@localhost:5432/product TRANSFORM_TOKENIZE_KEY=$(openssl rand -hex 24)
uv run transform-dev                              # set up, seed synthetic raw tables, run, report
uv run transform-check                            # the data-class report the PR check prints
# the Postgres tests: JOB_TEST_DATABASE_URL and TRANSFORM_TEST_DATABASE_URL (a database they may reset)
uv run pytest
```

## Checks

`unit` (required) runs the linters, type checks, `job-run --check` for every environment, the
tests with Postgres (every model built and tested, the deletion proof, the controls), and
`transform-check`; coverage must stay at 90% or more. `image` builds the image, checks it with
a read-only root file system and no capabilities (as the platform runs it), runs it for real in
conformance (seeded raw tables, a build, a publish, its audit record and traces), writes its SBOM and
fails on high or critical vulnerabilities with a fix. `dependency-review` checks new dependencies
on pull requests. `supply-chain-pins` keeps base images pinned to their digests, weekly and on
Dependabot's pull requests. CodeQL runs from GitHub's default setup.
