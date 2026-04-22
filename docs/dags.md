# STAMM DAGs — Specification

This document is the index of every Airflow DAG in the STAMM platform: the
one in production today and the ones under active development. Each DAG
has its own specification page under [`dags/`](./dags/) with its purpose,
trigger, task list, API dependencies, and downstream effects.

All planned DAGs assume the backend API + PostgreSQL state layer described
in the [project README](../README.md#architecture). They will talk to it
through a single shared client at [`../dags/common/api_client.py`](../dags/common/api_client.py).
Until that layer is available, `deployment_soft_sensors` continues to operate
against InfluxDB directly in its legacy form.

---

## DAGs

| # | DAG | Status | Trigger | Summary |
|---|-----|--------|---------|---------|
| 1 | [`deployment_soft_sensors`](./dags/deployment_soft_sensors.md) | In production; refactor planned | 20-second polling | Continuously generate soft-sensor predictions for every active bioreactor experiment. |
| 2 | [`experiment_lifecycle`](./dags/experiment_lifecycle.md) | Planned | 1-minute polling | Detect new bioreactor batches and register them as experiments in Postgres. |
| 3 | [`drift_detection`](./dags/drift_detection.md) | Planned | Daily + on-demand | Quantify divergence between predictions and ground truth; alert and optionally queue retraining. |
| 4 | [`retraining_trigger`](./dags/retraining_trigger.md) | Planned | 5-minute polling | Execute retraining jobs and publish resulting versions to the Model Registry. |
| 5 | [`experiment_close`](./dags/experiment_close.md) | Planned | Hourly | Transition stale experiments to `completed` and precompute UI summaries. |
| 6 | [`data_quality`](./dags/data_quality.md) | Planned | 10-minute | Gate raw sensor data before it reaches the models; quarantine failing experiments. |

---

## DAG dependency graph

    experiment_lifecycle  ──► deployment_soft_sensors  ──► experiment_close
                                    │
                                    ▼
                            data_quality (gate)
                                    │
                                    ▼
                             drift_detection  ──►  retraining_trigger

All planned DAGs share a single abstraction: `experiment_id`. That is what
makes the pipeline reproducible end-to-end.
