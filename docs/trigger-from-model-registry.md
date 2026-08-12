# Triggering `deployment_soft_sensors` from model-registry

When an experiment is created in model-registry (Dash or API), model-registry
should call Airflow's REST API to start the prediction loop for that
experiment's run. This is the only integration point needed on the
model-registry side — Airflow does not poll or watch for new experiments,
it waits to be told.

## 1. Get a token

```
POST http://<airflow-host>:8081/auth/token
Content-Type: application/json

{"username": "model-registry-service", "password": "<see secrets>"}
```

Response:
```json
{"access_token": "<jwt>"}
```

The token is a JWT (no visible expiry claim shorter than ~24h in testing);
treat it like any bearer token — fetch a fresh one if a call ever comes back
401, don't try to decode/cache expiry client-side.

`model-registry-service` is an Airflow user with the **Op** role (created via
`airflow users create`) — enough to trigger and read DAG runs, not an Admin
account. Credentials live in the deployment's secrets, not in this repo.

## 2. Trigger the DAG

```
POST http://<airflow-host>:8081/api/v2/dags/deployment_soft_sensors/dagRuns
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "logical_date": null,
  "conf": {
    "run_id": "<uuid of the run to monitor>",
    "experiment_id": "<uuid of the experiment>",
    "project_id": "<uuid of the project>",
    "project_name": "<project name, used to pick FEATURES_* / MODEL_REGISTRY_PROJECT_ID_* / MODEL_ID_*>",
    "user_id": "<uuid, optional, for audit>"
  }
}
```

`logical_date: null` is required by the API even though this DAG has
`schedule=None` and ignores it — Airflow still wants the field present.

Do this call **right after** the experiment + its first `run` row exist in
the database (the DAG's `wait_for_new_data` sensor will just wait, up to
24h, until `sensor_readings` starts getting rows for that `run_id` — it's
fine to trigger before the bioreactor is actually streaming).

On success (HTTP 200) you get back the `dag_run_id` — worth logging on the
model-registry side for correlation, but not required for anything.

## 3. What happens next

The DAG waits for the first sensor reading on that `run_id` (up to 24h, or
until the experiment's `end_time` passes, whichever comes first — see
`experiments.end_time`), then builds a feature snapshot, calls the
project's official model, and stores the prediction.

**This is currently one-shot**: each trigger produces exactly one
prediction and the DAG run ends there — it does not loop or re-trigger
itself. That's a deliberate, temporary simplification (the Dash has no way
to end/pause an experiment yet, so an auto-looping DAG had no way to know
when to stop). If you want a prediction on every new batch of sensor data
for a long-running experiment, trigger the DAG again yourself for the same
`run_id` — the `conf` schema is identical.

## Read predictions back

Predictions land in the `predictions` table via
`POST /api/v1/predictions/` on the Model Registry API itself — the Dash (or
anything else on the model-registry side) can read them the same way it
already reads anything else from that API, e.g.:

```
GET /api/v1/runs/{run_id}/predictions?since=<iso timestamp>
```

No new read endpoint is needed for this — `run_timeseries_router.py` already
exposes it.
