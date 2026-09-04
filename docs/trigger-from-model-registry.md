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
    "project_name": "<project name, used to pick MODEL_REGISTRY_PROJECT_ID_*>",
    "model_ids": ["<models.slug>", "..."],
    "vessel_id": "<uuid of the bioreactor this experiment runs on, optional>",
    "user_id": "<uuid, optional, for audit>"
  }
}
```

`model_ids` is the `slug` (not the `models.id` UUID) of **every** model
attached to the experiment — resolve each one from `experiment_models` →
`models.slug`. Airflow runs a prediction for each model in the list on every
cycle. If omitted, Airflow falls back to a single-item list from its own
`MODEL_ID_<PROJECT>` env var — only meant for triggers that don't have any
model attached yet, not the normal path.

`vessel_id` is `experiments.vessel_id` — the bioreactor the experiment runs
on. Airflow doesn't use it for anything today (sensors aren't scoped per
piece of equipment yet), it's passed through purely for provenance/audit.

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
`experiments.end_time`), then builds a feature snapshot, calls every model in
`model_ids`, and stores one prediction per model. It then re-triggers itself
for the same `run_id` (with `last_processed_time` advanced to this cycle's
snapshot) and repeats — so an experiment created for e.g. 2 hours keeps
producing predictions for the full 2 hours, not just once. The chain stops
itself once `experiments.end_time` passes; you only need to trigger it the
one time, right after the run is created.

Each attached model can have its own sampling cadence
(`models.input_time_interval`) — the DAG uses the **shortest** one across
`model_ids` to decide how often to check for new data, so no model goes
stale waiting on a slower one.

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
