# `deployment_soft_sensors`

[← back to DAG index](../dags.md)

**Status.** In production; refactor planned for the API migration.

**Purpose.** Continuously generate soft-sensor predictions for every active
bioreactor experiment.

**Trigger.** 20-second polling schedule.

**Legacy implementation.** Single DAG in [`../../dags/deployment_soft_sensors.py`](../../dags/deployment_soft_sensors.py),
supported by [`../../dags/tasks/influx.py`](../../dags/tasks/influx.py) and
[`../../dags/tasks/prediction.py`](../../dags/tasks/prediction.py). Reads from and writes to
InfluxDB directly; auto-discovers models from the Model Registry per run.

**Target implementation.** Reads snapshots through the backend API; posts
predictions through the backend API; Postgres records an audit row per
(experiment, model, version, timestamp); InfluxDB continues to hold the
time-series value.

## Tasks (target state)

    # 1. Health check
    check_api_health_task = PythonOperator(
        task_id="check_api_health",
        python_callable=ping_backend_api,
        doc_md="Pings the STAMM backend API before any read or write.",
    )

    # 2. Fetch latest snapshots for active experiments
    fetch_snapshots_task = PythonOperator(
        task_id="fetch_snapshots",
        python_callable=get_snapshots_from_api,
        doc_md="GET /experiments/active/snapshots — pivoted feature rows.",
    )

    # 3. Load active models per experiment
    load_models_task = PythonOperator(
        task_id="load_active_models",
        python_callable=get_experiment_models,
        doc_md="GET /experiments/{id}/models — resolves model id + version set.",
    )

    # 4. Run predictions
    run_predictions_task = PythonOperator(
        task_id="run_model_predictions",
        python_callable=call_models_from_snapshots,
        doc_md="Posts each (snapshot, model) pair to the Model Registry.",
    )

    # 5. Persist predictions
    store_predictions_task = PythonOperator(
        task_id="store_predictions",
        python_callable=post_predictions_to_api,
        doc_md="POST /experiments/{id}/predictions — dual-write via backend.",
    )

## Legacy walkthrough

The following example traces a single run of the legacy implementation
(the production code in `../../dags/deployment_soft_sensors.py`). It is kept here
because it is the most concrete illustration of the snapshot → predict →
write-back cycle that the target implementation will preserve.

Assume Node-RED has written the following points into the raw bucket
(`stamm_raw`, measurement `device_obs`), all sharing the same tags and
timestamp:

```text
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=actuator,observed_property=sugar_feed_rate value=37 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=actuator,observed_property=agitator value=100 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=sensor,observed_property=temperature value=297.99 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=sensor,observed_property=pH value=6.5097 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=sensor,observed_property=dissolved_oxygen_concentration value=13.243 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=computed_variable,observed_property=vessel_volume value=60531 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=sensor,observed_property=CO2_percent_in_off_gas value=0.91112 1763997388720000000
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=sensor,observed_property=oxygen_in_percent_in_off_gas value=0.19388 1763997388720000000
```

### 1) `check_influxdb_connection` — health check

- Opens an `InfluxDBClient` and calls `ping()`.
- If the ping succeeds, the DAG continues.
- If it fails, the sensor keeps retrying within the configured timeout. No XCom
  is produced here; it is purely a connectivity gate.

### 2) `check_new_data` — snapshot builder + XCom

This task detects whether there are *new* raw points per dynamic tag group
(e.g. `{device_id="R1", project_name="penicillin", batch_id="batch_78"}`) and,
if so, builds "wide" snapshots.

Internally it:

1. Runs a Flux query to collect the latest `_time` per tag-group over a sliding
   window (`LATEST_LOOKBACK`).
2. For each group, it checks:
   - Recency: the latest timestamp must be younger than `FRESHNESS_SECONDS`.
   - Novelty: the timestamp must be newer than the last one seen for this
     `group_id` (stored in an XCom map).
3. For groups that pass both checks, it queries a narrow time window around the
   latest timestamp and pivots by `observed_property`. This transforms the
   column of variables into a single "wide" row.

For the example above, the resulting snapshot (simplified) looks like:

```json
{
  "group_id": "a1b2c3d4e5f6",
  "snapshot_time": "2025-12-XXT11:56:28.720000Z",
  "device_id": "R1",
  "project_name": "penicillin",
  "batch_id": "batch_78",
  "sugar_feed_rate": 37.0,
  "agitator": 100.0,
  "temperature": 297.99,
  "pH": 6.5097,
  "dissolved_oxygen_concentration": 13.243,
  "vessel_volume": 60531.0,
  "CO2_percent_in_off_gas": 0.91112,
  "oxygen_in_percent_in_off_gas": 0.19388
}
```

Finally, the task pushes two XComs:

- `XCOM_TS_MAP_KEY` (default: `"last_timestamps_map"`): a dictionary
  `{group_id: last_snapshot_time_iso}` used to avoid reprocessing the same
  timestamp.
- `XCOM_SNAPSHOTS_KEY` (default: `"snapshots"`): a list of snapshot objects like
  the one above.

The `ShortCircuitOperator` wrapping this function uses its boolean return value
to decide whether the downstream model tasks should run. If no new snapshots are
found, the DAG run is short-circuited and the remaining tasks are skipped.

### 3) `run_model_predictions` — call models from snapshots

The `call_models_from_snapshots` task consumes these snapshots and produces
model predictions.

1. It pulls the snapshots from XCom:
   - `key=XCOM_SNAPSHOTS_KEY` (e.g. `"snapshots"`)
   - `task_ids=SENSOR_TASK_ID` (e.g. `"check_new_data"`).
2. It queries the Model Registry:
   `GET /{project_id}/list_models/`, obtaining a list of model IDs such as
   `["0002_[R]_penicillin_RF", "0006_[R]_penicillin_M5", ...]`.
3. For each snapshot, it builds a feature vector:
   - If the `FEATURES` env var is set, only those feature names are taken.
   - Otherwise, all numeric fields in the snapshot that are not tags are used
     as features.
4. For each model, it sends a request to:
   `POST /{project_id}/predict/{model_id}`
   with the body:
   ```json
   { "req": { "input_data": { "<feature>": <float>, ... } } }
   ```
5. It parses the response to extract:
   - A scalar prediction value.
   - The model version (either directly from the payload or via a follow-up
     `/metadata/{model_id}` call).

The final XCom structure (`XCOM_PREDICTIONS_KEY`, default `"predictions"`) looks
like:

```json
{
  "a1b2c3d4e5f6": {
    "snapshot_time": "2025-12-XXT11:56:28.720000Z",
    "tags": {
      "project_name": "penicillin",
      "device_id": "R1",
      "batch_id": "batch_78"
    },
    "features": { "...": "..." },
    "predictions": {
      "0002_[R]_penicillin_RF": 12.34,
      "0006_[R]_penicillin_M5": 11.98
    },
    "model_versions": {
      "0002_[R]_penicillin_RF": "1.0",
      "0006_[R]_penicillin_M5": "2.1"
    }
  }
}
```

### 4) `store_predictions` — write back to InfluxDB

The final task, `store_prediction`, reads the predictions from XCom and writes
them into the predictions bucket (`stamm_predictions`) as individual time-series
points:

- Measurement: `PRED_MEASUREMENT` (default: `device_obs`).
- Tags:
  - `device_id`, `project_name`, `batch_id` (propagated from the snapshot).
  - `source = PRED_SOURCE` (default: `soft_sensor`).
  - `observed_property = PRED_OBSERVED_PROPERTY`
    (default: `penicillin_concentration`).
  - `model_id` (logical model key, normalised to lowercase).
  - `version` (model version string).
- Field:
  - `value` = predicted concentration (float).
- Time:
  - `snapshot_time` from the snapshot (same timestamp as the raw data window).

For the RF model in the example above, this results in a line protocol similar
to:

```text
device_obs,device_id=R1,project_name=penicillin,batch_id=batch_78,source=soft_sensor,observed_property=penicillin_concentration,model_id=0002_[r]_penicillin_rf,version=1.0 value=12.34 1763997388720000000
```

Because the predictions share the same device/batch tags and timestamp as the
underlying raw data, they can be seamlessly joined in dashboards and Flux
queries, enabling side-by-side visualisation of process variables and
soft-sensor outputs.
