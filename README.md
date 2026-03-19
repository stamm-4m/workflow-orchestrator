# STAMM - Airflow Orchestrator

A production-ready Airflow stack for orchestrating the **STAMM** workflows.  
This orchestrator automates the complete data-processing and prediction pipeline by connecting to:
- An external **InfluxDB** time-series database (already deployed separately).
- The **Model Registry** (FastAPI service) used for automatic model discovery and prediction.

---

## Overview

This repository provides everything required to deploy a **modern, reproducible Airflow stack** with **CeleryExecutor**, including:

- Dockerized services: `airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `postgres`, `redis`
- Automatic initialization: one-shot `airflow-init` container for database migration and admin creation
- Pre-mounted directories: `dags/`, `plugins/`, `logs/`, `init/`, `config/`
- Environment-based integration for **InfluxDB** and **Model Registry**

---
## Technical Environment

The Airflow orchestrator is based on **Apache Airflow 3.0.6**, running with **Python 3.12.11**.

## Prerequisites

Before running this Airflow stack, ensure that the following components are **installed and running**:

### 1. Docker & Docker Compose
```bash
docker version
docker compose version
```

### 2. InfluxDB stack
- Deployed from your dedicated GitLab project.
- The `.env` file here must match your InfluxDB credentials and bucket names.
- Confirm that the external network exists:
  ```bash
  docker network ls | grep influxdb_default
  ```

### 3. Model Registry
- FastAPI service (port **8000**) + Streamlit UI (port **8501**)
- Must be reachable within the Airflow network, e.g.:
  ```
  http://model-registry:8000
  ```

---

## Configuration

### Step 1 - Copy and configure the environment file
```bash
cp .env.example .env
```
Then edit `.env` and replace placeholders with real values.

| Section | Description |
|----------|-------------|
| **Airflow Admin Bootstrap** | Admin credentials created by the init container. |
| **InfluxDB** | Must match organization, token, and buckets from the external InfluxDB stack. |
| **Model Registry** | FastAPI configuration for model discovery and prediction. |

**Example snippet:**
```bash
# Airflow
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASSWORD=airflow
AIRFLOW_ADMIN_EMAIL=admin@stamm.local

# InfluxDB
INFLUXDB_URL=http://influxdb:8086
INFLUXDB_ORG=stamm_org
INFLUXDB_TOKEN=YOUR_SECURE_TOKEN
INFLUXDB_BUCKET_RAW=stamm_raw
INFLUXDB_BUCKET_METADATA=stamm_metadata
INFLUXDB_BUCKET_PRED=stamm_predictions

# Model Registry
MODEL_REGISTRY_API_BASE=http://model-registry:8000
MODEL_REGISTRY_PROJECT_ID=P0001
MODEL_REGISTRY_TIMEOUT_SECONDS=30
MODEL_REGISTRY_VERIFY_TLS=true
```

---

### Step 2 — Verify the external network connection
```bash
docker network inspect influxdb_default
```
If it doesn’t exist:
```bash
docker network create influxdb_default
```

---

### Step 3 — Build and launch Airflow
```bash
docker compose up -d --build
docker compose ps
```

Access the UI:
```
http://localhost:8080
```
Login with the credentials defined in `.env`.

---

### Step 4 — Validate integration
1. **Connections** → Verify `influxdb_default`
2. **Variables** → Check Model Registry parameters
3. **Model Registry connectivity** → Ensure `/list_models/` endpoint works

---

## Workflow Overview

The main DAG (`STAMM_Predictions`) orchestrates the **end-to-end ML prediction cycle** for bioprocess monitoring.  
It ensures full automation — from checking system readiness to generating and storing model predictions.

| # | Task ID | Description |
|---|----------|-------------|
| **1** | `check_influxdb_connection` | Verifies that the InfluxDB instance is reachable and credentials are valid. |
| **2** | `check_new_data` | Sensor that detects newly arrived raw data points based on timestamps in the raw bucket. |
| **3** | `run_model_predictions` | Calls the Model Registry, retrieves active models, and performs predictions on the latest data snapshot. |
| **4** | `store_predictions` | Writes generated predictions and metadata back into InfluxDB (prediction bucket). |

---

### Simplified DAG Flow

```text
                 ┌────────────────────────────────┐
                 │    check_influxdb_connection   │
                 │          (Health check)        │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │         check_new_data         │
                 │    (snapshot builder + XCom)   │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │      run_model_predictions     │
                 │  (Call models from snapshots)  │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │       store_predictions        │
                 │    (Write back to InfluxDB)    │
                 └────────────────────────────────┘
```

---

To make the DAG behaviour more concrete, the following example walks through a
single execution using a batch of bioreactor data already stored in InfluxDB.

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

#### 1) `check_influxdb_connection` — health check

- Opens an `InfluxDBClient` and calls `ping()`.
- If the ping succeeds, the DAG continues.
- If it fails, the sensor keeps retrying within the configured timeout. No XCom
  is produced here; it is purely a connectivity gate.

#### 2) `check_new_data` — snapshot builder + XCom

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

#### 3) `run_model_predictions` — call models from snapshots

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

#### 4) `store_predictions` — write back to InfluxDB

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

---

## File Structure

```
stamm-airflow/
├─ dags/                     # DAG definitions (.py)
├─ plugins/                  # Custom operators, hooks, sensors
├─ logs/                     # Airflow logs (gitignored)
├─ config/                   # Optional Airflow configs
├─ .env.example              # Template for environment variables
├─ docker-compose.yaml       # Main service stack
├─ Dockerfile                # Custom Airflow image
├─ requirements.txt          # Python dependencies
└─ README.md                 # This documentation
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|----------|---------------|-----|
| `Webserver not reachable` | Container still initializing | Wait a few seconds and check logs: `docker compose logs -f airflow-webserver`. |
| `401 Unauthorized` from InfluxDB | Invalid or expired token | Update token in `.env` and restart services. |
| `Model discovery returned no models` | Wrong API base or project ID | Verify the `MODEL_REGISTRY_API_BASE` and `/list_models/` endpoint. |
| `Permission denied writing logs` | Missing `AIRFLOW_UID` mapping | Add `AIRFLOW_UID=50000` in `.env` or use your host user ID. |

---

## Maintenance Commands

| Action | Command |
|--------|----------|
| Start services | `docker compose up -d` |
| Stop services | `docker compose down` |
| View logs | `docker compose logs -f airflow-webserver` |
| Rebuild images | `docker compose up -d --build` |
| Clean all (volumes) | `docker compose down -v` |

---

## Summary

The STAMM Airflow Orchestrator provides a robust and modular framework for automating data-driven prediction pipelines.
It integrates with InfluxDB for real-time data ingestion and storage, and with a Model Registry for dynamic model discovery and execution.

Key Highlights:

- Full ML lifecycle automation: data detection → model prediction → result storage

- Environment-driven configuration for portability

- Reproducible deployment using Docker Compose

- Clear task flow for monitoring and scaling

This orchestrator enables transparent, scalable, and maintainable industrial AI workflows within the Bioindustry 4.0 ecosystem.

---

### 📬 Contact

For questions, contact Alexander Astudillo at jairo.astudillo-lagos@inrae.fr

---