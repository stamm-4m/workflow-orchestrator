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
                 │   check_influxdb_connection    │
                 │ (Verify InfluxDB connectivity) │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │        check_new_data          │
                 │ (Sensor: detect new raw data)  │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │     run_model_predictions      │
                 │ (Discover & execute ML models) │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │      store_predictions         │
                 │ (Write results to InfluxDB)    │
                 └────────────────────────────────┘
```

---

## File Structure

```
stamm-airflow/
├─ dags/                     # DAG definitions (.py)
├─ plugins/                  # Custom operators, hooks, sensors
├─ logs/                     # Airflow logs (gitignored)
├─ init/
│   └─ airflow/
│       └─ create_admin_and_conns.sh
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