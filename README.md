# STAMM — Airflow Orchestrator

A production-ready Airflow stack for orchestrating the **STAMM Bioindustry 4.0** workflows.  
This orchestrator automates the complete data-processing and prediction pipeline by connecting to:
- An external **InfluxDB** time-series database (already deployed separately).
- The **Model Registry** (FastAPI service) used for automatic model discovery and prediction.

---

## 📋 Overview

This repository provides everything needed to deploy Airflow 3.x with CeleryExecutor, including:
- Dockerized services (`airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `postgres`, `redis`).
- A one-shot initialization container (`airflow-init`) to migrate the metadata DB and create the admin user.
- A set of pre-mounted directories (`dags/`, `plugins/`, `logs/`, `init/`, `config/`).
- Environment-based configuration for InfluxDB and Model Registry integration.

---

## Prerequisites

Before running this Airflow stack, make sure that the following components are **already installed and running**:

1. **Docker & Docker Compose**
   - Install Docker Engine ≥ 25.0 and Docker Compose v2.
   - Verify installation:
     ```bash
     docker version
     docker compose version
     ```

2. **InfluxDB stack**
   - Deployed from your dedicated GitLab project.
   - The `.env` file in this Airflow project must use the same credentials and bucket names defined in the InfluxDB stack.
   - Ensure the external network `influxdb_default` exists:
     ```bash
     docker network ls | grep influxdb_default
     ```

3. **Model Registry**
   - FastAPI service (port 8000) + Streamlit UI (port 8501).
   - Must be reachable by this Airflow network using its service name (e.g., `http://model-registry:8000`).

---

## Configuration

### Step 1 — Copy and configure the environment file

Create your local environment configuration from the provided template:

```bash
cp .env.example .env
```

Then edit `.env` and replace placeholders with real values.

Key sections:

| Section | Description |
|----------|-------------|
| **Airflow Admin Bootstrap** | Credentials used by the init container to create the default admin user. |
| **InfluxDB** | Must match the organization, token, and buckets created in your InfluxDB stack. |
| **Model Registry** | Defines the FastAPI base URL, project ID, and HTTP parameters for discovery/prediction. |

Example snippet from `.env`:

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

Your InfluxDB and Airflow containers must share the same Docker network:

```bash
docker network inspect influxdb_default
```

If it doesn’t exist, create it manually before launching Airflow:

```bash
docker network create influxdb_default
```

---

### Step 3 — Build and launch Airflow

Build and start the Airflow services:

```bash
docker compose up -d --build
```

Check container status:

```bash
docker compose ps
```

Access the Airflow UI:
```
http://localhost:8080
```
Login with the credentials defined in `.env` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`).

---

### Step 4 — Validate integration

Once Airflow is up:

1. **Check Connections**
   - Open *Admin → Connections* and verify the connection to `influxdb_default` if you configured it via environment variable.
2. **Check Variables**
   - Open *Admin → Variables* and confirm that model registry parameters (if defined) are visible to the DAGs.
3. **Model Registry connectivity**
   - The DAGs will automatically call `GET /{project_id}/list_models/` to discover all available models.

---

## Workflow Overview

The main workflow (`STAMM_Predictions`) consists of **four sequential tasks** representing the typical prediction loop for bioprocess monitoring:

| # | Task ID | Description |
|---|----------|-------------|
| **1** | `check_new_data` | Sensor that queries InfluxDB to detect new data snapshots (based on timestamps). |
| **2** | `call_models_from_snapshots` | Python task that pulls snapshots, discovers available models from the Model Registry, and performs predictions. |
| **3** | `store_prediction` | Inserts model predictions and metadata back into InfluxDB under the prediction bucket. |
| **4** | `validate_and_cleanup` | Optional final step to verify integrity, remove temporary XCom entries, and finalize the run. |

### Simplified DAG Flow

```text
 ┌─────────────────────┐
 │  check_new_data     │
 │  (Sensor)           │
 └──────────┬──────────┘
            │
            ▼
 ┌─────────────────────┐
 │  call_models_from_  │
 │  snapshots          │
 │  (Model Registry)   │
 └──────────┬──────────┘
            │
            ▼
 ┌─────────────────────┐
 │  store_prediction   │
 │  (InfluxDB write)   │
 └──────────┬──────────┘
            │
            ▼
 ┌─────────────────────┐
 │  validate_and_      │
 │  cleanup            │
 └─────────────────────┘
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
├─ config/                   # Optional Airflow configs (cfg/env)
├─ .env.example              # Template for environment variables
├─ docker-compose.yaml       # Core service stack
├─ Dockerfile                # Custom Airflow image build
├─ requirements.txt          # Python dependencies
└─ README.md                 # This documentation
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|----------|---------------|-----|
| `Airflow webserver not reachable` | Container still starting | Wait a few seconds and check `docker compose logs -f airflow-webserver`. |
| `401 Unauthorized` from InfluxDB | Invalid or expired token | Update the token in `.env` and restart containers. |
| `Model discovery returned no models` | Wrong `MODEL_REGISTRY_API_BASE` or project ID | Check the Model Registry service URL and ensure `/list_models/` works. |
| Permission denied writing logs | Missing `AIRFLOW_UID` mapping | Add `AIRFLOW_UID=50000` in `.env` or use your host user ID. |

---

## Maintenance Commands

| Action | Command |
|--------|----------|
| Start services | `docker compose up -d` |
| Stop services | `docker compose down` |
| View logs | `docker compose logs -f airflow-webserver` |
| Rebuild | `docker compose up -d --build` |
| Clean everything (volumes) | `docker compose down -v` |

---