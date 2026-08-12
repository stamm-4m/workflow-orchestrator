"""
postgres.py — Model Registry API-backed data access for STAMM Airflow callables.

Replaces direct psycopg2/PostgresHook access to the model-registry database.
Everything here goes through the Model Registry HTTP API (tasks/registry_client.py),
authenticated with the shared service account — no direct DB connection, so this
works whether Airflow and model-registry sit on the same host or on separate VMs.

Functions exposed to the DAG:
  - check_db_connection():      PythonSensor callable — True when the API is reachable/authenticated
  - wait_for_new_data():        PythonSensor callable — True when there's a sensor reading newer than last_processed_time
  - build_snapshot():           PythonOperator callable — builds wide snapshot from latest readings
  - store_prediction():         PythonOperator callable — writes predictions via POST /api/v1/predictions/
  - show_prediction_summary():  PythonOperator callable — logs a readable result for the Airflow UI
  - check_experiment_active():  ShortCircuitOperator callable — True if run is still open.
                                 Not wired into the current one-shot DAG; kept for when
                                 the re-trigger loop comes back (see deployment_soft_sensors.py).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.utils.log.logging_mixin import LoggingMixin

from tasks import registry_client
from tasks.prediction import _select_config_by_project_name

log = LoggingMixin().log

# XCom keys / task IDs
XCOM_SNAPSHOTS_KEY   = "snapshots"
XCOM_SNAPSHOT_TIME   = "snapshot_time"
XCOM_PREDICTIONS_KEY = "predictions"
MODEL_TASK_ID        = "call_models_from_snapshots"

# How stale can the latest reading be (seconds) before we ignore it
FRESHNESS_SECONDS = int(os.getenv("FRESHNESS_SECONDS", "120"))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _conf(ctx: dict) -> dict:
    return ctx["dag_run"].conf or {}


def _since(conf: dict, lookback_seconds: Optional[int] = None) -> str:
    """
    Lower time bound for sensor/actuator queries.

    On a re-trigger, last_processed_time pins this to "since the last snapshot".
    On the FIRST trigger there's nothing to pin to yet — defaulting to the
    epoch would make the API page back through the run's entire history
    (the run-scoped endpoints only support ascending order + a row limit, so
    with more accumulated rows than the page size, "max(time) of the page"
    silently returns old data instead of the latest). Default to "now minus
    a lookback window" instead, matching the freshness threshold: nothing
    older than that would pass build_snapshot's freshness check anyway.

    lookback_seconds should be the model's own cadence (see
    _model_freshness_seconds) — a model that only samples every 12 minutes
    needs a lookback of more than a couple minutes or the first trigger will
    never see it as "new data". Falls back to FRESHNESS_SECONDS if the
    caller doesn't know the model yet.
    """
    explicit = conf.get("last_processed_time")
    if explicit:
        return explicit
    window = lookback_seconds if lookback_seconds is not None else FRESHNESS_SECONDS
    lookback = datetime.now(timezone.utc) - timedelta(seconds=window)
    return lookback.isoformat()


def _variable_maps() -> tuple[Dict[str, str], Dict[str, str]]:
    """id -> variable name, for sensors and actuators (small catalogs, fetched in full)."""
    sensors = registry_client.get_all_pages("/api/v1/sensors/")
    actuators = registry_client.get_all_pages("/api/v1/actuators/")
    sensor_var = {s["id"]: s["variable"] for s in sensors if s.get("variable")}
    actuator_var = {a["id"]: a["variable"] for a in actuators if a.get("variable")}
    return sensor_var, actuator_var


def _experiment_end_time(experiment_id: str) -> Optional[datetime]:
    """Fetch experiments.end_time (the planned end, set at creation) — None
    if unset or the experiment can't be fetched."""
    resp = registry_client.get(f"/api/v1/experiments/{experiment_id}")
    if resp.status_code != 200:
        return None
    end_time = resp.json().get("end_time")
    if not end_time:
        return None
    try:
        dt = datetime.fromisoformat(str(end_time).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _model_freshness_seconds(project_id: str, model_id: str) -> int:
    """How old a reading is allowed to be, derived from the official model's
    declared input_time_interval (e.g. "one measurement every 12 minutes")
    instead of a single hardcoded value for every model/project.

    Applies a 1.5x margin over the declared interval to tolerate normal
    jitter (a sensor reporting a few seconds late shouldn't be treated as
    "the bioreactor stopped sending data"). Falls back to FRESHNESS_SECONDS
    if the metadata is missing or malformed.
    """
    unit_seconds = {
        "second": 1, "seconds": 1, "sec": 1,
        "minute": 60, "minutes": 60, "min": 60,
        "hour": 3600, "hours": 3600, "hr": 3600,
        "day": 86400, "days": 86400,
    }
    try:
        resp = registry_client.get(f"/{project_id}/metadata/{model_id}")
        if resp.status_code != 200:
            raise ValueError(f"metadata HTTP {resp.status_code}")
        interval = (
            resp.json()
            .get("model_description", {})
            .get("input_time_interval", {})
            .get("time_interval", {})
        )
        value = float(interval["value"])
        unit = unit_seconds[str(interval["unit"]).strip().lower()]
        return int(value * unit * 1.5)
    except Exception as exc:
        log.warning(
            f"_model_freshness_seconds: could not read input_time_interval for "
            f"{project_id}/{model_id} ({exc}) — falling back to FRESHNESS_SECONDS={FRESHNESS_SECONDS}"
        )
        return FRESHNESS_SECONDS


def _soft_sensor_map(project_id: str) -> Dict[str, str]:
    """model_id (extracted from path_metadata) -> soft_sensor_id, for one project."""
    links = registry_client.get_all_pages("/api/v1/project_soft_sensors/")
    soft_sensors = {s["id"]: s for s in registry_client.get_all_pages("/api/v1/soft_sensors/")}

    ss_map: Dict[str, str] = {}
    for link in links:
        if link.get("project_id") != project_id:
            continue
        ss = soft_sensors.get(link.get("soft_sensor_id"))
        if not ss:
            continue
        path = ss.get("path_metadata") or ""
        if "/models/" in path:
            model_id_in_path = path.split("/models/")[1].split("/")[0]
            ss_map[model_id_in_path] = ss["id"]
    return ss_map


# ---------------------------------------------------------------------------
# 1) Health check
# ---------------------------------------------------------------------------

def check_db_connection() -> bool:
    """
    PythonSensor callable.
    Returns True when the Model Registry API is reachable and our service
    account can authenticate against it.
    """
    try:
        headers = registry_client.auth_headers()
        if "Authorization" not in headers:
            log.warning("check_db_connection: could not obtain an auth token.")
            return False
        log.info("Model Registry API reachable and authenticated.")
        return True
    except Exception as exc:
        log.warning(f"check_db_connection: {exc}")
        return False


# ---------------------------------------------------------------------------
# 2) Wait for new sensor data (replaces the SqlSensor)
# ---------------------------------------------------------------------------

def wait_for_new_data() -> bool:
    """
    PythonSensor callable.
    Returns True as soon as sensor_readings has a row newer than
    last_processed_time for this run_id. Gives up early (skips the rest of
    the DAG, doesn't fail it) once the experiment's planned end_time has
    passed — no point polling for data from an experiment that's already over.
    """
    ctx = get_current_context()
    conf = _conf(ctx)
    run_id = conf["run_id"]

    _, _, official_model_id = _select_config_by_project_name(conf.get("project_name", ""))
    lookback = (
        _model_freshness_seconds(conf.get("project_id", ""), official_model_id)
        if official_model_id else None
    )
    since = _since(conf, lookback)

    experiment_id = conf.get("experiment_id")
    if experiment_id:
        end_time = _experiment_end_time(experiment_id)
        if end_time and datetime.now(timezone.utc) > end_time:
            raise AirflowSkipException(
                f"[{run_id}] experiment {experiment_id} ended at {end_time.isoformat()} "
                f"— no longer waiting for data."
            )

    resp = registry_client.get(f"/api/v1/runs/{run_id}/sensor_readings", params={"since": since, "limit": 1})
    if resp.status_code != 200:
        log.warning(f"wait_for_new_data: HTTP {resp.status_code} — {resp.text}")
        return False

    found = bool(resp.json())
    log.info(f"[{run_id}] wait_for_new_data: {'new data found' if found else 'no new data yet'} (since={since})")
    return found


# ---------------------------------------------------------------------------
# 3) Build wide snapshot from latest sensor + actuator readings
# ---------------------------------------------------------------------------

def build_snapshot() -> None:
    """
    PythonOperator callable.

    Reads dag_run.conf for:
      - run_id              (UUID of the active run)
      - experiment_id       (UUID of the parent experiment)
      - project_id          (UUID of the project)
      - project_name        (str, used to select model registry project)
      - last_processed_time (ISO timestamp; empty on first trigger)

    Fetches sensor_readings/actuator_states since last_processed_time via the
    Model Registry API, builds a "wide" snapshot dict keyed by variable name,
    and pushes it to XCom.

    XCom out:
      key "snapshots"      → list[ {run_id, project_name, <variable>: value, ...} ]
      key "snapshot_time"  → ISO timestamp of the snapshot (used by re_trigger)
    """
    ctx = get_current_context()
    ti  = ctx["ti"]
    conf = _conf(ctx)

    run_id             = conf["run_id"]
    experiment_id      = conf["experiment_id"]
    project_id         = conf["project_id"]
    project_name       = conf.get("project_name", "")

    # Freshness/lookback threshold comes from the official model's declared
    # input_time_interval (e.g. "one measurement every 12 minutes") — not a
    # single hardcoded value for every model, since different models expect
    # different sampling cadences.
    _, _, official_model_id = _select_config_by_project_name(project_name)
    freshness_seconds = (
        _model_freshness_seconds(project_id, official_model_id) if official_model_id else FRESHNESS_SECONDS
    )
    since = _since(conf, freshness_seconds)

    sensor_resp = registry_client.get(f"/api/v1/runs/{run_id}/sensor_readings", params={"since": since, "limit": 5000})
    sensor_resp.raise_for_status()
    sensor_readings = sensor_resp.json()

    if not sensor_readings:
        raise ValueError(f"build_snapshot: no sensor readings found for run_id={run_id}")

    snapshot_time_str = max(r["time"] for r in sensor_readings)
    snapshot_time = datetime.fromisoformat(snapshot_time_str.replace("Z", "+00:00"))

    # Freshness guard: skip stale experiments that are still "running".
    now_utc = datetime.now(timezone.utc)
    st_aware = snapshot_time if snapshot_time.tzinfo else snapshot_time.replace(tzinfo=timezone.utc)
    age_s = (now_utc - st_aware).total_seconds()
    if age_s > freshness_seconds:
        raise ValueError(
            f"build_snapshot: latest reading is {age_s:.0f}s old "
            f"(threshold={freshness_seconds}s, from model input_time_interval). "
            f"Is the bioreactor still sending data?"
        )

    actuator_resp = registry_client.get(f"/api/v1/runs/{run_id}/actuator_states", params={"since": since, "limit": 5000})
    actuator_resp.raise_for_status()
    actuator_readings = actuator_resp.json()

    sensor_var, actuator_var = _variable_maps()

    # Sensor rows exactly at the snapshot timestamp (mirrors the old "= snapshot_time" SQL filter)
    latest_sensor_rows = [r for r in sensor_readings if r["time"] == snapshot_time_str]

    # Most recent actuator state per variable, at or before snapshot_time
    latest_actuator_by_var: Dict[str, dict] = {}
    for r in actuator_readings:
        if r["time"] > snapshot_time_str:
            continue
        var = actuator_var.get(r["actuator_id"])
        if not var:
            continue
        if var not in latest_actuator_by_var or r["time"] > latest_actuator_by_var[var]["time"]:
            latest_actuator_by_var[var] = r

    # Build the wide dict
    snapshot: Dict[str, Any] = {
        "group_id":      run_id,          # keeps prediction.py compatible
        "run_id":        run_id,
        "experiment_id": experiment_id,
        "project_id":    project_id,
        "project_name":  project_name,
        "snapshot_time": snapshot_time_str,
    }
    for row in latest_sensor_rows:
        var = sensor_var.get(row["sensor_id"])
        if var:
            snapshot[var] = row["value"]
    for var, row in latest_actuator_by_var.items():
        snapshot.setdefault(var, row["value"])  # sensor wins on collision

    ti.xcom_push(key=XCOM_SNAPSHOTS_KEY, value=[snapshot])
    ti.xcom_push(key=XCOM_SNAPSHOT_TIME,  value=snapshot_time_str)

    log.info(
        f"[{run_id}] snapshot built at {snapshot_time_str} "
        f"— {len(latest_sensor_rows)} sensor(s), {len(latest_actuator_by_var)} actuator(s) "
        f"— fields: {[k for k in snapshot if k not in ('group_id','run_id','experiment_id','project_id','project_name','snapshot_time')]}"
    )


# ---------------------------------------------------------------------------
# 4) Write predictions via the Model Registry API
# ---------------------------------------------------------------------------

def store_prediction() -> None:
    """
    PythonOperator callable.

    Reads XCom predictions from call_models_from_snapshots and POSTs one row
    per (run_id, soft_sensor_id, time) to /api/v1/predictions/.

    soft_sensor_id is resolved by matching the model_key returned by the
    Model Registry against the path_metadata column of soft_sensors:
        "projects/<proj>/models/<model_id>/metadata.yaml"
    """
    ctx  = get_current_context()
    ti   = ctx["ti"]
    conf = _conf(ctx)

    predictions_by_group: Dict[str, Any] = (
        ti.xcom_pull(task_ids=MODEL_TASK_ID, key=XCOM_PREDICTIONS_KEY) or {}
    )

    if not predictions_by_group:
        log.warning("store_prediction: no predictions found in XCom.")
        return

    wrote = 0
    summary: list[Dict[str, Any]] = []
    ss_map_cache: Dict[str, Dict[str, str]] = {}

    for group_id, payload in predictions_by_group.items():
        if not isinstance(payload, dict):
            log.warning(f"[{group_id}] invalid payload shape → skip.")
            continue

        tags       = payload.get("tags") or {}
        run_id     = tags.get("run_id") or group_id
        project_id = tags.get("project_id") or conf.get("project_id")
        snap_time  = payload.get("snapshot_time")
        preds      = payload.get("predictions") or {}

        if not snap_time or not preds:
            log.warning(f"[{group_id}] missing snapshot_time or empty predictions → skip.")
            continue
        if not project_id:
            log.warning(f"[{group_id}] no project_id available → skip.")
            continue

        if project_id not in ss_map_cache:
            ss_map_cache[project_id] = _soft_sensor_map(project_id)
        ss_map = ss_map_cache[project_id]

        for model_key, value in preds.items():
            if value is None:
                log.warning(f"[{run_id}] {model_key}: None value → skip.")
                continue
            try:
                val = float(value)
            except (TypeError, ValueError):
                log.warning(f"[{run_id}] {model_key}: cannot cast {value!r} to float → skip.")
                continue

            # Exact match first, then substring fallback
            soft_sensor_id: Optional[str] = ss_map.get(model_key)
            if not soft_sensor_id:
                for path_key, ss_id in ss_map.items():
                    if model_key in path_key or path_key in model_key:
                        soft_sensor_id = ss_id
                        break

            if not soft_sensor_id:
                log.warning(
                    f"[{run_id}] no soft_sensor_id for model_key={model_key!r}. "
                    f"Available: {list(ss_map.keys())}"
                )
                continue

            resp = registry_client.post(
                "/api/v1/predictions/",
                json_body={"time": snap_time, "run_id": run_id, "soft_sensor_id": soft_sensor_id, "value": val},
            )
            if resp.status_code == 201:
                wrote += 1
                summary.append({"run_id": run_id, "model": model_key, "value": val, "time": snap_time, "stored": True})
            elif resp.status_code == 409:
                log.info(f"[{run_id}] {model_key}: prediction already stored — skip.")
                summary.append({"run_id": run_id, "model": model_key, "value": val, "time": snap_time, "stored": False, "reason": "duplicate"})
            else:
                log.error(f"[{run_id}] {model_key}: failed to store prediction: HTTP {resp.status_code} {resp.text}")
                summary.append({"run_id": run_id, "model": model_key, "value": val, "time": snap_time, "stored": False, "reason": f"HTTP {resp.status_code}"})

    ti.xcom_push(key="stored_summary", value=summary)
    log.info(f"store_prediction: wrote {wrote} row(s) via API.")


# ---------------------------------------------------------------------------
# 5) Print a clean, human-readable result — the task to look at in the
#    Airflow UI to see what this DAG run actually predicted.
# ---------------------------------------------------------------------------

def show_prediction_summary() -> None:
    """PythonOperator callable. Logs one line per prediction this run stored."""
    ctx = get_current_context()
    ti = ctx["ti"]

    summary = ti.xcom_pull(task_ids="store_prediction", key="stored_summary") or []

    if not summary:
        log.info("RESULT: no predictions were stored this run.")
        return

    log.info("=" * 60)
    log.info("PREDICTION RESULT")
    for row in summary:
        if row["stored"]:
            log.info(
                f"  run_id={row['run_id']}  model={row['model']}  "
                f"value={row['value']:.6f}  time={row['time']}  -> stored"
            )
        else:
            log.info(
                f"  run_id={row['run_id']}  model={row['model']}  "
                f"value={row['value']:.6f}  time={row['time']}  -> NOT stored ({row['reason']})"
            )
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# 6) Check if the experiment is still active (used by ShortCircuitOperator)
# ---------------------------------------------------------------------------

def check_experiment_active(**context) -> bool:
    """
    ShortCircuitOperator callable.

    Returns True  → experiment still running, re-trigger the DAG.
    Returns False → experiment ended, stop the chain quietly.
    """
    run_id = context["dag_run"].conf.get("run_id")
    if not run_id:
        log.warning("check_experiment_active: no run_id in conf.")
        return False

    run_resp = registry_client.get(f"/api/v1/runs/{run_id}")
    if run_resp.status_code != 200:
        log.warning(f"check_experiment_active: could not fetch run {run_id}: HTTP {run_resp.status_code}")
        return False
    run = run_resp.json()

    if run.get("end_time"):
        log.info(f"[{run_id}] run has ended — stopping DAG chain.")
        return False

    experiment_id = run.get("experiment_id")
    exp_resp = registry_client.get(f"/api/v1/experiments/{experiment_id}")
    if exp_resp.status_code != 200:
        log.warning(f"check_experiment_active: could not fetch experiment {experiment_id}: HTTP {exp_resp.status_code}")
        return False
    experiment = exp_resp.json()

    active = experiment.get("status") == "running"
    if active:
        log.info(f"[{run_id}] experiment still active — will re-trigger.")
    else:
        log.info(f"[{run_id}] experiment ended (status={experiment.get('status')}) — stopping DAG chain.")
    return active
