"""
postgres.py — Model Registry API-backed data access for STAMM Airflow callables.

Replaces direct psycopg2/PostgresHook access to the model-registry database.
Everything here goes through the Model Registry HTTP API (tasks/registry_client.py),
authenticated with the shared service account — no direct DB connection, so this
works whether Airflow and model-registry sit on the same host or on separate VMs.

Functions exposed to the DAG:
  - check_db_connection():      PythonSensor callable — True when the API is reachable/authenticated
  - wait_for_new_data():        PythonSensor callable — True when there's a sensor reading newer than last_processed_time
  - build_snapshot():           PythonOperator callable — builds wide snapshot (+ lag history) from readings
  - store_prediction():         PythonOperator callable — writes predictions via POST /api/v1/predictions/
  - show_prediction_summary():  PythonOperator callable — logs a readable result for the Airflow UI
  - trigger_next_cycle():       PythonOperator callable — re-triggers this DAG for the same run_id so
                                 predictions keep happening for the experiment's whole start_time..end_time
                                 window, not just once. Stops itself once end_time has passed.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.utils.log.logging_mixin import LoggingMixin

from tasks import registry_client
from tasks.prediction import (
    _select_config_by_project_name,
    _model_interval_seconds,
    _fetch_models_catalog,
)

log = LoggingMixin().log

# Airflow 3.0 workers have no direct DB/ORM access (isolated task execution
# — see AIRFLOW__CORE__EXECUTION_API_SERVER_URL / the "Task Execution
# Interface"), so trigger_next_cycle re-triggers this DAG the same way an
# external caller would: over Airflow's own REST API, self-referentially.
AIRFLOW_SELF_API_BASE = os.getenv("AIRFLOW_SELF_API_BASE", "http://airflow-webserver:8080")
AIRFLOW_ADMIN_USER = os.getenv("AIRFLOW_ADMIN_USER", "")
AIRFLOW_ADMIN_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "")

# XCom keys / task IDs
XCOM_SNAPSHOTS_KEY   = "snapshots"
XCOM_HISTORY_KEY     = "history"
XCOM_SNAPSHOT_TIME   = "snapshot_time"
XCOM_PREDICTIONS_KEY = "predictions"
MODEL_TASK_ID        = "call_models_from_snapshots"
DAG_ID               = "deployment_soft_sensors"

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

    lookback_seconds should cover the shortest cadence among the models
    attached to this run (see _min_freshness_seconds) plus however far back
    any of them needs lagged history — a model sampling every 12 minutes
    needs a lookback of more than a couple minutes or the first trigger will
    never see it as "new data". Falls back to FRESHNESS_SECONDS if the
    caller doesn't know the models yet.
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


def _min_freshness_seconds(model_ids: List[str], catalog: Dict[str, dict]) -> int:
    """How old a reading is allowed to be, derived from the SHORTEST declared
    input_time_interval across every model attached to this run — with
    several models at different cadences, the most demanding one sets the
    pace so none of them go stale. 1.5x margin over the interval tolerates
    normal jitter. Falls back to FRESHNESS_SECONDS if nothing resolves."""
    intervals = [
        secs for mid in model_ids
        if (row := catalog.get(mid)) and (secs := _model_interval_seconds(row))
    ]
    if not intervals:
        return FRESHNESS_SECONDS
    return int(min(intervals) * 1.5)


def _max_lag_seconds(model_ids: List[str], catalog: Dict[str, dict]) -> int:
    """Largest lag*interval declared across every attached model's features —
    how far back build_snapshot needs to fetch raw readings to satisfy any
    lagged feature. 0 if nothing uses lag (the common case today)."""
    max_needed = 0
    for mid in model_ids:
        row = catalog.get(mid)
        if not row:
            continue
        interval_s = _model_interval_seconds(row) or 0
        for feat in (row.get("inputs") or {}).get("features", []) or []:
            lag = feat.get("lag") or 0
            if lag and interval_s:
                max_needed = max(max_needed, int(lag) * interval_s)
    return max_needed


def _model_row_ids() -> Dict[str, str]:
    """models.slug -> models.id. predictions.model_id is a UUID FK to
    models.id, but the model_key we get back from call_models_from_snapshots
    is the human-readable slug (e.g. "0001_python_penicillin_RF"), so this
    resolves one to the other before writing a prediction."""
    return {slug: row["id"] for slug, row in _fetch_models_catalog().items()}


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

    experiment_id = conf.get("experiment_id")
    if experiment_id:
        end_time = _experiment_end_time(experiment_id)
        if end_time and datetime.now(timezone.utc) > end_time:
            raise AirflowSkipException(
                f"[{run_id}] experiment {experiment_id} ended at {end_time.isoformat()} "
                f"— no longer waiting for data."
            )

    _, model_ids = _select_config_by_project_name(conf.get("project_name", ""), conf)
    lookback = None
    if model_ids:
        catalog = _fetch_models_catalog()
        lookback = _min_freshness_seconds(model_ids, catalog) + _max_lag_seconds(model_ids, catalog)
    since = _since(conf, lookback)

    resp = registry_client.get(f"/api/v1/runs/{run_id}/sensor_readings", params={"since": since, "limit": 1})
    if resp.status_code != 200:
        log.warning(f"wait_for_new_data: HTTP {resp.status_code} — {resp.text}")
        return False

    found = bool(resp.json())
    log.info(f"[{run_id}] wait_for_new_data: {'new data found' if found else 'no new data yet'} (since={since})")
    return found


# ---------------------------------------------------------------------------
# 3) Build wide snapshot (+ lag history) from sensor + actuator readings
# ---------------------------------------------------------------------------

def build_snapshot() -> None:
    """
    PythonOperator callable.

    Reads dag_run.conf for:
      - run_id              (UUID of the active run)
      - experiment_id       (UUID of the parent experiment)
      - project_id          (UUID of the project)
      - project_name        (str, used to select model registry project)
      - model_ids           (list[str], models.slug — every model attached to the experiment)
      - last_processed_time (ISO timestamp; empty on first trigger)

    Fetches sensor_readings/actuator_states since last_processed_time via the
    Model Registry API, builds a "wide" snapshot dict keyed by variable name
    (the latest value per variable), plus a raw history list wide enough to
    cover any lagged feature any attached model declares, and pushes both to
    XCom.

    XCom out:
      key "snapshots"     → list[ {run_id, project_name, <variable>: value, ...} ]
      key "history"       → list[ {time, variable, value} ] — only populated when
                             at least one attached model declares lag > 0
      key "snapshot_time" → ISO timestamp of the snapshot (used by trigger_next_cycle)
    """
    ctx = get_current_context()
    ti  = ctx["ti"]
    conf = _conf(ctx)

    run_id             = conf["run_id"]
    experiment_id      = conf["experiment_id"]
    project_id         = conf["project_id"]
    project_name       = conf.get("project_name", "")

    # Freshness/lookback threshold comes from the SHORTEST declared
    # input_time_interval across every model attached to this run — not a
    # single hardcoded value for every model, since different models expect
    # different sampling cadences. The lookback window is widened further
    # when any attached model needs lagged (historical) features.
    _, model_ids = _select_config_by_project_name(project_name, conf)
    catalog = _fetch_models_catalog() if model_ids else {}
    freshness_seconds = _min_freshness_seconds(model_ids, catalog) if model_ids else FRESHNESS_SECONDS
    max_lag_seconds = _max_lag_seconds(model_ids, catalog) if model_ids else 0
    since = _since(conf, freshness_seconds + max_lag_seconds)

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
            f"(threshold={freshness_seconds}s, from models' input_time_interval). "
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

    # Build the wide dict (lag=0 view — "current" value per variable)
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

    # Raw history — only worth building when some attached model actually
    # needs lagged features; otherwise it would just duplicate `snapshot`.
    history: List[Dict[str, Any]] = []
    if max_lag_seconds:
        for r in sensor_readings:
            var = sensor_var.get(r["sensor_id"])
            if var:
                history.append({"time": r["time"], "variable": var, "value": r["value"]})
        for r in actuator_readings:
            var = actuator_var.get(r["actuator_id"])
            if var:
                history.append({"time": r["time"], "variable": var, "value": r["value"]})

    ti.xcom_push(key=XCOM_SNAPSHOTS_KEY, value=[snapshot])
    ti.xcom_push(key=XCOM_HISTORY_KEY,   value=history)
    ti.xcom_push(key=XCOM_SNAPSHOT_TIME, value=snapshot_time_str)

    log.info(
        f"[{run_id}] snapshot built at {snapshot_time_str} "
        f"— {len(latest_sensor_rows)} sensor(s), {len(latest_actuator_by_var)} actuator(s), "
        f"{len(history)} history row(s) — "
        f"fields: {[k for k in snapshot if k not in ('group_id','run_id','experiment_id','project_id','project_name','snapshot_time')]}"
    )


# ---------------------------------------------------------------------------
# 4) Write predictions via the Model Registry API
# ---------------------------------------------------------------------------

def store_prediction() -> None:
    """
    PythonOperator callable.

    Reads XCom predictions from call_models_from_snapshots and POSTs one row
    per (run_id, model_id, time) to /api/v1/predictions/ — one row per model
    attached to the experiment, since call_models_from_snapshots now runs
    every one of them, not just a single "official" model.

    model_id is resolved by matching the model_key (models.slug) returned by
    the Model Registry against the models catalog (see _model_row_ids()).
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
    model_ids = _model_row_ids()

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

        for model_key, value in preds.items():
            if value is None:
                log.warning(f"[{run_id}] {model_key}: None value → skip.")
                continue
            try:
                val = float(value)
            except (TypeError, ValueError):
                log.warning(f"[{run_id}] {model_key}: cannot cast {value!r} to float → skip.")
                continue

            model_row_id: Optional[str] = model_ids.get(model_key)
            if not model_row_id:
                log.warning(
                    f"[{run_id}] no models row for slug={model_key!r}. "
                    f"Available: {list(model_ids.keys())}"
                )
                continue

            resp = registry_client.post(
                "/api/v1/predictions/",
                json_body={"time": snap_time, "run_id": run_id, "model_id": model_row_id, "value": val},
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
# 6) Keep predicting for the experiment's whole duration
# ---------------------------------------------------------------------------

def _airflow_self_token() -> Optional[str]:
    if not AIRFLOW_ADMIN_USER or not AIRFLOW_ADMIN_PASSWORD:
        log.error("trigger_next_cycle: AIRFLOW_ADMIN_USER/AIRFLOW_ADMIN_PASSWORD not set — cannot self-trigger.")
        return None
    try:
        resp = requests.post(
            f"{AIRFLOW_SELF_API_BASE}/auth/token",
            json={"username": AIRFLOW_ADMIN_USER, "password": AIRFLOW_ADMIN_PASSWORD},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as exc:
        log.error(f"trigger_next_cycle: failed to obtain a self-token: {exc}")
        return None


def trigger_next_cycle() -> None:
    """
    PythonOperator callable. Last task in the chain.

    Re-triggers this same DAG for the same run_id so the next batch of
    sensor data (per the fastest attached model's own cadence) gets its own
    prediction cycle — this is what keeps an experiment created for e.g. 2
    hours predicting for the full 2 hours instead of stopping after one shot.

    Airflow 3.0 workers have no direct DB access (see AIRFLOW_SELF_API_BASE
    above), so this goes over Airflow's own REST API — the same endpoint
    model-registry itself calls to start the first cycle.

    Uses a fresh run_id derived from the ORIGINAL run_id + a timestamp (never
    from dag_run.run_id) so it can't grow unboundedly across cycles like the
    old re-trigger design did. Passes last_processed_time forward so the next
    cycle only looks for data newer than what this cycle already used.

    Stops the chain (does nothing) once the experiment's end_time has passed
    — wait_for_new_data's AirflowSkipException already stops most chains
    earlier than this; this is the same check for the case where a full
    cycle completes right as end_time is reached.
    """
    ctx = get_current_context()
    ti = ctx["ti"]
    conf = _conf(ctx)
    run_id = conf["run_id"]
    experiment_id = conf.get("experiment_id")

    if experiment_id:
        end_time = _experiment_end_time(experiment_id)
        if end_time and datetime.now(timezone.utc) > end_time:
            log.info(f"[{run_id}] experiment {experiment_id} ended at {end_time.isoformat()} — not re-triggering.")
            return

    token = _airflow_self_token()
    if not token:
        return

    snapshot_time = ti.xcom_pull(task_ids="build_snapshot", key=XCOM_SNAPSHOT_TIME)
    next_conf = dict(conf)
    if snapshot_time:
        # The Model Registry API's `since` filter is inclusive (time >= since)
        # — passing the snapshot's own timestamp back unchanged would make the
        # next cycle immediately "find" that exact same reading again and
        # spin through empty cycles until genuinely new data arrives. Nudge
        # forward by 1us so the next cycle only matches readings strictly
        # after this one.
        try:
            dt = datetime.fromisoformat(str(snapshot_time).replace("Z", "+00:00"))
            next_conf["last_processed_time"] = (dt + timedelta(microseconds=1)).isoformat()
        except ValueError:
            next_conf["last_processed_time"] = snapshot_time

    next_run_id = f"cycle__{run_id}__{int(datetime.now(timezone.utc).timestamp())}"
    try:
        resp = requests.post(
            f"{AIRFLOW_SELF_API_BASE}/api/v2/dags/{DAG_ID}/dagRuns",
            headers={"Authorization": f"Bearer {token}"},
            json={"logical_date": None, "conf": next_conf, "dag_run_id": next_run_id},
            timeout=10,
        )
        if resp.status_code not in (200, 201):
            log.error(f"[{run_id}] failed to trigger next cycle: HTTP {resp.status_code} {resp.text}")
            return
        log.info(f"[{run_id}] triggered next cycle: {next_run_id} (since={next_conf.get('last_processed_time')})")
    except Exception as exc:
        log.error(f"[{run_id}] error triggering next cycle: {exc}")
