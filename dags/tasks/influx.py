"""
influx.py — InfluxDB helpers & Airflow callables for STAMM

Functions exposed to the DAG:
  - check_db_connection(): Sensor callable to verify InfluxDB is reachable
  - check_new_data():     Sensor callable to detect fresh snapshots per tag-group
  - store_prediction():   Operator callable to write model predictions to InfluxDB

All configuration is taken from environment variables (see .env template).
No secrets are hardcoded in this module.
"""

from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import get_current_context
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
log = LoggingMixin().log

# ---------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------
# You can override this path in Docker with: AIRFLOW_DOTENV_PATH=/opt/airflow/.env
ENV_PATH = os.getenv("AIRFLOW_DOTENV_PATH", "/opt/airflow/.env")
load_dotenv(dotenv_path=ENV_PATH)

# ---------------------------------------------------------------------
# Core Influx configuration
# ---------------------------------------------------------------------
INFLUXDB_URL   = os.getenv("INFLUXDB_URL", "").strip()
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "").strip()
INFLUXDB_ORG   = os.getenv("INFLUXDB_ORG", "").strip()

# Buckets & measurements
RAW_BUCKET            = os.getenv("RAW_BUCKET", "stamm_raw").strip()
RAW_MEASUREMENT       = os.getenv("RAW_MEASUREMENT", "bioreactor_obs").strip()

PRED_BUCKET           = os.getenv("PREDICTIONS_BUCKET", "stamm_predictions").strip()
PRED_MEASUREMENT      = os.getenv("PRED_MEASUREMENT", "bioreactor_obs").strip()
PRED_SOURCE           = os.getenv("PRED_SOURCE", "soft_sensor").strip()
PRED_OBSERVED_PROPERTY= os.getenv("PRED_OBSERVED_PROPERTY", "penicillin_concentration").strip()

# Model service (optional, for upstream calls if needed)
MODEL_ENDPOINT        = os.getenv("MODEL_ENDPOINT", "").strip()

# Snapshot detection parameters
LATEST_LOOKBACK     = os.getenv("LATEST_LOOKBACK", "7d").strip()  # Flux duration
FRESHNESS_SECONDS   = int(os.getenv("FRESHNESS_SECONDS", "60"))
TOLERANCE_SECONDS   = int(os.getenv("TOLERANCE_SECONDS", "0"))

# XCom keys / task ids
XCOM_TS_MAP_KEY       = os.getenv("XCOM_TS_MAP_KEY", "last_timestamps_map")
XCOM_SNAPSHOTS_KEY    = os.getenv("XCOM_SNAPSHOTS_KEY", "snapshots")
MODEL_TASK_ID         = os.getenv("MODEL_TASK_ID", "call_models_from_snapshots")
XCOM_PREDICTIONS_KEY  = os.getenv("XCOM_PREDICTIONS_KEY", "predictions")

# Internal/ignored columns when deriving tag groups
INTERNAL = {"_start", "_stop", "_time", "_value", "_field", "_measurement", "result", "table"}

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
def _rfc3339(dt: datetime) -> str:
    """Return RFC3339 string; ensure tz-aware (UTC) if missing."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def _flux_escape(s: str) -> str:
    """Escape Flux string literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"')

def _id_from_group(tags: dict) -> str:
    """Stable short id from sorted tag items."""
    items = sorted((k, str(v)) for k, v in tags.items())
    key_str = "|".join([f"{k}={v}" for k, v in items])
    return hashlib.md5(key_str.encode("utf-8")).hexdigest()[:12]

def _to_rfc3339_str(ts: str) -> str:
    """Normalize ISO-like string to RFC3339 (Z-suffix for UTC)."""
    if not ts:
        return ts
    iso = str(ts).replace(" ", "T")
    if iso.endswith("+00:00"):
        iso = iso[:-6] + "Z"
    return iso

def _normalize_version(ver: str) -> str:
    """'V.1.1' -> '1.1' (handy for filtering in dashboards)."""
    if not isinstance(ver, str):
        ver = str(ver)
    v = ver.strip()
    if v.upper().startswith("V."):
        v = v[2:]
    return v

def _require_influx_env() -> bool:
    """Validate mandatory Influx env vars are present."""
    missing = [k for k, v in {
        "INFLUXDB_URL": INFLUXDB_URL,
        "INFLUXDB_TOKEN": INFLUXDB_TOKEN,
        "INFLUXDB_ORG": INFLUXDB_ORG,
    }.items() if not v]
    if missing:
        log.error(f"Missing required InfluxDB env vars: {', '.join(missing)}")
        return False
    return True

# ---------------------------------------------------------------------
# 1) Health check
# ---------------------------------------------------------------------
def check_db_connection() -> bool:
    """
    Airflow Sensor callable.
    Return True if InfluxDB responds to ping, False otherwise.
    """
    try:
        if not _require_influx_env():
            return False

        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            if client.ping():
                log.info("InfluxDB connection successful.")
                return True
            log.warning("InfluxDB ping failed.")
            return False
    except Exception as e:
        log.error(f"check_db_connection: {e}")
        return False

# ---------------------------------------------------------------------
# 2) Detect fresh snapshots (wide pivot per group)
# ---------------------------------------------------------------------
def check_new_data() -> bool:
    """
    Detect new data per dynamic tag-group and build 'wide' snapshots (pivot by observed_property).

    XCom out:
      - XCOM_TS_MAP_KEY:    { group_id: last_snapshot_ts_iso }
      - XCOM_SNAPSHOTS_KEY: [ { <tags...>, snapshot_time, <variables...> }, ... ]
    """
    try:
        if not _require_influx_env():
            return False

        ctx = get_current_context()
        ti  = ctx["ti"]

        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            q = client.query_api()

            # 1) last _time per dynamic tag-group (collapse field/observed_property/source)
            latest_query = f'''
t = from(bucket: "{RAW_BUCKET}")
  |> range(start: -{LATEST_LOOKBACK}, stop: now())
  |> filter(fn: (r) => r._measurement == "{RAW_MEASUREMENT}")
  |> drop(columns: ["_start","_stop","source","_field","_value","observed_property"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
t
'''
            latest_result = q.query(org=INFLUXDB_ORG, query=latest_query)

            groups_latest: dict[str, dict] = {}
            for table in latest_result:
                for rec in table.records:
                    latest_ts = rec.get_time()  # tz-aware
                    tags = {k: str(v) for k, v in rec.values.items() if k not in INTERNAL}
                    group_id = _id_from_group(tags)
                    groups_latest[group_id] = {"ts": latest_ts, "tags": tags}

            if not groups_latest:
                log.info("No groups with recent data.")
                return False

            last_map = ti.xcom_pull(key=XCOM_TS_MAP_KEY, task_ids=ctx["task"].task_id) or {}
            if not isinstance(last_map, dict):
                last_map = {}

            now_utc       = datetime.now(timezone.utc)
            new_snapshots = []
            updated_map   = dict(last_map)

            # 2) For each group, check recency & novelty, then pivot a 'wide' snapshot
            for group_id, meta in groups_latest.items():
                latest_ts = meta["ts"]
                tags      = meta["tags"]

                # recency
                age = (now_utc - latest_ts).total_seconds()
                if age > FRESHNESS_SECONDS:
                    continue

                # novelty
                latest_iso = latest_ts.isoformat()
                last_seen  = last_map.get(group_id)
                if last_seen and latest_iso <= last_seen:
                    continue

                tol      = timedelta(seconds=TOLERANCE_SECONDS)
                start_ts = latest_ts - tol
                stop_ts  = latest_ts + timedelta(milliseconds=1)

                tag_filters = " and ".join(
                    [f'r["{_flux_escape(k)}"] == "{_flux_escape(v)}"' for k, v in (tags or {}).items()]
                ) or "true"

                tag_keys = list(tags.keys())
                if tag_keys:
                    group_cols_flux = 'group(columns: [' + ", ".join([f'"{_flux_escape(k)}"' for k in tag_keys]) + '])'
                    row_key_flux    = '[' + ", ".join([f'"{_flux_escape(k)}"' for k in tag_keys]) + ']'
                else:
                    group_cols_flux = "group()"
                    row_key_flux    = '["_stop"]'

                snapshot_query = f'''
from(bucket: "{RAW_BUCKET}")
  |> range(start: time(v: "{_rfc3339(start_ts)}"), stop: time(v: "{_rfc3339(stop_ts)}"))
  |> filter(fn: (r) => r._measurement == "{RAW_MEASUREMENT}" and {tag_filters})
  |> group(columns: ["observed_property"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
  |> {group_cols_flux}
  |> pivot(rowKey: {row_key_flux}, columnKey: ["observed_property"], valueColumn: "_value")
'''
                snap_result = q.query(org=INFLUXDB_ORG, query=snapshot_query)

                snapshot = None
                for t2 in snap_result:
                    for r2 in t2.records:
                        snapshot = dict(r2.values)
                        for kk in list(INTERNAL):
                            snapshot.pop(kk, None)
                        break

                if not snapshot:
                    continue

                snapshot["snapshot_time"] = latest_iso
                snapshot["group_id"]      = group_id
                snapshot.update(tags)
                new_snapshots.append(snapshot)
                updated_map[group_id] = latest_iso

            if not new_snapshots:
                log.info("No new snapshots.")
                return False

            ti.xcom_push(key=XCOM_TS_MAP_KEY,    value=updated_map)
            ti.xcom_push(key=XCOM_SNAPSHOTS_KEY, value=new_snapshots)
            log.info(f"New snapshots: {len(new_snapshots)} → {[s.get('group_id') for s in new_snapshots]}")
            return True

    except Exception as e:
        log.error(f"check_new_data: {e}")
        return False

# ---------------------------------------------------------------------
# 3) Write predictions
# ---------------------------------------------------------------------
def store_prediction() -> bool:
    """
    Read XCom predictions from MODEL_TASK_ID/XCOM_PREDICTIONS_KEY and write them to InfluxDB.

    Influx target:
      bucket: PRED_BUCKET
      measurement: PRED_MEASUREMENT
      tags:
        - device_id, project_name, batch_id (if present)
        - source = PRED_SOURCE
        - observed_property = PRED_OBSERVED_PROPERTY
        - model_id = <logical key from predictions payload>
        - version  = taken from payload["model_versions"][model_id] (or "unknown")
      fields:
        - value (float)
      time:
        - snapshot_time (RFC3339)
    """
    try:
        if not _require_influx_env():
            return False

        ctx = get_current_context()
        ti  = ctx["ti"]

        predictions_by_group = ti.xcom_pull(task_ids=MODEL_TASK_ID, key=XCOM_PREDICTIONS_KEY) or {}

        if not isinstance(predictions_by_group, dict) or not predictions_by_group:
            log.warning("store_prediction: no predictions found in XCom.")
            return False

        wrote = 0
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            for group_id, payload in predictions_by_group.items():
                if not isinstance(payload, dict):
                    log.warning(f"[{group_id}] invalid payload → skip.")
                    continue

                snap_time = _to_rfc3339_str(payload.get("snapshot_time"))
                preds     = payload.get("predictions") or {}
                tags_src  = payload.get("tags") or {}
                versions  = payload.get("model_versions") or {}

                if not snap_time:
                    log.warning(f"[{group_id}] missing snapshot_time → skip.")
                    continue
                if not preds:
                    log.warning(f"[{group_id}] empty predictions → skip.")
                    continue

                device_id    = tags_src.get("device_id")
                batch_id     = tags_src.get("batch_id")
                project_name = tags_src.get("project_name")

                for model_key, value in preds.items():
                    if value is None:
                        log.warning(f"[{group_id}] {model_key}: None value → skip.")
                        continue
                    try:
                        val = float(value)
                    except Exception:
                        log.warning(f"[{group_id}] {model_key}: cannot cast '{value}' to float → skip.")
                        continue

                    model_id_tag = str(model_key).lower()
                    version = versions.get(model_key) or versions.get(model_id_tag) or "unknown"

                    p = Point(PRED_MEASUREMENT)
                    if device_id    is not None: p = p.tag("device_id", device_id)
                    if project_name is not None: p = p.tag("project_name", project_name)
                    if batch_id     is not None: p = p.tag("batch_id", batch_id)

                    p = (p.tag("source", PRED_SOURCE)
                           .tag("observed_property", PRED_OBSERVED_PROPERTY)
                           .tag("model_id", model_id_tag)
                           .tag("version", version)
                           .field("value", val)
                           .time(snap_time))

                    write_api.write(bucket=PRED_BUCKET, org=INFLUXDB_ORG, record=p)
                    wrote += 1

        log.info(f"store_prediction: wrote {wrote} points → bucket='{PRED_BUCKET}', measurement='{PRED_MEASUREMENT}'.")
        return True

    except Exception as e:
        log.error(f"store_prediction: {e}")
        return False
