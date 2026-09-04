"""
STAMM — prediction caller for Airflow DAGs.

This module:
  1) Pulls a feature snapshot (+ short history, for lagged features) from
     XCom (produced by build_snapshot).
  2) Runs a prediction for EVERY model attached to the experiment — model-
     registry sends their models.slug list as conf["model_ids"] when it
     triggers the DAG (one per model selected in the experiment's dropdown).
  3) For each model, builds its own feature vector from its own declared
     `inputs.features` (name + lag + type), fetched from the models catalog
     (GET /api/v1/models/) — no more hardcoded per-project feature lists.
  4) Invokes POST /{project_id}/predict/{model_id} for each model.
  5) Pushes the results back to XCom (prediction value + model version, per
     model).

Environment (see workflow-orchestrator/.env):
  # Model Registry (FastAPI)
  MODEL_REGISTRY_API_BASE=
  MODEL_REGISTRY_TIMEOUT_SECONDS=30
  MODEL_REGISTRY_VERIFY_TLS=true
  MODEL_REGISTRY_SERVICE_EMAIL=      # service account used by tasks/registry_client.py
  MODEL_REGISTRY_SERVICE_PASSWORD=

  # Per-project config: <PROJECT> is ECOLI, PENICILLIN, ... (see
  # _select_config_by_project_name for how project_name maps to these)
  MODEL_REGISTRY_PROJECT_ID_<PROJECT>=
  FEATURES_<PROJECT>=                # fallback only, for models with no inputs.features metadata yet
  MODEL_ID_<PROJECT>=                # fallback only, used when a trigger doesn't send conf["model_ids"]

  # XCom / task wiring (optional, defaults shown)
  SENSOR_TASK_ID=build_snapshot
  XCOM_SNAPSHOTS_KEY=snapshots
  XCOM_PREDICTIONS_KEY=predictions

Notes:
- The Model Registry must expose:
    GET  /api/v1/models/
    POST /{project_id}/predict/{model_id}
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from airflow.operators.python import get_current_context

from tasks import registry_client


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# -----------------------------------------------------------------------------
# Core model registry configuration
# -----------------------------------------------------------------------------
MODEL_REGISTRY_API_BASE: str = os.getenv("MODEL_REGISTRY_API_BASE", "")
MODEL_REGISTRY_TIMEOUT: float = float(os.getenv("MODEL_REGISTRY_TIMEOUT_SECONDS", "30"))
MODEL_REGISTRY_VERIFY_TLS: bool = os.getenv("MODEL_REGISTRY_VERIFY_TLS", "true").lower() == "true"

MODEL_REGISTRY_SERVICE_EMAIL: str = os.getenv("MODEL_REGISTRY_SERVICE_EMAIL", "")
MODEL_REGISTRY_SERVICE_PASSWORD: str = os.getenv("MODEL_REGISTRY_SERVICE_PASSWORD", "")

SENSOR_TASK_ID: str = os.getenv("SENSOR_TASK_ID", "check_new_data")
XCOM_SNAPSHOTS_KEY: str = os.getenv("XCOM_SNAPSHOTS_KEY", "snapshots")
XCOM_HISTORY_KEY: str = os.getenv("XCOM_HISTORY_KEY", "history")
XCOM_PREDICTIONS_KEY: str = os.getenv("XCOM_PREDICTIONS_KEY", "predictions")

_UNIT_SECONDS = {
    "second": 1, "seconds": 1, "sec": 1,
    "minute": 60, "minutes": 60, "min": 60,
    "hour": 3600, "hours": 3600, "hr": 3600,
    "day": 86400, "days": 86400,
}


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _as_scalar(val: Any) -> Optional[float]:
    """Best-effort conversion of a value into a float, handling common containers."""
    try:
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, list) and val and isinstance(val[0], (int, float)):
            return float(val[0])
        if isinstance(val, dict):
            # Common patterns: {"value": 1.23}, {"yhat": 1.23}, {"prediction": 1.23}
            for key in ("value", "yhat", "prediction", "pred"):
                if key in val and isinstance(val[key], (int, float)):
                    return float(val[key])
    except Exception:
        pass
    return None


def _parse_value_and_version_from_predict(payload: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """
    Extract the predicted value and model version from a /predict response.
    This function is defensive to accommodate minor schema differences.
    Returns: (prediction_value, version_str or "unknown").
    """

    # 1) "simple" formats: {"prediction": x}, {"yhat": x}, etc.
    for key in ("prediction", "yhat", "value", "output"):
        if key in payload:
            val = payload[key]
            if isinstance(val, (int, float)):
                pred = float(val)
            else:
                pred = _as_scalar(val)  # try to pull a scalar out of a list/array
            if pred is not None:
                version = (
                    payload.get("model_version")
                    or payload.get("version")
                    or payload.get("modelVersion")
                    or "unknown"
                )
                return pred, str(version)

    # 2) Nested formats: {"data": {"prediction": x}}, {"result": {...}}, etc.
    for nest in ("data", "result", "response"):
        if nest in payload and isinstance(payload[nest], dict):
            nested = payload[nest]
            for key in ("prediction", "yhat", "value", "output"):
                if key in nested:
                    val = nested[key]
                    if isinstance(val, (int, float)):
                        pred = float(val)
                    else:
                        pred = _as_scalar(val)
                    if pred is not None:
                        version = (
                            nested.get("model_version")
                            or nested.get("version")
                            or payload.get("model_version")
                            or "unknown"
                        )
                        return pred, str(version)

    # 3) Special format for Model Registry:

    if "output_model" in payload and isinstance(payload["output_model"], list) and payload["output_model"]:
        first = payload["output_model"][0]
        if isinstance(first, dict) and "prediction" in first:
            preds = first["prediction"]
            if isinstance(preds, (list, tuple)) and preds:
                val = preds[0]
            else:
                val = preds

            if isinstance(val, (int, float)):
                pred = float(val)
            else:
                pred = _as_scalar(val)

            if pred is not None:
                return pred, "unknown"

    # 4) If nothing matches; it returns None, "unknown"
    return None, "unknown"


def _model_interval_seconds(model_row: Dict[str, Any]) -> Optional[int]:
    """Raw sampling interval (no margin) declared in models.input_time_interval."""
    try:
        interval = (model_row.get("input_time_interval") or {}).get("time_interval") or {}
        value = float(interval["value"])
        unit = _UNIT_SECONDS[str(interval["unit"]).strip().lower()]
        return int(value * unit)
    except Exception:
        return None


def _model_features(model_row: Dict[str, Any], project_env_key: str) -> List[Dict[str, Any]]:
    """Feature spec list (name/lag/type) for one model, from models.inputs.
    Falls back to the flat FEATURES_<PROJECT> env var (all lag=0) for models
    that don't have inputs.features populated yet (older/partial registry rows).
    """
    feats = ((model_row.get("inputs") or {}).get("features")) or []
    if feats:
        return feats
    fallback = _parse_csv_env(f"FEATURES_{project_env_key}") or _parse_csv_env("FEATURES")
    return [{"name": name, "lag": 0} for name in fallback]


def _extract_features_for_model(
    model_row: Dict[str, Any],
    snapshot: Dict[str, Any],
    history: List[Dict[str, Any]],
    snapshot_time: datetime,
    project_env_key: str,
) -> Tuple[Dict[str, Optional[float]], List[str]]:
    """Build one model's input vector from the shared snapshot/history,
    honoring each feature's declared lag (0 = current value, N = N sampling
    intervals back — see models.inputs.features[].lag).

    Returns (features, missing_required) — missing_required lists any
    feature the model needs that couldn't be resolved.
    """
    interval_s = _model_interval_seconds(model_row)
    features: Dict[str, Optional[float]] = {}
    missing: List[str] = []

    for feat in _model_features(model_row, project_env_key):
        name = feat.get("name")
        if not name:
            continue
        lag = int(feat.get("lag") or 0)

        if lag == 0:
            val = _as_scalar(snapshot.get(name))
        elif interval_s:
            target = snapshot_time - timedelta(seconds=lag * interval_s)
            val = _closest_history_value(history, name, target)
        else:
            val = None

        features[name] = val
        if val is None:
            missing.append(f"{name}(lag={lag})")

    return features, missing


def _closest_history_value(
    history: List[Dict[str, Any]], variable: str, target: datetime, tolerance_s: float = 120.0
) -> Optional[float]:
    """Closest reading for `variable` at-or-before `target`, within tolerance."""
    best_val = None
    best_delta = None
    for row in history:
        if row.get("variable") != variable:
            continue
        try:
            t = datetime.fromisoformat(str(row["time"]).replace("Z", "+00:00"))
        except Exception:
            continue
        if t > target:
            continue
        delta = (target - t).total_seconds()
        if delta > tolerance_s:
            continue
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_val = row.get("value")
    return _as_scalar(best_val)


# -----------------------------------------------------------------------------
# Model Registry helpers — auth (login + token caching) lives in
# tasks/registry_client.py and is shared with the data-access side (postgres.py).
# -----------------------------------------------------------------------------
def _parse_csv_env(name: str) -> List[str]:
    raw = os.getenv(name, "").strip()
    return [x.strip() for x in raw.split(",") if x.strip()] if raw else []


def _project_env_key(project_name: str) -> str:
    key = (project_name or "").strip().lower()
    if "ecoli" in key:
        return "ECOLI"
    if "penicillin" in key:
        return "PENICILLIN"
    return ""


def _select_config_by_project_name(project_name: str, conf: Optional[dict] = None) -> Tuple[str, List[str]]:
    """Return (project_id, model_ids) for the project.

    model_ids are the models.slug of every model attached to the experiment
    that triggered this DAG — model-registry resolves this per-experiment
    (models the user selected in the experiment's dropdown) and sends it as
    conf["model_ids"]. Falls back to a single-item list from MODEL_ID_<PROJECT>
    env vars only for triggers that don't specify one (e.g. manual runs).
    """
    env_key = _project_env_key(project_name)
    conf_model_ids = [m for m in ((conf or {}).get("model_ids") or []) if m]

    if env_key:
        project_id = os.getenv(f"MODEL_REGISTRY_PROJECT_ID_{env_key}", "").strip()
        fallback = os.getenv(f"MODEL_ID_{env_key}", "").strip()
    else:
        project_id = os.getenv("MODEL_REGISTRY_PROJECT_ID", "").strip()
        fallback = os.getenv("MODEL_ID", "").strip()

    model_ids = conf_model_ids or ([fallback] if fallback else [])
    return project_id, model_ids


def _fetch_models_catalog() -> Dict[str, Dict[str, Any]]:
    """models.slug -> full models row (id, inputs, input_time_interval,
    version, ...). Small catalog, fetched in full."""
    try:
        rows = registry_client.get_all_pages("/api/v1/models/")
        return {r["slug"]: r for r in rows if r.get("slug")}
    except Exception as exc:
        log.error(f"Unable to fetch models catalog: {exc}")
        return {}


def _invoke_model_api(project_id: str, model_slug: str, features: Dict[str, Optional[float]]) -> Tuple[Optional[float], str]:
    """
    Invoke:
      POST /{project_id}/predict/{model_id}
    Body:
      {"req": {"input_data": {<feature>: <float>}}}
    Returns:
      (prediction_value, version_str)
    """
    path = f"/{project_id}/predict/{model_slug}"
    payload = {"req": {"input_data": features}}

    try:
        r = registry_client.post(path, json_body=payload)
        if r.status_code != 200:
            log.error(f"[{model_slug}] HTTP {r.status_code} @ {path}: {r.text}")
            return None, "unknown"

        data = r.json()
        yhat, version = _parse_value_and_version_from_predict(data)
        return yhat, version

    except Exception as exc:
        log.error(f"[{model_slug}] error invoking API @ {path}: {exc}")
        return None, "unknown"


# -----------------------------------------------------------------------------
# Airflow task callable
# -----------------------------------------------------------------------------
def call_models_from_snapshots() -> bool:
    """
    Airflow task:
      1) Pull the snapshot (+ history) from XCom (pushed by build_snapshot)
      2) Resolve every model attached to this experiment (see
         _select_config_by_project_name)
      3) For each model, extract its own feature vector (lag-aware) and
         invoke it
      4) Push a consolidated result back to XCom

    XCom input:
      key = XCOM_SNAPSHOTS_KEY (default: "snapshots")
      key = XCOM_HISTORY_KEY (default: "history"), optional
      task_ids = SENSOR_TASK_ID (default: "build_snapshot")

    XCom output:
      key = XCOM_PREDICTIONS_KEY (default: "predictions")
      value = {
        "<group_id>": {
          "snapshot_time": "<iso>",
          "tags": {...},
          "predictions": {"<model_slug>": <float or None>, ...},
          "model_versions": {"<model_slug>": "<version|unknown>", ...}
        }, ...
      }
    """
    try:
        ctx = get_current_context()
        ti = ctx["ti"]
        conf = ctx["dag_run"].conf or {}

        snapshots: List[Dict[str, Any]] = ti.xcom_pull(
            key=XCOM_SNAPSHOTS_KEY,
            task_ids=SENSOR_TASK_ID,
        )
        history: List[Dict[str, Any]] = ti.xcom_pull(
            key=XCOM_HISTORY_KEY,
            task_ids=SENSOR_TASK_ID,
        ) or []

        if not snapshots:
            log.warning("call_models_from_snapshots: no snapshots found in XCom.")
            return False

        # ------------------------------------------------------------
        # Resolve project + attached models once per run, from the first
        # snapshot's project_name. A run is always scoped to one project.
        # ------------------------------------------------------------
        first = snapshots[0]
        project_name = first.get("project") or first.get("project_name") or ""
        project_id, model_ids = _select_config_by_project_name(str(project_name), conf)
        env_key = _project_env_key(str(project_name))

        if not project_id:
            log.error(f"No project_id resolved for project_name={project_name}. Check your .env variables.")
            return False
        if not model_ids:
            log.error(
                f"No models attached to this experiment for project_name={project_name}, and no "
                f"MODEL_ID_{env_key or ''} fallback set. Nothing to predict."
            )
            return False

        catalog = _fetch_models_catalog()
        log.info(f"Processing {len(snapshots)} snapshot(s). API_BASE={MODEL_REGISTRY_API_BASE} PROJECT={project_id} MODELS={model_ids}")

        results: Dict[str, Dict[str, Any]] = {}

        for snap in snapshots:
            group_id: str = str(snap.get("group_id", "unknown"))
            snapshot_time_str: str = str(snap.get("snapshot_time", ""))
            try:
                snapshot_time = datetime.fromisoformat(snapshot_time_str.replace("Z", "+00:00"))
            except Exception:
                snapshot_time = datetime.utcnow()

            tags = {
                "project_name":  snap.get("project") or snap.get("project_name"),
                "run_id":        snap.get("run_id"),
                "experiment_id": snap.get("experiment_id"),
                "project_id":    snap.get("project_id"),
            }

            preds: Dict[str, Optional[float]] = {}
            vers: Dict[str, str] = {}

            for model_slug in model_ids:
                model_row = catalog.get(model_slug)
                if not model_row:
                    log.error(f"[{group_id}] model '{model_slug}' not found in models catalog for project {project_id} — skip.")
                    preds[model_slug] = None
                    vers[model_slug] = "unknown"
                    continue

                feat, missing = _extract_features_for_model(model_row, snap, history, snapshot_time, env_key)
                if missing:
                    log.warning(f"[{group_id}] {model_slug}: missing required features in snapshot {snapshot_time_str}: {missing} → skip")
                    continue

                yhat, ver = _invoke_model_api(project_id, model_slug, feat)
                preds[model_slug] = yhat
                vers[model_slug] = ver or model_row.get("version") or "unknown"

            results[group_id] = {
                "snapshot_time": snapshot_time_str,
                "tags": tags,
                "predictions": preds,
                "model_versions": vers,
            }
            log.info(f"[{group_id}] predictions={preds} versions={vers}")

        if not results:
            log.warning("No predictions generated (incomplete snapshots or API errors).")
            return False

        ti.xcom_push(key=XCOM_PREDICTIONS_KEY, value=results)
        log.info(f"Predictions published to XCom key='{XCOM_PREDICTIONS_KEY}'. Groups: {list(results.keys())}")
        return True

    except Exception as exc:
        log.error(f"Exception in call_models_from_snapshots: {exc}")
        return False
