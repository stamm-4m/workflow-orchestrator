"""
STAMM — prediction caller for Airflow DAGs.

This module:
  1) Pulls a feature snapshot from XCom (produced by build_snapshot).
  2) Discovers the models registered for the project (GET /{project_id}/list_models/)
     and picks out the single "official" one configured for that project.
  3) Invokes POST /{project_id}/predict/{model_id} for that model.
  4) Pushes the result back to XCom (prediction value + model version).

Which model is "official" is a per-project, per-team decision, not something
this module infers — see _select_config_by_project_name().

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
  FEATURES_<PROJECT>=                # comma-separated, in the order the model was trained with
  MODEL_ID_<PROJECT>=                # the one model this module will call for that project

  # XCom / task wiring (optional, defaults shown)
  SENSOR_TASK_ID=build_snapshot
  XCOM_SNAPSHOTS_KEY=snapshots
  XCOM_PREDICTIONS_KEY=predictions

Notes:
- The Model Registry must expose:
    GET  /{project_id}/list_models/
    GET  /{project_id}/metadata/{model_id}
    POST /{project_id}/predict/{model_id}
"""

from __future__ import annotations

import os
import logging
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
MODEL_REGISTRY_PROJECT_ID: str = os.getenv("MODEL_REGISTRY_PROJECT_ID", "")
MODEL_REGISTRY_TIMEOUT: float = float(os.getenv("MODEL_REGISTRY_TIMEOUT_SECONDS", "30"))
MODEL_REGISTRY_VERIFY_TLS: bool = os.getenv("MODEL_REGISTRY_VERIFY_TLS", "true").lower() == "true"

MODEL_REGISTRY_SERVICE_EMAIL: str = os.getenv("MODEL_REGISTRY_SERVICE_EMAIL", "")
MODEL_REGISTRY_SERVICE_PASSWORD: str = os.getenv("MODEL_REGISTRY_SERVICE_PASSWORD", "")

SENSOR_TASK_ID: str = os.getenv("SENSOR_TASK_ID", "check_new_data")
XCOM_SNAPSHOTS_KEY: str = os.getenv("XCOM_SNAPSHOTS_KEY", "snapshots")
XCOM_PREDICTIONS_KEY: str = os.getenv("XCOM_PREDICTIONS_KEY", "predictions")

_FEATURES_FROM_ENV = os.getenv("FEATURES", "").strip()
FEATURES: List[str] = [f.strip() for f in _FEATURES_FROM_ENV.split(",") if f.strip()] if _FEATURES_FROM_ENV else []


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
                # Version isn't in this payload; _invoke_model_api() falls back
                # to _fetch_version_from_metadata() when it sees "unknown".
                return pred, "unknown"

    # 4) If nothing matches; it returns None, "unknown"
    return None, "unknown"


def _extract_features(snapshot: Dict[str, Any], required: List[str]) -> Dict[str, Optional[float]]:
    """
    Build the model input features from the snapshot.
    Priority:
      1) If `required` is provided: take only those keys (from snapshot or snapshot["features"])
      2) Else if snapshot has `features` dict: use it
      3) Else: infer numeric fields from top-level snapshot
    """
    features: Dict[str, Optional[float]] = {}

    # 1) Explicit list from env
    if required:
        source = snapshot.get("features") if isinstance(snapshot.get("features"), dict) else snapshot
        for k in required:
            v = source.get(k)
            if isinstance(v, (int, float)):
                features[k] = float(v)
            else:
                features[k] = _as_scalar(v)
        return features

    # 2) Provided "features" dict
    if isinstance(snapshot.get("features"), dict):
        for k, v in snapshot["features"].items():
            if isinstance(v, (int, float)):
                features[k] = float(v)
            else:
                features[k] = _as_scalar(v)
        return features

    # 3) Infer from top-level numeric fields
    for k, v in snapshot.items():
        if k in {"group_id", "snapshot_time", "device_id", "batch_id", "project", "project_name"}:
            continue
        if isinstance(v, (int, float)):
            features[k] = float(v)
        else:
            val = _as_scalar(v)
            if val is not None:
                features[k] = val

    return features


def _missing_keys(features: Dict[str, Optional[float]]) -> List[str]:
    """Return required keys that are missing or None (only when FEATURES is set)."""
    if not FEATURES:
        return []
    return [k for k in FEATURES if k not in features or features[k] is None]


# -----------------------------------------------------------------------------
# Model Registry helpers — auth (login + token caching) lives in
# tasks/registry_client.py and is shared with the data-access side (postgres.py).
# -----------------------------------------------------------------------------
def _parse_csv_env(name: str) -> List[str]:
    raw = os.getenv(name, "").strip()
    return [x.strip() for x in raw.split(",") if x.strip()] if raw else []

def _select_config_by_project_name(project_name: str) -> Tuple[str, List[str], str]:
    """Return (project_id, features, official_model_id) for the project.

    official_model_id is the single model call_models_from_snapshots() will
    invoke — not "whichever models happen to be registered". Which model is
    official is a product decision (pending a team call on which to use);
    for now it's pinned per project via MODEL_ID_<PROJECT> env vars.
    """
    key = (project_name or "").strip().lower()

    if "ecoli" in key:
        return (
            os.getenv("MODEL_REGISTRY_PROJECT_ID_ECOLI", "").strip(),
            _parse_csv_env("FEATURES_ECOLI"),
            os.getenv("MODEL_ID_ECOLI", "").strip(),
        )

    if "penicillin" in key:
        return (
            os.getenv("MODEL_REGISTRY_PROJECT_ID_PENICILLIN", "").strip(),
            _parse_csv_env("FEATURES_PENICILLIN"),
            os.getenv("MODEL_ID_PENICILLIN", "").strip(),
        )

    # Fallback for a single-project deployment (no per-project env suffix).
    return (
        os.getenv("MODEL_REGISTRY_PROJECT_ID", "").strip(),
        _parse_csv_env("FEATURES"),
        os.getenv("MODEL_ID", "").strip(),
    )

def _discover_models() -> Tuple[Dict[str, str], List[str]]:
    """
    Discover available models for the configured project via:
        GET /{project_id}/list_models/
    Expected responses:
      - list[str] (model IDs)
      - list[object] including 'model_id' or 'id' or 'name'
    Returns (MODEL_ID_MAP, MODEL_LIST) where keys are equal to model IDs.
    """
    path = f"/{MODEL_REGISTRY_PROJECT_ID}/list_models/"
    try:
        resp = registry_client.get(path)
        resp.raise_for_status()
        payload = resp.json()

        model_ids: List[str] = []
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, str):
                    model_ids.append(item)
                elif isinstance(item, dict):
                    mid = item.get("model_ID") or item.get("model_id") or item.get("id") or item.get("name")
                    if isinstance(mid, str):
                        model_ids.append(mid)

        model_map = {mid: mid for mid in model_ids}  # logical key == model_id
        return model_map, model_ids

    except Exception as exc:
        log.error(f"[discovery] Unable to list models from API ({path}): {exc}")
        return {}, []


def _fetch_version_from_metadata(api_model_id: str) -> str:
    """
    Retrieve version info as a fallback:
      GET /{project_id}/metadata/{model_id}
    Tries to read `model_identification.version`.
    """
    path = f"/{MODEL_REGISTRY_PROJECT_ID}/metadata/{api_model_id}"
    try:
        r = registry_client.get(path)
        if r.status_code != 200:
            log.warning(f"[metadata:{api_model_id}] HTTP {r.status_code}: {r.text}")
            return "unknown"
        meta = r.json()
        ident = meta.get("model_identification") or {}
        ver = _as_scalar(ident.get("version")) or ident.get("version")
        return str(ver) if ver is not None else "unknown"
    except Exception as exc:
        log.warning(f"[metadata:{api_model_id}] error: {exc}")
        return "unknown"


def _invoke_model_api(
    model_key: str,
    features: Dict[str, Optional[float]],
    model_id_map: Dict[str, str],
) -> Tuple[Optional[float], str]:
    """
    Invoke:
      POST /{project_id}/predict/{model_id}
    Body:
      {"req": {"input_data": {<feature>: <float>}}}
    Returns:
      (prediction_value, version_str)
    """
    api_model_id = model_id_map.get(model_key)
    if not api_model_id:
        log.error(f"No model_id for '{model_key}' in discovered catalog.")
        return None, "unknown"

    path = f"/{MODEL_REGISTRY_PROJECT_ID}/predict/{api_model_id}"
    payload = {"req": {"input_data": features}}

    try:
        r = registry_client.post(path, json_body=payload)
        if r.status_code != 200:
            log.error(f"[{model_key}] HTTP {r.status_code} @ {path}: {r.text}")
            return None, "unknown"

        data = r.json()

        yhat, version = _parse_value_and_version_from_predict(data)
        if version == "unknown":
            version = _fetch_version_from_metadata(api_model_id)

        return yhat, version

    except Exception as exc:
        log.error(f"[{model_key}] error invoking API @ {path}: {exc}")
        return None, "unknown"


# -----------------------------------------------------------------------------
# Airflow task callable
# -----------------------------------------------------------------------------
def call_models_from_snapshots() -> bool:
    """
    Airflow task:
      1) Pull snapshots from XCom (pushed by build_snapshot)
      2) Resolve the project's official model (see _select_config_by_project_name)
      3) For each snapshot, extract features and invoke that one model
      4) Push a consolidated result back to XCom

    XCom input:
      key = XCOM_SNAPSHOTS_KEY (default: "snapshots")
      task_ids = SENSOR_TASK_ID (default: "build_snapshot")

    XCom output:
      key = XCOM_PREDICTIONS_KEY (default: "predictions")
      value = {
        "<group_id>": {
          "snapshot_time": "<iso>",
          "tags": {...},
          "features": {...},
          "predictions": {"<model_key>": <float or None>, ...},
          "model_versions": {"<model_key>": "<version|unknown>", ...}
        }, ...
      }
    """
    try:
        ctx = get_current_context()
        ti = ctx["ti"]

        snapshots: List[Dict[str, Any]] = ti.xcom_pull(
            key=XCOM_SNAPSHOTS_KEY,
            task_ids=SENSOR_TASK_ID,
        )

        if not snapshots:
            log.warning("call_models_from_snapshots: no snapshots found in XCom.")
            return False

        # ------------------------------------------------------------
        # Resolve project config once per run, from the first snapshot's
        # project_name. A run is always scoped to one project, so this
        # never needs to change mid-loop.
        # ------------------------------------------------------------
        first = snapshots[0]
        project_name = first.get("project") or first.get("project_name") or ""
        pid, feats, official_model_id = _select_config_by_project_name(str(project_name))

        if not pid:
            log.error(f"No project_id resolved for project_name={project_name}. Check your .env variables.")
            return False
        if not feats:
            log.error(f"No features resolved for project_name={project_name}. Check your .env variables.")
            return False
        if not official_model_id:
            log.error(
                f"No official model configured for project_name={project_name}. "
                f"Set MODEL_ID_ECOLI / MODEL_ID_PENICILLIN / MODEL_ID in .env."
            )
            return False

        global MODEL_REGISTRY_PROJECT_ID, FEATURES
        MODEL_REGISTRY_PROJECT_ID = pid
        FEATURES = feats

        log.info(f"[config switch] project_name={project_name} -> PROJECT_ID={MODEL_REGISTRY_PROJECT_ID} FEATURES={FEATURES}")
        # ------------------------------------------------------------

        # 1) Discover models, then pin down to the one official model.
        # Calling every registered model isn't the product intent -- which
        # model is "the" soft sensor per project is a pending team decision;
        # for now exactly one gets invoked (MODEL_ID_* in .env).
        model_id_map, discovered = _discover_models()
        if official_model_id not in model_id_map:
            log.error(
                f"Configured official model '{official_model_id}' not found in discovered "
                f"catalog for project {MODEL_REGISTRY_PROJECT_ID}. Available: {discovered}"
            )
            return False
        model_list = [official_model_id]

        log.info(
            f"Processing {len(snapshots)} snapshot(s). "
            f"API_BASE={MODEL_REGISTRY_API_BASE} PROJECT={MODEL_REGISTRY_PROJECT_ID} "
            f"MODEL={official_model_id}"
        )

        results: Dict[str, Dict[str, Any]] = {}

        for snap in snapshots:
            group_id: str = str(snap.get("group_id", "unknown"))
            snapshot_time: str = str(snap.get("snapshot_time", ""))

            tags = {
                "project_name":  snap.get("project") or snap.get("project_name"),
                # PostgreSQL-backed fields (new)
                "run_id":        snap.get("run_id"),
                "experiment_id": snap.get("experiment_id"),
                "project_id":    snap.get("project_id"),
                # InfluxDB-backed fields (legacy, kept for backwards compat)
                "device_id": snap.get("device_id"),
                "batch_id":  snap.get("batch_id"),
            }

            # 2) Build the features vector
            feat = _extract_features(snap, FEATURES)
            missing = _missing_keys(feat)
            if missing:
                log.warning(f"[{group_id}] missing required features in snapshot {snapshot_time}: {missing} → skip")
                continue

            # 3) Invoke the official model (model_list has exactly one entry)
            preds: Dict[str, Optional[float]] = {}
            vers: Dict[str, str] = {}

            for model_key in model_list:
                yhat, ver = _invoke_model_api(model_key, feat, model_id_map)
                preds[model_key] = yhat
                vers[model_key] = ver

            results[group_id] = {
                "snapshot_time": snapshot_time,
                "tags": tags,
                "features": feat,
                "predictions": preds,
                "model_versions": vers,
            }
            log.info(f"[{group_id}] predictions={preds} versions={vers}")

        if not results:
            log.warning("No predictions generated (incomplete snapshots or API errors).")
            return False

        # 4) Publish consolidated predictions to XCom
        ti.xcom_push(key=XCOM_PREDICTIONS_KEY, value=results)
        log.info(f"Predictions published to XCom key='{XCOM_PREDICTIONS_KEY}'. Groups: {list(results.keys())}")
        return True

    except Exception as exc:
        log.error(f"Exception in call_models_from_snapshots: {exc}")
        return False