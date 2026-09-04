"""
auto_simulate_sensors.py — Fully automatic stand-in for real sensor hardware.

Where scripts/simulate_sensors.py needs a human to start it for one specific
run_id, this watches the Model Registry API for experiments that are
currently "running" and streams simulated data to all of them at once,
picking up new experiments and dropping finished ones automatically — no
one has to run anything by hand when a new experiment is created.

Talks to the Model Registry over the same HTTP API the DAG uses (no direct
DB access), so it can run anywhere — including a different machine than
model-registry itself, exactly like Airflow does.

Usage:
    python scripts/auto_simulate_sensors.py

Env vars (reuses the same ones Airflow uses, see .env):
    MODEL_REGISTRY_API_BASE
    MODEL_REGISTRY_SERVICE_EMAIL
    MODEL_REGISTRY_SERVICE_PASSWORD
    MODEL_REGISTRY_TIMEOUT_SECONDS=30
    MODEL_REGISTRY_VERIFY_TLS=true
    SIM_PROJECT_ID=ccccccc1-cccc-cccc-cccc-cccccccccccc   # which project to simulate for
    SIM_INTERVAL_SECONDS=20
"""

from __future__ import annotations

import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests

API_BASE = os.getenv("MODEL_REGISTRY_API_BASE", "").rstrip("/")
TIMEOUT = float(os.getenv("MODEL_REGISTRY_TIMEOUT_SECONDS", "30"))
VERIFY_TLS = os.getenv("MODEL_REGISTRY_VERIFY_TLS", "true").lower() == "true"
EMAIL = os.getenv("MODEL_REGISTRY_SERVICE_EMAIL", "")
PASSWORD = os.getenv("MODEL_REGISTRY_SERVICE_PASSWORD", "")

# Default: the Penicillin project (IndPenSim) from the shared seed data.
# Override with SIM_PROJECT_ID if your database uses a different UUID.
PROJECT_ID = os.getenv("SIM_PROJECT_ID", "ccccccc1-cccc-cccc-cccc-cccccccccccc")
INTERVAL = float(os.getenv("SIM_INTERVAL_SECONDS", "20"))

# The 8 variables the IndPenSim (penicillin) models expect — same set as
# scripts/simulate_sensors.py. kind picks which catalog/table to use.
VARIABLES = {
    "temperature": dict(kind="sensor", unit="K", lo=298, hi=308),
    "pH": dict(kind="sensor", unit="pH", lo=5.5, hi=7.5),
    "dissolved_oxygen_concentration": dict(kind="sensor", unit="mg/L", lo=0, hi=10),
    "CO2_percent_in_off_gas": dict(kind="sensor", unit="%", lo=0, hi=10),
    "oxygen_in_percent_in_off_gas": dict(kind="sensor", unit="%", lo=10, hi=21),
    "vessel_volume": dict(kind="sensor", unit="L", lo=50, hi=150),
    "agitator": dict(kind="actuator", unit="rpm", lo=100, hi=1200),
    "sugar_feed_rate": dict(kind="actuator", unit="L/h", lo=0, hi=2),
}

_running = True
_token_cache = {"access_token": None, "expires_at": 0.0}


def _stop(signum, frame):
    global _running
    _running = False


def _login() -> str:
    resp = requests.post(
        f"{API_BASE}/auth/login-json",
        json={"email": EMAIL, "password": PASSWORD},
        timeout=TIMEOUT,
        verify=VERIFY_TLS,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = time.time() + 50 * 60
    return token


def _auth_headers() -> dict:
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"]:
        return {"Authorization": f"Bearer {_token_cache['access_token']}"}
    return {"Authorization": f"Bearer {_login()}"}


def _get_all(path: str) -> list[dict]:
    out, offset, page_size = [], 0, 1000
    while True:
        resp = requests.get(
            f"{API_BASE}{path}",
            headers=_auth_headers(),
            params={"offset": offset, "limit": page_size},
            timeout=TIMEOUT,
            verify=VERIFY_TLS,
        )
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out


def _resolve_catalog() -> dict[str, dict]:
    """variable -> {kind, id}. Exits with a clear message if any variable's
    sensor/actuator row doesn't exist yet — this script never creates catalog
    rows itself, that's a one-time setup step (see docs/AIRFLOW_INTEGRATION.md)."""
    sensors = {s["variable"]: s["id"] for s in _get_all("/api/v1/sensors/") if s.get("variable")}
    actuators = {a["variable"]: a["id"] for a in _get_all("/api/v1/actuators/") if a.get("variable")}

    catalog, missing = {}, []
    for var, meta in VARIABLES.items():
        table = sensors if meta["kind"] == "sensor" else actuators
        if var not in table:
            missing.append(f"{var} ({meta['kind']})")
        else:
            catalog[var] = {"kind": meta["kind"], "id": table[var]}

    if missing:
        print("Missing sensor/actuator catalog rows, create them once first:", file=sys.stderr)
        for m in missing:
            print(f"  - {m}", file=sys.stderr)
        sys.exit(1)
    return catalog


def _active_runs() -> set[str]:
    """run_id of every run whose experiment is 'running' and has no end_time,
    for the configured project."""
    experiments = _get_all("/api/v1/experiments/")
    running_exp_ids = {
        e["id"] for e in experiments
        if e.get("status") == "running" and e.get("project_id") == PROJECT_ID
    }
    if not running_exp_ids:
        return set()

    runs = _get_all("/api/v1/runs/")
    return {
        r["id"] for r in runs
        if r.get("experiment_id") in running_exp_ids and not r.get("end_time")
    }


def _write_tick(run_id: str, catalog: dict, state: dict[str, float]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    for var, meta in VARIABLES.items():
        span = meta["hi"] - meta["lo"]
        step = random.uniform(-0.03, 0.03) * span
        value = min(meta["hi"], max(meta["lo"], state[var] + step))
        state[var] = value

        entry = catalog[var]
        table = "sensor_readings" if entry["kind"] == "sensor" else "actuator_states"
        id_field = "sensor_id" if entry["kind"] == "sensor" else "actuator_id"
        resp = requests.post(
            f"{API_BASE}/api/v1/{table}/",
            headers=_auth_headers(),
            json={"time": now, "run_id": run_id, id_field: entry["id"], "value": value},
            timeout=TIMEOUT,
            verify=VERIFY_TLS,
        )
        if resp.status_code not in (200, 201, 409):
            print(f"  [{run_id[:8]}] {var}: HTTP {resp.status_code} {resp.text}", file=sys.stderr)


def main() -> None:
    if not API_BASE or not EMAIL or not PASSWORD:
        sys.exit("MODEL_REGISTRY_API_BASE / _SERVICE_EMAIL / _SERVICE_PASSWORD must be set.")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"Logging in as {EMAIL} @ {API_BASE} ...")
    _login()
    catalog = _resolve_catalog()
    print(f"Watching project={PROJECT_ID} every {INTERVAL}s. Ctrl+C to stop.")

    states: dict[str, dict[str, float]] = {}

    while _running:
        try:
            active = _active_runs()

            for run_id in active - states.keys():
                print(f"[+] new active run {run_id} — starting simulated data")
                states[run_id] = {v: (m["lo"] + m["hi"]) / 2 for v, m in VARIABLES.items()}

            for run_id in states.keys() - active:
                print(f"[-] run {run_id} no longer active — stopping")
            states = {rid: st for rid, st in states.items() if rid in active}

            for run_id, state in states.items():
                _write_tick(run_id, catalog, state)

            if states:
                print(f"tick: {len(states)} active run(s)")
        except Exception as exc:
            print(f"poll failed: {exc}", file=sys.stderr)

        time.sleep(INTERVAL)

    print("Stopped.")


if __name__ == "__main__":
    main()
