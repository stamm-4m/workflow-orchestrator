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


def _resolve_catalog_by_bioreactor() -> dict[str, dict[str, dict]]:
    """equipment_id (bioreactor) -> {variable -> {kind, id}}.

    Built from equipment_components — the sensors/actuators actually
    registered to each bioreactor — instead of a single global "the"
    sensor per variable name. Two runs on two different bioreactors get
    two different sensor_ids for e.g. "temperature", exactly like real
    hardware would have two separate probes."""
    sensors = {s["id"]: s for s in _get_all("/api/v1/sensors/")}
    actuators = {a["id"]: a for a in _get_all("/api/v1/actuators/")}
    components = _get_all("/api/v1/equipment_components/")

    by_equipment: dict[str, dict[str, dict]] = {}
    for c in components:
        eq_id = c.get("equipment_id")
        if not eq_id:
            continue
        bucket = by_equipment.setdefault(eq_id, {})
        sensor = sensors.get(c.get("sensor_id"))
        if sensor and sensor.get("variable"):
            bucket[sensor["variable"]] = {"kind": "sensor", "id": sensor["id"]}
        actuator = actuators.get(c.get("actuator_id"))
        if actuator and actuator.get("variable"):
            bucket[actuator["variable"]] = {"kind": "actuator", "id": actuator["id"]}
    return by_equipment


def _catalog_for_vessel(by_equipment: dict[str, dict[str, dict]], vessel_id: str | None) -> dict[str, dict] | None:
    """The subset of VARIABLES this specific bioreactor actually has
    registered, or None (with a warning) if any are missing — never
    invents a sensor/actuator that isn't really assigned to this vessel."""
    if not vessel_id:
        print("  no vessel_id set on this experiment — can't resolve its sensors, skipping.", file=sys.stderr)
        return None
    available = by_equipment.get(vessel_id, {})
    missing = [f"{var} ({meta['kind']})" for var, meta in VARIABLES.items() if var not in available]
    if missing:
        print(f"  bioreactor {vessel_id} is missing catalog rows in equipment_components, skipping:", file=sys.stderr)
        for m in missing:
            print(f"    - {m}", file=sys.stderr)
        return None
    return {var: available[var] for var in VARIABLES}


def _active_runs() -> dict[str, str | None]:
    """run_id -> vessel_id (the bioreactor its experiment runs on) for every
    run whose experiment is 'running', still within its planned end_time (if
    any — e.g. duration/duration_unit set from FermOps), and has no end_time
    of its own, for the configured project.

    Without the end_time check, this kept generating sensor data forever
    for experiments that had already reached the end of their planned
    duration — the DAG's own re-trigger loop correctly stops predicting at
    that point, but the simulator had nothing telling it to stop too."""
    now = datetime.now(timezone.utc)
    experiments = _get_all("/api/v1/experiments/")
    running_exp_vessel: dict[str, str | None] = {}
    for e in experiments:
        if e.get("status") != "running" or e.get("project_id") != PROJECT_ID:
            continue
        end_time = e.get("end_time")
        if end_time:
            try:
                exp_end = datetime.fromisoformat(str(end_time).replace("Z", "+00:00"))
                if exp_end.tzinfo is None:
                    exp_end = exp_end.replace(tzinfo=timezone.utc)
                if now > exp_end:
                    continue  # planned duration is over
            except ValueError:
                pass
        running_exp_vessel[e["id"]] = e.get("vessel_id")
    if not running_exp_vessel:
        return {}

    runs = _get_all("/api/v1/runs/")
    return {
        r["id"]: running_exp_vessel[r["experiment_id"]]
        for r in runs
        if r.get("experiment_id") in running_exp_vessel and not r.get("end_time")
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
    print(f"Watching project={PROJECT_ID} every {INTERVAL}s. Ctrl+C to stop.")

    states: dict[str, dict[str, float]] = {}
    catalogs: dict[str, dict] = {}

    while _running:
        try:
            active = _active_runs()  # run_id -> vessel_id
            active_ids = active.keys()

            # Re-resolved every tick (cheap, paginated API calls) so a
            # bioreactor that gets its equipment_components links added
            # (or a new run on it) is picked up without restarting this
            # service — matters for runs that show up mid-tick.
            by_equipment = _resolve_catalog_by_bioreactor()

            for run_id in active_ids - states.keys():
                vessel_id = active[run_id]
                catalog = _catalog_for_vessel(by_equipment, vessel_id)
                if catalog is None:
                    continue  # retry next tick once the vessel's catalog is complete
                print(f"[+] new active run {run_id} (vessel {vessel_id}) — starting simulated data")
                catalogs[run_id] = catalog
                states[run_id] = {v: (m["lo"] + m["hi"]) / 2 for v, m in VARIABLES.items()}

            for run_id in states.keys() - active_ids:
                print(f"[-] run {run_id} no longer active — stopping")
            states = {rid: st for rid, st in states.items() if rid in active_ids}
            catalogs = {rid: c for rid, c in catalogs.items() if rid in states}

            for run_id, state in states.items():
                _write_tick(run_id, catalogs[run_id], state)

            if states:
                print(f"tick: {len(states)} active run(s)")
        except Exception as exc:
            print(f"poll failed: {exc}", file=sys.stderr)

        time.sleep(INTERVAL)

    print("Stopped.")


if __name__ == "__main__":
    main()
