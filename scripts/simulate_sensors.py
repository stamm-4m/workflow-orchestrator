"""
simulate_sensors.py — Stand-in data generator for the STAMM PostgreSQL DB.

While the real bioreactor/PLC integration isn't connected yet, this script
plays that role: it writes plausible sensor_readings/actuator_states rows
for one active run, on a loop, so the deployment_soft_sensors DAG has
something to detect and predict on. Swap it out once real telemetry lands —
nothing downstream needs to change, since the DAG only cares about rows
appearing in these tables.

Usage:
    python scripts/simulate_sensors.py --run-id 77777777-aaaa-bbbb-cccc-000000000001

Env vars (all optional, defaults match the local docker-compose setup):
    SIM_DB_HOST=127.0.0.1
    SIM_DB_PORT=5432
    SIM_DB_NAME=stamm
    SIM_DB_USER=stamm
    SIM_DB_PASSWORD=changeme
    SIM_INTERVAL_SECONDS=20
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# The 8 variables the IndPenSim (penicillin) models expect, per
# model_registry/api/projects/IndPenSim/configs/*.yaml (inputs.features)
# and project_info.yaml (renamed_variable). kind picks the catalog table.
# ---------------------------------------------------------------------------
VARIABLES = {
    "temperature": dict(kind="sensor", catalog_id="SIM-T", unit="K", lo=298, hi=308),
    "pH": dict(kind="sensor", catalog_id="SIM-PH", unit="pH", lo=5.5, hi=7.5),
    "dissolved_oxygen_concentration": dict(
        kind="sensor", catalog_id="SIM-DO", unit="mg/L", lo=0, hi=10
    ),
    "CO2_percent_in_off_gas": dict(
        kind="sensor", catalog_id="SIM-CO2", unit="%", lo=0, hi=10
    ),
    "oxygen_in_percent_in_off_gas": dict(
        kind="sensor", catalog_id="SIM-O2OFF", unit="%", lo=10, hi=21
    ),
    "vessel_volume": dict(kind="sensor", catalog_id="SIM-VOL", unit="L", lo=50, hi=150),
    "agitator": dict(kind="actuator", catalog_id="SIM-AGIT", unit="rpm", lo=100, hi=1200),
    "sugar_feed_rate": dict(
        kind="actuator", catalog_id="SIM-SUGAR", unit="L/h", lo=0, hi=2
    ),
}

_running = True


def _stop(signum, frame):
    global _running
    _running = False


def _connect():
    return psycopg2.connect(
        host=os.getenv("SIM_DB_HOST", "127.0.0.1"),
        port=os.getenv("SIM_DB_PORT", "5432"),
        dbname=os.getenv("SIM_DB_NAME", "stamm"),
        user=os.getenv("SIM_DB_USER", "stamm"),
        password=os.getenv("SIM_DB_PASSWORD", "changeme"),
    )


def _ensure_catalog(conn) -> dict[str, str]:
    """Create sensor/actuator catalog rows if missing, return variable -> uuid."""
    ids: dict[str, str] = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for variable, meta in VARIABLES.items():
            table, id_col = (
                ("sensors", "sensor_id") if meta["kind"] == "sensor" else ("actuators", "actuator_id")
            )
            cur.execute(
                f"""
                INSERT INTO {table} ({id_col}, name, variable, unit)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ({id_col}) DO NOTHING
                """,
                (meta["catalog_id"], f"Simulated {variable}", variable, meta["unit"]),
            )
            cur.execute(f"SELECT id FROM {table} WHERE {id_col} = %s", (meta["catalog_id"],))
            ids[variable] = cur.fetchone()["id"]
    conn.commit()
    return ids


def _check_run_exists(conn, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM runs WHERE id = %s::uuid", (run_id,))
        if cur.fetchone() is None:
            raise SystemExit(f"run_id {run_id} not found in 'runs' table — create it first.")


def _write_tick(conn, run_id: str, ids: dict[str, str], state: dict[str, float]) -> None:
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for variable, meta in VARIABLES.items():
            # Small random walk instead of pure noise, clipped to range.
            span = meta["hi"] - meta["lo"]
            step = random.uniform(-0.03, 0.03) * span
            value = min(meta["hi"], max(meta["lo"], state[variable] + step))
            state[variable] = value

            table = "sensor_readings" if meta["kind"] == "sensor" else "actuator_states"
            id_col = "sensor_id" if meta["kind"] == "sensor" else "actuator_id"
            cur.execute(
                f"""
                INSERT INTO {table} (time, run_id, {id_col}, value)
                VALUES (%s, %s::uuid, %s::uuid, %s)
                ON CONFLICT DO NOTHING
                """,
                (now, run_id, ids[variable], value),
            )
    conn.commit()
    print(f"[{now.isoformat()}] wrote 1 row per variable ({len(ids)} vars) for run={run_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="UUID of the run to stream data into")
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("SIM_INTERVAL_SECONDS", "20")),
        help="Seconds between ticks (default 20)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    conn = _connect()
    _check_run_exists(conn, args.run_id)
    ids = _ensure_catalog(conn)
    state = {v: (m["lo"] + m["hi"]) / 2 for v, m in VARIABLES.items()}

    print(f"Simulating {len(ids)} variables for run={args.run_id} every {args.interval}s. Ctrl+C to stop.")
    while _running:
        try:
            _write_tick(conn, args.run_id, ids, state)
        except Exception as exc:
            print(f"tick failed: {exc}", file=sys.stderr)
            conn.rollback()
        time.sleep(args.interval)

    conn.close()
    print("Stopped.")


if __name__ == "__main__":
    main()
