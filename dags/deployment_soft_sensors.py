# =====================================================================
# deployment_soft_sensors DAG  (API edition)
# ---------------------------------------------------------------------
# Description:
# Event-driven DAG that closes the loop between bioreactor sensor data
# and the STAMM Model Registry — entirely over the Model Registry HTTP
# API (tasks/registry_client.py), so it works whether Airflow and
# model-registry are on the same host or on separate machines.
#
# Trigger:
#   This DAG has no built-in schedule (schedule=None). It is triggered
#   externally — typically from the experiment-creation endpoint — with
#   the following conf payload:
#
#     {
#       "run_id":             "<uuid>",     -- active run to monitor
#       "experiment_id":      "<uuid>",
#       "project_id":         "<uuid>",
#       "project_name":       "<str>",      -- used to pick model registry project
#       "model_ids":          ["<slug>"],   -- models.slug of every model attached
#                                               to the experiment; every one gets a
#                                               prediction each cycle
#       "vessel_id":          "<uuid>",     -- bioreactor this experiment runs on (optional)
#       "last_processed_time":"<iso>",      -- omit on first trigger
#       "user_id":            "<uuid>"      -- optional, for audit
#     }
#
# Flow per cycle:
#   1. check_db_connection      — PythonSensor: verifies the Model Registry API
#                                 is reachable and the service account is valid
#   2. wait_for_new_data        — PythonSensor: blocks (reschedule mode) until
#                                 GET /api/v1/runs/{run_id}/sensor_readings
#                                 returns rows newer than last_processed_time
#   3. build_snapshot           — Pivots latest sensor + actuator readings into
#                                 a "wide" feature dict (+ lag history); pushes to XCom
#   4. call_models               — Calls every model attached to the experiment
#                                 (see tasks/prediction.py) via the Model Registry API
#   5. store_prediction         — POSTs predictions to /api/v1/predictions/
#   6. show_prediction_summary  — Logs a readable result for the Airflow UI
#   7. trigger_next_cycle       — Re-triggers this DAG for the same run_id, with
#                                 last_processed_time advanced, so predictions keep
#                                 happening for the experiment's whole start..end
#                                 window. Stops itself (no-op) once end_time has
#                                 passed — wait_for_new_data also stops the chain
#                                 early via AirflowSkipException in that case.
# =====================================================================

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from tasks.postgres import (
    build_snapshot,
    check_db_connection,
    show_prediction_summary,
    store_prediction,
    trigger_next_cycle,
    wait_for_new_data,
)
from tasks.prediction import call_models_from_snapshots

# ---------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------
default_args = {
    "owner": "STAMM",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

# ---------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="deployment_soft_sensors",
    description=(
        "Event-driven soft-sensor pipeline: triggered on experiment creation, "
        "polls the Model Registry API for new bioreactor readings, calls every "
        "model attached to the experiment, writes predictions back via the API, "
        "and re-triggers itself for the next batch of data until the "
        "experiment's end_time passes."
    ),
    default_args=default_args,
    schedule=None,                       # triggered externally / by itself only
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=10,                  # one concurrent cycle per experiment
    is_paused_upon_creation=True,
    params={                             # document expected conf keys
        "run_id": "",
        "experiment_id": "",
        "project_id": "",
        "project_name": "",
        "model_ids": [],
        "vessel_id": "",
        "last_processed_time": "",
        "user_id": "",
    },
    tags=["stamm", "api", "ml_models", "soft_sensor", "event_driven"],
) as dag:

    # -----------------------------------------------------------------
    # 1. Verify the Model Registry API is reachable
    #    Uses reschedule mode so it does not hold a worker slot.
    # -----------------------------------------------------------------
    check_db_task = PythonSensor(
        task_id="check_db_connection",
        python_callable=check_db_connection,
        poke_interval=10,
        timeout=120,
        mode="reschedule",
        doc_md="""
        #### Task: Check Model Registry API Connection
        Logs in with the Airflow service account before proceeding.
        Retries every 10 s for up to 2 minutes.
        """,
    )

    # -----------------------------------------------------------------
    # 2. Wait for new sensor readings
    #
    # Polls GET /api/v1/runs/{run_id}/sensor_readings?since=last_processed_time
    # and succeeds as soon as it returns at least one row. Gives up early
    # (skips the rest of the DAG) once the experiment's end_time has passed.
    #
    # In reschedule mode the worker slot is released between pokes.
    # Timeout is 24 h — long enough for overnight experiments.
    # -----------------------------------------------------------------
    wait_for_data_task = PythonSensor(
        task_id="wait_for_new_data",
        python_callable=wait_for_new_data,
        poke_interval=10,
        timeout=86400,                   # 24 hours
        mode="reschedule",
        doc_md="""
        #### Task: Wait for New Sensor Data
        Polls `GET /api/v1/runs/{run_id}/sensor_readings` for rows newer than
        `last_processed_time`. Fires when the bioreactor starts sending data.
        Skips the rest of the DAG once the experiment has ended.
        """,
    )

    # -----------------------------------------------------------------
    # 3. Build wide snapshot (+ lag history)
    #    Pivots the latest sensor + actuator readings into a flat dict
    #    keyed by variable name, pushed to XCom key "snapshots". Also
    #    fetches a wider window of raw readings when an attached model
    #    declares lagged features, pushed to XCom key "history".
    # -----------------------------------------------------------------
    build_snapshot_task = PythonOperator(
        task_id="build_snapshot",
        python_callable=build_snapshot,
        doc_md="""
        #### Task: Build Feature Snapshot
        Reads the most recent sensor_readings and actuator_states for the
        run, pivots them into a wide feature dict, and pushes to XCom.
        """,
    )

    # -----------------------------------------------------------------
    # 4. Run model predictions
    #    Runs every model attached to the experiment (conf["model_ids"]),
    #    each with its own feature list and lag handling, and invokes
    #    POST /{project_id}/predict/{model_id} for each one.
    # -----------------------------------------------------------------
    call_model_task = PythonOperator(
        task_id="call_models_from_snapshots",
        python_callable=call_models_from_snapshots,
        doc_md="""
        #### Task: Run Model Predictions
        Calls POST /{project_id}/predict/{model_id} once per model attached
        to the experiment. Results are pushed to XCom key "predictions".
        """,
    )

    # -----------------------------------------------------------------
    # 5. Store predictions via the Model Registry API
    #    POSTs to /api/v1/predictions/ (time, run_id, model_id, value).
    # -----------------------------------------------------------------
    store_prediction_task = PythonOperator(
        task_id="store_prediction",
        python_callable=store_prediction,
        doc_md="""
        #### Task: Store Predictions
        POSTs each model prediction to /api/v1/predictions/, matching
        the model_key (models.slug) to models.id via the models catalog.
        """,
    )

    # -----------------------------------------------------------------
    # 6. Show the result
    #    Prints a clean one-line-per-prediction summary — this is the task
    #    to open in the Airflow UI to see what this DAG run predicted.
    # -----------------------------------------------------------------
    show_result_task = PythonOperator(
        task_id="show_prediction_summary",
        python_callable=show_prediction_summary,
        doc_md="""
        #### Task: Show Prediction Summary
        Logs the run_id, model, value and whether it was stored, for every
        prediction produced this run. Check this task's log for the result.
        """,
    )

    # -----------------------------------------------------------------
    # 7. Keep predicting until the experiment ends
    #    Re-triggers this DAG for the same run_id with last_processed_time
    #    advanced. No-ops once the experiment's end_time has passed.
    # -----------------------------------------------------------------
    trigger_next_cycle_task = PythonOperator(
        task_id="trigger_next_cycle",
        python_callable=trigger_next_cycle,
        doc_md="""
        #### Task: Trigger Next Cycle
        Re-triggers deployment_soft_sensors for the same run_id so the next
        batch of sensor data gets its own prediction cycle, until the
        experiment's end_time passes.
        """,
    )

    # -----------------------------------------------------------------
    # Task sequence — one cycle, then re-triggers itself for the next one.
    # -----------------------------------------------------------------
    (
        check_db_task
        >> wait_for_data_task
        >> build_snapshot_task
        >> call_model_task
        >> store_prediction_task
        >> show_result_task
        >> trigger_next_cycle_task
    )
