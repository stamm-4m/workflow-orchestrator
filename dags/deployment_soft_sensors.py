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
#       "run_id":             "<uuid>",   -- active run to monitor
#       "experiment_id":      "<uuid>",
#       "project_id":         "<uuid>",
#       "project_name":       "<str>",    -- used to pick model registry project
#       "last_processed_time":"<iso>",    -- omit on first trigger
#       "user_id":            "<uuid>"    -- optional, for audit
#     }
#
# Flow per trigger:
#   1. check_db_connection      — PythonSensor: verifies the Model Registry API
#                                 is reachable and the service account is valid
#   2. wait_for_new_data        — PythonSensor: blocks (reschedule mode) until
#                                 GET /api/v1/runs/{run_id}/sensor_readings
#                                 returns rows newer than last_processed_time
#   3. build_snapshot           — Pivots latest sensor + actuator readings into
#                                 a "wide" feature dict; pushes to XCom
#   4. call_models              — Calls the project's one official model
#                                 (see tasks/prediction.py) via the Model Registry API
#   5. store_prediction         — POSTs predictions to /api/v1/predictions/
#   6. show_prediction_summary  — Logs a readable result for the Airflow UI
#
# NOTE (one-shot, temporary): this used to end with check_experiment_active
# (ShortCircuitOperator) + re_trigger (TriggerDagRunOperator) so it kept
# polling and predicting in a loop for as long as the experiment stayed
# "running". Turned off for now because the Dash has no way to end/pause an
# experiment yet, so the loop never stopped on its own. One DAG run now
# produces exactly one prediction per model and finishes. Re-enable the loop
# (see git history / tasks/postgres.py:check_experiment_active, still there
# unused) once the Dash can mark an experiment as ended.
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
        "polls the Model Registry API for new bioreactor readings, calls the "
        "prediction endpoints, writes predictions back via the API. Currently "
        "one-shot (no re-trigger loop) — see NOTE at top of this file."
    ),
    default_args=default_args,
    schedule=None,                       # triggered externally only
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=10,                  # one concurrent run per experiment
    is_paused_upon_creation=True,
    params={                             # document expected conf keys
        "run_id": "",
        "experiment_id": "",
        "project_id": "",
        "project_name": "",
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
    # and succeeds as soon as it returns at least one row.
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
        """,
    )

    # -----------------------------------------------------------------
    # 3. Build wide snapshot
    #    Pivots the latest sensor + actuator readings into a flat dict
    #    keyed by variable name, pushed to XCom key "snapshots".
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
    # 4. Run model prediction
    #    Resolves the project's one official model (MODEL_ID_* in .env) and
    #    invokes POST /{project_id}/predict/{model_id} for it.
    # -----------------------------------------------------------------
    call_model_task = PythonOperator(
        task_id="call_models_from_snapshots",
        python_callable=call_models_from_snapshots,
        doc_md="""
        #### Task: Run Model Prediction
        Confirms the project's official model is registered (GET
        /{project_id}/list_models/) and calls POST /{project_id}/predict/{model_id}
        for it. Result is pushed to XCom key "predictions".
        """,
    )

    # -----------------------------------------------------------------
    # 5. Store predictions via the Model Registry API
    #    POSTs to /api/v1/predictions/ (time, run_id, soft_sensor_id, value).
    # -----------------------------------------------------------------
    store_prediction_task = PythonOperator(
        task_id="store_prediction",
        python_callable=store_prediction,
        doc_md="""
        #### Task: Store Predictions
        POSTs each model prediction to /api/v1/predictions/, matching
        the model_key to soft_sensor_id via project_soft_sensors.
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
    # Task sequence — one-shot: stops after showing the result.
    # See the NOTE at the top of this file for why re_trigger is off.
    # -----------------------------------------------------------------
    (
        check_db_task
        >> wait_for_data_task
        >> build_snapshot_task
        >> call_model_task
        >> store_prediction_task
        >> show_result_task
    )
