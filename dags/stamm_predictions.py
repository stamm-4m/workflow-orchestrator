# =====================================================================
# stamm_predictions DAG
# ---------------------------------------------------------------------
# Description:
# This DAG orchestrates the end-to-end soft-sensor prediction workflow
# for the STAMM platform. It periodically:
#   1. Verifies connectivity to the external InfluxDB instance.
#   2. Detects newly arrived raw bioprocess data per dynamic tag group
#      (device_id, project_name, batch_id, …).
#   3. Builds “wide” feature snapshots around the latest timestamp for
#      each group and exposes them via XCom.
#   4. Calls the external Model Registry to discover all registered
#      models for the project and run predictions on each snapshot.
#   5. Writes the resulting soft-sensor predictions back to InfluxDB,
#      using the same measurement, tags and snapshot timestamp so that
#      predictions are time-aligned with the underlying process data.
#
# Architecture:
#   - The DAG runs periodically in a polling mode.
#   - On each run, `check_new_data` only emits snapshots when it finds
#     timestamps that are both fresh and strictly newer than the last
#     ones seen per group.
#   - If no new snapshots are found, the ShortCircuitOperator stops
#     the downstream tasks, preventing unnecessary model calls and
#     InfluxDB writes.
# =====================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from pendulum import datetime

# Import custom task functions
from tasks.influx import check_db_connection, check_new_data, store_prediction
from tasks.prediction import call_models_from_snapshots


# ---------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------
default_args = {
    "owner": "STAMM",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


# ---------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="stamm_predictions",
    description=(
        "Periodically query new bioreactor data from InfluxDB, execute ML "
        "models via the Model Registry, and store predictions back into InfluxDB."
    ),
    default_args=default_args,
    # Polling interval: adjust as needed (e.g., 30s, 60s, 120s)
    schedule=timedelta(seconds=20),
    start_date=datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["stamm", "influxdb", "ml_models", "snapshot"],
) as dag:

    # -----------------------------------------------------------------
    # 1. Check InfluxDB connectivity
    #
    # Runs at the beginning of each DAG run. Uses `mode="reschedule"`
    # so it does not block a worker while retrying the ping.
    # -----------------------------------------------------------------
    check_db_task = PythonSensor(
        task_id="check_db_connection",
        python_callable=check_db_connection,
        poke_interval=10,
        timeout=60,
        mode="reschedule",
        doc_md="""
        #### Task: Check InfluxDB Connection  
        Ensures that the InfluxDB service is reachable before proceeding.  
        If the connection fails, the DAG will retry according to `retries`.
        """,
    )

    # -----------------------------------------------------------------
    # 2. Detect new data and build snapshots
    #
    # `check_new_data`:
    #   - Queries InfluxDB.
    #   - Derives dynamic groups by tags (device_id, batch_id, etc.).
    #   - Builds "wide" snapshots and stores them in XCom.
    #   - Updates a map of latest timestamps per group.
    #
    # If it returns False → ShortCircuitOperator cuts the DAG and
    # the model tasks and prediction-writing tasks are NOT executed.
    # -----------------------------------------------------------------

    wait_for_data_task = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=check_new_data,
        doc_md="""
        #### Task: Check for New Data (Snapshots Builder)  
        Scans the InfluxDB raw bucket for new data per tag-group and builds
        'wide' snapshots.  
        If no new snapshots are found, the DAG run is short-circuited and
        downstream tasks are skipped.
        """,
    )

    # -----------------------------------------------------------------
    # 3. Run predictions with multiple ML models
    #
    # - Reads snapshots from XCom (key `snapshots`, task `check_new_data`).
    # - Discovers available models in the Model Registry.
    # - Calls the prediction API for each model.
    # - Writes consolidated predictions into XCom (key `predictions`).
    # -----------------------------------------------------------------
    
    call_model_task = PythonOperator(
        task_id="call_models_from_snapshots",
        python_callable=call_models_from_snapshots,
        doc_md="""
        #### Task: Run Model Predictions  
        Executes all registered ML models (e.g., SVR, RF, CART, LSTM, ...)
        on each snapshot provided by `check_new_data`, and publishes a
        consolidated prediction payload to XCom.
        """,
    )

    # -----------------------------------------------------------------
    # 4. Store model predictions back into InfluxDB
    #
    # - Read XCom (`predictions`) of task `call_models_from_snapshots`.
    # -----------------------------------------------------------------
    store_prediction_task = PythonOperator(
        task_id="store_prediction",
        python_callable=store_prediction,
        doc_md="""
        #### Task: Store Predictions  
        Writes the prediction results into the `stamm_predictions` bucket
        with appropriate metadata (batch ID, device ID, project name, model
        name, and version).
        """,
    )

    # -----------------------------------------------------------------
    # Task sequence
    # -----------------------------------------------------------------
    check_db_task >> wait_for_data_task >> call_model_task >> store_prediction_task