# =====================================================================
# STAMM_Predictions DAG
# ---------------------------------------------------------------------
# Description:
# This DAG orchestrates the complete ML prediction workflow for the
# STAMM platform. The process includes:
#   1. Checking connectivity to InfluxDB
#   2. Waiting for new raw sensor data
#   3. Running multiple ML model predictions
#   4. Storing predictions back into InfluxDB
#
# Author: Jairo Alexander Astudillo Lagos
# Organization: INRAE – Bioindustry 4.0 Project
# Repository: https://gitlab.com/stamm-4m
# =====================================================================

from datetime import timedelta
from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

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
    dag_id="STAMM_Predictions",
    description="Periodically query new bioreactor data, execute ML models, and store predictions in influxDB.",
    default_args=default_args,
    schedule=timedelta(seconds=15),  # execution interval
    start_date=datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["stamm", "bioindustry4.0", "prediction", "influxdb"],
) as dag:

    # -----------------------------------------------------------------
    # 1. Check InfluxDB connectivity
    # -----------------------------------------------------------------
    check_db_task = PythonSensor(
        task_id="check_db_connection",
        python_callable=check_db_connection,
        poke_interval=10,
        timeout=60,
        mode="poke",
        doc_md="""
        #### Task: Check InfluxDB Connection  
        Ensures that the InfluxDB service is reachable before proceeding.  
        If the connection fails, the DAG will retry according to `retries`.
        """,
    )

    # -----------------------------------------------------------------
    # 2. Wait for new data from the bioreactor (raw bucket)
    # -----------------------------------------------------------------
    wait_for_data_task = PythonSensor(
        task_id="check_new_data",
        python_callable=check_new_data,
        poke_interval=15,
        timeout=300,
        mode="poke",
        doc_md="""
        #### Task: Check for New Data  
        Monitors the InfluxDB raw data bucket and detects when new
        sensor readings are available, based on timestamp changes.
        """,
    )

    # -----------------------------------------------------------------
    # 3. Run predictions with multiple ML models
    # -----------------------------------------------------------------
    call_model_task = PythonOperator(
        task_id="call_models_from_snapshots",
        python_callable=call_models_from_snapshots,
        doc_md="""
        #### Task: Run Model Predictions  
        Executes all registered ML models (e.g., SVR, RF, CART, LSTM, ...)
        on the latest snapshot of sensor data retrieved from InfluxDB.
        """,
    )

    # -----------------------------------------------------------------
    # 4. Store model predictions back into InfluxDB
    # -----------------------------------------------------------------
    store_prediction_task = PythonOperator(
        task_id="store_prediction",
        python_callable=store_prediction,
        doc_md="""
        #### Task: Store Predictions  
        Writes the prediction results into the `stamm_predictions` bucket
        with appropriate metadata (batch ID, timestamp, model name, etc.).
        """,
    )

    # -----------------------------------------------------------------
    # Task sequence
    # -----------------------------------------------------------------
    check_db_task >> wait_for_data_task >> call_model_task >> store_prediction_task
