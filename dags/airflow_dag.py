# airflow_dag.py

# Airflow DAG to orchestrate the following process:
# 1. Check InfluxDB connection
# 2. Wait for new sensor data
# 3. Run multiple machine learning model predictions
# 4. Store the results back to InfluxDB

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from tasks.influx import check_db_connection, check_new_data, store_prediction
from tasks.prediction import call_model

# Default configuration in the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# DAG definition
with DAG(
    "STAMM_Predictions",
    default_args=default_args,
    description="Query data, run ML models and store predictions",
    schedule_interval=timedelta(seconds=15),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Sensor to verify that the InfluxDB service is reachable
    check_db = PythonSensor(
        task_id="check_db_connection",
        python_callable=check_db_connection,
        poke_interval=10,
        timeout=60,
        mode="poke",
    )

    # Sensor to wait until new data appears in InfluxDB
    wait_for_data = PythonSensor(
        task_id="new_data",
        python_callable=check_new_data,
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    # Task that runs ML model predictions on the new data
    call_model_task = PythonOperator(
        task_id="call_model",
        python_callable=call_model,
        provide_context=True,
    )

    # Task that stores the model predictions back into InfluxDB
    store_prediction_task = PythonOperator(
        task_id="store_prediction",
        python_callable=store_prediction,
        provide_context=True,
    )

    # Define the task sequence
    check_db >> wait_for_data >> call_model_task >> store_prediction_task
