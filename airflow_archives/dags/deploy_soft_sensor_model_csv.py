# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 15:02:24 2025

@author: David Camilo Corrales
@email: David-Camilo.Corrales-Munoz@inrae.fr

"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import requests
import time
import os
import json
import logging

# ✅ Paths and API settings
DATA_DIR = "/opt/airflow/data/preprocessed_data/"
CSV_FILE = os.path.join(DATA_DIR, "experiment_2.0.csv")  # Change this filename
ROW_INDEX_FILE = os.path.join(DATA_DIR, "row_index.txt")  # To track the last processed row
SIMULATION_FILE = os.path.join(DATA_DIR, "simulations.csv")  # Path to save results
ML_API_URL = "http://host.docker.internal:8000/predict/"  # Update with your REST API URL

# ✅ Task 1: Read one row from CSV
def read_row():
    if not os.path.exists(ROW_INDEX_FILE):
        row_index = 0  # Start from the first row
    else:
        with open(ROW_INDEX_FILE, "r") as f:
            row_index = int(f.read().strip())

    logging.info(f"🔍 Reading row {row_index} from {CSV_FILE}")

    df = pd.read_csv(CSV_FILE)  # Load full CSV once
    if row_index >= len(df):
        logging.info("🚀 No more data available.")
        return None  # Stop DAG if no more rows exist

    row = df.iloc[row_index].to_dict()  # Read the specific row
    logging.info(f"✅ Processed row: {row}")

    return row

# ✅ Task 2: Send row to the ML Model and save results
def send_row_to_model(**context):
    model_name = context["dag_run"].conf.get("model_name")  # ✅ Get from dag_run.conf
    logging.info(f"Sending data to {ML_API_URL} using model: {model_name}")
    row = context["task_instance"].xcom_pull(task_ids="read_row")
    if row is None:
        logging.info("No more rows to process. Exiting task.")
        return

    payload = json.dumps({
        "model": model_name,  # Dynamically selected model
        "req": {
            "input_data": {
                "temperature": float(row["temperature"]),
                "pH": float(row["pH"]),
                "dissolved_oxygen_concentration": float(row["dissolved_oxygen_concentration"]),
                "agitator": float(row["agitator"]),
                "CO2_percent_in_off_gas": float(row["CO2_percent_in_off_gas"]),
                "oxygen_in_percent_in_off_gas": float(row["oxygen_in_percent_in_off_gas"]),
                "vessel_volume": float(row["vessel_volume"]),
                "sugar_feed_rate": float(row["sugar_feed_rate"])
            }
        }
    })

    
    headers = {"Content-Type": "application/json"}
    response = requests.post(ML_API_URL, data=payload, headers=headers)

    try:
        response_json = response.json()
        predictions = response_json.get("predictions", [])
        prediction = predictions[0].get("value", [None])[0] if predictions else None

        logging.info(f"✅ Extracted Prediction: {prediction} g/L penicillin_concentration")
    except Exception as e:
        logging.error(f"❌ Error parsing API response: {e}")
        prediction = None

    # Save result
    result_data = row.copy()
    result_data["predicted_penicillin_concentration"] = prediction

    df_result = pd.DataFrame([result_data])
    if not os.path.exists(SIMULATION_FILE):
        df_result.to_csv(SIMULATION_FILE, index=False)
    else:
        df_result.to_csv(SIMULATION_FILE, mode='a', header=False, index=False)
    
    time.sleep(1)



# ✅ Task 3: Update row index
def update_row_index():
    if not os.path.exists(ROW_INDEX_FILE):
        row_index = 0
    else:
        with open(ROW_INDEX_FILE, "r") as f:
            row_index = int(f.read().strip())

    row_index += 1  # Move to the next row
    with open(ROW_INDEX_FILE, "w") as f:
        f.write(str(row_index))

    logging.info(f"Updated row index to {row_index}")

# ✅ Task 4: Check if more rows exist
def check_more_rows():
    df = pd.read_csv(CSV_FILE)
    with open(ROW_INDEX_FILE, "r") as f:
        row_index = int(f.read().strip())

    return row_index < len(df)  # Returns True if more rows exist

# ✅ Define Airflow DAG
with DAG(
    "deploy_soft_sensor_model_csv",
    default_args={"owner": "airflow", "start_date": datetime(2025, 3, 20)},
    schedule_interval=None,  # No automatic schedule, triggered manually
    catchup=False,
    params={"model_name": "LSTM"}  # ✅ Add default param
) as dag:

    start = DummyOperator(task_id="start")

    read_row_task = ShortCircuitOperator(
        task_id="read_row",
        python_callable=read_row
    )

    send_row_task = PythonOperator(
        task_id="send_row_to_ml_model",
        python_callable=send_row_to_model,
        provide_context=True,
        #op_kwargs={"model_name": "{{ dag_run.conf.get('model_name') }}"}
    )    

    update_row_index_task = PythonOperator(
        task_id="update_row_index",
        python_callable=update_row_index
    )

    check_more_rows_task = ShortCircuitOperator(
        task_id="check_more_rows",
        python_callable=check_more_rows
    )

    trigger_next_execution = TriggerDagRunOperator(
        task_id="trigger_next_execution",
        trigger_dag_id="deploy_soft_sensor_model_csv",
        conf={"model_name": "{{ dag_run.conf['model_name'] }}"},
        wait_for_completion=False  # Do not wait; trigger next execution and exit
    )

    end = DummyOperator(task_id="end")

    # ✅ DAG flow: Process one row per execution
    start >> read_row_task >> send_row_task >> update_row_index_task >> check_more_rows_task
    check_more_rows_task >> trigger_next_execution  # If more rows exist, trigger a new DAG execution
    check_more_rows_task >> end  # If no more rows, stop
