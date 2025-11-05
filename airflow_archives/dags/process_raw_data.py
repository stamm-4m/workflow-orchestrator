# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 15:02:24 2025

@author: David Camilo Corrales
@email: David-Camilo.Corrales-Munoz@inrae.fr

"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import yaml
import os
import logging

# Corrected file paths for Windows + Docker
DATA_DIR = "/opt/airflow/data/"
RAW_CSV_PATH = os.path.join(DATA_DIR, "100_Batches_IndPenSim_V3.1.csv")
YAML_PATH = os.path.join(DATA_DIR, "project_info.yaml")
OUTPUT_DIR = os.path.join(DATA_DIR, "preprocessed_data")

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to rename columns using project_info.yaml
def rename_columns():
    logging.info("Starting column renaming task...")

    # Load YAML configuration
    with open(YAML_PATH, "r") as file:
        config = yaml.safe_load(file)

    column_mapping = {var["name"]: var["renamed_variable"] for var in config["variables"]}
    logging.info(f"Column mapping loaded: {column_mapping}")

    # Load raw CSV
    df = pd.read_csv(RAW_CSV_PATH)
    logging.info(f"Raw data loaded. Shape: {df.shape}")

    # Rename columns
    df.rename(columns=column_mapping, inplace=True)

    # Save renamed data
    renamed_csv_path = os.path.join(OUTPUT_DIR, "renamed_data.csv")
    df.to_csv(renamed_csv_path, index=False)
    logging.info(f"Renamed data saved to: {renamed_csv_path}")

# Function to split CSV by experiment_ID
def split_by_experiment():
    logging.info("Starting split by experiment task...")

    # Load renamed CSV
    renamed_csv_path = os.path.join(OUTPUT_DIR, "renamed_data.csv")
    df = pd.read_csv(renamed_csv_path)
    logging.info(f"Renamed data loaded. Shape: {df.shape}")

    # Split by experiment_ID and save separately
    for exp_id, exp_df in df.groupby("experiment_ID"):
        exp_path = os.path.join(OUTPUT_DIR, f"experiment_{exp_id}.csv")
        exp_df.to_csv(exp_path, index=False)
        logging.info(f"Saved {exp_df.shape[0]} rows to {exp_path}")

# Define Airflow DAG
with DAG(
    "process_raw_data",
    default_args={"owner": "airflow", "start_date": datetime(2025, 3, 20)},
    schedule_interval=None,
    catchup=False,
) as dag:

    rename_task = PythonOperator(
        task_id="rename_columns",
        python_callable=rename_columns
    )

    split_task = PythonOperator(
        task_id="split_by_experiment",
        python_callable=split_by_experiment
    )

    rename_task >> split_task
