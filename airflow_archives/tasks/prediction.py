import json
import os
import requests
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv

# Initialize Airflow logger
log = LoggingMixin().log

# Load environment variables from .env file
load_dotenv(dotenv_path="/opt/airflow/.env")

# Model API endpoint
MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT")

# List of model names to be used for prediction
MODEL_LIST = ["CART", "LSTM", "GBM", "SVM"]

# Required features from InfluxDB data
FEATURES = [
    "temperature",
    "pH",
    "dissolved_oxygen_concentration",
    "agitator",
    "CO2_percent_in_off_gas",
    "oxygen_in_percent_in_off_gas",
    "vessel_volume",
    "sugar_feed_rate",
]


def call_model(ti):
    """
    Airflow task that prepares the feature vector from InfluxDB data,
    sends it to different models for prediction, and stores the results in XCom.
    """
    try:
        results = ti.xcom_pull(key="influx_data", task_ids="new_data")

        if not results:
            log.warning("No InfluxDB data found in XCom.")
            return

        # Initialize feature dictionary with None
        feature_dict = {field: None for field in FEATURES}

        # Populate available fields from InfluxDB records
        for record in results:
            field = record.get("field")
            if field in feature_dict:
                feature_dict[field] = record.get("value")

        # Check for missing features
        missing = [k for k, v in feature_dict.items() if v is None]
        if missing:
            log.warning(f"Missing features for prediction: {missing}")
            return

        # Collect predictions from each model
        predictions = {}
        for model in MODEL_LIST:
            predictions[model] = invoke_model_api(model, feature_dict)

        log.info(f"Model predictions: {predictions}")
        ti.xcom_push(key="predictions", value=predictions)

    except Exception as e:
        log.error(f"Exception in call_model task: {e}")


def invoke_model_api(model_name, feature_dict):
    """
    Sends a POST request to the ML API for a given model and returns the prediction.
    
    Args:
        model_name (str): Name of the model (e.g., CART, LSTM)
        feature_dict (dict): Dictionary of input features

    Returns:
        float or None: Predicted value or None if request fails
    """
    payload = {
        "model": model_name,
        "req": {
            "input_data": feature_dict
        }
    }

    try:
        response = requests.post(
            MODEL_ENDPOINT,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=20,
        )

        if response.status_code == 200:
            data = response.json()
            value = data.get("predictions", [{}])[0].get("value")

            # Normalize nested list formats
            if isinstance(value, list):
                return value[0][0] if isinstance(value[0], list) else value[0]
            return value
        else:
            log.error(f"Model {model_name} failed: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        log.error(f"Error calling model {model_name}: {e}")
        return None

