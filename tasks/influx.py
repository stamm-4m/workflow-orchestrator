import os
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point

# Logger provided by Airflow context
log = LoggingMixin().log

# Load environment variables from .env
load_dotenv(dotenv_path="/opt/airflow/.env")

# Load InfluxDB configuration from environment
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT")


def check_db_connection():
    """
    Sensor function to check if InfluxDB is reachable and responding.
    Returns True if the connection is successful, False otherwise.
    """
    try:
        if not all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG]):
            log.error("One or more InfluxDB environment variables are not set.")
            log.error(
                f"INFLUXDB_URL={INFLUXDB_URL}, INFLUXDB_TOKEN={INFLUXDB_TOKEN}, INFLUXDB_ORG={INFLUXDB_ORG}"
            )
            return False

        with InfluxDBClient(
            url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
        ) as client:
            if client.ping():
                log.info("InfluxDB connection successful.")
                return True
            else:
                log.warning("InfluxDB ping failed.")
                return False
    except Exception as e:
        log.error(f"Error checking InfluxDB connection: {e}")
        return False


def check_new_data(ti):
    """
    Sensor function to detect new data entries in InfluxDB from the last 15 seconds.
    Returns True if new data is found and pushes it via XCom, False otherwise.
    """
    try:
        with InfluxDBClient(
            url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
        ) as client:
            query_api = client.query_api()
            query = f'from(bucket: "{INFLUXDB_BUCKET}") |> range(start: -15s)'

            results = query_api.query(org=INFLUXDB_ORG, query=query)
            extracted_data = []
            latest_timestamp = None

            for table in results:
                for record in table.records:
                    timestamp_str = str(record.get_time())
                    extracted_data.append(
                        {
                            "time": timestamp_str,
                            "field": record.get_field(),
                            "value": record.get_value(),
                        }
                    )
                    latest_timestamp = timestamp_str

        if not extracted_data:
            log.info("No data found in InfluxDB.")
            return False

        last_timestamp = ti.xcom_pull(key="last_timestamp", task_ids="new_data")
        if latest_timestamp == last_timestamp:
            log.info(f"Same timestamp detected ({latest_timestamp}). Waiting for new data...")
            return False

        # Push new timestamp and data to XCom
        ti.xcom_push(key="last_timestamp", value=latest_timestamp)
        ti.xcom_push(key="influx_data", value=extracted_data)
        log.info(f"New data found at timestamp {latest_timestamp}.")
        return True

    except Exception as e:
        log.error(f"Error in check_new_data: {e}")
        return False


def store_prediction(ti):
    """
    Task function that writes the model predictions into InfluxDB.
    Expects predictions and timestamp from XCom.
    """
    try:
        predictions = ti.xcom_pull(task_ids="call_model", key="predictions")
        timestamp = ti.xcom_pull(task_ids="new_data", key="last_timestamp")

        if predictions is None or timestamp is None:
            log.warning("No predictions or timestamp found in XCom.")
            return

        with InfluxDBClient(
            url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
        ) as client:
            with client.write_api() as write_api:
                for model_name, prediction in predictions.items():
                    if prediction is not None:
                        point = (
                            Point("2")
                            .field(f"pred_penicillin_{model_name}", float(prediction))
                            .time(timestamp)
                        )
                        write_api.write(
                            bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point
                        )

        log.info("Predictions stored in InfluxDB.")

    except Exception as e:
        log.error(f"Error storing predictions: {e}")

