
# STAMM Predictions Pipeline

This project implements an automated ML prediction pipeline using **Apache Airflow** to orchestrate data extraction, model inference, and prediction storage based on sensor data retrieved from **InfluxDB**.

---

## Architecture Overview

The DAG (`STAMM_Predictions`) performs the following steps every 15 seconds:

1. **Check InfluxDB Connection**  
   Ensures the InfluxDB service is reachable before proceeding.

2. **Wait for New Sensor Data**  
   Monitors InfluxDB for new data entries in the last 15 seconds.

3. **Run Predictions**  
   Sends the extracted features to multiple ML models (`CART`, `LSTM`, `GBM`, `SVM`) via a REST API and gathers their predictions.

4. **Store Predictions**  
   Saves the predicted values to InfluxDB with the corresponding timestamp.

---

```mermaid
graph TD
    A[Start DAG: STAMM_Predictions<br>⏱ Every 15 seconds] --> B[Check InfluxDB Connection<br>Ensure service is reachable]
    B --> C[Wait for New Sensor Data<br>Monitor last 15s window]
    C --> D[Run Predictions<br>Send features to ML models<br>(CART, LSTM, GBM, SVM via REST API)]
    D --> E[Store Predictions<br>Write results to InfluxDB with timestamp]
    E --> F[End / Next Cycle]
```

## Technologies Used

- **Apache Airflow** for orchestration
- **InfluxDB** for real-time sensor data
- **Python + Requests** for API communication
- **.env + dotenv** for secure environment configuration
- **Docker** for isolated deployment

---

## Project Structure

```
.
├── dags/
│   └── airflow_dag.py         # Main DAG definition
├── tasks/
│   ├── influx.py              # InfluxDB I/O and sensors
│   └── prediction.py          # Model inference logic
├── config/
│   └── airflow.cfg            # Airflow configuration file
├── .env                       # Environment variables for secrets
└── docker-compose.yaml        # Container configuration
```

---

## Environment Variables (`.env`)

These variables must be configured before running the project:

```env
INFLUXDB_URL=http://influxdb:8086
INFLUXDB_TOKEN=your_token_here
INFLUXDB_ORG=your_org
INFLUXDB_BUCKET=sensor_data
MODEL_ENDPOINT=http://ml-api:8000/predict
```

---

## How to Run

1. Clone the repo
2. Create a `.env` file as shown above
3. Run using Docker:

```bash
docker compose up --build
```

4. Access Airflow at [http://localhost:8080](http://localhost:8080)

---

## Authors

- Developed by **Jairo Alexander Astudillo Lagos** && **David Camilo Corrales**
- Role: Data & AI Engineer – Bioindustry 4.0 Project

---
