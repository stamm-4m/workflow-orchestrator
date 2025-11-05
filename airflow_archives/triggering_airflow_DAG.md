# Triggering an Airflow DAG via REST API Services

This guide demonstrates how to trigger an Airflow DAG using REST API services, both via the Windows console (PowerShell) and a Python script.

## Prerequisites

- Apache Airflow is running and accessible on `http://localhost:8080`.
- The Airflow DAG `deploy_soft_sensor_model_csv` is already defined.
- Basic authentication (username and password) is set up for the Airflow web server.

## 1. Trigger the `deploy_soft_sensor_model_csv` DAG from the Windows Console (PowerShell)

To trigger the Airflow DAG from a Windows console (PowerShell), follow these steps:

### PowerShell Script

```powershell
# Define your DAG ID and base URL for triggering the run
$dag_id = "deploy_soft_sensor_model_csv"
$base_url = "http://localhost:8080/api/v1/dags/$dag_id/dagRuns"

# Set your username and password for basic authentication
$username = "corrales"
$password = "airflow123#"

# Create the base64 auth string
$base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${username}:${password}"))

# Set the headers with basic authentication
$headers = @{
    "Content-Type" = "application/json"
    "Authorization" = "Basic $base64AuthInfo"
}

# Create the body for the request with the 'model_name' parameter
$body = @{
    "conf" = @{
        "model_name" = "LSTM"  # The model you want to deploy, update if necessary
    }
} | ConvertTo-Json

# Trigger the DAG run
Invoke-RestMethod -Uri $base_url `
-Method Post `
-Headers $headers `
-Body $body
```

## 2. Python Script to Trigger the deploy_soft_sensor_model_csv DAG

```python
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 13:57:53 2025

@author: David Camilo Corrales
@email: David-Camilo.Corrales-Munoz@inrae.fr

"""

import requests
import base64
import json

# Set the necessary variables
dag_id = "deploy_soft_sensor_model_csv"  # Replace with your DAG ID
base_url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
username = "corrales"  # Replace with your username
password = "airflow123#"  # Replace with your password

# Create the base64 authentication string
auth_string = f"{username}:{password}"
base64AuthInfo = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

# Set the headers for basic authentication
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {base64AuthInfo}"
}

# Create the body for the request
conf = {
    "model_name": "LSTM"  # Pass any other parameters you need
}

body = {
    "conf": conf  # Directly pass the dictionary, no need to stringify it
}

# Send the POST request to trigger the DAG
response = requests.post(base_url, headers=headers, json=body)

# Check the response
if response.status_code == 200:
    print("DAG triggered successfully!")
else:
    print(f"Failed to trigger DAG. Status code: {response.status_code}")
    print(f"Response: {response.text}")

```