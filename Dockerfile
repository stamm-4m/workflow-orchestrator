# Build a custom Airflow image with your pinned Python deps preinstalled.
# This avoids installing pip packages at container start (faster, reproducible).

FROM apache/airflow:2.10.3

# Install system packages only if you really need to compile wheels, etc.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ build-essential curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt