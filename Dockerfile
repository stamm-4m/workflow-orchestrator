# Build a custom Airflow image with your pinned Python deps preinstalled.
# This avoids installing pip packages at container start (faster, reproducible).

FROM apache/airflow:3.0.6

# Install system packages only if you really need to compile wheels, etc.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ build-essential curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python deps
# Constrained against Airflow's own compatibility matrix — without this, pip's
# resolver can silently pull in the legacy `apache-airflow` 2.x meta-package
# alongside `apache-airflow-core` 3.0.6 and break provider loading at startup.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt"