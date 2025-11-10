#!/usr/bin/env bash
# ----------------------------------------------------------------------
# Airflow Bootstrap Script
# ----------------------------------------------------------------------
# Purpose:
#   Prepare Airflow for first-time startup:
#     1) Upgrade or initialize the metadata database
#     2) Create the default Admin user (if it does not exist)
#
# Notes:
#   - All environment variables are loaded from `.env`
#   - This script does NOT interact with InfluxDB or any external DB
#   - Tokens, URLs, and bucket names for external systems (InfluxDB,
#     Grafana, etc.) should be configured via the `.env` file
#
# Usage:
#   Executed automatically by the `airflow-init` service in docker-compose.
# ----------------------------------------------------------------------

set -euo pipefail

log() { printf "[bootstrap] %s\n" "$*"; }

# ------------------------- Configuration -----------------------------
AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-airflow}"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-airflow}"
AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@stamm.local}"

# ---------------------- Database Migration ---------------------------
log "Upgrading Airflow metadata database..."
airflow db migrate || airflow db upgrade || true

# ----------------------- Create Admin User ---------------------------
if airflow users list 2>/dev/null | awk '{print $1}' | grep -qx "$AIRFLOW_ADMIN_USER"; then
  log "Admin user '${AIRFLOW_ADMIN_USER}' already exists. Skipping creation."
else
  log "Creating Admin user '${AIRFLOW_ADMIN_USER}'..."
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "STAMM" \
    --lastname "Admin" \
    --role "Admin" \
    --email "${AIRFLOW_ADMIN_EMAIL}"
fi

log "Bootstrap completed successfully."