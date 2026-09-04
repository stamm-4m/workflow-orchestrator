#!/usr/bin/env bash
# send_data_curl.sh — push one "tick" of sensor/actuator data via the Model
# Registry HTTP API (curl only, no direct DB access), so the
# deployment_soft_sensors DAG's wait_for_new_data sensor picks it up and the
# prediction runs. Stand-in for a real sensor push until hardware is connected.
#
# Usage:
#   ./send_data_curl.sh <run_id> <email> <password>
#
# Example:
#   ./send_data_curl.sh 9fc38a38-ce5b-4045-8b08-5d0b3c0231e6 sam2800ml@gmail.com 'mypassword'

set -euo pipefail

RUN_ID="${1:?Usage: $0 <run_id> <email> <password>}"
EMAIL="${2:?Usage: $0 <run_id> <email> <password>}"
PASSWORD="${3:?Usage: $0 <run_id> <email> <password>}"
API_BASE="${API_BASE:-http://localhost:8080}"
NOW="$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")"

echo "Logging in as ${EMAIL}..."
TOKEN=$(curl -s -X POST "${API_BASE}/auth/login-json" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${EMAIL}\",\"password\":\"${PASSWORD}\"}" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
  echo "Login failed." >&2
  exit 1
fi

post_reading() {
  local table="$1" id_field="$2" id_value="$3" value="$4"
  curl -s -o /dev/null -w "  ${table} ${id_value:0:8}... -> HTTP %{http_code}\n" \
    -X POST "${API_BASE}/api/v1/${table}/" \
    -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    -d "{\"time\":\"${NOW}\",\"run_id\":\"${RUN_ID}\",\"${id_field}\":\"${id_value}\",\"value\":${value}}"
}

echo "Sending sensor readings for run_id=${RUN_ID} at ${NOW}..."
# Catalog IDs from the SIM-* stand-in sensors/actuators already registered
# in this environment (see scripts/simulate_sensors.py).
post_reading sensor_readings sensor_id 6220fd96-09ef-4ebe-9829-e7b09bc37529 303.1   # temperature
post_reading sensor_readings sensor_id 579e159e-a85e-42b9-9399-ca18097b8b1f 6.5     # pH
post_reading sensor_readings sensor_id 576550e4-f708-4778-991f-5b1a08b16e87 5.2     # dissolved_oxygen_concentration
post_reading sensor_readings sensor_id a4cc74a0-9581-4a2c-9f1e-f678d687ced0 3.1     # CO2_percent_in_off_gas
post_reading sensor_readings sensor_id dd3a5427-ee51-49ca-8769-e547ca88c9fd 18.4    # oxygen_in_percent_in_off_gas
post_reading sensor_readings sensor_id 08a92262-5095-4c9a-8a9c-ea7665237a67 102.0   # vessel_volume
post_reading actuator_states actuator_id f39cf5ba-a66e-429b-9ef3-c2b672cb8348 610.0 # agitator
post_reading actuator_states actuator_id 4a04f950-aee9-48ec-b491-e37a37333365 1.05  # sugar_feed_rate

echo "Done. The DAG (if triggered and waiting) should pick this up within ~10s."
