#!/bin/bash

# Get parameters from environment variables (set by service.py)
JOBID="${JOBID:-1000861509}"
METADATA_URL="${METADATA_URL:-s3://958825666686-dpservices-testing-data/conductor-poc/1000861509.WBNameParse.json}"
EXECUTION_ID="${EXECUTION_ID:-WBNameParse}"
DAG_ID="${DAG_ID:-nua-nameparse-process-stage-v02-00-06-tiny}"
MWAA_ENDPOINT="${MWAA_ENDPOINT:-https://a53c6d7a-ec07-465a-9824-6cc199145a7a-vpce.c75.us-east-1.airflow.amazonaws.com:443}"
MWAA_SESSION_TOKEN="${MWAA_SESSION_TOKEN:-2f02e33a-98c7-407d-b446-3daff8eb1d3b.9KxQbGdQ5UZ2eWHmoOXjc7cDi2s}"

# Optional stats_url (can be provided in event data if needed)
STATS_URL="${STATS_URL:-}"

# Make jobid unique by appending session ID
SESSION_ID=$$
JOBID="${JOBID}-${SESSION_ID}"

echo "Triggering Airflow DAG: ${DAG_ID}"
echo "Job ID: ${JOBID}"
echo "Metadata URL: ${METADATA_URL}"

# Build the JSON payload
CONF_JSON="{\"jobid\":\"${JOBID}\",\"metadata_url\":\"${METADATA_URL}\",\"execution_id\":\"${EXECUTION_ID}\""

# Add stats_url if provided
if [ -n "${STATS_URL}" ]; then
  CONF_JSON="${CONF_JSON},\"stats_url\":\"${STATS_URL}\""
fi

CONF_JSON="${CONF_JSON}}"

# Trigger the DAG run
curl -X POST "${MWAA_ENDPOINT}/api/v1/dags/${DAG_ID}/dagRuns" \
     --silent \
     -b "session=${MWAA_SESSION_TOKEN}" \
     -H 'Content-Type: application/json' \
     --data-binary "{
       \"dag_run_id\": \"${JOBID}\",
       \"conf\": ${CONF_JSON}
     }"

# Return the curl exit code
exit $?
