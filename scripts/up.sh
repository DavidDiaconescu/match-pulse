#!/usr/bin/env bash
# Start the whole stack (building local images if needed) and wait until it is healthy.
source "$(dirname "$0")/_common.sh"
docker compose up -d --build --wait
echo
echo "Kafka UI      http://localhost:${KAFKA_UI_PORT:-8080}"
echo "MinIO console http://localhost:${MINIO_CONSOLE_PORT:-9001}  (minioadmin / minioadmin)"
echo "Streamlit     http://localhost:${STREAMLIT_PORT:-8501}"
echo "Kafka (host)  localhost:${KAFKA_HOST_PORT:-9094}"
