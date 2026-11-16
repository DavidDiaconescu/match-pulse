#!/usr/bin/env bash
# Check every service defined in docker-compose.yml and print OK / FAIL / SKIP.
#
# Services we know get a real probe (HTTP health endpoint, Kafka API call, Redis PING).
# Any other service a teammate adds is checked generically: its container must be
# running (and healthy, if it has a healthcheck). Exit code 1 if anything FAILs.
#
# Usage: scripts/healthcheck.sh      (or: make health)
source "$(dirname "$0")/_common.sh"
set +e  # keep going after a failed check

KAFKA_UI_PORT="${KAFKA_UI_PORT:-8080}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"

failures=0
report() {  # report <OK|FAIL|SKIP> <service> <detail>
  local colour reset=$'\033[0m'
  case "$1" in OK) colour=$'\033[32m' ;; FAIL) colour=$'\033[31m'; failures=$((failures + 1)) ;; *) colour=$'\033[33m' ;; esac
  printf "  %s%-4s%s  %-16s %s\n" "$colour" "$1" "$reset" "$2" "$3"
}

http_ok() { curl -fs -o /dev/null --max-time 5 "$1" 2> /dev/null; }

# State of each container: "<service> <state> <health> <exit code>"
states="$(docker compose ps -a --format '{{.Service}} {{.State}} {{if .Health}}{{.Health}}{{else}}-{{end}} {{.ExitCode}}' 2>/dev/null)"
state_of() { awk -v s="$1" '$1 == s {print $2, $3, $4}' <<< "$states"; }

# Generic check: running, and healthy if the container defines a healthcheck.
container_ok() {
  read -r state health _ <<< "$(state_of "$1")"
  [ "$state" = "running" ] && { [ "$health" = "-" ] || [ "$health" = "healthy" ]; }
}

echo "MatchPulse health check"
for service in $(docker compose config --services 2>/dev/null); do
  read -r state health exit_code <<< "$(state_of "$service")"
  if [ -z "$state" ]; then
    # On-demand tools (profile "tools") have no container until they are run.
    if [ "$service" = "replay-producer" ]; then
      report SKIP "$service" "on-demand tool (make replay-match)"
    else
      report FAIL "$service" "no container: run 'make up'"
    fi
    continue
  fi

  case "$service" in
    kafka)
      if docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 > /dev/null 2>&1; then
        topics="$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null)"
        missing=""
        for t in match-events match-control match-alerts; do grep -qx "$t" <<< "$topics" || missing="$missing $t"; done
        if [ -z "$missing" ]; then report OK kafka "broker answers; topics match-events, match-control, match-alerts exist"
        else report FAIL kafka "broker up but topics missing:$missing (make topics)"; fi
      else
        report FAIL kafka "broker does not answer on kafka:9092"
      fi ;;
    kafka-ui)
      http_ok "http://localhost:${KAFKA_UI_PORT}/actuator/health" \
        && report OK kafka-ui "http://localhost:${KAFKA_UI_PORT}" \
        || report FAIL kafka-ui "no answer on http://localhost:${KAFKA_UI_PORT}" ;;
    minio)
      http_ok "http://localhost:${MINIO_API_PORT}/minio/health/live" \
        && report OK minio "S3 API http://localhost:${MINIO_API_PORT}" \
        || report FAIL minio "no answer on http://localhost:${MINIO_API_PORT}/minio/health/live" ;;
    streamlit)
      http_ok "http://localhost:${STREAMLIT_PORT}/_stcore/health" \
        && report OK streamlit "http://localhost:${STREAMLIT_PORT}" \
        || report FAIL streamlit "no answer on http://localhost:${STREAMLIT_PORT}/_stcore/health" ;;
    grafana)
      http_ok "http://localhost:${GRAFANA_PORT}/api/health" \
        && report OK grafana "http://localhost:${GRAFANA_PORT}" \
        || report FAIL grafana "no answer on http://localhost:${GRAFANA_PORT}/api/health" ;;
    redis)
      [ "$(docker compose exec -T redis redis-cli ping 2>/dev/null | tr -d '\r')" = "PONG" ] \
        && report OK redis "PING -> PONG" \
        || report FAIL redis "redis-cli ping did not answer PONG" ;;
    *-init)
      # One-shot setup containers: they must have finished successfully.
      [ "$state" = "exited" ] && [ "$exit_code" = "0" ] \
        && report OK "$service" "finished successfully" \
        || report FAIL "$service" "state $state, exit code $exit_code (docker logs $service)" ;;
    *)
      # Anything else (e.g. Spark services added later): generic container check.
      if container_ok "$service"; then report OK "$service" "container $state${health:+ ($health)}"
      else report FAIL "$service" "container $state, health $health"; fi ;;
  esac
done

echo
if [ "$failures" -eq 0 ]; then echo "All checks passed."; else echo "$failures check(s) FAILED."; fi
[ "$failures" -eq 0 ]
