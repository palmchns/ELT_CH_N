#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    cat <<'HELP'
Usage: bash ELT_V2.sh [start|run|status|stop|logs|check|help]

  start   Prepare local credentials, build, start services and run the pipeline (default).
  run     Run the pipeline against existing services; forward extra CLI arguments.
  status  Check the published snapshot; forward extra CLI arguments.
  stop    Stop services and retain all volumes and credentials.
  logs    Follow the service logs; Ctrl-C leaves services running.
  check   Validate Compose configuration without starting services.
  help    Show this help without requiring Docker.

Requires Docker Compose v2; start also requires Python 3 (standard library only).
Run this script from any working directory. No files outside this folder are needed.
HELP
}

ACTION="${1:-start}"
if (($#)); then shift; fi
case "$ACTION" in
    help|-h|--help) usage; exit 0 ;;
    start|stop|check)
        if (($#)); then usage >&2; exit 2; fi
        ;;
    run|status|logs) ;;
    *) usage >&2; exit 2 ;;
esac

command -v docker >/dev/null 2>&1 || { echo "Docker is required." >&2; exit 1; }
docker compose version >/dev/null
COMPOSE=(docker compose --project-directory "$SCRIPT_DIR" -f "$SCRIPT_DIR/docker-compose.prod.yml")

if [[ "$ACTION" == check ]]; then
    "${COMPOSE[@]}" config --quiet
    exit 0
fi
docker info >/dev/null 2>&1 || { echo "Start Docker before running this command." >&2; exit 1; }

run_pipeline() {
    "${COMPOSE[@]}" --profile jobs run --rm --no-deps runner python run_pipeline.py "$@"
}

case "$ACTION" in
    start)
        command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required to create local credentials." >&2; exit 1; }
        # Preserve user configuration and every existing secret on repeated starts.
        umask 077
        if [[ ! -e .env ]]; then cp .env.example .env; fi
        python3 -B utils/provision_secrets.py
        "${COMPOSE[@]}" config --quiet
        "${COMPOSE[@]}" --profile jobs build runner
        "${COMPOSE[@]}" up -d --no-build --wait --wait-timeout 180 prefect warehouse
        "${COMPOSE[@]}" run --rm --no-deps bi_init
        "${COMPOSE[@]}" up -d --no-build --no-deps --wait --wait-timeout 300 metabase
        run_pipeline run
        echo "Pipeline complete. Open the Prefect and Metabase ports configured in .env."
        ;;
    run|status) run_pipeline "$ACTION" "$@" ;;
    stop) "${COMPOSE[@]}" --profile jobs down ;;
    logs) "${COMPOSE[@]}" logs --follow "$@" ;;
esac
