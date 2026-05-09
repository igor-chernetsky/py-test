#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/ec2-user/py-test"
LOG_FILE="/home/ec2-user/logs/gdelt_fetcher.log"
LOCK_FILE="/tmp/gdelt_fetch.lock"

mkdir -p "$(dirname "$LOG_FILE")"
cd "$PROJECT_DIR"

# Single source of truth for env vars used by cron and manual runs.
if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

{
  echo "========================================="
  echo "Run at: $(date -u)"

  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "Skip: another fetcher is running ($(date -u))"
    exit 0
  fi

  if [[ -f "$PROJECT_DIR/venv311/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/venv311/bin/activate"
  fi

  # Default source: Actually Relevant (see NEWS_SOURCE / scripts/gdelt_fetch_to_s3.py --source).
  python -u scripts/gdelt_fetch_to_s3.py --maxrecords 10 --max-attempts 6 --retry-base-sec 90 --retry-cap-sec 900

  echo "Done."
  echo "Finished at: $(date -u)"
} >>"$LOG_FILE" 2>&1
