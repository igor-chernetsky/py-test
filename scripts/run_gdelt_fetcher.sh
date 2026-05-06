#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/ec2-user/py-test"
LOCK_FILE="/tmp/gdelt_fetch.lock"
LOG_FILE="/var/log/gdelt_fetcher.log"

mkdir -p "$(dirname "$LOG_FILE")"

{
  echo "========================================="
  echo "Run at: $(date -u)"

  # Keep lock open for entire script lifetime.
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "Skip: another fetcher is running ($(date -u))"
    exit 0
  fi

  cd "$PROJECT_DIR"

  # Load optional shell profile (if present) so cron gets env vars too.
  [[ -f "$HOME/.bash_profile" ]] && source "$HOME/.bash_profile"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"

  if [[ -f "$PROJECT_DIR/venv311/bin/activate" ]]; then
    source "$PROJECT_DIR/venv311/bin/activate"
  fi

  # NOTE: tune args if needed for your API limits.
  python -u scripts/gdelt_fetch_to_s3.py --maxrecords 50 --max-attempts 6 --retry-base-sec 90 --retry-cap-sec 900

  echo "Done."
  echo "Finished at: $(date -u)"
} >> "$LOG_FILE" 2>&1
