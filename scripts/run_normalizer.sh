#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/ec2-user/py-test"
LOG_FILE="/home/ec2-user/logs/normalizer.log"

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

  if [[ -f "$PROJECT_DIR/venv311/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/venv311/bin/activate"
  fi

  python -u scripts/normalize_news_from_s3.py --positive-only

  echo "Done."
  echo "Finished at: $(date -u)"
} >>"$LOG_FILE" 2>&1
#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/ec2-user/py-test"
LOG_FILE="/var/log/normalizer.log"

mkdir -p "$(dirname "$LOG_FILE")"

{
  echo "========================================="
  echo "Run at: $(date -u)"

  cd "$PROJECT_DIR"

  [[ -f "$HOME/.bash_profile" ]] && source "$HOME/.bash_profile"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"

  if [[ -f "$PROJECT_DIR/venv311/bin/activate" ]]; then
    source "$PROJECT_DIR/venv311/bin/activate"
  fi

  # Positive-only mode keeps feed sentiment cleaner.
  python -u scripts/normalize_news_from_s3.py --positive-only

  echo "Done."
  echo "Finished at: $(date -u)"
} >> "$LOG_FILE" 2>&1
