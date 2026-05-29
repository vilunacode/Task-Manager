#!/usr/bin/env bash
set -euo pipefail

commit_message=${1:-"Auto-commit: $(date '+%Y-%m-%d %H:%M:%S')"}

cd "$(dirname "$0")/.."

status=$(git status --porcelain)
if [[ -n "$status" ]]; then
  echo "Aenderungen gefunden. Committe und pushe..."
  git add .
  git commit -m "$commit_message"
  git push origin main
  echo "Erfolgreich veroeffentlicht!"
else
  echo "Keine Aenderungen gefunden."
fi
