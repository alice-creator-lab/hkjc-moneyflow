#!/bin/bash
# Rebuild the static snapshot and push it to GitHub Pages — only when the data changed.
#   */10 * * * * /home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/pages_push.sh
set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$DIR/logs/pages.log"
say() { echo "$(date -Is) $*" >>"$LOG"; }

if [ -f "$DIR/config.env" ]; then
  # shellcheck disable=SC1091
  . "$DIR/config.env"
fi
PYTHON="${PYTHON:-python3}"

cd "$DIR" || exit 1

BEFORE="$(sha256sum .pages/data.js 2>/dev/null | cut -d' ' -f1)"
"$PYTHON" "$DIR/build_pages.py" >>"$LOG" 2>&1 || { say "build failed"; exit 1; }
AFTER="$(sha256sum .pages/data.js | cut -d' ' -f1)"

if [ "$BEFORE" = "$AFTER" ]; then
  exit 0
fi

cd "$DIR/.pages" || exit 1
if [ ! -d .git ]; then
  git init -q -b main
  git remote add origin https://github.com/alice-creator-lab/moneyflow-monitor.git
fi

git -c user.name=alice-creator-lab -c user.email=alice-creator-lab@users.noreply.github.com add -A
git -c user.name=alice-creator-lab -c user.email=alice-creator-lab@users.noreply.github.com \
    commit -q -m "snapshot: $(date '+%Y-%m-%d %H:%M')" || exit 0

if git push -q origin main 2>>"$LOG"; then
  say "pushed snapshot $(date '+%H:%M')"
else
  say "push failed (will retry)"
fi
