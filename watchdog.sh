#!/bin/bash
# Keep the HKJC Phase-0 collector + monitor webapp alive.
#   */3 * * * * /home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/watchdog.sh
# Idempotent: exits quietly when exactly one of each is healthy.
set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$DIR/logs/watchdog.log"

# cron gives a minimal PATH and /usr/bin/python3 here is 3.6 -> pin interpreters
PY="$(command -v python3)"
if [ -f "$DIR/config.env" ]; then
  # shellcheck disable=SC1091
  . "$DIR/config.env"
fi
[ -x "${PYTHON:-}" ] || PYTHON="$PY"
mkdir -p "$DIR/logs" "$DIR/data"

say() { echo "$(date -Is) $*" >>"$LOG"; }

# 1. log rotation (over 20 MB)
for f in "$DIR/logs/collector.log" "$DIR/logs/daemon.out" "$DIR/logs/mqtt.log" "$DIR/logs/webapp.log"; do
  if [ -f "$f" ] && [ "$(stat -c%s "$f")" -gt 20971520 ]; then
    gzip -c "$f" > "$f.1.gz" && : > "$f"
    say "rotated $(basename "$f")"
  fi
done

# ---------------------------------------------------------------- collector
mapfile -t PIDS < <(pgrep -f "python3? .*collect\.py --daemon$" 2>/dev/null | sort -n)
COUNT=${#PIDS[@]}

if [ "$COUNT" -eq 1 ]; then
  echo "${PIDS[0]}" >"$DIR/data/collector.pid"
elif [ "$COUNT" -gt 1 ]; then
  KEEP="${PIDS[-1]}"
  for p in "${PIDS[@]}"; do
    [ "$p" = "$KEEP" ] && continue
    kill "$p" 2>/dev/null && say "killed duplicate daemon pid=$p (keeping $KEEP)"
  done
  echo "$KEEP" >"$DIR/data/collector.pid"
else
  cd "$DIR" || exit 1
  setsid nohup "$PYTHON" "$DIR/collect.py" --daemon >>"$DIR/logs/daemon.out" 2>&1 &
  sleep 3
  if pgrep -f "python3? .*collect\.py --daemon$" >/dev/null 2>&1; then
    pgrep -f "python3? .*collect\.py --daemon$" | tail -1 >"$DIR/data/collector.pid"
    say "started daemon pid=$(cat "$DIR/data/collector.pid")"
  else
    say "daemon failed to start (see logs/daemon.out)"
  fi
fi

# 1b. a live collector writes a poll row at least every ~10 min (600s idle cadence);
#     silence means it is wedged on a stale DB lock, which a plain restart clears.
AGE="$(sqlite3 -readonly "$DIR/data/moneyflow.db" \
  "select cast((julianday('now')-julianday(max(ts)))*1440 as int) from poll;" 2>/dev/null)"
if [ -n "${AGE:-}" ] && [ "$AGE" -gt 30 ]; then
  say "collector stale (${AGE} min since last poll row) - restarting"
  pkill -9 -f "collect\.py --daemon" 2>/dev/null
  sleep 2
  sqlite3 "$DIR/data/moneyflow.db" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null 2>&1
  sqlite3 -readonly "$DIR/data/moneyflow.db" "select 1" >/dev/null 2>&1 || sleep 3
  cd "$DIR" || exit 1
  setsid nohup "$PYTHON" "$DIR/collect.py" --daemon >>"$DIR/logs/daemon.out" 2>&1 &
  sleep 3
  pgrep -f "collect\.py --daemon" | tail -1 >"$DIR/data/collector.pid" 2>/dev/null
  say "collector restarted pid=$(cat "$DIR/data/collector.pid" 2>/dev/null)"
fi

# ------------------------------------------------------------------ webapp
mapfile -t WEBPIDS < <(pgrep -f "python3? .*serve\.py --port 8787" 2>/dev/null | sort -n)
WCOUNT=${#WEBPIDS[@]}
if [ "$WCOUNT" -eq 1 ]; then
  echo "${WEBPIDS[0]}" >"$DIR/data/webapp.pid"
  # keep the real-time tunnel alive as well
  if ! pgrep -f "cloudflared tunnel --url http://127.0.0.1:8787" >/dev/null 2>&1; then
    if [ -x "$DIR/bin/cloudflared" ]; then
      setsid nohup "$DIR/bin/cloudflared" tunnel --url http://127.0.0.1:8787 \
        --no-autoupdate --logfile "$DIR/logs/tunnel.log" --loglevel info >/dev/null 2>&1 &
      sleep 10
      URL="$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$DIR/logs/tunnel.log" | tail -1)"
      if [ -n "$URL" ]; then
        echo "$URL" >"$DIR/data/live_url.txt"
        say "tunnel restarted: $URL"
      fi
    fi
  fi
  exit 0
fi
if [ "$WCOUNT" -gt 1 ]; then
  WKEEP="${WEBPIDS[-1]}"
  for p in "${WEBPIDS[@]}"; do
    [ "$p" = "$WKEEP" ] && continue
    kill "$p" 2>/dev/null && say "killed duplicate webapp pid=$p (keeping $WKEEP)"
  done
  echo "$WKEEP" >"$DIR/data/webapp.pid"
  exit 0
fi

cd "$DIR" || exit 1
setsid nohup "$PYTHON" "$DIR/serve.py" --port 8787 --host 0.0.0.0 >>"$DIR/logs/webapp.log" 2>&1 &
sleep 2
if pgrep -f "python3? .*serve\.py --port 8787" >/dev/null 2>&1; then
  pgrep -f "python3? .*serve\.py --port 8787" | tail -1 >"$DIR/data/webapp.pid"
  say "started webapp pid=$(cat "$DIR/data/webapp.pid")"
else
  say "webapp failed to start (see logs/webapp.log)"
fi
