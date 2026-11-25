#!/bin/bash
# Print the current real-time (tunnel) URL with the access token, plus the snapshot URL.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$DIR/config.env" ] && . "$DIR/config.env"

URL="$(cat "$DIR/data/live_url.txt" 2>/dev/null)"
TOKEN="$(cat "$DIR/data/webapp_token.txt" 2>/dev/null)"
if [ -z "$URL" ]; then
  URL="$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$DIR/logs/tunnel.log" 2>/dev/null | tail -1)"
fi
echo "固定網址 : http://47.83.216.177/moneyflow/?k=${TOKEN}   (nginx 反代，唔會變)"
echo "即時備用 : ${URL}/?k=${TOKEN}   (HTTPS，tunnel 重開會變)"
echo "本機   : http://127.0.0.1:8787/?k=${TOKEN}"
echo "快照   : https://alice-creator-lab.github.io/moneyflow-monitor/"
