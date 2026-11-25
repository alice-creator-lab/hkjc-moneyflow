#!/bin/bash
# Pause switches for the moneyflow monitor.
#
#   ./switch.sh                    show the current state
#   ./switch.sh push off           stop publishing snapshots to GitHub Pages
#   ./switch.sh push on            resume publishing
#   ./switch.sh collect off        stop recording data (the daemon shuts itself down)
#   ./switch.sh collect on         resume recording
#   ./switch.sh all off|on         both at once
#
# The webapp and the cloudflared tunnel are never touched: the page keeps serving the last recorded
# data, it just stops changing.
set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="$DIR/data"
PUSH_FLAG="$DATA/.nopush"
COLLECT_FLAG="$DATA/.nocollect"

dcollector() { pgrep -f "python3? .*collect\.py --daemon$" >/dev/null 2>&1; }
dwebapp() { pgrep -f "python3? .*serve\.py --port 8787" >/dev/null 2>&1; }

flag_for() {
  case "$1" in
    push) echo "$PUSH_FLAG" ;;
    collect) echo "$COLLECT_FLAG" ;;
    *) return 1 ;;
  esac
}

show() {
  if [ -f "$PUSH_FLAG" ]; then
    echo "推送 Pages：已暫停"
  else
    echo "推送 Pages：運行中"
  fi
  if [ -f "$COLLECT_FLAG" ]; then
    echo "收集數據：已暫停（$(dcollector && echo '進程仍在退出中' || echo '無收集進程')）"
  else
    echo "收集數據：$(dcollector && echo '運行中' || echo '未有進程（watchdog 會於 1 分鐘內啟動）')"
  fi
  echo "監控網頁：$(dwebapp && echo '運行中' || echo '未運行')"
}

set_flag() {
  local target="$1" mode="$2" f
  if [ "$target" = "all" ]; then
    set_flag push "$mode" && set_flag collect "$mode"
    return $?
  fi
  f="$(flag_for "$target")" || { echo "unknown switch: $target (want push|collect|all)" >&2; return 2; }
  case "$mode" in
    off) : >"$f"; touch "$f"; echo "已暫停：$target" ;;
    on)  rm -f "$f";  echo "已恢復：$target" ;;
    *)   echo "unknown mode: $mode (want on|off)" >&2; return 2 ;;
  esac
}

mkdir -p "$DATA"
case "${1:-status}" in
  status|"") show ;;
  push|collect|all) set_flag "$1" "${2:-status}"; show ;;
  *) echo "usage: $0 [status|push|collect|all] [on|off]" >&2; exit 2 ;;
esac
