#!/bin/bash
# Coded Spark health check — no Claude tokens. Runs from cron every 15 min.
# Pushes to the ntfy "alerts" channel ONLY on state change (new failure or recovery),
# so a persistent outage alerts once, not every 15 minutes.
cd "$(dirname "$0")/.." || exit 1
# cron has no session env — without this, `systemctl --user` fails and false-alarms
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
TOPIC=$(python3 -c "import json;print(json.load(open('config/ntfy.json'))['channels']['alerts'])") || exit 1
STATE=state/health.state
mkdir -p state
FAIL=()

chk(){ curl -sf -m 5 -o /dev/null "http://${3:-localhost}:$1/" || FAIL+=("$2(:$1)"); }
# retired, no longer checked: ApplyNow(:3000), OpenWebUI(:8080), opensearch, ollama,
# FamilyVault(:5000) archived 2026-08-08
chk 8000 ClientCoWiki
chk 8088 Pokerlog <host-ip>   # pokerlog binds the tailscale IP, not localhost
chk 8787 StocksDash
chk 8900 MissionControl
chk 19999 Netdata
curl -sf -m 5 -o /dev/null "http://127.0.0.1:11434/api/tags" || FAIL+=("ollama(:11434)")   # local AI is production now (sentinel/digest/scoring depend on it)
# Every local job binds to a ROLE in config/models.json and pre-checks it before running.
# A role with no installed model means those jobs exit 75 tonight — catch it now, not from
# an empty report tomorrow. Exit 2 = unservable (alert); exit 1 = running on a fallback (don't).
python3 bin/models.py check >/dev/null 2>&1; [ $? -ge 2 ] && FAIL+=("local-model-role-unservable")

systemctl --user is-active --quiet pokerlog 2>/dev/null || FAIL+=("pokerlog.service")

USE=$(df --output=pcent / | tail -1 | tr -dc '0-9')
[ "${USE:-0}" -ge 90 ] && FAIL+=("disk-${USE}%")

# Every declared backup source must be fresh (config/backups.json is the one list —
# this used to hardcode Stocks, which is why nothing noticed that the poker app's live
# database had no backup at all). backup.py check exits 1 if anything is stale.
python3 bin/backup.py check >/dev/null 2>&1 || FAIL+=("backup-stale")

NOW=$(printf '%s\n' "${FAIL[@]}" | sort)
PREV=$(cat "$STATE" 2>/dev/null)
if [ "$NOW" != "$PREV" ]; then
  printf '%s' "$NOW" > "$STATE"
  if [ ${#FAIL[@]} -gt 0 ]; then
    echo "$(date -Is) ALERT: ${FAIL[*]}"
    curl -s -m 10 -H "Title: Spark health" -H "Priority: high" -H "Tags: warning" \
      -d "DOWN: ${FAIL[*]}" "https://ntfy.sh/$TOPIC" >/dev/null
  elif [ -n "$PREV" ]; then
    echo "$(date -Is) recovered"
    curl -s -m 10 -H "Title: Spark health" -H "Tags: white_check_mark" \
      -d "Recovered — all checks green" "https://ntfy.sh/$TOPIC" >/dev/null
  fi
fi
