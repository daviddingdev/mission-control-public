#!/bin/bash
# notify.sh [--tier critical|activity|actionable|digest] <channel> <title> <message...>
# notify.sh --dry [--tier T] <channel> <title> <message...>   what would happen; sends and records nothing
#
# The one choke point for phone pushes. Channels map to topics in ../config/ntfy.json.
#
# Since 2026-08-29 every call is TIERED by config/notify_policy.json before it can reach the
# phone: critical always sends, activity (a scheduled job is running — headless Claude or a
# local model; always sends, David 2026-08-31), actionable sends but is deduped and capped per
# channel per day, digest never sends on its own and is consolidated into the 23:00 UTC rollup.
# Pass --tier to override the policy for a caller that knows better.
#
# Everything is recorded to state/notifications.jsonl either way (with tier + pushed), which is
# the permanent history the dashboard renders and the rollup reads — ntfy.sh keeps only ~12h.
# If the policy layer errors, the push is SENT: a throttle bug must never eat an outage alert.
#
# A channel config/ntfy.json does not have (a typo, a project with no channel of its own) is
# sent as `maintenance`, tiered by maintenance's rules, and its ledger row keeps the channel the
# caller asked for (`asked_channel`). Until 2026-10-03 it exited 1 before the ledger: the push was
# lost and nothing on the box recorded that it had ever been tried.
case "${1:-}" in -h|--help) sed -n '2,/^[^#]/{/^#/s/^# \{0,1\}//p}' "$0"; exit 0 ;; esac   # `--help` never runs the job (2026-09-26)
cd "$(dirname "$0")/.." || exit 1

DRY=""; TIER=""
while :; do
  case "${1:-}" in
    --dry|--dry-run) DRY=1; shift ;;
    --tier) TIER=$2; shift 2 ;;
    *) break ;;
  esac
done
CH=${1:?usage: notify.sh [--dry] [--tier T] <channel> <title> <message>}; TITLE=${2:?title required}; shift 2
MSG="$*"

topic(){ python3 -c "import json,sys;print(json.load(open('config/ntfy.json'))['channels'][sys.argv[1]])" "$1" 2>/dev/null; }
TOPIC=$(topic "$CH")
if [ -z "$TOPIC" ]; then
  echo "notify.sh: no channel '$CH' in config/ntfy.json — sent as maintenance" >&2
  export NOTIFY_ASKED_CHANNEL=$CH
  CH=maintenance
  TOPIC=$(topic "$CH") || exit 1
  [ -n "$TOPIC" ] || exit 1
fi

if [ -n "$DRY" ]; then
  echo "channel=$CH${NOTIFY_ASKED_CHANNEL:+ (asked: $NOTIFY_ASKED_CHANNEL)} $(python3 bin/notify_policy.py explain "$CH" "$TITLE" "$TIER" 2>&1)"
  exit 0
fi
mkdir -p state

DECISION=$(python3 bin/notify_policy.py "$CH" "$TITLE" "$MSG" "$TIER" 2>/dev/null) || DECISION=SEND
[ "$DECISION" = "SEND" ] || exit 0

curl -s -m 10 -H "Title: $TITLE" -d "$MSG" "https://ntfy.sh/$TOPIC" >/dev/null
