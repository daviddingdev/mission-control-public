#!/bin/bash
# notify.sh [--tier critical|actionable|digest] <channel> <title> <message...>
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
cd "$(dirname "$0")/.." || exit 1

TIER=""
if [ "$1" = "--tier" ]; then TIER=$2; shift 2; fi
CH=${1:?usage: notify.sh [--tier T] <channel> <title> <message>}; TITLE=${2:?title required}; shift 2
MSG="$*"

TOPIC=$(python3 -c "import json,sys;print(json.load(open('config/ntfy.json'))['channels'][sys.argv[1]])" "$CH") || exit 1
mkdir -p state

DECISION=$(python3 bin/notify_policy.py "$CH" "$TITLE" "$MSG" "$TIER" 2>/dev/null) || DECISION=SEND
[ "$DECISION" = "SEND" ] || exit 0

curl -s -m 10 -H "Title: $TITLE" -d "$MSG" "https://ntfy.sh/$TOPIC" >/dev/null
