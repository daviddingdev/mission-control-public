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
# Everything is recorded to state/notifications.jsonl either way (with tier + pushed, and for a send
# `delivered`, written after the attempt; an undelivered send is spooled for healthcheck.sh), which is
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

# Delivered, not decided (memo wa-alerts-and-boot, 2026-10-05). The policy records a HOLD at once;
# a SEND is recorded only after the attempt, with `delivered`. A push counts only when ntfy answers
# 2xx with a message id (`curl -fsS`; a captive portal's 200 page is not delivery). An undelivered
# push, or any push while a backlog stands, goes to healthcheck.sh's spool (state/alerts_pending),
# which every 15-min run retries oldest first and drops after 24 h; exit 1 says it did not arrive.
DECISION=$(python3 bin/notify_policy.py decide "$CH" "$TITLE" "$MSG" "$TIER" 2>/dev/null) \
  || DECISION=$'SEND\tcritical\tpolicy error, sent anyway'
[ "${DECISION%%$'\t'*}" = "SEND" ] || exit 0
IFS=$'\t' read -r _ PTIER PWHY <<<"$DECISION"
SPOOL=${NOTIFY_SPOOL:-state/alerts_pending}

if [ ! -s "$SPOOL" ] && RESP=$(curl -fsS -m 10 -H "Title: $TITLE" -d "$MSG" "https://ntfy.sh/$TOPIC" 2>/dev/null) \
   && [[ $RESP == *'"id"'* ]]; then
  python3 bin/notify_policy.py delivered "$CH" "$TITLE" "$MSG" "$PTIER" 1 "$PWHY" "$RESP" || true
  exit 0
fi
python3 bin/notify_policy.py spool "$CH" "$TITLE" "$MSG" "$PTIER" "$PWHY" \
  || echo "notify.sh: could not spool either: $TITLE" >&2
python3 bin/notify_policy.py delivered "$CH" "$TITLE" "$MSG" "$PTIER" 0 "$PWHY" || true
echo "notify.sh: ntfy did not take '$TITLE'; spooled to $SPOOL (healthcheck.sh retries it every 15 min)" >&2
exit 1
