#!/bin/bash
# notify.sh <channel> <title> <message...> — push to David's phone via ntfy.sh.
# Channels map to topics in ../config/ntfy.json (one channel per project — PROJECT_STANDARDS §1).
# Every push is also appended to state/notifications.jsonl — the permanent history the
# dashboard shows (ntfy.sh itself only keeps ~12h).
cd "$(dirname "$0")/.." || exit 1
CH=${1:?usage: notify.sh <channel> <title> <message>}; TITLE=${2:?title required}; shift 2
TOPIC=$(python3 -c "import json,sys;print(json.load(open('config/ntfy.json'))['channels'][sys.argv[1]])" "$CH") || exit 1
mkdir -p state
python3 - "$CH" "$TITLE" "$*" <<'EOF'
import json, sys, time
ch, title, msg = sys.argv[1], sys.argv[2], sys.argv[3]
with open("state/notifications.jsonl", "a") as f:
    f.write(json.dumps({"time": int(time.time()), "channel": ch, "title": title, "message": msg}) + "\n")
EOF
curl -s -m 10 -H "Title: $TITLE" -d "$*" "https://ntfy.sh/$TOPIC" >/dev/null
