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
chk 8910 HBSCasework
chk 8790 JustinDesk
chk 19999 Netdata
curl -sf -m 5 -o /dev/null "http://127.0.0.1:11434/api/tags" || FAIL+=("ollama(:11434)")   # local AI is production now (sentinel/digest/scoring depend on it)
# Every local job binds to a ROLE in config/models.json and pre-checks it before running.
# A role with no installed model means those jobs exit 75 tonight — catch it now, not from
# an empty report tomorrow. Exit 2 = unservable (alert); exit 1 = running on a fallback (don't).
python3 bin/models.py check >/dev/null 2>&1; [ $? -ge 2 ] && FAIL+=("local-model-role-unservable")

systemctl --user is-active --quiet pokerlog 2>/dev/null || FAIL+=("pokerlog.service")

# Headless-Claude auth canary — coded, zero tokens (memo 2026-08-28 from hbs: OAuth refresh
# died ~08-27 and every `claude -p` job failed silently for a day). The CLI refreshes the
# token on use; an expiry >2h in the PAST with jobs scheduled hourly means refresh is dead
# and only David's interactive /login fixes it. Reads the file, spends nothing.
python3 - <<'PYEOF' || FAIL+=("claude-cli-auth(needs /login)")
import json, time, sys, os, glob, re
# The CLI refreshes this credential LAZILY, on use. A passed expiry is therefore not proof
# that refresh is broken — usually it just means nothing has called Claude for a while. That
# became the normal state on 2026-08-29 when the headless fleet moved onto
# ~/.claude/headless-token and stopped exercising the rotating one at all.
#
# Expiry alone fired three false alarms in three days (08-28 20:15, 08-29 14:15, 08-30 00:30),
# each waking David for a login he did not need. On the third, the credential refreshed itself
# two seconds after the alarm and both auth paths tested fine.
#
# So: a stale expiry is the TRIGGER, and a real auth failure in a job log is the EVIDENCE.
# Both, or it stays quiet. Still zero tokens — this only reads files.
try:
    c = json.load(open("/home/user/.claude/.credentials.json"))
    exp = (c.get("claudeAiOauth") or {}).get("expiresAt", 0)
    exp = exp / 1000 if exp > 1e12 else exp
    if not (exp and time.time() - exp > 2 * 3600):
        sys.exit(0)                       # not stale — nothing to think about
    # A bare "401" matches sshd[401242] and any log that happens to contain the digits, which
    # made the first version of this fire on an apt log. The number must stand alone AND sit
    # near auth words; the rest are phrases that only ever mean one thing.
    pat = re.compile(r"\b401\b[^\n]{0,60}(?:auth|unauthoriz|token|api[ _-]?key)"
                     r"|(?:auth|unauthoriz|token|api[ _-]?key)[^\n]{0,60}\b401\b"
                     r"|invalid[ _-]?api[ _-]?key"
                     r"|oauth token .{0,25}expired"
                     r"|please run /login"
                     r"|authentication_error", re.I)
    cut = time.time() - 3 * 3600
    logs = glob.glob(os.path.expanduser("~/*/logs/*.log")) + \
           glob.glob(os.path.expanduser("~/*/*/logs/*.log"))
    for p in logs:
        try:
            if os.path.getmtime(p) < cut:
                continue                  # nothing written recently — no evidence either way
            with open(p, errors="replace") as fh:
                fh.seek(max(0, os.path.getsize(p) - 20000))
                if pat.search(fh.read()):
                    sys.exit(1)           # stale AND a job actually failed on auth
        except Exception:
            continue
    sys.exit(0)                           # stale but nothing is failing — the box is just idle
except Exception:
    sys.exit(0)   # unreadable file is not proof of dead auth — don't false-alarm
PYEOF

# Sample the GPU temperature on the watchdog's guaranteed cadence. This lived only in the
# dashboard's request path, so the 48h thermal record only accumulated while somebody had
# the page open — exactly backwards for a question ("can it run this 24/7?") that is about
# the hours nobody is watching.
T=$(nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader,nounits 2>/dev/null | head -1)
G=$(nvidia-smi --query-gpu=power.draw --format=csv,noheader,nounits 2>/dev/null | head -1)
# Wall power from the UPS. It reports in 1% steps of 900W and updates slowly, so it is
# useless for a short test and fine on a 15-minute cadence — which is exactly what this is.
UL=$(upsc cyberpower ups.load 2>/dev/null)
[ -n "$T" ] && printf '{"at": %s, "c": %s, "gpu_w": %s, "wall_w": %s}\n' \
  "$(date +%s)" "$T" "${G:-null}" "$([ -n "$UL" ] && echo "$UL * 9" | bc -l | cut -d. -f1 || echo null)" \
  >> state/thermal.jsonl

# Thermal: alert only if the card is ACTIVELY being held back, not on a temperature number.
# 80C means nothing on its own — what matters is whether the part had to give up clocks.
nvidia-smi -q -d PERFORMANCE 2>/dev/null | grep -q "HW Thermal Slowdown *: Active" && FAIL+=("gpu-thermal-throttling")

USE=$(df --output=pcent / | tail -1 | tr -dc '0-9')
[ "${USE:-0}" -ge 90 ] && FAIL+=("disk-${USE}%")

# Every declared backup source must be fresh (config/backups.json is the one list —
# this used to hardcode Stocks, which is why nothing noticed that the poker app's live
# database had no backup at all). backup.py check exits 1 if anything is stale.
python3 bin/backup.py check >/dev/null 2>&1 || FAIL+=("backup-stale")

# Dead CLI auth is fixable from David's phone — when it NEWLY fails, auto-start the
# remote re-auth flow (pty scrape of `claude setup-token`, zero tokens): the OAuth link
# lands on his phone via ntfy and the code comes back through Mission Control's Claude
# tab. claude-relogin.py is idempotent while a flow is already waiting.
if printf '%s\n' "${FAIL[@]}" | grep -q "claude-cli-auth" && \
   ! grep -q "claude-cli-auth" "$STATE" 2>/dev/null; then
  python3 bin/claude-relogin.py start >> logs/relogin.log 2>&1 &
fi

# Drop in-dev services (config/dev.json) BEFORE the state diff — otherwise a flapping WIP
# service fires a DOWN/recovered pair every time the 5-min watchdog bounces it.
if [ ${#FAIL[@]} -gt 0 ]; then
  mapfile -t FAIL < <(printf '%s\n' "${FAIL[@]}" | python3 bin/dev.py filter)
fi

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
