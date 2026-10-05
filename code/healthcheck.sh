#!/bin/bash
# Coded Spark health check — no Claude tokens. Runs from cron every 15 min.
# Pushes to the ntfy "alerts" channel ONLY on state change (new failure or recovery),
# so a persistent outage alerts once, not every 15 minutes.
#
# The dead-box alarm (2026-10-03): a box that is off, offline or has lost cron cannot say so,
# so ntfy.sh says it for the box. Each live run keeps ONE scheduled message on the alerts
# topic ("Spark silent for 2h", sequence id spark-deadman) two hours in the future, re-arming it
# when the last arm is 55+ minutes old (~24 posts a day against ntfy.sh's 250/day limit; the box
# peaks near 70). Re-publishing the same sequence id REPLACES a scheduled message on the server
# (docs.ntfy.sh/publish, "Updating scheduled notifications"; probed on a throwaway topic
# 2026-10-03), so nothing reaches the phone while the box is alive. If the box stops, the last
# one is delivered. A run that finds the alarm's time already passed knows it went out, re-arms,
# and pushes "Spark heartbeat back". 75 minutes with no good re-arm is a FAIL
# (dead-box-alarm-unarmed): an alarm that cannot be armed must not stay quiet.
#
#   healthcheck.sh                 the live run (cron)
#   healthcheck.sh --dry|--dry-run what this run finds and what the alarm would do; writes and sends nothing
#   healthcheck.sh selftest        the alarm's plan, arm, cancel and the ledger rows, on a stub curl
#   healthcheck.sh deadman-cancel  delete the scheduled alarm before a planned shutdown (the next
#                                  live run re-arms it; run it as the last thing before powering off)
# Any other argument exits 2 and checks and sends nothing. HEALTHCHECK_DRY=1 is --dry.
case "${1:-}" in -h|--help) sed -n '2,/^[^#]/{/^#/s/^# \{0,1\}//p}' "$0"; exit 0 ;; esac   # `--help` never runs the job (2026-09-26)
MODE=live
case "${1:-}" in
  "") ;;
  --dry|--dry-run) HEALTHCHECK_DRY=1 ;;
  selftest) MODE=selftest ;;
  deadman-cancel) MODE=cancel ;;
  *) MODE=bad ;;
esac
if [ "$MODE" = bad ] || [ $# -gt 1 ]; then
  echo "healthcheck.sh: unknown argument(s) '$*' — nothing checked or sent. usage: healthcheck.sh [--dry|--dry-run] | selftest | deadman-cancel | --help" >&2
  exit 2
fi
cd "$(dirname "$0")/.." || exit 1
# cron has no session env — without this, `systemctl --user` fails and false-alarms
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
STATE=state/health.state
mkdir -p state
FAIL=()
WARN=()    # heads-ups, not outages: pushed once through notify.sh (end of file), never "DOWN"

# >>> dead-box alarm (see the header). Functions first, so `selftest` runs them on a stub.
DM_STATE=${DM_STATE:-state/deadman.json}   # {"at": last good arm, "due": when ntfy delivers it}
DM_SEQ=spark-deadman
DM_REARM=3300        # re-arm when the last good arm is this old (55 min)
DM_AFTER=7200        # ntfy delivers the alarm this long after the last good arm (2 h)
DM_UNARMED=4500      # no good arm for this long (75 min, two failed tries): FAIL dead-box-alarm-unarmed
DM_CURL=${DM_CURL:-curl}
HC_LEDGER=${HC_LEDGER:-state/notifications.jsonl}
dm_plan(){ # <now> <at> <due> -> skip | arm | recovered
  local now=$1 at=${2:-0} due=${3:-0}
  if [ "$due" -gt 0 ] && [ "$now" -ge "$due" ]; then echo recovered
  elif [ $((now - at)) -ge $DM_REARM ]; then echo arm
  else echo skip; fi
}
dm_read(){ # -> "<at> <due>" from the state file, "0 0" when it is missing or unreadable
  python3 -c 'import json,sys
try: d = json.load(open(sys.argv[1]))
except Exception: d = {}
print(int(d.get("at") or 0), int(d.get("due") or 0))' "$DM_STATE" 2>/dev/null || echo "0 0"
}
dm_et(){ TZ=America/New_York date -d "@$1" '+%-I:%M %p ET' 2>/dev/null; }
dm_arm(){ # <topic> <now>: (re)schedule the alarm; 0 and the state written only on HTTP 200
  local topic=$1 now=$2 due=$(($2 + DM_AFTER)) code
  code=$("$DM_CURL" -s -m 10 -o /dev/null -w '%{http_code}' -H "At: $due" -H "Title: Spark silent for 2h" \
    -H "Priority: high" -H "Tags: warning" \
    -d "Nothing from the Spark since its watchdog last re-armed this alarm at $(dm_et "$now"); it re-arms every hour while the box is up. It may be off, off the network, or cron has stopped. ntfy.sh sent this for the box; 'Spark heartbeat back' follows when it returns." \
    "https://ntfy.sh/$topic/$DM_SEQ" 2>/dev/null)
  [ "$code" = 200 ] || return 1
  printf '{"at": %s, "due": %s, "seq": "%s"}\n' "$now" "$due" "$DM_SEQ" > "$DM_STATE.tmp" && mv "$DM_STATE.tmp" "$DM_STATE"
}
dm_cancel(){ # <topic>: delete the scheduled alarm (a planned shutdown) and clear the state
  local code
  code=$("$DM_CURL" -s -m 10 -o /dev/null -w '%{http_code}' -X DELETE "https://ntfy.sh/$1/$DM_SEQ" 2>/dev/null)
  [ "$code" = 200 ] || return 1
  printf '{"at": 0, "due": 0, "cancelled": %s}\n' "$(date +%s)" > "$DM_STATE"
}
hc_record(){ # <title> <message> <ntfy response> -> one alerts row in the notifications ledger
  # The outage alarm posts raw (never throttled); until 2026-10-03 its pushes reached the ledger
  # only when the dashboard's ntfy poll happened to back-fill them, so the rollup's BROKEN line
  # missed an outage nobody opened the page for. ntfy's own `time` keys the row, which is what
  # the dashboard's back-fill dedupes on, so the two never double up.
  # An optional 4th argument is appended to the reason (a late delivery, a dropped spool line).
  python3 - "$1" "$2" "$3" "$HC_LEDGER" "${4:-}" <<'PYEOF' 2>/dev/null
import json, sys, time
title, msg, resp, path, note = sys.argv[1:6]
try:
    r = json.loads(resp)
except Exception:
    r = {}
ok = isinstance(r, dict) and bool(r.get("id"))
row = {"time": int(r.get("time") or time.time()) if ok else int(time.time()), "channel": "alerts",
       "title": title, "message": msg, "tier": "critical", "pushed": ok,
       "reason": "healthcheck's outage alarm: a raw POST, never throttled" + ("" if ok else " (ntfy did not accept it)") + note}
with open(path, "a") as f:
    f.write(json.dumps(row) + "\n")
PYEOF
}
# Delivery that is checked (memo 2026-10-03 wa-alerts-and-boot, ask 1). Until then the outage alarm
# wrote $STATE BEFORE an unchecked `curl -s`, so a push sent during a Wi-Fi drop or an ntfy 429/5xx
# was lost for good: on the next run NOW==PREV and it never repeated (download-slow, 09-23..25).
# Now: `curl -fsS`, and a push counts only when ntfy answers 2xx with a message id (a captive portal's
# 200 page is not delivery). $STATE is "what David has been told", so it moves only on delivery. An
# undelivered push goes to $HC_SPOOL (one JSON line: ts, title, tags, priority, body, and the state it
# reports); every run retries the spool first, oldest first, stopping at the first failure so the
# order holds, and drops a line older than 24 h with a log line and a pushed:false ledger row. While
# a backlog stands a new push joins the spool rather than overtaking it. The ledger gets a row when a
# push is delivered (late ones say so) or dropped, never one per failed retry.
HC_SPOOL=${HC_SPOOL:-state/alerts_pending}
HC_SPOOL_MAX=86400
hc_send(){ # <title> <tags> <priority or ""> <message> -> ntfy's JSON on stdout; 0 only when ntfy took it
  local h=(-H "Title: $1" -H "Tags: $2") resp
  [ -n "$3" ] && h+=(-H "Priority: $3")
  resp=$(curl -fsS -m 10 "${h[@]}" -d "$4" "https://ntfy.sh/$TOPIC") || return 1
  printf '%s' "$resp"
  [[ $resp == *'"id"'* ]]
}
hc_spool(){ # <title> <tags> <priority> <message> <state it reports>: append one line to the spool
  python3 - "$HC_SPOOL" "$@" <<'PYEOF'
import json, sys, time
path, title, tags, prio, body, state = sys.argv[1:7]
with open(path, "a") as f:
    f.write(json.dumps({"ts": int(time.time()), "title": title, "tags": tags, "priority": prio,
                        "body": body, "state": state}) + "\n")
PYEOF
}
hc_post(){ # <title> <tags> <priority or ""> <message> <state>: deliver and ledger it, else spool. 0 = delivered
  local resp
  if [ ! -s "$HC_SPOOL" ] && resp=$(hc_send "$1" "$2" "$3" "$4"); then
    hc_record "$1" "$4" "$resp"; return 0
  fi
  hc_spool "$@"
  echo "$(date -Is) push not delivered (ntfy unreachable or refused, or a backlog stands); spooled to $HC_SPOOL: $1 — $4"
  return 1
}
hc_flush(){ # retry the spool oldest first; on delivery $STATE takes the state that push reported
  [ -s "$HC_SPOOL" ] || { rm -f "$HC_SPOOL"; return 0; }
  local line now keep=() stop=0 resp f
  now=$(date +%s)
  while IFS= read -r line || [ -n "$line" ]; do
    [ -n "$line" ] || continue
    if [ $stop = 1 ]; then keep+=("$line"); continue; fi
    mapfile -d '' -t f < <(python3 -c 'import json, sys
d = json.loads(sys.argv[1]); int(d["ts"])
sys.stdout.write("\0".join(str(d.get(k) or "") for k in ("ts", "title", "tags", "priority", "body", "state")) + "\0")' "$line" 2>/dev/null)
    if [ ${#f[@]} -ne 6 ]; then
      echo "$(date -Is) spool: dropped an unreadable line: ${line:0:200}"; continue
    fi
    if [ $((now - f[0])) -gt $HC_SPOOL_MAX ]; then
      echo "$(date -Is) spool: dropped undelivered after 24h (first tried $(date -u -d "@${f[0]}" +%FT%TZ)): ${f[1]} — ${f[4]}"
      hc_record "${f[1]}" "${f[4]}" '' " (dropped from the spool after 24 h undelivered; first tried $(date -u -d "@${f[0]}" +%FT%TZ))"
      continue
    fi
    if resp=$(hc_send "${f[1]}" "${f[2]}" "${f[3]}" "${f[4]}"); then
      printf '%s' "${f[5]}" > "$STATE"
      hc_record "${f[1]}" "${f[4]}" "$resp" " (delivered late from the spool; first tried $(date -u -d "@${f[0]}" +%FT%TZ))"
      echo "$(date -Is) spool: delivered late (first tried $(date -u -d "@${f[0]}" +%FT%TZ)): ${f[1]} — ${f[4]}"
    else
      stop=1; keep+=("$line")
    fi
  done < "$HC_SPOOL"
  if [ ${#keep[@]} -gt 0 ]; then
    printf '%s\n' "${keep[@]}" > "$HC_SPOOL.tmp" && mv "$HC_SPOOL.tmp" "$HC_SPOOL"
  else
    rm -f "$HC_SPOOL"
  fi
}
hc_prev(){ # what David was last told: the newest spooled state while a backlog stands, else $STATE
  if [ -s "$HC_SPOOL" ] && python3 -c 'import json, sys
s = None
for l in open(sys.argv[1]):
    try: s = json.loads(l)["state"]
    except Exception: pass
if s is None: sys.exit(1)
sys.stdout.write(s)' "$HC_SPOOL" 2>/dev/null; then return 0; fi
  cat "$STATE" 2>/dev/null
}
hc_report(){ # NOW, PREV and FAIL set by the caller: push the change; $STATE moves only on delivery
  [ "$NOW" != "$PREV" ] || return 0
  if [ ${#FAIL[@]} -gt 0 ]; then
    echo "$(date -Is) ALERT: ${FAIL[*]}"
    hc_post "Spark health" warning high "DOWN: ${FAIL[*]}" "$NOW" && printf '%s' "$NOW" > "$STATE"
  elif [ -n "$PREV" ]; then
    echo "$(date -Is) recovered"
    hc_post "Spark health" white_check_mark "" "Recovered — all checks green" "$NOW" && printf '%s' "$NOW" > "$STATE"
  fi
  return 0
}

if [ "$MODE" = selftest ]; then
  T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
  DM_STATE=$T/deadman.json; HC_LEDGER=$T/notifications.jsonl; F=0
  ok(){ if [ "$2" = "$3" ]; then echo "PASS $1"; else echo "FAIL $1 — got '$2', want '$3'"; F=$((F+1)); fi; }
  N=1791100000
  ok "plan: no state yet -> arm" "$(dm_plan $N 0 0)" arm
  ok "plan: armed a minute ago -> skip" "$(dm_plan $N $((N-60)) $((N-60+DM_AFTER)))" skip
  ok "plan: armed 54m59s ago -> skip" "$(dm_plan $N $((N-DM_REARM+1)) $((N-DM_REARM+1+DM_AFTER)))" skip
  ok "plan: armed 55 min ago -> arm" "$(dm_plan $N $((N-DM_REARM)) $((N-DM_REARM+DM_AFTER)))" arm
  ok "plan: the alarm's time passed (it went out) -> recovered" "$(dm_plan $N $((N-DM_AFTER-5)) $((N-5)))" recovered
  ok "plan: a cancelled alarm (0, 0) re-arms quietly" "$(dm_plan $N 0 0)" arm
  ok "read: no state file -> 0 0" "$(dm_read)" "0 0"
  echo 'not json' > "$DM_STATE"; ok "read: a corrupt state file -> 0 0" "$(dm_read)" "0 0"; rm -f "$DM_STATE"
  # a stub curl: records its argv, answers with $STUB_CODE
  cat > "$T/curl" <<'STUB'
#!/bin/bash
printf '%s\n' "$@" > "$(dirname "$0")/args"
printf '%s' "${STUB_CODE:-200}"
STUB
  chmod +x "$T/curl"; DM_CURL=$T/curl
  STUB_CODE=200 dm_arm selftest-topic $N; ok "arm: HTTP 200 -> 0" "$?" 0
  ok "arm: the state says when it was armed and when it fires" "$(dm_read)" "$N $((N+DM_AFTER))"
  A=$(cat "$T/args")
  ok "arm: scheduled at the due time, on the alarm's sequence id, high priority" \
    "$(grep -c -e "^At: $((N+DM_AFTER))$" -e "^https://ntfy.sh/selftest-topic/$DM_SEQ$" -e '^Priority: high$' <<<"$A")" 3
  ok "arm: a POST (no -X), never a DELETE" "$(grep -c -e '^-X$' <<<"$A")" 0
  rm -f "$DM_STATE"; STUB_CODE=000 dm_arm selftest-topic $N; ok "arm: ntfy unreachable -> 1" "$?" 1
  ok "arm: ...and no state written (the next run retries)" "$(dm_read)" "0 0"
  STUB_CODE=200 dm_cancel selftest-topic; ok "cancel: HTTP 200 -> 0" "$?" 0
  ok "cancel: a DELETE of the alarm's sequence id" "$(tr '\n' ' ' < "$T/args")" "-s -m 10 -o /dev/null -w %{http_code} -X DELETE https://ntfy.sh/selftest-topic/$DM_SEQ "
  ok "cancel: the state is cleared, so the next live run re-arms" "$(dm_read)" "0 0"
  hc_record "Spark health" "DOWN: x" '{"id":"abc","time":1791100123,"event":"message"}'
  hc_record "Spark health" "Recovered" ''
  ok "ledger: ntfy's own time keys the row (the dashboard's back-fill dedupes on it), tier critical" \
    "$(python3 -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1])]; print(r[0]["time"], r[0]["channel"], r[0]["tier"], r[0]["pushed"], r[1]["pushed"])' "$HC_LEDGER")" \
    "1791100123 alerts critical True False"
  # Checked delivery (memo wa-alerts-and-boot ask 1): a fake curl first on PATH, exiting $STUB_RC
  # (22 = what `curl -f` returns on an HTTP 4xx/5xx), a temp $STATE, spool and ledger.
  mkdir -p "$T/fb"; cat > "$T/fb/curl" <<'STUB'
#!/bin/bash
printf '%s\n' "$*" >> "$(dirname "$0")/calls"
[ "${STUB_RC:-0}" = 0 ] || { echo "curl: (22) The requested URL returned error: 429" >&2; exit "$STUB_RC"; }
printf '{"id":"st%s","time":1791100200,"event":"message"}' "$RANDOM"
STUB
  chmod +x "$T/fb/curl"; PATH="$T/fb:$PATH"; TOPIC=selftest-topic
  STATE=$T/health.state; HC_SPOOL=$T/alerts_pending; HC_LEDGER=$T/deliv.jsonl; L=$T/hc.log
  run(){ hc_flush >>"$L" 2>&1; PREV=$(hc_prev); hc_report >>"$L" 2>&1; }
  calls(){ cat "$T/fb/calls" 2>/dev/null | grep -c .; }
  rows(){ cat "$HC_LEDGER" 2>/dev/null | grep -c .; }
  printf 'old-x' > "$STATE"; FAIL=(new-y); NOW=new-y
  STUB_RC=22 run
  ok "delivery: curl is -fsS (an HTTP error is a failure, not a body)" "$(grep -c -- '^-fsS -m 10 ' "$T/fb/calls")" 1
  ok "delivery: curl fails -> \$STATE unchanged" "$(cat "$STATE")" old-x
  ok "delivery: ...and the push is spooled with the state it reports" \
    "$(python3 -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1])]; print(len(r), r[0]["state"], r[0]["body"], r[0]["priority"])' "$HC_SPOOL")" \
    "1 new-y DOWN: new-y high"
  ok "delivery: ...and no ledger row claims it was pushed" "$(rows)" 0
  STUB_RC=22 run
  ok "delivery: still down next run -> retried once, not spooled twice" "$(calls) $(grep -c . "$HC_SPOOL")" "2 1"
  : > "$T/fb/calls"; STUB_RC=0 run
  ok "delivery: next run with curl working delivers the spooled push exactly once" "$(calls)" 1
  ok "delivery: ...\$STATE takes its state and the spool is cleared" "$(cat "$STATE") $([ -e "$HC_SPOOL" ] && echo spool || echo none)" "new-y none"
  ok "delivery: ...one ledger row, pushed, marked late" \
    "$(python3 -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1])]; print(len(r), r[0]["pushed"], "delivered late" in r[0]["reason"])' "$HC_LEDGER")" \
    "1 True True"
  : > "$T/fb/calls"; STUB_RC=0 run
  ok "delivery: a quiet run after it sends nothing" "$(calls)" 0
  FAIL=(); NOW=""; STUB_RC=22 run; STUB_RC=0 run
  ok "delivery: a recovery that failed once goes out on the next run, \$STATE cleared" \
    "$(tail -1 "$HC_LEDGER" | python3 -c 'import json,sys; print(json.load(sys.stdin)["message"])') [$(cat "$STATE")]" \
    "Recovered — all checks green []"
  : > "$HC_LEDGER"; : > "$T/fb/calls"; NOWS=$(date +%s)
  printf '{"ts": %s, "title": "Spark health", "tags": "warning", "priority": "high", "body": "DOWN: stale", "state": "stale"}\n{"ts": %s, "title": "Spark health", "tags": "warning", "priority": "high", "body": "DOWN: fresh", "state": "fresh"}\n' \
    $((NOWS - HC_SPOOL_MAX - 60)) $((NOWS - 60)) > "$HC_SPOOL"
  STUB_RC=0 hc_flush >>"$L" 2>&1
  ok "spool: a line older than 24 h is dropped with a log line, never sent" \
    "$(grep -c 'dropped undelivered after 24h.*DOWN: stale' "$L") $(grep -c 'DOWN: stale' "$T/fb/calls")" "1 0"
  ok "spool: ...its ledger row says dropped, pushed false; the fresh one is delivered" \
    "$(python3 -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1])]; print(r[0]["pushed"], "dropped" in r[0]["reason"], r[1]["message"], r[1]["pushed"])' "$HC_LEDGER") $(cat "$STATE")" \
    "False True DOWN: fresh True fresh"
  printf '{"ts": %s, "title": "A", "tags": "t", "priority": "", "body": "first", "state": "a"}\n' "$NOWS" > "$HC_SPOOL"
  : > "$T/fb/calls"; STUB_RC=22 hc_flush >>"$L" 2>&1; STUB_RC=0 hc_post B t "" second b >>"$L" 2>&1
  ok "spool: while a backlog stands a new push queues behind it (order holds), not sent" \
    "$(calls) $(python3 -c 'import json,sys; print(" ".join(json.loads(l)["body"] for l in open(sys.argv[1])))' "$HC_SPOOL")" \
    "1 first second"
  ok "the outage alarm's own checks are still read by the back office (chk lines)" \
    "$(grep -c '^chk [0-9]' "$0" | awk '{print ($1 >= 8)}')" 1
  [ $F -eq 0 ] && echo "ALL PASS" || echo "$F FAILED"
  exit $([ $F -eq 0 ] && echo 0 || echo 1)
fi

TOPIC=$(python3 -c "import json;print(json.load(open('config/ntfy.json'))['channels']['alerts'])") || exit 1
if [ "$MODE" = cancel ]; then
  if dm_cancel "$TOPIC"; then echo "$(date -Is) dead-box alarm cancelled (the next live run re-arms it)"; exit 0; fi
  echo "$(date -Is) dead-box alarm: cancel FAILED (ntfy did not answer 200)" >&2; exit 1
fi
# <<< dead-box alarm functions

# chk PORT NAME [HOST] [EXTRA_OK_CODES]: up is any 2xx/3xx, plus the codes a service answers
# on / by design (the Justin desk says 403 there). A port David switched off on purpose
# (config/paused.json, in force while its keepalive cron line is commented `# PAUSED`) is not
# checked at all — it is paused, not down (2026-09-26, memo from stocks).
chk(){
  python3 bin/paused.py port "$1" >/dev/null 2>&1 && return
  local c; c=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://${3:-localhost}:$1/")
  case "$c" in 2??|3??) return ;; esac
  case " ${4:-} " in *" $c "*) return ;; esac
  FAIL+=("$2(:$1)")
}
# chkts PORT NAME: a `tailscale serve` HTTPS front. The box can't resolve ts.net
# (--accept-dns=false; curl exits 6), so pin the name to our tailnet IP.
chkts(){
  local h=<host>.<tailnet>.ts.net c
  c=$(curl -s -m 5 -o /dev/null -w '%{http_code}' --resolve "$h:$1:<host-ip>" "https://$h:$1/")
  case "$c" in 2??|3??) return ;; esac
  FAIL+=("$2(:$1)")
}
# retired, no longer checked: ApplyNow(:3000), OpenWebUI(:8080), opensearch, ollama,
# FamilyVault(:5000) archived 2026-08-08
chk 8000 ClientCoWiki
chk 8001 ClientCoApp                # the clientco app, its front door since 10-03 (memo clientco-app-8001)
chk 8088 Pokerlog <host-ip>   # pokerlog binds the tailscale IP, not localhost
chkts 443 PokerlogHTTPS            # its tailscale serve front — the phone app's own URL
# :8090 is the App Store build (pokerapp.service) and :8443 its own HTTPS origin — always-on, how
# David opens that build on his phone (memo from poker-appstore, 2026-10-02). Same IP binding.
chk 8090 PokerAppStore <host-ip>
chkts 8443 PokerAppStoreHTTPS
chk 8787 StocksDash
chk 8900 MissionControl
chk 8910 HBSCasework
# :8911 is the tailscale serve HTTPS front for :8910 — David's Home Screen icon (memo from hbs, 2026-09-29)
chkts 8911 HBSCaseworkHTTPS
# The Spark browser (2026-10-04, bin/browser.py): Chrome's DevTools and the web viewer, both loopback;
# :8912 is the viewer's tailnet HTTPS front. :5999 (VNC) is not HTTP; the viewer answering covers it.
chk 9222 SparkBrowser
chk 6080 SparkBrowserView
chkts 8912 SparkBrowserHTTPS
chk 8790 JustinDesk localhost 403
chk 19999 Netdata
curl -sf -m 5 -o /dev/null "http://127.0.0.1:11434/api/tags" || FAIL+=("ollama(:11434)")   # local AI is production now (sentinel/digest/scoring depend on it)
# Every local job binds to a ROLE in config/models.json and pre-checks it before running.
# A role with no installed model means those jobs exit 75 tonight — catch it now, not from
# an empty report tomorrow. Exit 2 = unservable (alert); exit 1 = running on a fallback (don't).
python3 bin/models.py check >/dev/null 2>&1; [ $? -ge 2 ] && FAIL+=("local-model-role-unservable")

systemctl --user is-active --quiet pokerlog 2>/dev/null || FAIL+=("pokerlog.service")
# >>> always-on user units (2026-10-01; one block for :8900 since 2026-09-28). cron = jobs that
# finish; anything always up is a systemd user unit whose source lives in its project repo and is
# installed with `systemctl --user link` (PROJECT_STANDARDS §4). Every ENABLED user unit whose
# file resolves into a project folder (~/<project>/…, not ~/.config) is picked up here without an
# edit: it must be active AND capped. MemoryMax is a guardrail — CPU and GPU share one memory
# pool with Ollama and the Stocks model jobs — so `infinity` (an edit or a drop-in that lost the
# line) is loud. Disabled = its owner rolled back to cron on purpose: stands down. Labels:
# `<unit>.service` (not active), `<unit>-uncapped`. A unit copied into ~/.config instead of linked
# is NOT seen (pokerlog's own line above stays). The chk probes above stay: systemd only sees a
# process that exits; a hung one needs the probe. Discovery is itself a guardrail: if it misses
# maintenance-dashboard while that unit is enabled, it is inert and says so.
UNITS_SEEN=()
while IFS='|' read -r id frag ufs act mmax; do
  [ -n "$id" ] || continue
  real=$(readlink -f "$frag" 2>/dev/null)
  case "$real" in "$HOME"/.*|"") continue ;; "$HOME"/*/*) ;; *) continue ;; esac
  [ "$ufs" = enabled ] || continue
  n=${id%.service}; UNITS_SEEN+=("$n")
  [ "$act" = active ] || FAIL+=("$n.service")
  [ "$mmax" = infinity ] && FAIL+=("$n-uncapped")
done < <(systemctl --user list-unit-files --type=service --state=enabled --no-legend 2>/dev/null | awk '{print $1}' \
  | xargs -r systemctl --user show -p Id,FragmentPath,UnitFileState,ActiveState,MemoryMax 2>/dev/null \
  | awk -v RS= -F'\n' '{ delete v; for (k = 1; k <= NF; k++) { e = index($k, "="); v[substr($k, 1, e - 1)] = substr($k, e + 1) }
                          print v["Id"] "|" v["FragmentPath"] "|" v["UnitFileState"] "|" v["ActiveState"] "|" v["MemoryMax"] }')
if systemctl --user is-enabled --quiet maintenance-dashboard 2>/dev/null \
   && ! printf '%s\n' "${UNITS_SEEN[@]}" | grep -qx maintenance-dashboard; then
  FAIL+=("always-on-discovery-inert")
fi
# <<< always-on user units

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

# The phone's Remote Control sign-in — its OWN credential (memo 2026-09-28 from home). The Claude
# app and claude.ai/code reach this box through the RC host, which runs on
# ~/.claude/.credentials.json → claudeAiOauth: not the fleet's headless-token, not the desktop
# app's token. It lasts about a month (refreshTokenExpiresAt) and was dead 09-24 → 09-28 with this
# watchdog green: the canary above needs a job-log auth failure, and no job uses that credential.
# Definitive and zero-token — `check-login` reads booleans and timestamps, never prints a token:
# accessToken or refreshToken empty/missing = FAIL (auto-starts the phone sign-in below); refresh
# expiry within 72 h = a WARN, pushed once.
RCA=$(python3 bin/claude-relogin.py check-login 2>/dev/null)
case "$RCA" in
  dead:*) FAIL+=("remote-control-auth(needs sign-in)") ;;
  warn:*) WARN+=("${RCA#warn: }") ;;
esac

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

# NVMe health (memo 2026-10-03 wa-alerts-and-boot, ask 2): smartd's only hook mails root and the box
# has no mail, so a failing disk went nowhere. Once a day (state/smart.json older than 23 h, and at
# most one try an hour while it can't be read) `smart_check.py read` runs `sudo -n smartctl` and saves
# the reading; every other run reads the last verdict back with `status` (no sudo), so a FAIL holds
# between daily reads instead of "recovering" 15 minutes later. Exit 1 = FAIL disk-smart; exit 3 =
# not readable (sudo refused): logged, never a disk failure. A dry run only reads the verdict.
SM_READ=
if [ -z "${HEALTHCHECK_DRY:-}" ] && [ -z "$(find state/smart.json -mmin -1380 2>/dev/null)" ] \
   && [ -z "$(find state/smart.attempt -mmin -60 2>/dev/null)" ]; then
  touch state/smart.attempt; SM_READ=1
  SM=$(python3 bin/smart_check.py read 2>&1); SM_RC=$?
else
  SM=$(python3 bin/smart_check.py status 2>&1); SM_RC=$?
fi
case $SM_RC in
  0) [ -n "$SM_READ" ] && echo "$(date -Is) $SM" ;;
  1) FAIL+=("disk-smart"); [ -n "$SM_READ" ] && echo "$(date -Is) $SM" ;;
  3) [ -n "$SM_READ" ] && echo "$(date -Is) disk SMART not read (not a disk failure; retried hourly): $SM" ;;
  *) echo "$(date -Is) smart_check.py exited $SM_RC: $SM" ;;
esac

# Every declared backup source must be fresh (config/backups.json is the one list —
# this used to hardcode Stocks, which is why nothing noticed that the poker app's live
# database had no backup at all). backup.py check exits 1 if anything is stale.
python3 bin/backup.py check >/dev/null 2>&1 || FAIL+=("backup-stale")

# Download-speed floor — alert when rx throughput drops below ~1 MB/s. Fetch 1 MB from
# Cloudflare with a 2-second ceiling: at ≥1 MB/s the transfer completes in time; below
# that (the wlP9s9 link fell to 28 KB/s for 19 days from 09-03 unnoticed) it times out.
# DNS + TLS setup on a good link costs well under the 2-second budget.
curl -sf -m 2 -o /dev/null \
  "https://speed.cloudflare.com/__down?bytes=1000000" 2>/dev/null \
  || FAIL+=("download-slow(<1MB/s)")

# The dead-box alarm: what to do this run (header). A dry run only says.
NOW_S=$(date +%s)
read -r DM_AT DM_DUE <<<"$(dm_read)"
DM_DO=$(dm_plan "$NOW_S" "$DM_AT" "$DM_DUE")

# HEALTHCHECK_DRY=1 (or --dry) stops here and prints what this run found: no state written, no
# push, no re-auth started, the alarm neither armed nor moved — how a session checks a change
# against the live box without paging David.
if [ -n "${HEALTHCHECK_DRY:-}" ]; then
  printf 'FAIL: %s\n' "${FAIL[*]:-none}"
  printf 'WARN: %s\n' "${WARN[*]:-none}"
  printf 'remote-control-auth: %s\n' "${RCA:-no answer from check-login}"
  printf 'undelivered alerts spooled: %s (%s; a live run retries them first)\n' \
    "$(cat "$HC_SPOOL" 2>/dev/null | grep -c .)" "$HC_SPOOL"
  printf 'disk SMART: %s\n' "${SM:-not checked}"
  if [ "$DM_AT" -gt 0 ]; then
    printf 'dead-box alarm: would %s (armed %s min ago; ntfy delivers it at %s unless re-armed)\n' \
      "$DM_DO" "$(( (NOW_S - DM_AT) / 60 ))" "$(dm_et "$DM_DUE")"
  else
    printf 'dead-box alarm: would %s (not armed yet)\n' "$DM_DO"
  fi
  exit 0
fi

if [ "$DM_DO" != skip ]; then
  if dm_arm "$TOPIC" "$NOW_S"; then
    if [ "$DM_DO" = recovered ]; then
      echo "$(date -Is) dead-box alarm had gone out (due $(dm_et "$DM_DUE")); heartbeat back, re-armed"
      bin/notify.sh --tier critical alerts "Spark heartbeat back" "The watchdog is checking in again ($(dm_et "$NOW_S")). It last re-armed the dead-box alarm at $(dm_et "$DM_AT"), so 'Spark silent' went out at $(dm_et "$DM_DUE"). A reboot also sends 'Spark back up'; Mission Control › Box › Health has the rest."
    fi
    DM_AT=$NOW_S
  else
    echo "$(date -Is) dead-box alarm: re-arm failed (ntfy did not answer 200); retrying next run"
  fi
fi
[ $((NOW_S - DM_AT)) -gt $DM_UNARMED ] && FAIL+=("dead-box-alarm-unarmed")

# Undelivered pushes first (hc_flush, above), then what David was last told is PREV: the newest
# spooled state while a backlog stands, so neither the re-auth trigger below nor the diff repeats
# a message that is still waiting to go out.
hc_flush
PREV=$(hc_prev)

# Dead CLI auth is fixable from David's phone — when it NEWLY fails, auto-start the
# remote re-auth flow (pty scrape of `claude setup-token`, zero tokens): the OAuth link
# lands on his phone via ntfy and the code comes back through Mission Control's Claude
# tab. claude-relogin.py is idempotent while a flow is already waiting.
if printf '%s\n' "${FAIL[@]}" | grep -q "claude-cli-auth" && \
   ! grep -q "claude-cli-auth" <<<"$PREV"; then
  python3 bin/claude-relogin.py start --token >> logs/relogin.log 2>&1 &
# The phone's sign-in takes the same flow in login mode: `claude auth login --claudeai`, verified
# by `claude auth status`, then the RC host restarted. One state machine and one paste box, so
# never two flows from one run.
elif printf '%s\n' "${FAIL[@]}" | grep -q "remote-control-auth" && \
   ! grep -q "remote-control-auth" <<<"$PREV"; then
  python3 bin/claude-relogin.py start --login >> logs/relogin.log 2>&1 &
fi

# Drop in-dev services (config/dev.json) BEFORE the state diff — otherwise a flapping WIP
# service fires a DOWN/recovered pair every time the 5-min watchdog bounces it.
if [ ${#FAIL[@]} -gt 0 ]; then
  mapfile -t FAIL < <(printf '%s\n' "${FAIL[@]}" | python3 bin/dev.py filter)
fi

NOW=$(printf '%s\n' "${FAIL[@]}" | sort)
hc_report

# Heads-ups: once, when the set changes and is not empty, through notify.sh (tiered and ledgered;
# the raw POST above is the outage alarm's deliberate exemption). Clearing is silent: a renewal
# pushes its own confirmation, and an expiry turns into a FAIL.
WSTATE=state/health.warn
WNOW=$(printf '%s\n' "${WARN[@]}" | sort)
if [ "$WNOW" != "$(cat "$WSTATE" 2>/dev/null)" ]; then
  printf '%s' "$WNOW" > "$WSTATE"
  if [ ${#WARN[@]} -gt 0 ]; then
    echo "$(date -Is) WARN: ${WARN[*]}"
    bin/notify.sh --tier actionable alerts "Claude sign-in expiring" "$(printf '%s\n' "${WARN[@]}")
Renew before then: Mission Control › Sessions › New sessions → Start re-auth (while this warning stands it starts the phone sign-in): http://<host-ip>:8900/#sessions/setup — or let it lapse and the watchdog starts it for you."
  fi
fi

# Usage window (memo 2026-10-03 wa-usage-window, asks 3 and 5): limit hits from every transcript into
# state/window.jsonl at most hourly (the scan state file's mtime is the clock), then one observe row:
# what a window gate would do beside what claudeq's minute proxy does. Read-only on claudeq, never
# fails this run.
[ -n "$(find state/window_limits_state.json -mmin -60 2>/dev/null)" ] || timeout 120 python3 bin/window_limits.py scan >/dev/null 2>&1 || true
timeout 60 python3 bin/window_observe.py observe >/dev/null 2>&1 || true
