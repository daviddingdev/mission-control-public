#!/usr/bin/env python3
"""Log-anomaly sentinel — hourly, zero Claude tokens.
The local model reads every job's status + last log line and flags anything that looks
broken. Alerts (ntfy `alerts`) only on NEW issues, with a 24h per-issue cooldown — the
layer that would have caught the clientco silent failure within an hour."""
import json, os, re, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
sys.path.insert(0, f"{HOME}/maintenance/dashboard")
from localllm import ask_json
import server

STATE = f"{HOME}/maintenance/state/sentinel.json"
COOLDOWN = 24 * 3600
# Authoritative state files — these OUTRANK log tails (day-1 lesson: a stale
# "CYCLE CHECK FAIL" tail caused a false alarm while cycle_state.json said ok)
STATE_PROBES = [
    ("clientco monthly cycle", f"{HOME}/clientco-db/logs/cycle_state.json"),
]


def main():
    jobs = server.cron_jobs()
    # Exclude narrative local-AI jobs: their log tails are LLM prose (including THIS
    # sentinel's own past alerts) — feeding them back in creates recursive echo alarms.
    NARRATIVE = ("sentinel", "daily-log", "evening-digest", "Daily log", "Evening digest", "Anomaly")
    jobs = [j for j in jobs if not any(n.lower() in (j["desc"] + " " + (j.get("log") or "")).lower()
                                       for n in NARRATIVE)]
    lines = []
    for j in jobs:
        age = f"{j['age_min']}m" if j.get("age_min") is not None else "no-log"
        exp = f"{j['expect_min']}m" if j.get("expect_min") else "n/a"
        lines.append(f"[{j['project']}] {j['desc'][:60]} | expected~{exp} | last:{age} | tail: {j['tail'][:110]}")
    w = server.watchdog()
    probes = []
    for name, path in STATE_PROBES:
        try:
            probes.append(f"{name}: {open(path).read().strip()[:300]}")
        except Exception:
            pass
    prompt = (
        "You are a server ops sentinel. Below: authoritative STATE FILES, every scheduled job "
        "(expected cadence, last-run age, last log line) and the watchdog state. STATE FILES "
        "OUTRANK log tails — a log ending in FAIL is NOT an issue if the state file says ok "
        "(logs keep stale lines; state files are current). Flag ONLY genuine problems: "
        "failures confirmed by state/watchdog, error lines with no contradicting state file, "
        "jobs far beyond expected cadence (monthly jobs weeks old are FINE; weekday jobs quiet "
        "on weekends are FINE; 'no-log' boot tasks are FINE). A job REFUSING to overwrite "
        "existing output ('already exists', 'pass --force') is duplicate-run PROTECTION, not a "
        "failure — the work product exists. Be conservative — false alarms "
        'erode trust. Return JSON: {"issues":[{"key":"<short-stable-id>","summary":"<one line>"}]} '
        'or {"issues":[]}.\n\n'
        "STATE FILES (authoritative):\n" + ("\n".join(probes) or "(none)") + "\n\n"
        f"WATCHDOG: {'green' if w['ok'] else 'FAILING: ' + w['state']}\n" + "\n".join(lines))
    verdict = ask_json(prompt, num_predict=400)
    issues = verdict.get("issues", []) if isinstance(verdict, dict) else []
    # STABLE cooldown keys: the model invents different key strings each run, which
    # defeated the cooldown (4 alerts for one benign event, 2026-08-10). Re-key each
    # issue to the job it mentions — job identity, not model phrasing.
    job_names = [j["desc"] for j in jobs]
    for i in issues:
        if isinstance(i, dict):
            text = (i.get("summary", "") + " " + i.get("key", "")).lower()
            for name in job_names:
                if name.lower()[:25] in text or all(
                        w in text for w in name.lower().split()[:3]):
                    i["key"] = "job:" + re.sub(r"[^a-z0-9]+", "-", name.lower())[:40]
                    break

    state = {}
    try:
        state = json.load(open(STATE))
    except Exception:
        pass
    now = int(time.time())
    fresh = [i for i in issues
             if isinstance(i, dict) and i.get("key")
             and now - state.get(i["key"], 0) > COOLDOWN]
    for i in fresh:
        state[i["key"]] = now
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(state, open(STATE, "w"))

    if fresh:
        msg = "; ".join(i["summary"][:120] for i in fresh[:4])
        subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "alerts",
                        "Sentinel (local model)", msg], timeout=30)
        print(f"{time.strftime('%F %T')} ALERT: {msg}")
    else:
        print(f"{time.strftime('%F %T')} clear ({len(issues)} known/cooldown)")


if __name__ == "__main__":
    main()
