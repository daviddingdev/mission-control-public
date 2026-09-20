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


SELF_ALERT_AT = 3          # canvas.py FAIL_ALERT_AT and the Stocks runners escalate at three
_MISS = re.compile(r"fail(?:ed|ure)?s? \((\d+)x\)", re.I)
# memo 2026-09-12 (stocks): the lab Claude launchers (agent/ops.py, agent/loop.py) consult
# _engine/config/mode.json and log `lab mode <m>: <job> does not run (mode.py)` when the lab is
# frozen/hibernating. A skip by design, not a failure — same treatment as a self-alerting job.
_LAB_SKIP = re.compile(r"lab mode \w+: .*does not run \(mode\.py\)", re.I)


def _miss_count(tail):
    m = _MISS.search(tail or "")
    return int(m.group(1)) if m else None


def _rekey(issues, jobs, below=()):
    """Cooldown keys by IDENTITY, never by model phrasing. The model invents a new key string
    every run ('clientco-snapshot-fail', 'clientco-monthly-cycle-fail', ... 12 variants for ONE
    unchanged failure, 2026-09-01..07), which defeated the 24h cooldown and paged David six
    times in 13h (memo 2026-09-04). Ladder: the job it names -> the project it names -> the
    phrase itself. Issues that only echo a self-alerting job's below-threshold miss are dropped."""
    out = []
    for i in issues:
        if not isinstance(i, dict):
            continue
        text = (str(i.get("summary", "")) + " " + str(i.get("key", ""))).lower()
        job = next((j for j in jobs if j["desc"].lower()[:25] in text
                    or all(w in text for w in j["desc"].lower().split()[:3])), None)
        if job is not None:
            if job["desc"] in below:
                continue
            i["key"] = "job:" + re.sub(r"[^a-z0-9]+", "-", job["desc"].lower())[:40]
        else:
            projects = sorted({j["project"] for j in jobs}, key=len, reverse=True)
            proj = next((p for p in projects
                         if p.lower() in text or p.lower().split("-")[0] in text), None)
            if proj:
                i["key"] = f"project:{proj.lower()}"
            else:
                i["key"] = "misc:" + re.sub(r"[^a-z0-9]+", "-", str(i.get("key", "")).lower())[:40]
        out.append(i)
    return out


def main():
    jobs = server.cron_jobs()
    # Exclude narrative local-AI jobs: their log tails are LLM prose (including THIS
    # sentinel's own past alerts) — feeding them back in creates recursive echo alarms.
    NARRATIVE = ("sentinel", "daily-log", "evening-digest", "Daily log", "Evening digest", "Anomaly")
    jobs = [j for j in jobs if not any(n.lower() in (j["desc"] + " " + (j.get("log") or "")).lower()
                                       for n in NARRATIVE)]
    lines = []
    below = set()   # jobs whose tail is a counted miss under their own alert threshold
    for j in jobs:
        age = f"{j['age_min']}m" if j.get("age_min") is not None else "no-log"
        exp = f"{j['expect_min']}m" if j.get("expect_min") else "n/a"
        line = f"[{j['project']}] {j['desc'][:60]} | expected~{exp} | last:{age} | tail: {j['tail'][:110]}"
        n = _miss_count(j["tail"])
        if n is not None and n < SELF_ALERT_AT:
            # memo 2026-09-05 (hbs): canvas.py counts misses and pages itself at 3; the sentinel
            # paged on the FIRST. A job that escalates on its own owns its alerting below that.
            below.add(j["desc"])
            line += f"  <- self-alerting job, {n} miss(es) below its own threshold of {SELF_ALERT_AT}: NOT an issue"
        elif _LAB_SKIP.search(j["tail"] or ""):
            below.add(j["desc"])
            line += "  <- lab-mode skip by design (Stocks mode.py said not to run): NOT an issue"
        lines.append(line)
    w = server.watchdog()
    # memo 2026-09-04 (stocks): health.state is written on CHANGE only; the model read a quiet
    # file as a dead watchdog. The log mtime is the "last ran" fact.
    w_age = f"{int((time.time() - w['last_check']) / 60)}m ago" if w.get("last_check") else "unknown"
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
        "or deferred (deferred = a human chose to wait for next month; not an issue) "
        "(logs keep stale lines; state files are current). Flag ONLY genuine problems: "
        "failures confirmed by state/watchdog, error lines with no contradicting state file, "
        "jobs far beyond expected cadence (monthly jobs weeks old are FINE; weekday jobs quiet "
        "on weekends are FINE; 'no-log' boot tasks are FINE). A job REFUSING to overwrite "
        "existing output ('already exists', 'pass --force') is duplicate-run PROTECTION, not a "
        "failure — the work product exists. Be conservative — false alarms "
        'erode trust. Return JSON: {"issues":[{"key":"<short-stable-id>","summary":"<one line>"}]} '
        'or {"issues":[]}.\n\n'
        "STATE FILES (authoritative):\n" + ("\n".join(probes) or "(none)") + "\n\n"
        f"WATCHDOG: {'green' if w['ok'] else 'FAILING: ' + w['state']} · last ran {w_age} "
        f"(every 15m; its state file only changes on a transition, so a quiet file is healthy)\n"
        + "\n".join(lines))
    if "--dry" in sys.argv:
        print(prompt)
        return
    verdict = ask_json(prompt, num_predict=400)
    issues = verdict.get("issues", []) if isinstance(verdict, dict) else []
    # STABLE cooldown keys: the model invents different key strings each run, which
    # defeated the cooldown (4 alerts for one benign event, 2026-08-10). Re-key each
    # issue to the job it mentions — job identity, not model phrasing.
    issues = _rekey(issues, jobs, below)

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
