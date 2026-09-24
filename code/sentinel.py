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


def _job_key(desc):
    return "job:" + re.sub(r"[^a-z0-9]+", "-", desc.lower())[:40]


# memo 2026-09-23 (stocks): the snapshot the model judges is taken BEFORE the GPU wait, and at
# maintenance's tier that wait is bounded only by ageing (~40 min). On 09-23 the sentinel read
# the 14:15 mcp_sync DNS failure at 14:20, queued 1905 s behind Stocks' judges, and paged
# CRITICAL at 14:51 — after the 14:30 and 14:45 runs had both succeeded. So the job's newest
# run is re-read right before paging. A line that reads like any of these is still a failure.
_FAILED = re.compile(r"traceback|error|exception|fail|fatal|alert|refused|denied|timed? ?out|"
                     r"abort|crash|killed|unreachable|no such|not found|errno", re.I)


def _recovered(issue, before, jobs_now):
    """-> unix time of a clean run the model never saw, or None (page as usual).

    Only a job-keyed issue can recover: every cron line behind that key must have written its
    log since the snapshot, and the newest line must not read as a failure. Anything short of
    that — no new run, a new failure, an ambiguous line, a job missing from the snapshot —
    pages exactly as before. The fix can only remove a page about a run the model never saw."""
    key = issue.get("key", "")
    if not key.startswith("job:"):
        return None
    rows = [j for j in jobs_now if _job_key(j["desc"]) == key and j.get("log")]
    if not rows:
        return None
    for j in rows:
        ident = (j["desc"], j["log"])
        if ident not in before:
            return None
        if (not j.get("last_run") or j["last_run"] <= (before[ident] or 0)
                or _FAILED.search(j.get("tail") or "")):
            return None
    return max(j["last_run"] for j in rows)


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
            i["key"] = _job_key(job["desc"])
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
    before = {(j["desc"], j.get("log")): j.get("last_run") for j in jobs}   # what the model sees
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
    # Re-read each flagged job's newest run NOW, not at snapshot time (memo 2026-09-23). A
    # recovered issue is not stamped into the cooldown, so a relapse still pages.
    back = []
    if any(i["key"].startswith("job:") for i in fresh):
        try:
            jobs_now = server.cron_jobs()
        except Exception:
            jobs_now = []
        for i in fresh:
            at = _recovered(i, before, jobs_now)
            if at:
                i["recovered_at"] = at
                back.append(i)
        fresh = [i for i in fresh if "recovered_at" not in i]
    for i in fresh:
        state[i["key"]] = now
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(state, open(STATE, "w"))

    if back:
        # "at most put it in the digest": held, never pushed; the 23:00 rollup's HELD line names it.
        rmsg = "; ".join(f"{i['summary'][:100]} — recovered, newest run "
                         f"{time.strftime('%H:%MZ', time.gmtime(i['recovered_at']))} clean"
                         for i in back[:4])
        subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "--tier", "digest", "maintenance",
                        "Sentinel recovered", rmsg], timeout=30)
        print(f"{time.strftime('%F %T')} RECOVERED before paging: {rmsg}")
    if fresh:
        msg = "; ".join(i["summary"][:120] for i in fresh[:4])
        subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "alerts",
                        "Sentinel (local model)", msg], timeout=30)
        print(f"{time.strftime('%F %T')} ALERT: {msg}")
    elif not back:
        print(f"{time.strftime('%F %T')} clear ({len(issues)} known/cooldown)")


# ---------- selftest: `sentinel.py selftest` (back office runs it daily, rule guardrail-inert) ----
# Replays the 2026-09-23 incident through main() with the model, the job table, the state
# file and notify.sh stubbed. Tails and times are the real agent_sync.log lines (account
# mask and balance removed).
def _selftest(target=None):
    import calendar, contextlib, io, tempfile, types
    t = target or sys.modules[__name__]
    at = lambda hms: calendar.timegm(time.strptime(f"2026-09-23 {hms}", "%Y-%m-%d %H:%M:%S"))
    dns = "urllib.error.URLError: <urlopen error [Errno -3] Temporary failure in name resolution>"
    ok = "2026-09-23T14:45:08Z code-sync: 4 positions · orders: 25 updated, 0 new"  # figures cut
    retried = "2026-09-23T15:00:14Z code-sync FAILED (network, after retries): name resolution"
    verdict = {"issues": [{"key": "mcp-sync-dns", "summary": "MCP sync job failing with DNS "
               "resolution errors (Temporary failure in name resolution) across multiple cadences"}]}

    def rows(last_run, tail):
        return [{"project": "Stocks", "desc": "Mcp sync", "log": "/replay/agent_sync.log",
                 "schedule": s, "expect_min": 15, "age_min": 5, "last_run": last_run, "tail": tail}
                for s in ("30,45 13 * * 1-5", "*/15 14-19 * * 1-5", "20 10 * * *")]

    snap = rows(at("14:15:04"), dns)                    # what the 14:20 run read
    cases = [  # (name, table at page time, expect a critical page?)
        ("09-23 replay: 14:30/14:45 runs clean by the time the slot came", rows(at("14:45:08"), ok), False),
        ("newest run failed again", rows(at("15:00:14"), retried), True),
        ("no run since the snapshot", snap, True),
    ]
    saved = {k: getattr(t, k) for k in ("server", "ask_json", "subprocess", "STATE")}
    argv, bad = sys.argv, []
    with tempfile.TemporaryDirectory() as tmp:
        for n, (name, later, want_page) in enumerate(cases):
            calls, tables = [], iter([snap, later])
            t.server = types.SimpleNamespace(
                cron_jobs=lambda: next(tables, later),
                watchdog=lambda: {"ok": True, "state": "", "last_check": time.time()})
            t.ask_json = lambda *a, **k: json.loads(json.dumps(verdict))
            t.subprocess = types.SimpleNamespace(run=lambda cmd, **k: calls.append(cmd))
            t.STATE = os.path.join(tmp, f"sentinel{n}.json")
            sys.argv = ["sentinel.py"]
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    t.main()
            finally:
                sys.argv = argv
                for k, v in saved.items():
                    setattr(t, k, v)
            paged = any("alerts" in c for c in calls)
            held = any("--tier" in c and "digest" in c for c in calls)
            stamped = "job:mcp-sync" in json.load(open(os.path.join(tmp, f"sentinel{n}.json")))
            good = paged == want_page and stamped == want_page and (want_page or held)
            print(f"{'PASS' if good else 'FAIL'}  {name}: paged={paged} held={held} "
                  f"cooldown={stamped} (want paged={want_page})")
            bad += [] if good else [name]
    print("ALL PASS" if not bad else f"{len(bad)} FAIL")
    return not bad


if __name__ == "__main__":
    if sys.argv[1:2] == ["selftest"]:
        sys.exit(0 if _selftest() else 1)
    main()
