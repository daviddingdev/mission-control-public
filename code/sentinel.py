#!/usr/bin/env python3
"""Log-anomaly sentinel — hourly, zero Claude tokens.
The local model reads every job's status + last log line and flags anything that looks
broken. Alerts (ntfy `alerts`) only on NEW issues, with a 24h per-issue cooldown — the
layer that would have caught the clientco silent failure within an hour.

    sentinel.py              the hourly pass (local model; may page `alerts`)
    sentinel.py --dry        print the prompt the model would get; no model call, no push
                             (--dry-run is the same thing)
    sentinel.py selftest     the 09-23 replay and the 10-04 dedupe cases, with the model, job
                             table, state files, notifications log and notify.sh stubbed
  Any other argument exits 2 with the usage line and runs nothing (2026-10-03: until then
  `--dry-run`, or any typo, ran the live pass — the GPU, the state file, maybe a page)."""
import json, os, re, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
sys.path.insert(0, f"{HOME}/maintenance/dashboard")
from localllm import ask_json
import server

STATE = f"{HOME}/maintenance/state/sentinel.json"
COOLDOWN = 24 * 3600
# memo review-dedupe-pages (2026-10-05): the clientco outage of 10-03/10-04 paged David five times
# for ONE stuck cycle (10-03 04:21, 10-04 01:20 in his protected hours, 06:20, ...) under three
# job keys (factory-data-refresh, factory-cycle-retry, project:clientco-db) while its owner,
# run_cycle.py, had already paged "ClientCo refresh FAILED - snapshot.py". A page now goes out only
# when its CAUSE (project + failing script/job, digits stripped) is new: not paged by us in 6 h,
# not already paged by its owner in 24 h, and -- inside 20:00-04:00Z -- not seen in the last 24 h.
# Every drop is a log line and an entry in PAGED_STATE with its reason; nothing is silent.
PAGED_STATE = f"{HOME}/maintenance/state/sentinel_paged.json"
NOTIFY_LOG = f"{HOME}/maintenance/state/notifications.jsonl"
CAUSE_REPAGE = 6 * 3600
OWNER_WINDOW = 24 * 3600
KNOWN_WINDOW = 24 * 3600
PROTECTED = (20, 4)        # David's HBS hours, UTC: hour >= 20 or hour < 4
SELF_TITLES = ("sentinel",)


def _clock():
    return int(time.time())
# Authoritative state files — these OUTRANK log tails (day-1 lesson: a stale
# "CYCLE CHECK FAIL" tail caused a false alarm while cycle_state.json said ok).
# Catalog ids, not paths (box rule 8; the 2026-W39 catalog review found this the one hard-coded
# reach into another project left in bin/): clientco-db declares us a reader of cycle_state.
STATE_PROBES = [
    ("clientco monthly cycle", "clientco-db/cycle_state"),
    # the Data Desk's per-source health (2026-09-29): its header keys status / at / worse come
    # first by design, so they fall inside the 300 characters pasted below
    ("Data Desk health", "data-desk/health"),
]


SELF_ALERT_AT = 3          # canvas.py FAIL_ALERT_AT and the Stocks runners escalate at three
_MISS = re.compile(r"fail(?:ed|ure)?s? \((\d+)x\)", re.I)
# memo 2026-09-12 (stocks): the lab Claude launchers (agent/ops.py, agent/loop.py) consult
# _engine/config/mode.json and log `lab mode <m>: <job> does not run (mode.py)` when the lab is
# frozen/hibernating. A skip by design, not a failure — same treatment as a self-alerting job.
_LAB_SKIP = re.compile(r"lab mode \w+: .*does not run \(mode\.py\)", re.I)


# memo 2026-10-01 (data-desk): an hourly desk job gets one grace. One failed run is
# `"<src>:retrying"` in data-desk/health's head and its log ends 'failed (1x), retries on its next
# run' (the job line is then exempt through _MISS); the desk pages itself on a second failure in
# a row. A head whose every worse entry is retrying is that grace, said beside the state file.
_WORSE = re.compile(r'"worse"\s*:\s*\[([^\]]*)\]')


def _probe_note(head):
    m = _WORSE.search(head or "")
    items = re.findall(r'"([^"]+)"', m.group(1)) if m else []
    if items and all(i.endswith(":retrying") for i in items):
        return ("  <- every source listed is retrying after ONE failed run of an hourly job that "
                "pages itself on a second: NOT an issue")
    return ""


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


_SCRIPT = re.compile(r"\b([a-z_][a-z0-9_.-]*\.(?:py|sh|js))\b")
_STOP = {"the", "a", "an", "is", "are", "was", "and", "or", "of", "in", "on", "for", "with", "to",
         "after", "due", "job", "jobs", "its", "has", "have", "been", "status", "failed", "failing",
         "fails", "failure", "error", "errors", "stuck", "since", "last", "run", "ran", "ago",
         "expected", "every", "not", "from", "but"}


def _proj_token(project):
    return re.split(r"[\s-]+", project.lower().strip())[0]


def _cause(issue, jobs):
    """-> (cause key, project, what): the incident's identity, never its phrasing. Project is the
    job's (or the one the summary names); what is the failing script if one is named (the
    10-04 summaries named three different jobs but always snapshot.py), else the job, else the
    summary's first content words with digits, dates and times stripped."""
    text = str(issue.get("summary", "")).lower()
    job = next((j for j in jobs if _job_key(j["desc"]) == issue.get("key")), None)
    if job is not None:
        project = job["project"]
    else:
        projects = sorted({j["project"] for j in jobs}, key=len, reverse=True)
        project = next((p for p in projects
                        if p.lower() in text or _proj_token(p) in text), "misc")
    m = _SCRIPT.search(text)
    if m:
        what = re.sub(r"\d+", "", m.group(1))
    elif job is not None:
        what = _job_key(job["desc"])[4:]
    else:
        words = [w for w in re.findall(r"[a-z]+", re.sub(r"\d[\d:.\-tz]*", " ", text))
                 if len(w) > 2 and w not in _STOP and w != _proj_token(project)]
        what = "-".join(words[:4]) or "unknown"
    project = project.lower()
    return f"{project}:{what}", project, (m and m.group(1)) or (job and job["desc"]) or ""


def _owner_rows(now, path=None):
    """Critical pushes from anyone but the sentinel in the last OWNER_WINDOW."""
    out = []
    try:
        with open(path or NOTIFY_LOG) as f:
            for line in f:
                if '"critical"' not in line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if (r.get("tier") == "critical" and r.get("pushed") is not False
                        and 0 <= now - int(r.get("time", 0)) <= OWNER_WINDOW
                        and not str(r.get("title", "")).lower().startswith(SELF_TITLES)):
                    out.append(r)
    except OSError:
        pass
    return out


def _owner_paged(project, what, rows):
    """The owner's own critical row naming the same cause: the script (or a job name of 8+
    characters) as a whole word, and the project's first word. -> that row, or None."""
    if not what or len(what) < 8 and not what.endswith((".py", ".sh", ".js")):
        return None
    pat = re.compile(r"(?<![\w.])" + re.escape(what.lower()) + r"(?![\w])")
    tok = _proj_token(project)
    for r in rows:
        blob = f"{r.get('title', '')} {r.get('message', '')} {r.get('channel', '')}".lower()
        if pat.search(blob) and tok in blob:
            return r
    return None


_DOWN = re.compile(r"\bDOWN\b:?\s*([^\n]*)")


def _down_paged(summary, rows):
    """memo ask (c) "Health on :8001: one page per outage": healthcheck.sh's own critical
    "Spark health" / "DOWN: ClientCoApp(:8001) ..." row is the owner's page for any finding that
    names one of the DOWN services or its port (10-04 19:15 DOWN, 19:20 sentinel page). Ports stay
    in this match even though the word key strips digits. A "Recovered"/UP row names no DOWN
    service, so it never counts. -> (row, the service it matched) or (None, None)."""
    text = str(summary or "")
    for r in rows:
        for seg in _DOWN.findall(f"{r.get('title', '')}\n{r.get('message', '')}"):
            for tok in re.split(r"[\s,;]+", seg):
                name = re.match(r"[A-Za-z][\w.-]*", tok)
                port = re.search(r"\(:(\d+)\)", tok)
                if name and len(name.group(0)) >= 4 and re.search(
                        r"(?<![\w.-])" + re.escape(name.group(0)) + r"(?![\w-])", text, re.I):
                    return r, tok
                if port and re.search(r"(?:[:]|\bport\s*)" + port.group(1) + r"(?!\d)", text, re.I):
                    return r, tok
    return None, None


def _night(now):
    h = time.gmtime(now).tm_hour
    return h >= PROTECTED[0] or h < PROTECTED[1]


def _gate(fresh, jobs, now, pstate, owner_rows, seen_before):
    """Split the issues about to page into (page, dropped). Order of reasons: the owner already
    paged it, we paged this cause < 6 h ago, it is a known incident inside David's hours.
    `seen_before` is the cause -> last-seen map as it stood BEFORE this run."""
    page, dropped = [], []
    for i in fresh:
        cause, project, what = _cause(i, jobs)
        i["cause"] = cause
        reason = None
        own = _owner_paged(project, what, owner_rows)
        down = None
        if own is None:
            own, down = _down_paged(i.get("summary"), owner_rows)
        last = pstate.get("paged", {}).get(cause, 0)
        if own is not None:
            reason = (f"owner-paged: {str(own.get('title', ''))[:60]!r}"
                      f"{f' DOWN {down}' if down else ''} at "
                      f"{time.strftime('%m-%d %H:%MZ', time.gmtime(int(own['time'])))}")
        elif now - last < CAUSE_REPAGE:
            reason = f"repeat: paged {(now - last) // 60} min ago"
        elif _night(now) and now - seen_before.get(cause, 0) < KNOWN_WINDOW:
            reason = (f"protected-hours: known since "
                      f"{time.strftime('%m-%d %H:%MZ', time.gmtime(seen_before[cause]))}")
        if reason:
            dropped.append(dict(i, reason=reason))
        else:
            page.append(i)
    return page, dropped


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
    import catalog
    for name, cid in STATE_PROBES:
        try:
            head = open(catalog.path(cid, proj='maintenance')).read().strip()[:300]
            probes.append(f"{name}: {head}{_probe_note(head)}")
        except Exception as e:
            # an id that stopped resolving is a broken contract, not a quiet skip
            probes.append(f"{name}: UNREADABLE — catalog id {cid}: {type(e).__name__}: {str(e)[:160]}")
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
    if "--dry" in sys.argv or "--dry-run" in sys.argv:
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
    now = _clock()
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
    # Dedupe by CAUSE (memo review-dedupe-pages). Every issue the model raised this run is a
    # sighting; the gate reads the sightings from BEFORE this run.
    pstate = {}
    try:
        pstate = json.load(open(PAGED_STATE))
    except Exception:
        pass
    seen = pstate.setdefault("seen", {})
    seen_before = dict(seen)
    fresh, dropped = _gate(fresh, jobs, now, pstate, _owner_rows(now), seen_before)
    for i in issues:
        seen[_cause(i, jobs)[0]] = now
    for i in fresh:
        state[i["key"]] = now
        pstate.setdefault("paged", {})[i["cause"]] = now
    log = pstate.setdefault("dropped", [])
    for d in dropped:
        log.append({"at": now, "cause": d["cause"], "key": d["key"], "reason": d["reason"],
                    "summary": str(d.get("summary", ""))[:160]})
        print(f"{time.strftime('%F %T', time.gmtime(now))} DROPPED page ({d['reason']}): "
              f"{d['cause']} — {str(d.get('summary', ''))[:120]}")
    pstate["dropped"] = log[-300:]
    week = now - 7 * 86400
    for k in ("seen", "paged"):
        pstate[k] = {c: t for c, t in pstate.get(k, {}).items() if t >= week}
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(state, open(STATE, "w"))
    tmp = PAGED_STATE + ".tmp"
    json.dump(pstate, open(tmp, "w"), indent=1)
    os.replace(tmp, PAGED_STATE)

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
        print(f"{time.strftime('%F %T')} clear ({len(issues)} known/cooldown"
              f"{f', {len(dropped)} page(s) dropped' if dropped else ''})")


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
    # memo 2026-10-01 (data-desk): ONE failed hourly desk run must not page — the desk's last
    # log line is the self-alerting form below SELF_ALERT_AT; a third failure in a row still pages
    desk_tail = "2026-10-01T10:03:05Z [edgar_live] failed ({}x), retries on its next run"
    desk_verdict = {"issues": [{"key": "edgar-live-fail", "summary": "Same-day filings job failed "
                                "on its last run (Data Desk edgar_live)"}]}

    def desk(n):
        return [{"project": "data-desk", "desc": "Same-day filings", "log": "/replay/edgar_live.log",
                 "schedule": "3 0-2,10-23 * * 1-6", "expect_min": 60, "age_min": 17,
                 "last_run": at("10:03:05"), "tail": desk_tail.format(n)}]
    cases = [  # (name, snapshot, table at page time, verdict, cooldown key, page?, held?)
        ("09-23 replay: 14:30/14:45 runs clean by the time the slot came", snap,
         rows(at("14:45:08"), ok), verdict, "job:mcp-sync", False, True),
        ("newest run failed again", snap, rows(at("15:00:14"), retried), verdict, "job:mcp-sync", True, False),
        ("no run since the snapshot", snap, snap, verdict, "job:mcp-sync", True, False),
        ("data-desk: one failed hourly run (1x) is the desk's own grace", desk(1), desk(1),
         desk_verdict, "job:same-day-filings", False, False),
        ("data-desk: a third failure in a row (3x) pages", desk(3), desk(3),
         desk_verdict, "job:same-day-filings", True, False),
    ]
    saved = {k: getattr(t, k) for k in ("server", "ask_json", "subprocess", "STATE",
                                         "PAGED_STATE", "NOTIFY_LOG", "_clock")}
    argv, bad = sys.argv, []
    head = '{\n "status": "degraded",\n "at": "2026-10-01T10:20:00Z",\n "worse": [\n  "embed:retrying"\n ],'
    for label, h, want in (("health head: retrying only", head, True),
                           ("health head: retrying + degraded",
                            head.replace('"embed:retrying"', '"embed:retrying", "fedreg:degraded"'), False),
                           ("health head: failing", head.replace("retrying", "failing"), False)):
        good = bool(_probe_note(h)) == want
        print(f"{'PASS' if good else 'FAIL'}  {label}: NOT-an-issue note={bool(_probe_note(h))} (want {want})")
        bad += [] if good else [label]
    with tempfile.TemporaryDirectory() as tmp:
        for n, (name, snap, later, verdict, key, want_page, want_held) in enumerate(cases):
            calls, tables = [], iter([snap, later])
            t.server = types.SimpleNamespace(
                cron_jobs=lambda: next(tables, later),
                watchdog=lambda: {"ok": True, "state": "", "last_check": time.time()})
            t.ask_json = lambda *a, **k: json.loads(json.dumps(verdict))
            t.subprocess = types.SimpleNamespace(run=lambda cmd, **k: calls.append(cmd))
            t.STATE = os.path.join(tmp, f"sentinel{n}.json")
            t.PAGED_STATE = os.path.join(tmp, f"paged{n}.json")
            t.NOTIFY_LOG = os.path.join(tmp, "no-notifications.jsonl")
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
            stamped = key in json.load(open(os.path.join(tmp, f"sentinel{n}.json")))
            good = paged == want_page and stamped == want_page and held == want_held
            print(f"{'PASS' if good else 'FAIL'}  {name}: paged={paged} held={held} "
                  f"cooldown={stamped} (want paged={want_page})")
            bad += [] if good else [name]
        bad += _selftest_dedupe(t, tmp, saved)
    for label, a, want in (("argv: the cron line, --dry, --dry-run and selftest are the modes",
                            ([], ["--dry"], ["--dry-run"], ["selftest"]), False),
                           ("argv: --selftest, --bogus, -n, selftest --dry, a bare word are errors (exit 2)",
                            (["--selftest"], ["--bogus"], ["-n"], ["selftest", "--dry"], ["dry"]), True)):
        good = all(bool(argv_error(x)) == want for x in a)
        print(f"{'PASS' if good else 'FAIL'}  {label}")
        bad += [] if good else [label]
    print("ALL PASS" if not bad else f"{len(bad)} FAIL")
    return not bad


def _selftest_dedupe(t, tmp, saved):
    """memo review-dedupe-pages: the 10-04 clientco pages, replayed. Summaries and the owner's title
    are the real rows from state/notifications.jsonl (10-03 04:21 .. 10-05 06:21)."""
    import calendar, contextlib, io, types
    at = lambda s: calendar.timegm(time.strptime(s, "%Y-%m-%d %H:%M"))
    fl = [{"project": "clientco-db", "desc": d, "log": "/replay/refresh.log", "schedule": c,
           "expect_min": None, "age_min": 600, "last_run": at("2026-10-04 09:00"),
           "tail": "CYCLE ABORTED in snapshot.py"}
          for d, c in (("Factory data refresh", "0 4 3 * *"), ("Factory cycle retry", "0 9 * * *"))]
    fl.append({"project": "Stocks", "desc": "Mcp sync", "log": "/replay/agent_sync.log",
               "schedule": "*/15 14-19 * * 1-5", "expect_min": 15, "age_min": 5,
               "last_run": at("2026-10-04 00:15"), "tail": "Errno -3 name resolution"})
    stuck = "ClientCo monthly cycle (2026-10) is stuck in 'pending' status after 2 failed attempts due to snapshot.py crash."
    retry = "Factory cycle retry job aborted with 'CYCLE ABORTED in snapshot.py'."
    refresh = "ClientCo-db Factory data refresh and retry jobs are failing with 'CYCLE ABORTED in snapshot.py'."
    dns = "MCP sync job failing with DNS resolution errors (Temporary failure in name resolution)"
    owner = {"time": at("2026-10-03 08:20"), "channel": "alerts", "tier": "critical", "pushed": True,
             "title": "ClientCo refresh FAILED - snapshot.py",
             "message": "Attempt 2 for cycle 2026-10 died in snapshot.py."}
    own_sentinel = dict(owner, title="Sentinel (local model)", message=refresh)
    snap = "clientco-db:snapshot.py"
    app = "ClientCoApp (:8001) is failing health checks (last ran 4m ago, expected every 15m)"
    wiki = "ClientCoWiki (:8000) is failing health checks (last ran 4m ago, expected every 15m)"
    down = {"time": at("2026-10-04 19:15"), "channel": "alerts", "tier": "critical", "pushed": True,
            "title": "Spark health", "message": "DOWN: ClientCoApp(:8001)"}
    cases = [  # (name, now, summary, paged state, notifications rows, page?, reason starts)
        ("repeat of one cause within 6 h is dropped (10-05 05:20 after 03:20, another job key)",
         at("2026-10-05 05:20"), retry, {"paged": {snap: at("2026-10-05 03:20")}}, [],
         False, "repeat"),
        ("the owner already paged it (10-04 06:20, run_cycle.py paged 10-03 08:20) is dropped",
         at("2026-10-04 06:20"), stuck, {"paged": {snap: at("2026-10-03 04:21")}}, [owner],
         False, "owner-paged"),
        ("the sentinel's own past page is not an owner page", at("2026-10-04 12:20"), stuck,
         {}, [own_sentinel], True, ""),
        ("a known incident at 01:20Z is dropped (seen 10-03 23:20)", at("2026-10-04 01:20"), refresh,
         {"seen": {snap: at("2026-10-03 23:20")}, "paged": {snap: at("2026-10-03 04:21")}}, [],
         False, "protected-hours"),
        ("a NEW incident at 01:20Z still pages", at("2026-10-04 01:20"), refresh, {}, [], True, ""),
        ("a known incident at 01:20Z seen 25 h ago pages", at("2026-10-04 01:20"), refresh,
         {"seen": {snap: at("2026-10-03 00:20")}}, [], True, ""),
        ("a different cause pages beside a dropped one (6 h repeat of clientco)",
         at("2026-10-05 05:20"), [retry, dns], {"paged": {snap: at("2026-10-05 03:20")}}, [owner],
         True, "repeat"),
        ("health :8001 (10-04 19:20): healthcheck's DOWN ClientCoApp(:8001) at 19:15 is the owner page",
         at("2026-10-04 19:20"), app, {}, [down], False, "owner-paged"),
        ("health: the same DOWN row, a finding about another service still pages",
         at("2026-10-04 19:20"), wiki, {}, [down], True, ""),
        ("health: a Recovered row is not an owner page", at("2026-10-04 19:20"), app, {},
         [dict(down, message="Recovered — all checks green"),
          dict(down, message="UP: ClientCoApp(:8001)")], True, ""),
        ("health: a port-only finding (:8001) matches the DOWN row's port", at("2026-10-04 19:20"),
         "Service on :8001 is not answering health checks", {}, [down], False, "owner-paged"),
        ("past 6 h, the owner silent > 24 h, daytime: the same cause pages again",
         at("2026-10-05 12:30"), stuck, {"paged": {snap: at("2026-10-05 06:21")}}, [owner], True, ""),
    ]
    bad = []
    for n, (name, now, summ, pst, notes, want_page, want_reason) in enumerate(cases):
        summs = summ if isinstance(summ, list) else [summ]
        verdict = {"issues": [{"key": f"k{k}", "summary": x} for k, x in enumerate(summs)]}
        calls = []
        t.server = types.SimpleNamespace(
            cron_jobs=lambda: fl,
            watchdog=lambda: {"ok": True, "state": "", "last_check": time.time()})
        t.ask_json = lambda *a, **k: json.loads(json.dumps(verdict))
        t.subprocess = types.SimpleNamespace(run=lambda cmd, **k: calls.append(cmd))
        t.STATE = os.path.join(tmp, f"dd{n}.json")
        t.PAGED_STATE = os.path.join(tmp, f"ddpaged{n}.json")
        json.dump(pst, open(t.PAGED_STATE, "w"))
        t.NOTIFY_LOG = os.path.join(tmp, f"ddnotes{n}.jsonl")
        with open(t.NOTIFY_LOG, "w") as f:
            f.writelines(json.dumps(r) + "\n" for r in notes)
        t._clock = lambda now=now: now
        out = io.StringIO()
        argv = sys.argv
        sys.argv = ["sentinel.py"]
        try:
            with contextlib.redirect_stdout(out):
                t.main()
        finally:
            sys.argv = argv
            for k, v in saved.items():
                setattr(t, k, v)
        paged = [c for c in calls if "alerts" in c]
        after = json.load(open(os.path.join(tmp, f"ddpaged{n}.json")))
        drops = after.get("dropped", [])
        reason_ok = (not want_reason or (drops and drops[-1]["reason"].startswith(want_reason)
                                         and "DROPPED page (" + want_reason in out.getvalue()))
        page_ok = bool(paged) == want_page
        if want_page and want_reason:      # the mixed case: only the new cause is in the push
            page_ok = page_ok and "DNS" in paged[0][-1] and "snapshot" not in paged[0][-1]
        seen_ok = snap in after.get("seen", {}) if "snapshot" in " ".join(summs) else True
        good = page_ok and reason_ok and seen_ok and (want_page or not want_reason or drops)
        print(f"{'PASS' if good else 'FAIL'}  dedupe: {name}: paged={bool(paged)} "
              f"dropped={drops[-1]['reason'] if drops else None}")
        bad += [] if good else [name]
    # the cause key is identity, not phrasing: the three 10-04 summaries are ONE cause
    keys = {_cause(i, fl)[0] for i in _rekey([{"key": "x", "summary": x}
                                            for x in (stuck, retry, refresh)], fl)}
    good = keys == {snap}
    print(f"{'PASS' if good else 'FAIL'}  dedupe: three phrasings, three job keys -> one cause {sorted(keys)}")
    bad += [] if good else ["cause key"]
    return bad


USAGE = "usage: sentinel.py [--dry|--dry-run] | selftest | --help"


def argv_error(argv):
    """'' when argv is one of this script's modes, else why not (the caller exits 2, runs nothing)."""
    if argv in ([], ["selftest"], ["--dry"], ["--dry-run"]):
        return ""
    return f"unknown argument(s) {' '.join(argv)!r}"


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    _err = argv_error(sys.argv[1:])
    if _err:
        print(f"sentinel.py: {_err} — nothing run. {USAGE}", file=sys.stderr)
        sys.exit(2)
    if sys.argv[1:2] == ["selftest"]:
        sys.exit(0 if _selftest() else 1)
    main()
