#!/usr/bin/env python3
"""Daily rollup — 23:00 UTC (19:00 ET). The ONE Mission Control push of the day.

Zero tokens of any kind since 2026-09-03: David — "the end of day should be more crisp".
The local-model prose it used to send ("All other scheduled jobs completed successfully,
with no other new issues detected") was filler around three facts. Now it is the facts,
one line each, built straight from the ledgers:

    Thu Sep 3
    DECIDE  6 wait on you — thesis: which of the two draft cards should go deeper first? (+5 more)
    BROKEN  ClientCo refresh FAILED - snapshot.py x2 · VP night sweep · Sentinel x5
    AGENTS  Stocks 8 — VP 5m · Analyst 10m · Signals 8m · Bug Hunter 16m · Numbers 133m ...
    PAGED   21 — stocks 12 · alerts 8 · money 1
    HELD    33 — local-model runs 30 · Memo bus needs attention · Today at HBS · Canvas
    AUTO    1 done for you — catalog stale: rebuilt the index — undo: git revert 1a2b3c4   (a day with any)
    ANSWERS 2 done · 1 back to you — VP night sweep failed: memo filed · …   (only on a day with any)
    LEADS   4 need you · 3 worked today · 1 quiet
            needs you: Stocks 3 · data-desk 1 · poker-appstore 1 · thesis 1
            worked: Mission Control (team) · clientco-db (lead) · hbs (team)
            quiet: poker

Since the 2026-08-29 tiering (config/notify_policy.json) this is the delivery mechanism for
everything the policy HELD during the day; digest-tier notifications never reach the phone
on their own. It sends with --tier actionable so the policy can't hold the rollup as digest,
and the policy's `cap_exempt` entry (2026-10-02) keeps the maintenance channel's 1/day cap from
holding it: until then a morning memo push used that slot first, and the rollup was held on
11 of the 12 days 09-20 to 10-01. Back-office rule `rollup-held` (high) catches a held one.
Critical pushes bypass all of this and go immediately.

ANSWERS (2026-09-24): what became of the answers David gave on the dashboard's "Needs attention"
list — carried out, or handed back to him — read from state/decisions.jsonl, the one place the
daily check and the janitor record outcomes. Nothing else told him when an answer had been acted
on. The line is left out on a day with none, so a quiet day stays four lines.

Single-threaded owners (2026-10-03, David: "we already have pushes for stuff with ntfy, check
that and make sure it's up to date"). Three things moved into the existing rollup; no new push:
  DECIDE  every day something waits on David: the count and the top item, in the page's own order
          (Overview › Needs attention, /api/status: what counts, a decision only he makes first).
  AUTO    what the auto-fix policy did for him that day (decisions whose first event is
          `by: "policy"`, bin/decisions.py), each with how to undo it; given-back ones say so.
          No policy decision, no line; a missing field leaves its part out.
  LEADS   daily now, not Sunday only: every lead's day from /api/team and /api/crew: needs you
          (its Needs-attention count), worked (the lead's own run, or else some agent or job it
          owns ran today), or quiet. The page's seats and this line read the same payloads.
The three payloads come from the running dashboard (localhost:8900), else the same module called
in-process; if both fail the line says so in plain words and the rollup still goes out.

WEEK (2026-10-02 as the Sunday LEADS line, renamed 2026-10-03): what the project leads did that
week, from state/lead/runs.jsonl (bin/lead.py) and the open needs-david rows in ~/memos/LEDGER.md:

    WEEK    6 looked after · 3 fixed in place
            thesis needs you · poker 2 fixes committed · hbs nothing needed · …

"Looked after" counts projects with a good run (weekly or memo pass) in the last 7 days, and
"fixed in place" counts their commits (each carries a Lead-Run trailer). How many need David is
DECIDE's and LEADS' job every day, so WEEK no longer counts it; an open needs-david row on a lead
project still makes that project's outcome "needs you". While any lead is in its pilot (lead.json
pilot_until), the second line names every lead project with a 2-4 word outcome. After that it
names only a project that needs David, or whose lead missed or failed its run. A project led
through its own harness (Stocks: its PM) is not lead.py's to judge and is left out.

A rollup over ntfy's 4,096-byte message limit (ntfy.sh turns it into an attachment) is cut at a
line, with "… cut" as its last line.

    evening-digest.py            build and push
    evening-digest.py --dry-run  print what would be pushed, send nothing (on a weekday it also
                                 prints the Sunday WEEK lines, marked as Sunday-only)
    evening-digest.py selftest   the ANSWERS line on a throwaway store; sends nothing
  --dry is the same as --dry-run. Any other argument exits 2 with the usage line and sends nothing
  (2026-10-03: until then `--dry`, or any typo, built AND PUSHED the rollup).
"""
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter, OrderedDict

HOME = os.path.expanduser("~")
MC = f"{HOME}/maintenance"
NOTIF = f"{MC}/state/notifications.jsonl"
SESSIONS = f"{MC}/state/claude_sessions.jsonl"
DECISIONS = f"{MC}/state/decisions.jsonl"
DRY = "--dry-run" in sys.argv or "--dry" in sys.argv
USAGE = "usage: evening-digest.py [--dry-run|--dry] | selftest | --help"


def argv_error(argv):
    """'' when argv is one of this script's modes, else why not (the caller exits 2 and sends
    nothing). An argument nobody parsed used to fall through to the push (2026-10-03)."""
    if argv in ([], ["selftest"], ["--dry-run"], ["--dry"]):
        return ""
    return f"unknown argument(s) {' '.join(argv)!r}"


def rows(path, since):
    out = []
    try:
        with open(path, errors="replace") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("time", 0) >= since:
                    out.append(r)
    except FileNotFoundError:
        pass
    return out


def fmt_dur(s):
    s = int(s or 0)
    return f"{s // 3600}h{(s % 3600) // 60:02d}" if s >= 3600 else f"{max(1, round(s / 60))}m"


ROLE_RE = re.compile(r"^\s*(?:#\s*)?(?:You are (?:the |David's )?)?(.+?)(?:\s+(?:for|of|at|to)\s|\s*[(\[:.,—-]|$)")


ACRONYMS = {"VP", "PM", "COO", "CEO", "CFO", "CTO", "QA", "AI", "OK"}


# A lead's run and a memo pass, by their prompt's opening (2026-10-03): the AGENTS line read
# "thesis 1 — Mission Control's 2m" and "poker-appstore 1 — a poker 3m". Rows written since
# 2026-10-03 also carry `lead` / `lead_run` (bin/claude-session-notify.sh), which win.
LEAD_MODES = {"inbox": "lead memo pass", "weekly": "lead upkeep"}
TASK_NAMES = ((re.compile(r"^Mission Control's bin/memo-process\.py"), "lead memo pass"),
              (re.compile(r"^#\s*Project lead\b"), "lead upkeep"),
              (re.compile(r"^You are [\w-]+_lead, dispatched", re.I), "lead (dispatched)"),
              (re.compile(r"^You are an? [\w-]+ session, launched unattended", re.I), "memo pass"))


def run_name(r):
    """What a headless session did, for the AGENTS line: the lead's run when the row says so,
    else a named prompt opening, else role(task)."""
    if r.get("lead"):
        m = re.search(r"-([a-z]{2,16})$", str(r.get("lead_run") or ""))
        return LEAD_MODES.get(m.group(1), f"lead {m.group(1)}") if m else "lead (dispatched)"
    task = str(r.get("task") or "")
    for rx, name in TASK_NAMES:
        if rx.match(task):
            return name
    return role(task)


def role(task):
    """'You are the NUMBERS ENGINEER (the role...' -> 'Numbers Engineer';
    '# Day review (headless...' -> 'Day review'; 'update <TICKER> research — ...' -> 'update <TICKER> research'.

    The example ticker here is deliberately a placeholder: this file is on the public
    allowlist, and a real researched ticker in a docstring is a leak the scanner quarantines
    (it did, daily, from 2026-09-05 to 09-20 — 15 days of a high finding for one word)."""
    if PLACEHOLDER.match(task or ""):
        return "(no prompt)"
    m = ROLE_RE.match(task or "")
    name = (m.group(1) if m else task or "").strip()
    words = []
    for w in name.split()[:3]:
        if w.isupper() and w not in ACRONYMS:   # NUMBERS -> Numbers, but VP / COO / PM stay
            w = w.title()
        if len(" ".join(words + [w])) > 24:     # cut on a word boundary, never mid-word
            break
        words.append(w)
    return " ".join(words) or "(no prompt)"


def _short(s, n):
    """At most n characters, cut on a word boundary (as role() does)."""
    s = " ".join(str(s or "").split())
    return s if len(s) <= n else (s[:n - 1].rsplit(" ", 1)[0] or s[:n - 1]).rstrip(" ,;:—-") + "…"


# A session started outside ~ (a /tmp scratch dir, a build worktree) is nobody's scheduled work,
# and a template token is not a prompt (fix round 2026-09-24: two build-agent test runs of
# `claude -p "<prompt>"` read "home 2 — <prompt> 1m · <prompt> 1m" on the AGENTS line).
PLACEHOLDER = re.compile(r"^\s*(?:<[^<>\s]{1,40}>|\{\{?\s*\w{1,40}\s*\}?\}|\$\{?\w{1,40}\}?|%s)\s*$")


def _outside_home(cwd):
    """Outside ~ — and outside the real home too, so a test copy's HOME changes nothing."""
    import pwd
    homes = {HOME}
    try:
        homes.add(pwd.getpwuid(os.getuid()).pw_dir)
    except Exception:
        pass
    c = str(cwd or "")
    return bool(c) and not any(c == h or c.startswith(h.rstrip("/") + "/") for h in homes)


def answers_line(since, path=None):
    """'2 done · 1 back to you — <item>: <result> · …' for the outcomes recorded since `since`
    (the daily check's `decisions.py done`, the janitor's closes, the gate's give-ups), one per
    answer, newest outcome wins; None when there were none."""
    path = path or os.environ.get("MC_DECISIONS_FILE") or DECISIONS
    last = OrderedDict()
    policy = set()
    try:
        with open(path, errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if isinstance(e, dict) and e.get("id") and e.get("by") == "policy":
                    policy.add(e["id"])             # the AUTO line's, never an answer of David's
                if (isinstance(e, dict) and e.get("id") and e.get("status") in ("done", "failed")
                        and e.get("by") != "david" and (e.get("at") or 0) >= since):
                    last.pop(e["id"], None)
                    last[e["id"]] = e
    except OSError:
        return None
    for did in policy:
        last.pop(did, None)
    if not last:
        return None
    evs = list(last.values())
    done = sum(1 for e in evs if e["status"] == "done")
    head = " · ".join(x for x in (f"{done} done" if done else "",
                                  f"{len(evs) - done} back to you" if len(evs) > done else "") if x)
    bits = [f"{_short(e.get('title') or e.get('key'), 40)}: {_short(e.get('result') or e['status'], 60)}"
            for e in sorted(evs, key=lambda e: e["status"] == "done")[:3]]
    return f"{head} — " + " · ".join(bits) + (" · …" if len(evs) > 3 else "")


def policy_outcomes(since, path=None):
    """The decisions the auto-fix policy made (bin/decisions.py, dashboard/tt_decide.py), newest
    event per decision at or after `since`, oldest first. A decision is the policy's when an event
    of it carries `by: "policy"` (its first one does; follow-ups keep it). Read defensively: a
    row missing a field leaves that part of its line out, and no policy decision means []."""
    path = path or os.environ.get("MC_DECISIONS_FILE") or DECISIONS
    first, last, pol = {}, OrderedDict(), set()
    try:
        with open(path, errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if not (isinstance(e, dict) and e.get("id")):
                    continue
                first.setdefault(e["id"], e)
                last.pop(e["id"], None)
                last[e["id"]] = e
                if e.get("by") == "policy":
                    pol.add(e["id"])
    except OSError:
        return []
    out = []
    for did, e in last.items():
        if did not in pol or (e.get("at") or 0) < since:
            continue
        if e.get("by") == "david" and e.get("status") == "cancelled":
            continue                                # David undid it himself: his, not news
        out.append(dict(e, undo=e.get("undo") or first[did].get("undo") or ""))
    return sorted(out, key=lambda e: e.get("at") or 0)


def auto_lines(since, path=None):
    """'AUTO    2 done for you · 1 back to you' and one line per decision (at most 3): what it was,
    what was done, how to undo it. [] on a day the policy did nothing."""
    evs = policy_outcomes(since, path)
    if not evs:
        return []

    def state(e):
        st = e.get("status")
        if st == "done":
            return "done"
        if st == "failed" or e.get("handback"):
            return "back"
        return "queued" if st in ("queued", "started", "pending") else None
    evs = [e for e in evs if state(e)]
    if not evs:
        return []
    n = Counter(state(e) for e in evs)
    head = " · ".join(x for x in (f"{n['done']} done for you" if n["done"] else "",
                                  f"{n['queued']} queued for the 07:50 check" if n["queued"] else "",
                                  f"{n['back']} back to you" if n["back"] else "") if x)
    out = [f"AUTO    {head}"]
    order = {"back": 0, "done": 1, "queued": 2}
    for e in sorted(evs, key=lambda e: order[state(e)])[:3]:
        what = _short(e.get("title") or e.get("key") or "a policy decision", 40)
        res = e.get("result") or ""
        if state(e) == "back" and not res:
            res = "given back to you"
        bit = f"{what}: {_short(res, 70)}" if res else what
        if e.get("undo") and state(e) != "back":
            bit += f" — undo: {_short(e['undo'], 80)}"
        out.append(f"        {bit}")
    if len(evs) > 3:
        out.append(f"        … +{len(evs) - 3} more on the dashboard")
    return out


# Decisions only David makes rank first among what counts (tt_now.DECISION_KINDS / attention_order).
DECISION_KINDS = ("memo", "desk-request")
_SEV = {"crit": 0, "warn": 1, "info": 2}


def _counts(i):
    """The page's ONE needs-you rule (index.html needsYou(), tt_team.needs_you): `counts` when the
    item says, else anything that is not a note."""
    c = i.get("counts")
    return (i.get("sev") != "info") if c is None else bool(c)


def decide_line(st):
    """'DECIDE  6 wait on you — thesis: <the question> (+5 more)' from /api/status, or None when
    nothing waits on David. The top item is the page's first: a decision only he makes before any
    ops item, then severity, then newest."""
    items = [i for i in ((st or {}).get("items") or []) if isinstance(i, dict) and _counts(i)]
    if not items:
        return None
    items.sort(key=lambda i: (i.get("kind") not in DECISION_KINDS, _SEV.get(i.get("sev"), 2), -(i.get("since") or 0)))
    top = items[0]
    ask = None
    if top.get("kind") in DECISION_KINDS:
        ask = (top.get("memo") or {}).get("ask") or (top.get("request") or {}).get("ask")
    ask = ask or top.get("title") or top.get("key") or "?"
    n = len(items)
    who = f"{top['owner']}: " if top.get("owner") else ""
    return (f"DECIDE  {n} wait{'s' if n == 1 else ''} on you — {who}{_short(ask, 90)}"
            + (f" (+{n - 1} more)" if n > 1 else ""))


def _seat_name(s):
    """A lead's seat as David reads it: its project ('thesis', 'Mission Control', 'Stocks'), for
    every seat. The Stocks seat's own name is "Stocks PM · lead" (its lead is the PM), and the
    count after it read "Stocks PM · lead 3", as if it were lead number 3 (2026-10-03)."""
    return s.get("project_name") or s.get("project") or s.get("name") or str(s.get("id") or "")


def _wrap(label, bits, width=78):
    """'        <label>: a · b · c' wrapped under the 8-space indent, no line past `width`."""
    out, cur = [], f"        {label}: "
    first = True
    for b in bits:
        add = b if first else f" · {b}"
        if not first and len(cur) + len(add) > width:
            out.append(cur)
            cur, add = "          " + b, ""
        cur += add
        first = False
    out.append(cur)
    return out


def leads_today_lines(team, crew, now=None):
    """The daily LEADS lines (module docstring) from /api/team's seats and /api/crew's agents.
    Each lead is ONE of: needs you (its seat's needs_you_n), worked today (its own run, or some
    agent or job it owns: `owner_lead` when the crew says, else the agent's project), quiet.
    [] when there are no seats."""
    now = now or time.time()
    day = now - (now % 86400)
    seats = [x for x in ((team or {}).get("seats") or []) if isinstance(x, dict) and x.get("id")]
    if not seats:
        return []
    agents = [a for a in ((crew or {}).get("agents") or []) if isinstance(a, dict)]

    def ran(a):
        return ((a.get("last") or {}).get("s") or 0) >= day
    need, worked, quiet = [], [], []
    for x in seats:
        sid, name = x["id"], _seat_name(x)
        n = int(x.get("needs_you_n") or 0)
        lead_ran = ((x.get("last_run") or {}).get("ts") or 0) >= day or \
            any(a.get("id") == sid and ran(a) for a in agents)
        projects = {p for p in (x.get("project_name"), x.get("project")) if p}
        team_ran = any(a.get("id") != sid and ran(a)
                       and (a.get("owner_lead") == sid if a.get("owner_lead") else a.get("project") in projects)
                       for a in agents)
        if n:
            need.append(f"{name} {n}")
        elif lead_ran or team_ran:
            worked.append(f"{name} ({'lead' if lead_ran else 'team'})")
        else:
            quiet.append(name)
    head = " · ".join(x for x in (f"{len(need)} need{'s' if len(need) == 1 else ''} you" if need else "",
                                  f"{len(worked)} worked today" if worked else "",
                                  f"{len(quiet)} quiet" if quiet else "") if x)
    out = [f"LEADS   {head}"]
    for label, bits in (("needs you", need), ("worked", worked), ("quiet", quiet)):
        if bits:
            out += _wrap(label, bits)
    return out


API = os.environ.get("MC_ROLLUP_API") or "http://localhost:8900"
API_MODULES = {"/api/status": ("tt_now", "status"), "/api/team": ("tt_team", "team"),
               "/api/crew": ("tt_crew", "crew")}


def _api(path, timeout=25):
    """One dashboard payload: the running server's (what the page shows), else the same module
    called in-process (the server may be down at 23:00), else raises."""
    try:
        from urllib.request import Request, urlopen
        with urlopen(Request(API + path, headers={"Accept": "application/json"}), timeout=timeout) as r:
            return json.load(r)
    except Exception as e1:
        try:
            import importlib
            mod, fn = API_MODULES[path]
            if f"{MC}/dashboard" not in sys.path:
                sys.path.insert(0, f"{MC}/dashboard")
            return getattr(importlib.import_module(mod), fn)({})
        except Exception as e2:
            raise RuntimeError(f"server: {type(e1).__name__}; in-process: {type(e2).__name__}") from None


def _safe(fn, fallback):
    """A line the rollup can always afford: fn()'s, or one plain sentence when it raised."""
    try:
        return fn()
    except Exception as e:
        return fallback(e)


LEAD_WINDOW_S = 7 * 86400
ATTENTION = ("needs you", "run failed", "ran out of time", "missed its run", "config invalid", "no lead yet")


def _lead_mod():
    sys.path.insert(0, f"{MC}/bin")
    import lead
    return lead


def _needs_item(status):
    """The question in a needs-david status: the bold span when the row bolds it
    ('**needs-david — keep the teal accent?** Item 1 …' -> 'keep the teal accent?'), else
    everything after the marker. None when there is no text."""
    raw = str(status or "")
    m = re.search(r"\*\*\s*needs-david\b[\s—:–-]*(.+?)\*\*", raw, re.I)
    if not m:
        m = re.search(r"needs-david\b[\s—:–-]*(.+)", re.sub(r"[*`]", "", raw), re.I)
    return (m.group(1).strip() or None) if m else None


def _outcome(e, wk, mine, needs, overdue, wd):
    """One project's week in 2-4 words."""
    if e.get("state") == "missing":
        return "no lead yet"
    if e.get("state") == "invalid":
        return "config invalid"
    if needs:
        return "needs you"
    if wk:
        r = wk[-1]
        st, c = r.get("state"), int(r.get("commits") or 0)
        if st == "failed":
            return "run failed"
        if st == "partial":
            return "ran out of time"
        if st == "skipped":
            return "run skipped"
        if c:
            return f"{c} fix{'es' if c > 1 else ''} committed"
        return "looked, nothing changed" if r.get("claude") else "nothing needed"
    if any(r.get("state") in ("ok", "partial") for r in mine):
        return "memos handled"
    if overdue:
        return "missed its run"
    return f"first run {wd}" if wd else "not run yet"


def _own_harness(e):
    """A project led through its own harness (Stocks: its PM), not by a lead.py lead: however
    bin/lead.py's roster says so (state excluded or external, kind external)."""
    cfg = e.get("cfg") if isinstance(e.get("cfg"), dict) else {}
    return e.get("state") in ("excluded", "external") or "external" in (e.get("kind"), cfg.get("kind"))


def leads_lines(now=None, rs=None, ros=None, rows=None, inst=None):
    """The Sunday WEEK lines (see the module docstring). Every input can be passed in for the
    selftest. Left out, each one is read from bin/lead.py, read-only: lead.status() is not
    called, because it writes the install clock. [] when no project has a lead."""
    now = now or time.time()
    L = _lead_mod() if None in (rs, ros, rows, inst) else None
    ros = [e for e in (L.roster() if ros is None else ros) if not _own_harness(e)]
    if not ros:
        return []
    rs = L.runs() if rs is None else rs
    rows = L.ledger_rows() if rows is None else rows
    inst = L._installed() if inst is None else inst
    overdue_days = getattr(L, "OVERDUE_DAYS", 10) if L else 10
    aliases = (L._aliases if L else lambda s, d: {s.lower(), d.lower()})
    today = time.strftime("%Y-%m-%d", time.gmtime(now))
    week = [r for r in rs if (r.get("ts") or 0) >= now - LEAD_WINDOW_S]
    looked = {r.get("slug") for r in week if r.get("state") in ("ok", "partial")}
    fixed = sum(int(r.get("commits") or 0) for r in week)
    needs, outcomes, pilot = [], [], False
    for e in ros:
        slug = e["slug"]
        al = aliases(slug, e.get("dir") or slug)
        nd = [r for r in rows if str(r.get("target", "")).lower() in al
              and "needs-david" in re.sub(r"[*_`]", "", str(r.get("status", ""))).lower()]
        needs += [(r.get("date", ""), slug, _needs_item(r.get("status")) or r.get("memo", "?")) for r in nd]
        cfg = e.get("cfg") or {}
        pu = str(cfg.get("pilot_until") or "")
        if e.get("state") == "ok" and (not re.match(r"^\d{4}-\d{2}-\d{2}$", pu) or today < pu):
            pilot = True                    # a missing or unparseable date counts as pilot, as the guard reads it
        mine = [r for r in week if r.get("slug") == slug]
        wk = [r for r in mine if r.get("mode") == "weekly"]
        oks = [r.get("ts") for r in rs if r.get("slug") == slug and r.get("mode") == "weekly"
               and r.get("state") in ("ok", "partial")]
        base = max(oks) if oks else inst.get(slug)
        overdue = e.get("state") == "ok" and bool(base) and (now - base) / 86400 > overdue_days
        outcomes.append((slug, _outcome(e, wk, mine, nd, overdue, cfg.get("weekday"))))
    head = f"WEEK    {len(looked)} looked after · {fixed} fixed in place"   # needs-you: DECIDE and LEADS, daily
    named = [f"{s} {o}" for s, o in outcomes if pilot or o in ATTENTION]
    out, cur = [head], ""
    for bit in named:                       # wrap at ~70 characters, indented like AGENTS
        if cur and len(cur) + 3 + len(bit) > 70:
            out.append("        " + cur)
            cur = bit
        else:
            cur = f"{cur} · {bit}" if cur else bit
    if cur:
        out.append("        " + cur)
    return out


def _safe_leads_lines(now):
    """The rollup must go out even if bin/lead.py is broken: one plain line instead."""
    try:
        return leads_lines(now)
    except Exception as e:
        return [f"WEEK    could not read the leads (bin/lead.py: {type(e).__name__})"]


NTFY_MAX = 3900          # bytes; ntfy.sh turns a message over 4,096 bytes into an attachment


def fit(body, limit=NTFY_MAX):
    """The rollup's lines, cut at a line so the message stays a message (module docstring)."""
    out, size = [], 0
    for i, ln in enumerate(body):
        n = len(ln.encode()) + 1
        if size + n > limit - 40:
            out.append(f"… cut: {len(body) - i} more line{'s' if len(body) - i != 1 else ''} on the dashboard")
            break
        out.append(ln)
        size += n
    return out


def build(now=None, leads=None, api=None):
    """`leads`: add the WEEK lines (default: on Sundays, UTC). `api(path)` gives a dashboard
    payload (default _api: the running server, else in-process); the selftest passes fixtures."""
    now = now or time.time()
    api = api or _api
    day_start = now - (now % 86400)
    notes = rows(NOTIF, day_start)
    ends = [r for r in rows(SESSIONS, day_start)
            if r.get("headless") and r.get("event") == "SessionEnd"
            and r.get("project") != "scratch" and not _outside_home(r.get("cwd"))]

    pushed = [m for m in notes if m.get("pushed", True)]
    held = [m for m in notes if m.get("pushed") is False]

    # BROKEN — everything that paged as critical (alerts channel or critical tier), by title.
    broken = OrderedDict()
    for m in sorted(pushed, key=lambda m: m.get("time", 0)):
        if m.get("channel") == "alerts" or m.get("tier") == "critical":
            t = re.sub(r"\s+", " ", m.get("title", "")).strip()
            broken[t] = broken.get(t, 0) + 1
    broken_line = " · ".join(f"{t} x{n}" if n > 1 else t for t, n in broken.items()) or "nothing"

    # AGENTS — every headless Claude session that finished today, grouped by project.
    by_proj = OrderedDict()
    for r in ends:
        by_proj.setdefault(r.get("project", "?"), []).append(r)
    agent_lines = []
    for proj, runs in by_proj.items():
        parts = [f"{run_name(r)} {fmt_dur(r.get('duration_s'))}" for r in runs]
        agent_lines.append(f"{proj} {len(runs)} — " + " · ".join(parts))
    if not agent_lines:
        agent_lines = ["none"]

    # PAGED — what already reached the phone, by channel, money alerts called out.
    per_ch = Counter(m.get("channel") for m in pushed)
    money = [m for m in pushed if m.get("channel") == "stocks" and m.get("tier") == "actionable"]
    paged_bits = [f"{ch} {n}" for ch, n in per_ch.most_common()]
    if money:
        paged_bits.append("money " + " / ".join(m["title"].replace(" · ", "/") for m in money[:3]))
    paged_line = f"{len(pushed)} — " + " · ".join(paged_bits) if pushed else "0"

    # HELD — routine traffic collapsed; the few non-routine held titles named.
    local = sum(1 for m in held if str(m.get("title", "")).startswith(("Local model", "Claude ")))
    others = Counter(re.sub(r"\s+—.*$", "", m.get("title", "")) for m in held
                     if not str(m.get("title", "")).startswith(("Local model", "Claude ")))
    held_bits = ([f"local-model/agent runs {local}"] if local else []) + \
                [f"{t} x{n}" if n > 1 else t for t, n in others.most_common(5)]
    held_line = f"{len(held)} — " + " · ".join(held_bits) if held else "0"

    body = [time.strftime("%a %b %-d", time.gmtime(now))]
    dec = _safe(lambda: decide_line(api("/api/status")),
                lambda e: f"DECIDE  could not read Needs attention ({e})")
    if dec:
        body.append(dec)
    body += [f"BROKEN  {broken_line}",
             f"AGENTS  {agent_lines[0]}"]
    body += [f"        {l}" for l in agent_lines[1:]]
    body += [f"PAGED   {paged_line}", f"HELD    {held_line}"]
    body += _safe(lambda: auto_lines(day_start), lambda e: [f"AUTO    could not read the decisions ({type(e).__name__})"])
    ans = answers_line(day_start)
    if ans:
        body.append(f"ANSWERS {ans}")
    body += _safe(lambda: leads_today_lines(api("/api/team"), api("/api/crew"), now),
                  lambda e: [f"LEADS   could not read the team ({e})"])
    if leads if leads is not None else time.gmtime(now).tm_wday == 6:
        body += _safe_leads_lines(now)
    return "\n".join(fit(body)), len(held), len(pushed)


def _leads_selftest(check):
    """The Sunday LEADS lines on fixtures: no file is read or written."""
    import calendar
    sun = calendar.timegm((2026, 10, 11, 23, 0, 0))          # a Sunday, 23:00Z
    day = 86400

    def lead(slug, wd, pilot="2026-10-16", state="ok"):
        return {"slug": slug, "dir": slug, "state": state, "valid": state == "ok",
                "cfg": {"weekday": wd, "pilot_until": pilot}}
    ros = [{"slug": "stocks", "dir": "Stocks", "state": "excluded", "cfg": None},
           lead("data-desk", "Sat"), lead("hbs", "Wed"), lead("maintenance", "Thu"), lead("poker", "Tue"),
           lead("poker-appstore", "Fri"), lead("thesis", "Mon")]
    rs = [{"ts": sun - 6 * day, "slug": "thesis", "mode": "weekly", "state": "ok", "claude": True, "commits": 2},
          {"ts": sun - 5 * day, "slug": "poker", "mode": "weekly", "state": "ok", "claude": False, "commits": 0},
          {"ts": sun - 4 * day, "slug": "hbs", "mode": "weekly", "state": "partial", "claude": True, "commits": 1},
          {"ts": sun - 1 * day, "slug": "data-desk", "mode": "weekly", "state": "failed", "claude": True, "commits": 0},
          {"ts": sun - 2 * day, "slug": "maintenance", "mode": "inbox", "state": "ok", "claude": True, "commits": 0},
          {"ts": sun - 20 * day, "slug": "poker", "mode": "weekly", "state": "ok", "claude": True, "commits": 9}]
    rows = [{"date": "2026-10-10", "memo": "first-draft", "target": "thesis",
             "status": "**needs-david — pick the first draft** (two options in the memo)"},
            {"date": "2026-09-29", "memo": "teal", "target": "poker-appstore",
             "status": "needs-david — keep the teal accent? Item 1 implemented"},
            {"date": "2026-10-11", "memo": "x", "target": "stocks", "status": "needs-david — not a lead project"},
            {"date": "2026-10-09", "memo": "y", "target": "poker", "status": "**verified 2026-10-09**"}]
    inst = {r["slug"]: sun - 15 * day for r in ros}
    got = leads_lines(sun, rs, ros, rows, inst)
    check("WEEK: counts the week (looked after, commits); needs-you is DECIDE's and LEADS' daily", got[0],
          "WEEK    4 looked after · 3 fixed in place")
    check("LEADS: the question is the bolded span, else the text after the marker",
          [_needs_item(rows[0]["status"]), _needs_item(rows[1]["status"]), _needs_item("needs-david")],
          ["pick the first draft", "keep the teal accent? Item 1 implemented", None])
    check("LEADS: in the pilot every lead project is named with its outcome", " · ".join(
        x.strip() for x in got[1:]).split(" · "),
          ["data-desk run failed", "hbs ran out of time", "maintenance memos handled", "poker nothing needed",
           "poker-appstore needs you", "thesis needs you"])
    check("LEADS: wrapped under the AGENTS indent, no line past 78 characters",
          all(x.startswith("        ") and len(x) <= 78 for x in got[1:]), True)
    post = [dict(e, cfg=dict(e["cfg"] or {}, pilot_until="2026-10-01")) for e in ros]
    got = leads_lines(sun, rs, post, rows, inst)
    check("LEADS: after the pilot only what needs David or failed is named",
          " · ".join(x.strip() for x in got[1:]).split(" · "),
          ["data-desk run failed", "hbs ran out of time", "poker-appstore needs you", "thesis needs you"])
    clean = [r for r in rs if r["slug"] in ("thesis", "poker")]
    got = leads_lines(sun, clean, [e for e in post if e["slug"] in ("thesis", "poker")], rows[2:], inst)
    check("LEADS: a clean week after the pilot is one line", got,
          ["WEEK    2 looked after · 2 fixed in place"])
    ext = [dict(lead("stocks", None), state="external", kind="external", cfg={"kind": "external"})]
    check("WEEK: a project led through its own harness (Stocks' PM) is not lead.py's to judge",
          leads_lines(sun, [], ext, [], {}), [])
    got = leads_lines(sun, [], [lead("poker", "Tue")], [], {"poker": sun - 12 * day})
    check("LEADS: a lead with no good weekly run past the overdue line missed its run", got[1].strip(),
          "poker missed its run")
    got = leads_lines(sun, [], [lead("poker", "Tue")], [], {"poker": sun - 2 * day})
    check("LEADS: a new lead that has not reached its day says when it runs", got[1].strip(), "poker first run Tue")
    check("LEADS: no lead projects, no line", leads_lines(sun, [], [ros[0]], [], {}), [])
    real = globals()["leads_lines"]

    def boom(now=None, *a):
        raise ValueError("x")
    globals()["leads_lines"] = boom
    try:
        check("LEADS: a broken lead.py costs one plain line, never the rollup", _safe_leads_lines(sun),
              ["WEEK    could not read the leads (bin/lead.py: ValueError)"])
    finally:
        globals()["leads_lines"] = real


def _sto_selftest(check):
    """DECIDE, AUTO, the daily LEADS, the AGENTS names and the size cap, on fixtures: no file
    outside a temp dir is read, nothing is sent, the dashboard is never called."""
    import calendar
    import tempfile
    now = calendar.timegm((2026, 10, 7, 23, 0, 0))          # a Wednesday, 23:00Z
    day = now - now % 86400
    # DECIDE ---------------------------------------------------------------------------------
    st = {"items": [
        {"sev": "crit", "kind": "service-down", "title": "Stocks dashboard down", "owner": "maintenance",
         "since": now - 60, "counts": True},
        {"sev": "warn", "kind": "memo", "title": "Memo for you: thesis_lead-needs-you-first-draft", "owner": "thesis",
         "since": now - 9000, "counts": True, "memo": {"ask": "which draft card goes deeper first?"}},
        {"sev": "warn", "kind": "job-failed", "title": "Ops · COO failed", "owner": "stocks", "since": now - 50},
        {"sev": "warn", "kind": "memo", "title": "answered", "owner": "poker", "counts": False},
        {"sev": "info", "kind": "os-update", "title": "318 OS updates pending", "owner": "maintenance"}]}
    check("DECIDE: counts what the page counts; a decision only David makes is the top item",
          decide_line(st), "DECIDE  3 wait on you — thesis: which draft card goes deeper first? (+2 more)")
    check("DECIDE: one ops item, its title; owner left out when there is none",
          decide_line({"items": [{"sev": "warn", "kind": "job-failed", "title": "Ops · COO failed"}]}),
          "DECIDE  1 waits on you — Ops · COO failed")
    check("DECIDE: nothing waits (notes, answered items, junk) -> no line",
          (decide_line({"items": [st["items"][3], st["items"][4], "junk"]}), decide_line({}), decide_line(None)),
          (None, None, None))
    # AUTO -----------------------------------------------------------------------------------
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for e in ({"id": "p1", "at": now - 3600, "by": "policy", "status": "done", "title": "a dashboard check is failing",
                   "result": "fixed the test; check.py green", "undo": "git revert 2945cec in ~/maintenance"},
                  {"id": "p2", "at": day - 10, "by": "policy", "status": "queued", "title": "yesterday"},
                  {"id": "p2", "at": now - 600, "by": "policy", "status": "failed", "handback": True,
                   "title": "catalog stale: thesis/feeds", "result": "came back twice in 14 days; yours now"},
                  {"id": "p3", "at": now - 500, "by": "policy", "status": "queued", "title": "Memo for you: x"},
                  {"id": "p4", "at": now - 400, "by": "policy", "status": "done", "title": "undone one", "undo": "u"},
                  {"id": "p4", "at": now - 300, "by": "david", "status": "cancelled", "title": "undone one"},
                  {"id": "p5", "at": now - 200, "by": "policy", "status": "done"},
                  {"id": "d1", "at": now - 100, "by": "daily check", "status": "done", "title": "David's answer",
                   "result": "memo filed"},
                  {"id": "p6", "at": day - 99, "by": "policy", "status": "done", "title": "before today"}):
            f.write(json.dumps(e) + "\n")
    try:
        got = auto_lines(day, f.name)
        check("AUTO: the policy's decisions today, given-back first, each with how to undo it", got,
              ["AUTO    2 done for you · 1 queued for the 07:50 check · 1 back to you",
               "        catalog stale: thesis/feeds: came back twice in 14 days; yours now",
               "        a dashboard check is failing: fixed the test; check.py green — undo: git revert 2945cec in "
               "~/maintenance",
               "        a policy decision",
               "        … +1 more on the dashboard"])
        check("AUTO: David's undo ends it; ANSWERS never repeats a policy decision",
              ("undone one" in " ".join(got), answers_line(day, f.name)), (False, "1 done — David's answer: memo filed"))
        check("AUTO: no policy decision today, no store -> no line",
              (auto_lines(now + 10, f.name), auto_lines(day, f.name + ".missing")), ([], []))
    finally:
        os.unlink(f.name)
    # LEADS (daily) ---------------------------------------------------------------------------
    team = {"seats": [
        {"id": "maintenance_lead", "project": "maintenance", "project_name": "Mission Control", "needs_you_n": 0,
         "last_run": None},
        {"id": "pm", "project": "stocks", "project_name": "Stocks", "name": "Stocks PM · lead", "needs_you_n": 3,
         "last_run": {"ts": now - 7200}},
        {"id": "clientco_db_lead", "project": "clientco-db", "project_name": "clientco-db", "needs_you_n": 0,
         "last_run": {"ts": now - 3600}},
        {"id": "hbs_lead", "project": "hbs", "project_name": "hbs", "needs_you_n": 0, "last_run": None},
        {"id": "poker_lead", "project": "poker", "project_name": "poker", "needs_you_n": 0,
         "last_run": {"ts": day - 60}},
        {"id": "thesis_lead", "project": "thesis", "project_name": "thesis"}]}
    crew = {"agents": [
        {"id": "watchdog", "project": "Mission Control", "last": {"s": now - 900}},
        {"id": "brief-writer", "project": "hbs", "last": {"s": day - 5}},
        {"id": "flag-scan", "project": "hbs", "owner_lead": "thesis_lead", "last": {"s": now - 100}},
        {"id": "case-reader", "project": "hbs", "last": None},
        {"id": "poker_lead", "project": "poker", "last": {"s": day - 60}}]}
    got = leads_today_lines(team, crew, now)
    check("LEADS: every lead's day: needs you, worked (its own run, else an agent it owns), quiet", got,
          ["LEADS   1 needs you · 3 worked today · 2 quiet",
           "        needs you: Stocks 3",
           "        worked: Mission Control (team) · clientco-db (lead) · thesis (team)",
           "        quiet: hbs · poker"])
    check("LEADS: every seat reads as its project, the Stocks PM's too (never 'Stocks PM · lead 3')",
          [_seat_name(x) for x in team["seats"][:2]] + [_seat_name({"id": "x_lead", "name": "x lead"}),
                                                        _seat_name({"id": "y_lead"})],
          ["Mission Control", "Stocks", "x lead", "y_lead"])
    check("LEADS: no seats (or no team at all) -> no lines", (leads_today_lines({}, crew, now),
                                                             leads_today_lines(None, None, now)), ([], []))
    many = {"seats": [{"id": f"p{i}_lead", "project_name": f"project-number-{i}", "needs_you_n": i + 1}
                      for i in range(9)]}
    got = leads_today_lines(many, {}, now)
    check("LEADS: a long group wraps under the indent, no line past 78 characters",
          (len(got) > 2, all(len(x) <= 78 and x.startswith("        ") for x in got[1:])), (True, True))
    # AGENTS names ----------------------------------------------------------------------------
    check("AGENTS: a lead's run by its row, else by its prompt's opening; a memo pass is named",
          [run_name(r) for r in ({"lead": "thesis_lead", "lead_run": "202610030820-thesis-inbox", "task": "x"},
                                 {"lead": "data_desk_lead", "lead_run": "202610030702-data-desk-weekly"},
                                 {"lead": "poker_lead"},
                                 {"task": "Mission Control's bin/memo-process.py (the daily memo pass) runs"},
                                 {"task": "# Project lead: standing duties You are **data_desk_lead**"},
                                 {"task": "You are a poker-appstore session, launched unattended by Mission Control"},
                                 {"task": "You are the INDUSTRY ANALYST for David's Stocks project"})],
          ["lead memo pass", "lead upkeep", "lead (dispatched)", "lead memo pass", "lead upkeep", "memo pass",
           "Industry Analyst"])
    # the whole rollup with fixtures, and with the dashboard unreachable ----------------------
    payloads = {"/api/status": st, "/api/team": team, "/api/crew": crew}
    body = build(now, leads=False, api=lambda p: payloads[p])[0].splitlines()
    check("build: DECIDE is the line under the date; LEADS follows the facts",
          (body[1].startswith("DECIDE  3 wait on you"), body[2].startswith("BROKEN"),
           any(x.startswith("LEADS   1 needs you") for x in body)), (True, True, True))

    def down(p):
        raise RuntimeError("server: URLError; in-process: ImportError")
    body = build(now, leads=False, api=down)[0].splitlines()
    check("build: an unreachable dashboard costs a plain line each, never the rollup",
          [x for x in body if x.startswith(("DECIDE", "LEADS"))],
          ["DECIDE  could not read Needs attention (server: URLError; in-process: ImportError)",
           "LEADS   could not read the team (server: URLError; in-process: ImportError)"])
    big = [f"line {i} " + "x" * 90 for i in range(60)]
    cut = fit(big)
    check("fit: a rollup over ntfy's limit is cut at a line and says so",
          (len("\n".join(cut).encode()) <= NTFY_MAX, cut[-1].startswith("… cut: "), fit(big[:3]) == big[:3]),
          (True, True, True))


def selftest():
    import tempfile
    fails = []

    def check(name, got, want):
        ok = got == want
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else f" — got {got!r}, want {want!r}"))
        if not ok:
            fails.append(name)
    t = time.time()
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for e in ({"id": "a1", "at": t - 90000, "status": "done", "by": "daily check", "title": "old", "result": "x"},
                  {"id": "b2", "at": t - 60, "status": "queued", "by": "david", "title": "Memo processor failed"},
                  {"id": "b2", "at": t - 30, "status": "done", "by": "daily check", "title": "Memo processor failed",
                   "result": "confirmed the 4:20 run was clean"},
                  {"id": "c3", "at": t - 20, "status": "failed", "by": "daily check", "title": "VP night sweep failed",
                   "result": "The daily check was handed this on 3 mornings and never finished it"},
                  {"id": "d4", "at": t - 10, "status": "done", "by": "david", "title": "not an outcome"}):
            f.write(json.dumps(e) + "\n")
    try:
        line = answers_line(t - 3600, f.name)
        check("ANSWERS: today's outcomes, handed-back first, never an answer event itself", line,
              "1 done · 1 back to you — VP night sweep failed: The daily check was handed this on 3 mornings "
              "and never… · Memo processor failed: confirmed the 4:20 run was clean")
        check("ANSWERS: no line on a day with none", answers_line(t + 10, f.name), None)
        check("ANSWERS: no store, no line", answers_line(0, f.name + ".missing"), None)
        check("AGENTS: a placeholder prompt is no role", role("<prompt>"), "(no prompt)")
        check("AGENTS: a session outside ~ is not counted",
              (_outside_home("/tmp/x/build/wt/be"), _outside_home(HOME), _outside_home(HOME + "/Stocks")),
              (True, False, False))
        _leads_selftest(check)
        _sto_selftest(check)
        check("argv: the cron line, --dry-run, --dry and selftest are the modes",
              [argv_error(a) for a in ([], ["--dry-run"], ["--dry"], ["selftest"])], ["", "", "", ""])
        check("argv: --selftest, --bogus, -n, selftest --dry, a bare word are errors (exit 2, nothing sent)",
              all(argv_error(a) for a in (["--selftest"], ["--bogus"], ["-n"], ["selftest", "--dry"], ["dry"])), True)
    finally:
        os.unlink(f.name)
    print("ALL PASS" if not fails else f"{len(fails)} FAILED")
    return 1 if fails else 0


def main():
    now = time.time()
    digest, n_held, n_pushed = build(now)
    if DRY:
        print(digest)
        if time.gmtime(now).tm_wday != 6:
            print("--- Sunday only (not in today's push):")
            print("\n".join(_safe_leads_lines(now)))
        return
    subprocess.run([f"{MC}/bin/notify.sh", "--tier", "actionable", "maintenance",
                    "Daily rollup", digest], timeout=30)
    print(f"{time.strftime('%F %T')} rollup: {n_held} held, {n_pushed} pushed — "
          f"{digest.splitlines()[1][:80]}")


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    _err = argv_error(sys.argv[1:])
    if _err:
        print(f"evening-digest.py: {_err} — nothing sent. {USAGE}", file=sys.stderr)
        sys.exit(2)
    if sys.argv[1:2] == ["selftest"]:
        sys.exit(selftest())
    main()
