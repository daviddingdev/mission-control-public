#!/usr/bin/env python3
"""Daily rollup — 23:00 UTC (19:00 ET). The ONE Mission Control push of the day.

Zero tokens of any kind since 2026-09-03: David — "the end of day should be more crisp".
The local-model prose it used to send ("All other scheduled jobs completed successfully,
with no other new issues detected") was filler around three facts. Now it is the facts,
one line each, built straight from the ledgers:

    Thu Sep 3
    BROKEN  ClientCo refresh FAILED - snapshot.py x2 · VP night sweep · Sentinel x5
    AGENTS  Stocks 8 — VP 5m · Analyst 10m · Signals 8m · Bug Hunter 16m · Numbers 133m ...
    PAGED   21 — stocks 12 · alerts 8 · money 1
    HELD    33 — local-model runs 30 · Memo bus needs attention · Today at HBS · Canvas
    ANSWERS 2 done · 1 back to you — VP night sweep failed: memo filed · …   (only on a day with any)

Since the 2026-08-29 tiering (config/notify_policy.json) this is the delivery mechanism for
everything the policy HELD during the day; digest-tier notifications never reach the phone
on their own. It sends with --tier actionable so the policy can't hold the rollup itself,
and the maintenance channel's 1/day cap makes this the only non-critical push there.
Critical pushes bypass all of this and go immediately.

ANSWERS (2026-09-24): what became of the answers David gave on the dashboard's "Needs attention"
list — carried out, or handed back to him — read from state/decisions.jsonl, the one place the
daily check and the janitor record outcomes. Nothing else told him when an answer had been acted
on. The line is left out on a day with none, so a quiet day stays four lines.

    evening-digest.py            build and push
    evening-digest.py --dry-run  print what would be pushed, send nothing
    evening-digest.py selftest   the ANSWERS line on a throwaway store; sends nothing
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
DRY = "--dry-run" in sys.argv


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
    try:
        with open(path, errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if (isinstance(e, dict) and e.get("id") and e.get("status") in ("done", "failed")
                        and e.get("by") != "david" and (e.get("at") or 0) >= since):
                    last.pop(e["id"], None)
                    last[e["id"]] = e
    except OSError:
        return None
    if not last:
        return None
    evs = list(last.values())
    done = sum(1 for e in evs if e["status"] == "done")
    head = " · ".join(x for x in (f"{done} done" if done else "",
                                  f"{len(evs) - done} back to you" if len(evs) > done else "") if x)
    bits = [f"{_short(e.get('title') or e.get('key'), 40)}: {_short(e.get('result') or e['status'], 60)}"
            for e in sorted(evs, key=lambda e: e["status"] == "done")[:3]]
    return f"{head} — " + " · ".join(bits) + (" · …" if len(evs) > 3 else "")


def build(now=None):
    now = now or time.time()
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
        parts = [f"{role(r.get('task'))} {fmt_dur(r.get('duration_s'))}" for r in runs]
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

    body = [time.strftime("%a %b %-d", time.gmtime(now)),
            f"BROKEN  {broken_line}",
            f"AGENTS  {agent_lines[0]}"]
    body += [f"        {l}" for l in agent_lines[1:]]
    body += [f"PAGED   {paged_line}", f"HELD    {held_line}"]
    ans = answers_line(day_start)
    if ans:
        body.append(f"ANSWERS {ans}")
    return "\n".join(body), len(held), len(pushed)


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
    finally:
        os.unlink(f.name)
    print("ALL PASS" if not fails else f"{len(fails)} FAILED")
    return 1 if fails else 0


def main():
    digest, n_held, n_pushed = build()
    if DRY:
        print(digest)
        return
    subprocess.run([f"{MC}/bin/notify.sh", "--tier", "actionable", "maintenance",
                    "Daily rollup", digest], timeout=30)
    print(f"{time.strftime('%F %T')} rollup: {n_held} held, {n_pushed} pushed — "
          f"{digest.splitlines()[1][:80]}")


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    if sys.argv[1:2] == ["selftest"]:
        sys.exit(selftest())
    main()
