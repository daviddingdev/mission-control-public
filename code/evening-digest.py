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

Since the 2026-08-29 tiering (config/notify_policy.json) this is the delivery mechanism for
everything the policy HELD during the day; digest-tier notifications never reach the phone
on their own. It sends with --tier actionable so the policy can't hold the rollup itself,
and the maintenance channel's 1/day cap makes this the only non-critical push there.
Critical pushes bypass all of this and go immediately.

    evening-digest.py            build and push
    evening-digest.py --dry-run  print what would be pushed, send nothing
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


def build(now=None):
    now = now or time.time()
    day_start = now - (now % 86400)
    notes = rows(NOTIF, day_start)
    ends = [r for r in rows(SESSIONS, day_start)
            if r.get("headless") and r.get("event") == "SessionEnd"]

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
    return "\n".join(body), len(held), len(pushed)


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
    main()
