#!/usr/bin/env python3
"""Memo-bus triage — daily 10:35 UTC (6:35am ET), local model, zero Claude tokens.

THE BACKSTOP, NOT THE WORKER (rescoped 2026-09-20). It used to nudge on any memo older
than 24h, and did so 14 mornings in a row while the backlog sat unchanged — the exact
"a signal nobody acts on isn't a signal" failure. memo-process.py now works every
project's inbox daily, so a memo older than one processing cycle means the processor
could not finish it, which is a different and rarer thing worth saying out loud.

So: silent under STUCK_H. Past it, the memo is genuinely stuck (needs-david, a session
that died, an inbox with no project folder) and the push is `actionable` rather than
digest — it reaches the phone once instead of riding the rollup forever."""
import os, re, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
from localllm import ask
if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
    print((__doc__ or "").strip() or "usage: see the header of " + __file__)
    sys.exit(0)
if __name__ == "__main__" and sys.argv[1:] and not any(a in ("-h", "--help") for a in sys.argv[1:]):
    # no modes and no dry run: any argument exits 2 and runs nothing (2026-10-03 — until then an
    # argument nobody parsed, `--dry` included, ran the live job)
    print(f"memo-triage.py: unknown argument(s) {' '.join(sys.argv[1:])!r} — nothing run. "
          "usage: memo-triage.py | --help  (it takes no arguments and has no dry mode)", file=sys.stderr)
    sys.exit(2)

BUS = f"{HOME}/memos"
# memo-process.py runs daily at 08:20Z: one session per inbox (a project lead takes its whole
# inbox, Stocks one memo), up to 6 inboxes a run (MAX_SESSIONS, ef51486). A memo that has survived
# seven of those passes is not waiting its turn — it is stuck. One parked on purpose with
# `waiting-until <date>` (a lead's gate, at most 30 days, lead.memo_state) is not, until that day:
# it is left out (2026-10-02). Nor is one parked `needs-david` (2026-10-04): it already stands on
# Needs attention, where David answers it, and a push about it restates an ask ("ask once, never
# restate"); stocks/learn-valuation-test-and-global-tnum would have paged at 10-05 10:35Z for it.
# Since 2026-10-04 the COO's daily ops build (bin/coo.py, 11:57Z) is the working signal: it hands a
# memo eligible for 48 h or an ask owed for 14 days to its OWNING lead as a judgment trigger. This
# script stays the phone backstop for what even that leaves behind.
STUCK_H = 24 * 7
SKIP_STATES = ("waiting", "needs-david")
# Rows whose target is paused for every lead are not stuck, they are held: Stocks runs only through
# its PM's own harness while David keeps it closed (David 2026-10-02: "don't touch stock for now";
# lead_base.md class `stocks`). The COO's ops map still counts them.
PAUSED_TARGETS = {"stocks"}
try:
    import lead as _lead
    _rows = _lead.ledger_rows()
except Exception:                               # the backstop never depends on the lead code
    _lead, _rows = None, []
LEDGER_STUCK_D = 14
stale, now = [], time.time()

for proj in sorted(os.listdir(f"{BUS}/inbox")) if os.path.isdir(f"{BUS}/inbox") else []:
    d = f"{BUS}/inbox/{proj}"
    for f in sorted(os.listdir(d)):
        p = os.path.join(d, f)
        age_h = (now - os.path.getmtime(p)) / 3600
        if age_h > STUCK_H:
            try:
                if _lead and _lead.memo_state(proj, f, _rows, None, os.path.getmtime(p))[0] in SKIP_STATES:
                    continue
            except Exception:
                pass
            head = open(p, errors="replace").read()[:600]
            try:
                one = ask(f"One line (<=15 words): what does this memo ask for?\n\n{head}",
                          num_predict=40)
            except Exception:
                one = f.replace(".md", "")
            stale.append(f"inbox/{proj}: '{f}' unprocessed {int(age_h)}h — {one}")

try:
    for line in open(f"{BUS}/LEDGER.md", errors="replace"):
        m = re.match(r"\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([^|]+)\|[^|]*\|\s*([^|]*)\|\s*([^|]+)\|", line)
        if m:
            # markdown marks stripped first: "**accepted**" read as not-accepted, so a row accepted
            # 27 days (research-session-wall-clock-cap) was invisible here (2026-10-04)
            date, memo = m.group(1), m.group(2).strip()
            target, status = m.group(3).strip().lower(), re.sub(r"[*_`]", "", m.group(4)).strip().lower()
            if target in PAUSED_TARGETS:
                continue
            if ("proposed" in status or status.startswith("accepted")) and "implemented" not in status:
                age_d = (now - time.mktime(time.strptime(date, "%Y-%m-%d"))) / 86400
                if age_d > LEDGER_STUCK_D:
                    stale.append(f"ledger: '{memo}' still {status.split()[0]} after {int(age_d)}d")
except Exception:
    pass

if stale:
    subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "--tier", "actionable", "maintenance",
                    "Memo bus is STUCK", "; ".join(stale[:4])], timeout=30)
    print(f"{time.strftime('%F %T')} nudged: {len(stale)} stale")
else:
    print(f"{time.strftime('%F %T')} bus clean")
