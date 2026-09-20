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

BUS = f"{HOME}/memos"
# memo-process.py runs daily at 08:20Z and takes 3 memos per run, one per inbox. A memo that
# has survived seven of those passes is not waiting its turn — it is stuck.
STUCK_H = 24 * 7
LEDGER_STUCK_D = 14
stale, now = [], time.time()

for proj in sorted(os.listdir(f"{BUS}/inbox")) if os.path.isdir(f"{BUS}/inbox") else []:
    d = f"{BUS}/inbox/{proj}"
    for f in sorted(os.listdir(d)):
        p = os.path.join(d, f)
        age_h = (now - os.path.getmtime(p)) / 3600
        if age_h > STUCK_H:
            head = open(p, errors="replace").read()[:600]
            try:
                one = ask(f"One line (<=15 words): what does this memo ask for?\n\n{head}",
                          num_predict=40)
            except Exception:
                one = f.replace(".md", "")
            stale.append(f"inbox/{proj}: '{f}' unprocessed {int(age_h)}h — {one}")

try:
    for line in open(f"{BUS}/LEDGER.md", errors="replace"):
        m = re.match(r"\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([^|]+)\|[^|]*\|[^|]*\|\s*([^|]+)\|", line)
        if m:
            date, memo, status = m.group(1), m.group(2).strip(), m.group(3).strip().lower()
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
