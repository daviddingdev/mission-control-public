#!/usr/bin/env python3
"""Memo-bus triage — daily 10:35 UTC (6:35am ET), local model, zero Claude tokens.
Nudges David's maintenance channel when inbox memos sit unprocessed >24h or ledger
rows linger in proposed/accepted. Quiet when the bus is clean (the usual case)."""
import os, re, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
from localllm import ask

BUS = f"{HOME}/memos"
stale, now = [], time.time()

for proj in sorted(os.listdir(f"{BUS}/inbox")) if os.path.isdir(f"{BUS}/inbox") else []:
    d = f"{BUS}/inbox/{proj}"
    for f in sorted(os.listdir(d)):
        p = os.path.join(d, f)
        age_h = (now - os.path.getmtime(p)) / 3600
        if age_h > 24:
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
                if age_d > 2:
                    stale.append(f"ledger: '{memo}' still {status.split()[0]} after {int(age_d)}d")
except Exception:
    pass

if stale:
    subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "maintenance",
                    "Memo bus needs attention", "; ".join(stale[:4])], timeout=30)
    print(f"{time.strftime('%F %T')} nudged: {len(stale)} stale")
else:
    print(f"{time.strftime('%F %T')} bus clean")
