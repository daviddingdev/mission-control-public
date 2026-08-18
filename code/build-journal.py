#!/usr/bin/env python3
"""Weekly build journal — Mondays 08:35 UTC (4:35am ET), local model, zero Claude tokens.
One push + one archive entry: what got built across all repos last week."""
import os, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
from localllm import ask

# derived, not declared: every project on the box is whatever has a git repo in ~,
# so a new project appears in the journal without anyone editing this line
REPOS = sorted(d for d in os.listdir(HOME)
               if os.path.isdir(os.path.join(HOME, d, ".git")) and not d.startswith("."))

logs = []
for r in REPOS:
    p = os.path.join(HOME, r)
    if os.path.isdir(os.path.join(p, ".git")):
        out = subprocess.run(["git", "-C", p, "log", "--since=7 days ago",
                              "--pretty=%ad %s", "--date=format:%a"],
                             capture_output=True, text=True, timeout=10).stdout.strip()
        if out:
            logs.append(f"== {r} ({len(out.splitlines())} commits) ==\n{out[:3000]}")

if not logs:
    print("quiet week, no commits — no journal")
    sys.exit(0)

try:
    journal = ask(
        "Write David's weekly build journal from these git logs: 4-7 sentences, plain "
        "English, grouped by project, focused on what CHANGED for him (features, fixes, "
        "automation), not commit mechanics. End with one sentence on the week's theme. "
        "No preamble.\n\n" + "\n\n".join(logs), num_predict=350)
except Exception as e:
    journal = f"(local model unavailable: {e})"

week = time.strftime("%G-W%V")
os.makedirs(f"{HOME}/maintenance/state/journal", exist_ok=True)
open(f"{HOME}/maintenance/state/journal/{week}.md", "w").write(
    f"# Build journal {week}\n\n{journal}\n")
subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "maintenance",
                f"Build journal {week}", journal[:900]], timeout=30)
print(f"{time.strftime('%F %T')} journal {week} written + pushed")
