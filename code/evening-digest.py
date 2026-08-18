#!/usr/bin/env python3
"""Evening digest — 23:00 UTC (19:00 ET): one push summarizing the whole day.
Local model, zero Claude tokens. Reads the notification ledger + daily-log narrative."""
import json, os, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
from localllm import ask

today = time.strftime("%Y-%m-%d")
notifs = []
try:
    for line in open(f"{HOME}/maintenance/state/notifications.jsonl"):
        m = json.loads(line)
        if time.strftime("%Y-%m-%d", time.localtime(m["time"])) == today:
            notifs.append(f"[{m['channel']}] {m['title']}: {m['message'][:100]}")
except Exception:
    pass
narrative = ""
try:
    for line in open(f"{HOME}/maintenance/state/dailylog.jsonl"):
        r = json.loads(line)
        if r.get("date") == today:
            narrative = r.get("narrative", "")
except Exception:
    pass

prompt = (
    "Write tonight's 2-4 sentence evening digest for the owner of a personal automation "
    "server. Inputs: today's push notifications and the morning ops narrative. Focus on "
    "what he'd want to know before ending the day; if it was quiet, say so in one sentence. "
    "No preamble.\n\n"
    f"MORNING NARRATIVE: {narrative or '(none)'}\n\n"
    "TODAY'S NOTIFICATIONS:\n" + ("\n".join(notifs) if notifs else "(none)"))
try:
    digest = ask(prompt, num_predict=220)
except Exception as e:
    digest = f"(local model unavailable: {e})"
if digest:
    subprocess.run([f"{HOME}/maintenance/bin/notify.sh", "maintenance",
                    "Evening digest", digest], timeout=30)
    print(f"{time.strftime('%F %T')} sent: {digest[:120]}")
