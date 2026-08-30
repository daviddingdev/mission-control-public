#!/usr/bin/env python3
"""Daily rollup — 23:00 UTC (19:00 ET). The ONE Mission Control push of the day.

Local model, zero Claude tokens. Since the 2026-08-29 tiering (config/notify_policy.json),
this is not just a summary — it is the delivery mechanism for everything the policy HELD
during the day. Digest-tier notifications never reach the phone on their own; they wait
here and go out consolidated, once.

It sends with --tier actionable so the policy can't hold the rollup itself, and the
maintenance channel's 1/day cap makes this the only non-critical push on that channel.
Critical pushes (spark down, auth down, backup failure) bypass all of this and go
immediately, which is the point of the tiering.
"""
import json, os, subprocess, sys, time
from collections import Counter

HOME = os.path.expanduser("~")
MC = f"{HOME}/maintenance"
sys.path.insert(0, f"{MC}/bin")
from localllm import ask

today = time.strftime("%Y-%m-%d")
day_start = time.time() - (time.time() % 86400)

held, pushed = [], []
try:
    for line in open(f"{MC}/state/notifications.jsonl", errors="replace"):
        try:
            m = json.loads(line)
        except Exception:
            continue
        if m.get("time", 0) < day_start:
            continue
        (pushed if m.get("pushed", True) else held).append(m)
except Exception:
    pass

narrative = ""
try:
    for line in open(f"{MC}/state/dailylog.jsonl", errors="replace"):
        r = json.loads(line)
        if r.get("date") == today:
            narrative = r.get("narrative", "")
except Exception:
    pass

# Held items are the bulk (routine session traffic, scans, journals). Roll them up by
# title rather than listing them — 70 lines of "Claude done — Stocks" teaches nothing.
rolled = Counter(m["title"] for m in held)
roll_lines = [f"{n}x {t}" if n > 1 else t for t, n in rolled.most_common(12)]

prompt = (
    "Write tonight's 2-4 sentence rollup for the owner of a personal automation server. "
    "He gets exactly one of these a day; anything genuinely broken already paged him "
    "separately, so do NOT manufacture alarm. Lead with what actually changed or completed "
    "today. Mention routine volume only as a count. If it was a quiet, normal day, say so "
    "plainly in one sentence. No preamble, no markdown.\n\n"
    f"MORNING OPS NARRATIVE: {narrative or '(none)'}\n\n"
    f"ALREADY PAGED HIM TODAY ({len(pushed)}):\n"
    + ("\n".join(f"[{m['channel']}] {m['title']}: {m['message'][:90]}" for m in pushed[:15]) or "(none)")
    + f"\n\nHELD FOR THIS ROLLUP ({len(held)} routine events):\n"
    + ("\n".join(roll_lines) or "(none)"))

try:
    digest = ask(prompt, num_predict=220)
except Exception as e:
    digest = f"(local model unavailable: {e})"

# The counts go out even if the model is down — the rollup must still carry the facts.
digest = f"{digest}\n\n— {len(held)} routine events held · {len(pushed)} paged today"

subprocess.run([f"{MC}/bin/notify.sh", "--tier", "actionable", "maintenance",
                "Daily rollup", digest], timeout=30)
print(f"{time.strftime('%F %T')} rollup: {len(held)} held, {len(pushed)} pushed — {digest[:100]}")
