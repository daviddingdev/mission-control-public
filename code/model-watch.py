#!/usr/bin/env python3
"""
Open-model release watcher — code + local model, zero Claude tokens.

Weekly cron. Watches notable orgs on the Hugging Face API for new text-gen
model releases, dedupes against state, has qwen (ollama) write a two-line
"is this worth pulling for the Spark?" assessment per release, then pushes a
digest via notify.sh (maintenance channel) and appends reports/model-watch.md.

Rationale (David, 2026-08-11): the Spark's local-model layer should keep
itself current — better open models directly upgrade the summarize/search
assist work — and the WATCHING itself must not cost Claude tokens.

CLI: model-watch.py run | model-watch.py run --days 30   (first run: use --days 30)
"""
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOME = Path.home()
STATE = HOME / "maintenance/state/model_watch.json"
REPORT = HOME / "maintenance/reports/model-watch.md"
ORGS = ["Qwen", "meta-llama", "mistralai", "deepseek-ai", "openai", "google",
        "microsoft", "allenai", "nvidia", "moonshotai", "zai-org"]
sys.path.insert(0, str(HOME / "maintenance/bin"))
import models  # noqa: E402  — the local-model registry (config/models.json)
import gpu     # noqa: E402  — the GPU queue (config/gpu.json)

OLLAMA = models.chat_url()
LOCAL_MODEL = models.require("dense", job="model-watch")

# GPU reality on the DGX Spark (~99-120GB usable for weights): flag things we could run.
# The current driver is interpolated, not typed: this job's whole purpose is to find the
# model that replaces it, and a hardcoded name here would go stale the day it succeeds.
ASSESS = f"""You advise on open-weight LLMs for a single NVIDIA DGX Spark (~100GB VRAM-equivalent,
runs quantized models via ollama; current daily driver: {LOCAL_MODEL}). For the model release
below, output EXACTLY two lines:
VERDICT: PULL-CANDIDATE | WATCH | SKIP
WHY: <one concrete sentence — size/fit, claimed strengths, what Spark job it would improve (news
summarize/rank, filing navigation notes, log triage), or why it's irrelevant (too big, vision-only,
base-not-instruct, dedup of existing)>"""


def hf(url):
    req = urllib.request.Request(url, headers={"User-Agent": "spark-model-watch/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def ask_local(text):
    body = json.dumps({"model": LOCAL_MODEL, "think": False, "stream": False,
                       "messages": [{"role": "system", "content": ASSESS},
                                    {"role": "user", "content": text}],
                       "options": {"num_predict": 120, "temperature": 0.1}}).encode()
    req = urllib.request.Request(OLLAMA, data=body, headers={"Content-Type": "application/json"})
    with gpu.slot(job="model watch", model=LOCAL_MODEL):
        with urllib.request.urlopen(req, timeout=180) as r:
            return json.loads(r.read())["message"]["content"].strip()


def run(days=8):
    STATE.parent.mkdir(exist_ok=True)
    REPORT.parent.mkdir(exist_ok=True)
    seen = set(json.loads(STATE.read_text())["seen"]) if STATE.exists() else set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    fresh = []
    for org in ORGS:
        try:
            models = hf(f"https://huggingface.co/api/models?author={org}&sort=createdAt&direction=-1&limit=12")
        except Exception:
            continue
        for m in models:
            mid = m.get("id", "")
            created = m.get("createdAt", "")
            tags = m.get("tags", [])
            if not mid or mid in seen:
                continue
            try:
                if datetime.fromisoformat(created.replace("Z", "+00:00")) < cutoff:
                    continue
            except Exception:
                continue
            if not any(t in tags for t in ("text-generation", "text2text-generation", "conversational")):
                continue
            fresh.append({"id": mid, "created": created[:10], "likes": m.get("likes", 0),
                          "downloads": m.get("downloads", 0)})
        time.sleep(0.3)

    if not fresh:
        print("model-watch: nothing new")
        return
    lines, notable = [], 0
    for f in sorted(fresh, key=lambda x: -x["likes"]):
        seen.add(f["id"])
        try:
            v = ask_local(json.dumps(f))
        except Exception as e:
            v = f"VERDICT: WATCH\nWHY: (local assess failed: {str(e)[:40]})"
        f["assessment"] = v
        if "PULL-CANDIDATE" in v:
            notable += 1
        lines.append(f"- **{f['id']}** ({f['created']}, ♥{f['likes']}) — {v.replace(chr(10), ' · ')}")
    STATE.write_text(json.dumps({"seen": sorted(seen), "updated": datetime.now(timezone.utc).isoformat()}, indent=1))
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(REPORT, "a") as fh:
        fh.write(f"\n## {stamp} — {len(fresh)} new, {notable} pull-candidate(s)\n" + "\n".join(lines) + "\n")
    msg = f"{len(fresh)} new open-model release(s), {notable} pull-candidate(s). reports/model-watch.md"
    subprocess.run([str(HOME / "maintenance/bin/notify.sh"), "maintenance", "Open models", msg],
                   capture_output=True, timeout=30)
    print(f"model-watch: {len(fresh)} new, {notable} pull-candidates -> report + ntfy")


if __name__ == "__main__":
    days = 30 if "--days" in sys.argv and "30" in sys.argv else 8
    if sys.argv[1:2] == ["run"]:
        run(days)
    else:
        sys.exit("usage: model-watch.py run [--days 30]")
