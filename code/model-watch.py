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

CLI: model-watch.py run [--days N] [--dry|--dry-run]   (first run: use --days 30)
     model-watch.py selftest
`--dry` reads Hugging Face and prints what it found — no local model, no state, no report,
no push. Any other argument exits 2 without running.

state/model_watch.json is written on EVERY run, "nothing new" included (2026-10-04): a
weekly job that only wrote when it found something read as stale for weeks, and a fetch
error was swallowed, so "nothing new" and "Hugging Face unreachable" looked the same. The
file carries checked_at, the orgs checked, and errors (count + list); every org failing
exits 1.
"""
import json
import os
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
    t0 = time.time()
    with gpu.slot(job="model watch", model=LOCAL_MODEL) as g:
        t_body = time.time()                 # the grant, if the slot yields no Grant
        with urllib.request.urlopen(req, timeout=180) as r:
            d = json.loads(r.read())
    t_grant = getattr(g, "t_grant", None) or t_body
    hold = time.time() - t_grant             # the inference, never the queue wait
    gpu.record_usage(job="model watch", model=LOCAL_MODEL, prompt_tokens=d.get("prompt_eval_count"),
                     output_tokens=d.get("eval_count"), seconds=hold,
                     wait_s=max(0.0, t_grant - t0), hold_s=hold)
    return d["message"]["content"].strip()


def _state(seen, checked, errors, fresh_n, days, prev=None):
    """The state row written on every run — nothing-new and all-failed included."""
    now = datetime.now(timezone.utc).isoformat()
    return {"seen": sorted(seen),
            "updated": now if fresh_n else (prev or {}).get("updated", now),  # last time seen grew
            "checked_at": now, "days": days, "orgs_checked": checked,
            "new": fresh_n, "errors": len(errors), "error_list": errors[:20]}


def _write_state(row):
    tmp = STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(row, indent=1))
    os.replace(tmp, STATE)


def scan(seen, days, fetch=None):
    """-> (fresh, orgs_checked, errors). A fetch that fails is COUNTED, never skipped silently."""
    fetch = fetch or hf
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    fresh, checked, errors = [], [], []
    for org in ORGS:
        try:
            models = fetch(f"https://huggingface.co/api/models?author={org}&sort=createdAt&direction=-1&limit=12")
        except Exception as e:
            errors.append({"org": org, "error": f"{type(e).__name__}: {e}"[:160]})
            continue
        checked.append(org)
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
                continue                     # no/odd createdAt: not a dated release, not an error
            if not any(t in tags for t in ("text-generation", "text2text-generation", "conversational")):
                continue
            fresh.append({"id": mid, "created": created[:10], "likes": m.get("likes", 0),
                          "downloads": m.get("downloads", 0)})
        time.sleep(0.3)
    return fresh, checked, errors


def run(days=8, dry=False):
    prev = json.loads(STATE.read_text()) if STATE.exists() else {}
    seen = set(prev.get("seen", []))
    fresh, checked, errors = scan(seen, days)
    tail = f" · {len(errors)} fetch error(s): " + ", ".join(e["org"] for e in errors) if errors else ""
    if dry:
        print(f"model-watch --dry: {len(checked)}/{len(ORGS)} orgs read, {len(fresh)} new{tail}")
        for f in fresh:
            print(f"  would assess {f['id']} ({f['created']}, likes {f['likes']})")
        print("  (no local model, no state, no report, no push)")
        return 1 if not checked else 0
    STATE.parent.mkdir(exist_ok=True)
    REPORT.parent.mkdir(exist_ok=True)

    if not fresh:
        _write_state(_state(seen, checked, errors, 0, days, prev))
        print(f"model-watch: nothing new ({len(checked)}/{len(ORGS)} orgs read){tail}")
        return 1 if not checked else 0       # every org failed: "nothing new" would be a lie
    lines, notable = [], 0
    for f in sorted(fresh, key=lambda x: -x["likes"]):
        seen.add(f["id"])
        try:
            v = ask_local(json.dumps(f))
        except Exception as e:
            v = f"VERDICT: WATCH\nWHY: (local assess failed: {str(e)[:40]})"
            errors.append({"model": f["id"], "error": f"assess: {type(e).__name__}: {e}"[:160]})
        f["assessment"] = v
        if "PULL-CANDIDATE" in v:
            notable += 1
        lines.append(f"- **{f['id']}** ({f['created']}, ♥{f['likes']}) — {v.replace(chr(10), ' · ')}")
    _write_state(_state(seen, checked, errors, len(fresh), days, prev))
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(REPORT, "a") as fh:
        fh.write(f"\n## {stamp} — {len(fresh)} new, {notable} pull-candidate(s)\n" + "\n".join(lines) + "\n")
    msg = f"{len(fresh)} new open-model release(s), {notable} pull-candidate(s). reports/model-watch.md"
    subprocess.run([str(HOME / "maintenance/bin/notify.sh"), "maintenance", "Open models", msg],
                   capture_output=True, timeout=30)
    print(f"model-watch: {len(fresh)} new, {notable} pull-candidates -> report + ntfy{tail}")
    return 0


def _selftest():
    """Fakes only: no Hugging Face, no local model, no push, and the live state file is
    never touched (STATE is pointed at a temp file)."""
    import tempfile
    global STATE
    bad, cases = 0, []
    def fetch(url):
        if "author=openai" in url or "author=google" in url:
            raise OSError("HTTP 503")
        return [{"id": "old/one", "createdAt": "2020-01-01T00:00:00Z", "tags": ["text-generation"]}]
    fresh, checked, errors = scan(set(), 8, fetch=fetch)
    cases += [("fetch errors are counted, not swallowed", len(errors), 2),
              ("the other orgs still count as checked", len(checked), len(ORGS) - 2),
              ("nothing new inside the window", len(fresh), 0)]
    real, d = STATE, tempfile.mkdtemp()
    try:
        STATE = Path(d) / "model_watch.json"
        _write_state(_state({"a/b"}, checked, errors, 0, 8, {"updated": "2026-09-21"}))
        row = json.loads(STATE.read_text())
        cases += [("nothing-new still writes checked_at", bool(row.get("checked_at")), True),
                  ("it records the orgs checked", row.get("orgs_checked"), checked),
                  ("and the error count", row.get("errors"), 2),
                  ("'updated' stays the last time seen grew", row.get("updated"), "2026-09-21"),
                  ("seen is kept", row.get("seen"), ["a/b"])]
    finally:
        STATE = real
        for f in Path(d).iterdir():
            f.unlink()
        os.rmdir(d)
    cases += [("argv: run", _args(["run"]), (8, False)),
              ("argv: run --days 30 --dry-run", _args(["run", "--days", "30", "--dry-run"]), (30, True)),
              ("argv: unknown flag is refused", _args(["run", "--selftest"]), None),
              ("argv: no command is refused", _args([]), None)]
    for name, got, want in cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<44} -> {got}")
    print(f"{len(cases) - bad}/{len(cases)} passed")
    return bad


def _args(argv):
    """-> (days, dry) for `run [--days N] [--dry|--dry-run]`, or None for anything else."""
    if argv[:1] != ["run"]:
        return None
    days, dry, rest = 8, False, argv[1:]
    while rest:
        a = rest.pop(0)
        if a in ("--dry", "--dry-run"):
            dry = True
        elif a == "--days" and rest and rest[0].isdigit() and int(rest[0]) > 0:
            days = int(rest.pop(0))
        else:
            return None
    return days, dry


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    if sys.argv[1:] == ["selftest"]:
        sys.exit(1 if _selftest() else 0)
    parsed = _args(sys.argv[1:])
    if parsed is None:                       # an unknown argument never runs the live job
        print("usage: model-watch.py run [--days N] [--dry|--dry-run] | selftest", file=sys.stderr)
        sys.exit(2)
    sys.exit(run(*parsed) or 0)
