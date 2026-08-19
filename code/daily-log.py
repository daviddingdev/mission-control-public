#!/usr/bin/env python3
"""Daily automated-jobs summary — runs 12:05 UTC (end of quiet window).
Snapshots every cron job's state + writes a plain-English narrative via the LOCAL
model (ollama — zero Claude tokens; the token-saving pilot). Appends one record/day
to state/dailylog.jsonl; the dashboard's Daily log tab renders it."""
import json, os, sys, time, urllib.request

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/dashboard")
import server  # reuse the crontab parser — one source of truth

sys.path.insert(0, f"{HOME}/maintenance/bin")
import models  # noqa: E402  — the local-model registry (config/models.json)
import gpu     # noqa: E402  — the GPU queue (config/gpu.json)

# No tag here: the "dense" role resolves to the best model the box actually has, and the
# pre-check exits 75 before we snapshot anything if ollama can't serve it. Whatever it
# resolves to is hybrid-thinking qwen today — MUST pass think:false or it burns the whole
# budget in <think> (verified 2026-08-08); the call below does.
OLLAMA = models.chat_url()
MODEL = os.environ.get("DAILYLOG_MODEL") or models.require("dense", job="daily-log")


def previous_runs(path, today_date):
    """{(project, desc): last_run} from the most recent record before today.
    Lets the narrative tell a job's CURRENT outcome from one it already reported."""
    if not os.path.exists(path):
        return {}
    prior = [json.loads(l) for l in open(path, errors="replace").read().splitlines()
             if l.strip() and json.loads(l).get("date") != today_date]
    if not prior:
        return {}
    return {(j.get("project"), j.get("desc")): j.get("last_run")
            for j in prior[-1].get("jobs", [])}


def narrative(jobs, prev):
    lines = []
    for j in jobs:
        age = f"{j['age_min']}m ago" if j.get("age_min") is not None else "no log"
        exp = f"{j['expect_min']}m" if j.get("expect_min") else "n/a"
        # A weekly job's log tail sits unchanged for six days. Without this flag the
        # model re-reports the same failure every night, long after it was fixed —
        # the log only refreshes when the job next runs.
        key = (j.get("project"), j.get("desc"))
        fresh = j.get("last_run") is not None and (key not in prev or j["last_run"] != prev[key])
        lines.append(f"- [{j['project']}] {j['desc'][:70]} | runs: {j['freq']} (expected every ~{exp}) "
                     f"| last ran: {age} | ran since yesterday's report: {'yes' if fresh else 'no'} "
                     f"| last line: {j['tail'][:100]}")
    prompt = (
        "You are the nightly ops summarizer for a personal server. Below is every scheduled job "
        "with its expected cadence. A job is ONLY late if 'last ran' is much older than its "
        "expected interval — a monthly job that ran 20 days ago is fine. 'no log' on boot-only "
        "or brand-new jobs is fine. Log lines saying FAIL/error matter. "
        "CRITICAL: 'ran since yesterday's report: no' means the log line is OLD and was already "
        "reported on a previous day — a job's last line only changes when it next runs. Never put "
        "such a job in 'Attention needed', even if its last line is an error; that error is history, "
        "already seen, and possibly already fixed. Only flag a job as needing attention if it ran "
        "since yesterday's report and failed, or if it is genuinely overdue against its cadence. "
        "Write 3-6 plain-English "
        "sentences: what ran fine today, anything genuinely late or failing (be specific about "
        "why), and end with 'Attention needed: ...' or 'Attention needed: none'. "
        "No preamble, no markdown.\n\n" + "\n".join(lines))
    req = urllib.request.Request(OLLAMA, json.dumps({
        "model": MODEL, "messages": [{"role": "user", "content": prompt}],
        "think": False, "stream": False,
        "options": {"num_predict": 350, "temperature": 0.2}}).encode(),
        {"Content-Type": "application/json"})
    t0 = time.time()
    with gpu.slot(job="daily log", model=MODEL), urllib.request.urlopen(req, timeout=900) as r:
        d = json.loads(r.read().decode())
    gpu.record_usage(job="daily log", model=MODEL, prompt_tokens=d.get("prompt_eval_count"),
                     output_tokens=d.get("eval_count"), seconds=time.time() - t0)
    import re as _re
    txt = _re.sub(r"<think>.*?</think>", "", d.get("message", {}).get("content", ""),
                  flags=_re.S).strip()
    return txt, d.get("eval_count", 0)


def main():
    jobs = server.cron_jobs()
    path = f"{HOME}/maintenance/state/dailylog.jsonl"
    rec = {"date": time.strftime("%Y-%m-%d"), "ts": int(time.time()),
           "jobs": [{k: j[k] for k in ("project", "desc", "freq", "age_min", "last_run", "tail", "ai")}
                    for j in jobs],
           "narrative": "", "model": None}
    prev = previous_runs(path, rec["date"])
    try:
        rec["narrative"], toks = narrative(jobs, prev)
        rec["model"] = MODEL
        print(f"narrative via {MODEL} ({toks} tok)")
    except Exception as e:
        rec["narrative"] = f"(local model unavailable: {e})"
        print(f"ollama failed: {e}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # one record per day — replace today's if re-run
    keep = []
    if os.path.exists(path):
        keep = [l for l in open(path, errors="replace").read().splitlines()
                if l.strip() and json.loads(l).get("date") != rec["date"]]
    keep.append(json.dumps(rec))
    open(path, "w").write("\n".join(keep) + "\n")
    print(f"dailylog: {rec['date']} recorded, {len(jobs)} jobs")


if __name__ == "__main__":
    main()
