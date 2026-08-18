#!/usr/bin/env python3
"""Monthly sweep pre-pass — local model, zero Claude tokens.
Runs 1st of month 09:35 UTC (quiet window), 85 min before the Claude sweep: digests a
month of logs + git history into state/sweep_brief.md so the expensive sweep session
reads a brief and verifies claims, instead of trawling everything raw."""
import json, os, subprocess, sys, time

HOME = os.path.expanduser("~")
sys.path.insert(0, f"{HOME}/maintenance/bin")
sys.path.insert(0, f"{HOME}/maintenance/dashboard")
from localllm import ask
import server

REPOS = ["Stocks", "clientco-db", "poker", "maintenance"]


def tail(path, n=120):
    try:
        with open(path, "rb") as f:
            f.seek(max(0, os.path.getsize(path) - 40000))
            return "\n".join(f.read().decode(errors="replace").splitlines()[-n:])
    except Exception:
        return ""


def main():
    sections = []
    # per-job log digests
    for j in server.cron_jobs():
        if not j.get("log") or not os.path.exists(j["log"]):
            continue
        t = tail(j["log"])
        if not t.strip():
            continue
        try:
            s = ask(f"Summarize this month of log output from the job '{j['desc']}' in 2-3 "
                    "sentences: run pattern, failures/retries (with dates if visible), current "
                    "state. Terse, factual, no advice.\n\n" + t[-8000:], num_predict=160)
        except Exception as e:
            s = f"(local model failed: {e})"
        sections.append(f"### {j['project']} · {j['desc']}\n{s}")
        time.sleep(5)   # thermal/pacing courtesy between inferences
    # git activity
    gits = []
    for r in REPOS:
        p = os.path.join(HOME, r)
        if os.path.isdir(os.path.join(p, ".git")):
            log = subprocess.run(["git", "-C", p, "log", "--since=32 days ago", "--oneline"],
                                 capture_output=True, text=True, timeout=10).stdout
            gits.append(f"{r}: {len(log.splitlines())} commits\n{log[:2500]}")
    try:
        gsum = ask("Summarize the month's development across these repos in 4-6 sentences "
                   "(what shipped, per project):\n\n" + "\n\n".join(gits), num_predict=300)
    except Exception as e:
        gsum = f"(local model failed: {e})"
    out = (f"# Sweep pre-pass brief — {time.strftime('%Y-%m-%d')}\n"
           "_Generated locally (qwen). The Claude sweep should VERIFY these claims against "
           "live state, not trust them blindly._\n\n## Month's development\n" + gsum +
           "\n\n## Job-by-job log digest\n" + "\n\n".join(sections))
    os.makedirs(f"{HOME}/maintenance/state", exist_ok=True)
    open(f"{HOME}/maintenance/state/sweep_brief.md", "w").write(out)
    print(f"{time.strftime('%F %T')} brief written: {len(sections)} job sections")


if __name__ == "__main__":
    main()
