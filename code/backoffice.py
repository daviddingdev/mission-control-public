#!/usr/bin/env python3
"""The daily back-office pass — Mission Control's janitor. Zero Claude tokens.

The dashboard's *machine* state (crons, ports, git, sessions, usage) has always been
derived live, so it can't rot. What rots is the written layer: the project roster, the
cron registry, the port tables, the "what's next" lines. Those were maintained by
whoever remembered, and audited once a month by the Claude sweep — so drift lived up to
30 days. On 2026-08-18 the box had 32 commits of poker-appstore work invisible to the
dashboard and two nightly Stocks jobs missing from the registry.

This job closes that loop daily:

    census   observe the box            -> state/census.json
    audit    coded rules over census    -> state/findings.json
             + the declared docs
    fix      repair the mechanical drift Mission Control owns, and commit it
    brief    local model reads each project's day of commits -> state/project_status.json
    run      all of the above, then push ONLY if something changed (state-change doctrine)

Design rules, in case a later session wants to extend it:
  * DERIVE, DON'T DECLARE. The roster is what's on disk; config/projects.json only carries
    hand-written flavour. A check that needs a human to keep a list up to date is a check
    that will be wrong in a month.
  * The local model NEVER gates. Every finding is produced by coded rules; the model only
    writes prose (what changed in a repo). A model outage costs the narrative, not the audit.
  * FIX WHAT WE OWN, FILE WHAT WE DON'T. Mission Control's own files get repaired in place;
    anything inside another project becomes a memo (~/memos/), per the reach rule in CLAUDE.md.
  * Findings are durable and fingerprinted, so "new" is a real event and a known-accepted
    deviation can be muted in config/backoffice_mute.json instead of nagging forever.

CLI: backoffice.py [census|audit|fix|brief|run|show] [--dry]
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

HOME = os.path.expanduser("~")
MC = os.path.join(HOME, "maintenance")
STATE = os.path.join(MC, "state")
CONFIG = os.path.join(MC, "config")
CENSUS = os.path.join(STATE, "census.json")
FINDINGS = os.path.join(STATE, "findings.json")
STATUS = os.path.join(STATE, "project_status.json")
HISTORY = os.path.join(STATE, "backoffice.jsonl")
MUTE = os.path.join(CONFIG, "backoffice_mute.json")

# ~ is the project parent (see ~/CLAUDE.md). These top-level folders are infrastructure,
# not projects: they have no CLAUDE.md contract and no lifecycle of their own.
NOT_PROJECTS = {"backups", "archive", "memos", "poker-data", "snap", "Desktop", "Documents",
                "Downloads", "Music", "Pictures", "Public", "Templates", "Videos", "vault"}

SEV = {"high": 0, "med": 1, "low": 2}


def now():
    return int(time.time())


def sh(cmd, cwd=None, timeout=20):
    try:
        r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""


def load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save(path, obj, indent=1):
    """indent=2 for the hand-edited config files — matching their existing style keeps the
    janitor's diffs readable instead of reformatting the whole file every time."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=indent)
        f.write("\n")
    os.replace(tmp, path)


# ---------------------------------------------------------------- census

def _cron_lines():
    """Parsed crontab. Keeps the preceding comment — it's the job's name in the registry."""
    out, comment = [], ""
    for raw in sh(["crontab", "-l"]).splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            comment = line.lstrip("# ").strip()
            continue
        if line.startswith("@"):
            sched, cmd = line.split(None, 1) if " " in line else (line, "")
        else:
            parts = line.split(None, 5)
            if len(parts) < 6:
                continue
            sched, cmd = " ".join(parts[:5]), parts[5]
        log = ""
        m = re.search(r">>\s*(\S+)", cmd)
        if m:
            log = m.group(1).replace("$HOME", HOME).replace("~", HOME)
            if not log.startswith("/"):
                # `cd ~/Stocks/_engine/agent && ... >> ../logs/x.log` — resolve it there
                cd = re.search(r"cd\s+(\S+)", cmd)
                base = cd.group(1).replace("~", HOME) if cd else HOME
                log = os.path.normpath(os.path.join(base, log))
        scripts = re.findall(r"[\w./-]+\.(?:py|sh)", cmd)
        out.append({"sched": sched, "cmd": cmd, "comment": comment, "log": log,
                    "scripts": [os.path.basename(s) for s in scripts],
                    "project": _project_of(cmd)})
        comment = ""
    return out


def _project_of(text):
    """Which project a cron command belongs to, by the path it cds into or runs from."""
    for name in sorted(_project_dirs(), key=len, reverse=True):
        if re.search(r"[~/]" + re.escape(name) + r"[/\s]", text) or f"/{name}/" in text:
            return name
    if ".claude/" in text:
        return "maintenance"
    return ""


def _project_dirs():
    out = []
    for name in sorted(os.listdir(HOME)):
        p = os.path.join(HOME, name)
        if name.startswith(".") or name in NOT_PROJECTS or not os.path.isdir(p):
            continue
        if os.path.isdir(os.path.join(p, ".git")) or os.path.exists(os.path.join(p, "CLAUDE.md")):
            out.append(name)
    return out


def expected_gap_h(sched):
    """Max plausible hours between runs, from the cron schedule. Used to catch a job that
    stopped producing output — the generalisation of the ERP-cycle lesson (a pipeline
    that fails silently is worse than one that fails loudly)."""
    if sched.startswith("@reboot"):
        return None
    parts = sched.split()
    if len(parts) != 5:
        return None
    minute, hour, dom, mon, dow = parts
    m = re.match(r"\*/(\d+)", minute)
    if m and hour == "*":
        return max(int(m.group(1)) / 60.0, 0.25)
    gap = 24.0
    if dom != "*":
        gap = 24 * 31
    elif dow != "*":
        gap = 24 * 7
    elif m:                       # */N minutes inside an hour window
        gap = 24.0
    # weekday-only jobs sit idle across the weekend
    if dow != "*" and re.match(r"^[1-5](-[1-5])?$", dow):
        gap = max(gap, 72.0)
    if hour != "*" and "-" in hour and dow in ("1-5", "*"):
        gap = max(gap, 72.0)
    return gap


def _git(repo):
    g = {"repo": os.path.isdir(os.path.join(repo, ".git"))}
    if not g["repo"]:
        return g
    last = sh(["git", "-C", repo, "log", "-1", "--format=%ct|%s"])
    if last and "|" in last:
        ct, subj = last.split("|", 1)
        g["last_commit"] = int(ct)
        g["subject"] = subj[:120]
    g["commits_24h"] = len(sh(["git", "-C", repo, "log", "--since=24 hours ago",
                               "--oneline"]).splitlines())
    g["commits_7d"] = len(sh(["git", "-C", repo, "log", "--since=7 days ago",
                              "--oneline"]).splitlines())
    g["dirty"] = len([l for l in sh(["git", "-C", repo, "status", "-s"]).splitlines() if l.strip()])
    g["branch"] = sh(["git", "-C", repo, "rev-parse", "--abbrev-ref", "HEAD"])
    unpushed = sh(["git", "-C", repo, "log", "@{u}..HEAD", "--oneline"])
    g["unpushed"] = len(unpushed.splitlines()) if unpushed else 0
    return g


def _backup_age_h(name):
    d = os.path.join(HOME, "backups", name.lower())
    if not os.path.isdir(d):
        return None
    newest = 0
    for root, _, files in os.walk(d):
        for f in files:
            try:
                newest = max(newest, os.path.getmtime(os.path.join(root, f)))
            except OSError:
                pass
    return round((time.time() - newest) / 3600, 1) if newest else None


def _listening_ports():
    out = set()
    for line in sh(["ss", "-tln"]).splitlines()[1:]:
        m = re.search(r":(\d+)\s", line)
        if m:
            out.add(int(m.group(1)))
    return sorted(out)


def _declared():
    """What the written layer claims — the other half of every drift check."""
    d = {}
    reg = ""
    try:
        reg = open(os.path.join(MC, "CRON_REGISTRY.md")).read()
    except Exception:
        pass
    d["registry_text"] = reg
    pcfg = load(os.path.join(CONFIG, "projects.json"), {}).get("projects", {})
    d["projects_cfg"] = list(pcfg)
    d["projects_match"] = [m for name, meta in pcfg.items()
                           for m in ([name] + list(meta.get("match", [])))]
    d["weights"] = [w.get("match", "") for w in
                    load(os.path.join(CONFIG, "job_weights.json"), {}).get("weights", [])]
    d["names"] = [n.get("match", "") for n in
                  load(os.path.join(CONFIG, "job_names.json"), {}).get("names", [])]
    infra = ""
    try:
        infra = open(os.path.join(HOME, "INFRASTRUCTURE.md")).read()
    except Exception:
        pass
    d["infra_ports"] = [int(x) for x in re.findall(r"^\|\s*(\d{2,5})\s*\|", infra, re.M)]
    srv = ""
    try:
        srv = open(os.path.join(MC, "dashboard/server.py")).read()
    except Exception:
        pass
    block = re.search(r"KNOWN_PORTS\s*=\s*\{(.*?)\}", srv, re.S)
    d["known_ports"] = [int(x) for x in re.findall(r"(\d{2,5}):", block.group(1))] if block else []
    hc = ""
    try:
        hc = open(os.path.join(MC, "bin/healthcheck.sh")).read()
    except Exception:
        pass
    d["healthcheck_ports"] = [int(x) for x in re.findall(r"^chk (\d+)", hc, re.M)]
    return d


def _ollama_callers():
    """Every file that talks to ollama, and whether it goes through the shared client.

    The GPU is one card shared by every project; a job that issues its own HTTP request
    skips the priority queue and lands in ollama's FIFO, where it can push a market-hours
    Stocks job behind a housekeeping sweep. Cheaper to catch the new caller than to debug
    the contention later.
    """
    out = []
    for name in _project_dirs():
        root = os.path.join(HOME, name)
        hits = sh(["grep", "-rl", "--include=*.py", "-e", "api/chat", "-e", ":11434",
                   "-e", "chat_url(", "-e", "api/generate", "-e", "api/embeddings",
                   root]).splitlines()
        for f in hits:
            if any(skip in f for skip in ("/.git/", "/node_modules/", "/worktrees/",
                                          "/archive/", "/backups/")):
                continue
            try:
                text = open(f, errors="replace").read()
            except OSError:
                continue
            managed = ("import gpu" in text or "from gpu import" in text
                       or "localllm" in text or os.path.basename(f) in ("gpu.py", "models.py"))
            out.append({"file": os.path.relpath(f, HOME), "project": name, "managed": managed})
    return out


def _public_state():
    """Freshness and safety of the public mirrors. A public repo that stops tracking the
    project is a dead resume entry; a public repo that starts leaking is much worse, so
    the leak scan runs daily rather than only at publish time."""
    out = {"repos": [], "leaks": []}
    try:
        cfg = load(os.path.join(CONFIG, "public_repos.json"), {})
        root = os.path.expanduser(cfg.get("root", "~/public"))
        for name, spec in cfg.get("projects", {}).items():
            d = os.path.join(root, spec["repo"])
            readme = os.path.join(d, "README.md")
            src = os.path.join(HOME, name)
            since = ""
            if os.path.exists(readme) and os.path.isdir(os.path.join(src, ".git")):
                stamp = dt_from(os.path.getmtime(readme))
                since = sh(["git", "-C", src, "log", f"--since={stamp}", "--oneline"])
            out["repos"].append({
                "project": name, "repo": spec["repo"], "publish": bool(spec.get("publish")),
                "built": os.path.exists(readme),
                "commits_since_readme": len(since.splitlines()) if since else 0})
        r = subprocess.run([sys.executable, os.path.join(MC, "bin/publish.py"), "scan"],
                           capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            out["leaks"] = [l for l in r.stdout.splitlines() if l and not l.startswith("   ")][:10]
    except Exception as e:
        out["error"] = str(e)[:120]
    return out


def dt_from(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


def census():
    c = {"at": now(), "projects": {}, "crons": _cron_lines(),
         "ports": _listening_ports(), "declared": _declared(), "logs": {}}
    for name in _project_dirs():
        repo = os.path.join(HOME, name)
        c["projects"][name] = {
            "path": repo,
            "git": _git(repo),
            "claude_md": os.path.exists(os.path.join(repo, "CLAUDE.md")),
            "gitignore": os.path.exists(os.path.join(repo, ".gitignore")),
            "backup_age_h": _backup_age_h(name),
        }
    for job in c["crons"]:
        p = job["log"]
        if p and p not in c["logs"]:
            try:
                st = os.stat(p)
                c["logs"][p] = {"age_h": round((time.time() - st.st_mtime) / 3600, 1),
                                "size": st.st_size}
            except OSError:
                c["logs"][p] = {"age_h": None, "size": None}
    c["ollama_callers"] = _ollama_callers()
    c["public"] = _public_state()
    try:
        r = subprocess.run([sys.executable, os.path.join(MC, "bin/models.py"), "check"],
                           capture_output=True, text=True, timeout=30)
        c["models_check"] = {"exit": r.returncode, "out": r.stdout.strip()[-400:]}
    except Exception as e:
        c["models_check"] = {"exit": -1, "out": str(e)[:200]}
    save(CENSUS, c)
    return c


# ---------------------------------------------------------------- audit

def _finding(out, kind, sev, title, detail, project="", fix="human"):
    fid = f"{kind}:{project}:{re.sub(r'[^a-z0-9]+', '-', title.lower())[:60]}"
    out.append({"id": fid, "kind": kind, "sev": sev, "title": title, "detail": detail,
                "project": project, "fix": fix})


def audit(c=None):
    c = c or load(CENSUS, None) or census()
    f = []
    d = c["declared"]

    # 1. a project on disk that Mission Control doesn't show. "If it isn't on the
    #    dashboard, it isn't real" cuts both ways.
    for name, p in c["projects"].items():
        if name not in d["projects_match"]:
            _finding(f, "roster-missing", "high", f"{name} is not on the dashboard",
                     f"{p['git'].get('commits_7d', 0)} commits in the last 7d, no card in "
                     "config/projects.json", name, fix="auto")

    # 2/3. cron <-> registry drift, both directions
    for job in c["crons"]:
        for s in job["scripts"]:
            if s and s not in d["registry_text"]:
                _finding(f, "registry-missing", "med", f"{s} runs but is not in CRON_REGISTRY",
                         f"`{job['sched']}` — {job['cmd'][:110]}", job["project"], fix="auto")
    live_scripts = {s for j in c["crons"] for s in j["scripts"]}
    for row in re.findall(r"^\|[^|]*\|\s*`?([\w./-]+\.(?:py|sh))", d["registry_text"], re.M):
        base = os.path.basename(row)
        if base not in live_scripts:
            _finding(f, "registry-orphan", "low", f"{base} is documented but no longer scheduled",
                     "CRON_REGISTRY has a row with no matching crontab line — move it to "
                     "Retired or drop it", "", fix="human")

    # 4. silent job: the log a cron writes hasn't been touched within its own cadence.
    for job in c["crons"]:
        gap = expected_gap_h(job["sched"])
        info = c["logs"].get(job["log"])
        if not gap or not job["log"] or not info or gap < 1:
            continue
        age = info.get("age_h")
        name = job["scripts"][0] if job["scripts"] else job["cmd"][:40]
        if age is None:
            # a job added mid-cycle hasn't reached its first run yet — that's not drift
            src = next((os.path.join(MC, "bin", n) for n in job["scripts"]
                        if os.path.exists(os.path.join(MC, "bin", n))), None)
            if src and (time.time() - os.path.getmtime(src)) / 3600 < gap:
                continue
            _finding(f, "log-missing", "med",
                     f"{name} ({job['sched']}) has never written its log",
                     f"expected at {job['log']}", job["project"])
        elif age > max(gap * 3, gap + 24):
            _finding(f, "job-silent", "high", f"{name} has not run in {age:.0f}h",
                     f"schedule `{job['sched']}` expects output every ~{gap:.0f}h — "
                     f"{job['log']}", job["project"])

    # 5. a listening port nobody declared (PROJECT_STANDARDS §4 wants it in three places)
    ignore_ports = {53, 631, 5355, 11000, 19999, 3493, 4317, 8125, 22, 41641, 5353}
    for port in c["ports"]:
        if port in ignore_ports or port < 1024 or port >= 32768:
            continue
        where = []
        if port not in d["known_ports"]:
            where.append("dashboard KNOWN_PORTS")
        if port not in d["infra_ports"]:
            where.append("INFRASTRUCTURE.md")
        if len(where) == 2:
            _finding(f, "port-undeclared", "med", f"port {port} is listening but undeclared",
                     "missing from " + " and ".join(where) + " (PROJECT_STANDARDS §4)")

    # 6. Day-1 checklist drift
    for name, p in c["projects"].items():
        if not p["claude_md"]:
            _finding(f, "no-claude-md", "med", f"{name} has no CLAUDE.md",
                     "Day-1 checklist item 1 — a project without one is out of compliance",
                     name, fix="memo")
        if p["git"].get("repo") and not p["gitignore"]:
            _finding(f, "no-gitignore", "low", f"{name} has no .gitignore",
                     "Day-1 checklist item 2 (whitelist .gitignore)", name, fix="memo")
        if not p["git"].get("repo"):
            _finding(f, "no-repo", "med", f"{name} is not a git repo",
                     "Day-1 checklist item 2", name, fix="memo")

    # 7. work that exists only on this box
    for name, p in c["projects"].items():
        g = p["git"]
        if g.get("unpushed", 0) >= 5:
            _finding(f, "unpushed", "med", f"{name} has {g['unpushed']} unpushed commits",
                     f"branch {g.get('branch', '?')} — the offsite copy is behind", name)
        if g.get("dirty", 0) >= 20:
            _finding(f, "dirty-tree", "low", f"{name} has {g['dirty']} uncommitted files",
                     "long-lived working tree — either commit it or add it to .gitignore", name)

    # 8. local-model roles
    if c.get("models_check", {}).get("exit", 0) >= 2:
        _finding(f, "model-role-dead", "high", "a local-model role cannot run",
                 c["models_check"]["out"][:200], "maintenance")
    elif c.get("models_check", {}).get("exit", 0) == 1:
        _finding(f, "model-fallback", "low", "a local-model role fell back to a weaker model",
                 c["models_check"]["out"][:200], "maintenance")

    # 9. an AI cron with no display name / no load weight is invisible in the dashboard's
    #    cost view — the Usage tab silently under-reports the box.
    for job in c["crons"]:
        if not re.search(r"(^|[|&;\s])claude\s+-", job["cmd"]):
            continue
        if not any(w and w in job["cmd"] for w in d["weights"]):
            _finding(f, "weight-missing", "low",
                     f"Claude job has no token weight: {(job['scripts'] or [job['cmd'][:30]])[0]}",
                     "config/job_weights.json — measure it from the Usage tab, then add it",
                     job["project"])
    for job in c["crons"]:
        if not any(n and n in job["cmd"] for n in d["names"]):
            _finding(f, "name-missing", "low",
                     f"cron job has no display name: {(job['scripts'] or [job['cmd'][:30]])[0]}",
                     "config/job_names.json", job["project"], fix="auto")

    # 10. a local-model caller that skips the GPU queue lands in ollama's FIFO, where
    #     priority no longer exists (PROJECT_STANDARDS §3).
    for caller in c.get("ollama_callers", []):
        if not caller["managed"]:
            _finding(f, "gpu-unmanaged", "med",
                     f"{os.path.basename(caller['file'])} calls ollama outside the GPU queue",
                     f"{caller['file']} — wrap the call in `gpu.slot(...)` or use "
                     "localllm.ask(); otherwise it queues FIFO and can outrank Stocks",
                     caller["project"], fix="memo")

    # 11. the public mirrors — the resume layer. Two ways they go wrong: they go stale,
    #     or they start leaking. The second is the one that matters.
    pub = c.get("public", {})
    q = load(os.path.join(STATE, "publish_refresh.json"), {}).get("quarantined", [])
    if q:
        _finding(f, "public-quarantine", "high",
                 f"{len(q)} published file(s) started leaking and were pulled",
                 "; ".join(f"{x['file']} [{x['why']}]" for x in q[:3])[:200]
                 + " — the daily refresh removed them rather than republishing", "maintenance")
    if pub.get("leaks"):
        _finding(f, "public-leak", "high", "a public repo would leak private content",
                 "publish.py scan: " + "; ".join(pub["leaks"][:3])[:200], "maintenance")
    for r in pub.get("repos", []):
        if not r["publish"]:
            continue
        if not r["built"]:
            _finding(f, "public-missing", "med", f"{r['project']} has no public counterpart built",
                     f"config/public_repos.json declares {r['repo']} but nothing is rendered",
                     r["project"])
        elif r["commits_since_readme"] >= 60:
            _finding(f, "public-stale", "low",
                     f"{r['repo']} is {r['commits_since_readme']} commits behind the project",
                     "the public overview describes work that has moved on — refresh the "
                     "authored README (it is written, not mirrored, so this is a judgement call)",
                     r["project"])

    # 12. backup coverage (PROJECT_STANDARDS §5.6). Driven by config/backups.json so this
    #     asks once per project and then stops: a project is covered, delegated, or
    #     deliberately exempt with a written reason.
    bk = load(os.path.join(CONFIG, "backups.json"), {})
    declared, exempt = bk.get("sources", {}), bk.get("exempt", {})
    for name, p in c["projects"].items():
        if name in exempt:
            continue
        if name not in declared:
            _finding(f, "backup-undeclared", "med", f"{name} has no entry in backups.json",
                     "Day-1 checklist item 6 — declare what's gitignored-and-unrecoverable, "
                     "or add it to `exempt` with the reason it needs nothing", name)
            continue
        age = p["backup_age_h"]
        limit = declared[name].get("max_gap_h", 30)
        if age is None:
            _finding(f, "backup-missing", "high", f"{name} is declared but has no backup on disk",
                     f"~/backups/{name.lower()}/ is empty — backup.py has never written it", name)
        elif age > max(limit * 3, 72):
            _finding(f, "backup-stale", "high", f"{name} backup is {age / 24:.1f} days old",
                     f"limit is {limit}h; the watchdog alerts too, so this one is already loud",
                     name)

    uniq = {}
    for x in f:
        uniq.setdefault(x["id"], x)
    return sorted(uniq.values(), key=lambda x: SEV[x["sev"]])


def merge_findings(fresh):
    """Fold today's findings into the durable store. Fingerprints make 'new' meaningful:
    a finding that has been open for a week must not push again, and one that disappears
    is recorded as resolved rather than forgotten."""
    store = load(FINDINGS, {})
    muted = set(load(MUTE, {}).get("muted", []))
    seen, new = set(), []
    for f in fresh:
        if f["id"] in muted or f["kind"] in muted:
            continue
        seen.add(f["id"])
        old = store.get(f["id"])
        if old and old.get("state") == "open":
            old.update(title=f["title"], detail=f["detail"], sev=f["sev"], last_seen=now())
        else:
            f.update(first_seen=now(), last_seen=now(), state="open")
            store[f["id"]] = f
            new.append(f)
    resolved = []
    for fid, f in store.items():
        if f.get("state") == "open" and fid not in seen:
            f["state"] = "resolved"
            f["resolved_at"] = now()
            resolved.append(f)
    save(FINDINGS, store)
    return new, resolved, [f for f in store.values() if f.get("state") == "open"]


# ---------------------------------------------------------------- fix

def _desc_of(project):
    p = os.path.join(HOME, project, "CLAUDE.md")
    try:
        for line in open(p):
            line = line.strip()
            if line.startswith("# "):
                return line[2:].split("—")[-1].strip()[:70] or project
    except Exception:
        pass
    return project


def fix(open_findings, dry=False):
    """Repair the drift Mission Control owns. Everything here edits this project's own
    files — never another project's (see the reach rule in CLAUDE.md)."""
    done = []
    pcfg_path = os.path.join(CONFIG, "projects.json")
    pcfg = load(pcfg_path, {})
    reg_path = os.path.join(MC, "CRON_REGISTRY.md")
    names_path = os.path.join(CONFIG, "job_names.json")
    rel = {pcfg_path: "config/projects.json", reg_path: "CRON_REGISTRY.md",
           names_path: "config/job_names.json"}
    # whatever another session already had open stays theirs to commit
    theirs = dirty_before(set(rel.values()))

    for f in open_findings:
        if f.get("fix") != "auto" or f.get("fixed_at"):
            continue

        if f["kind"] == "roster-missing" and f["project"]:
            name = f["project"]
            act = None
            for cand in (f"~/{name}/logs", f"~/{name}/_engine/logs"):
                if os.path.isdir(cand.replace("~", HOME)):
                    act = None       # a directory isn't an activity file; leave it for a human
            pcfg.setdefault("projects", {})[name] = {
                "desc": _desc_of(name), "match": [name], "activity_file": act,
                "next": "(auto-added by the back-office pass — set the next step)"}
            done.append((f, f"added {name} to the dashboard roster"))

        elif f["kind"] == "registry-missing":
            script = f["title"].split()[0]
            job = next((j for j in load(CENSUS, {}).get("crons", [])
                        if script in j["scripts"]), None)
            if job:
                if not dry:
                    _append_registry_row(reg_path, job, script)
                done.append((f, f"documented {script} in CRON_REGISTRY.md"))

        elif f["kind"] == "name-missing":
            script = f["title"].split(":")[-1].strip()
            names = load(names_path, {})
            if script and not any(n.get("match") == script for n in names.get("names", [])):
                pretty = re.sub(r"[-_]", " ", os.path.splitext(script)[0]).strip().capitalize()
                if not dry:
                    names.setdefault("names", []).append({"match": script, "name": pretty})
                    save(names_path, names, indent=2)
                done.append((f, f"named {script} in job_names.json"))

    if dry:
        return done
    if any(x[0]["kind"] == "roster-missing" for x in done):
        save(pcfg_path, pcfg, indent=2)
    for f, _ in done:
        f["fixed_at"] = now()
        f["state"] = "fixed"
    if done:
        store = load(FINDINGS, {})
        for f, _ in done:
            store[f["id"]] = f
        save(FINDINGS, store)
        _commit([rel[p] for p in (pcfg_path, reg_path, names_path)],
                "Back office: " + "; ".join(w for _, w in done)[:180], skip=theirs)
        if theirs:
            print("  left uncommitted (another session has them open): " + ", ".join(sorted(theirs)))
    return done


def _append_registry_row(reg_path, job, script):
    """Insert a row in the section for the job's project, creating the section if new."""
    text = open(reg_path).read()
    # Match the registry's own headings rather than hardcoding a project list: a new
    # project would otherwise land in "Unfiled" forever, and the list would be one more
    # hand-maintained inventory of the kind this whole job exists to eliminate.
    heads = re.findall(r"^## .+$", text, re.M)
    proj = (job["project"] or "").lower()
    head = next((h for h in heads if proj and proj in h.lower()),
                next((h for h in heads if "infrastructure" in h.lower()), None)
                or "## Unfiled — added by the back-office pass")
    row = (f"| {job['sched']} | `{script}` — {job['comment'] or 'undocumented; describe it'} "
           f"(auto-documented {datetime.now(timezone.utc):%Y-%m-%d}) | "
           f"`{job['log'] or '—'}` |\n")
    if head in text:
        idx = text.index(head)
        nxt = text.find("\n## ", idx + 1)
        block = text[idx:nxt if nxt > 0 else len(text)]
        lines = block.rstrip().splitlines()
        while lines and not lines[-1].startswith("|"):
            lines.pop()
        insert_at = idx + len("\n".join(lines)) + 1 if lines else idx + len(block)
        text = text[:insert_at] + row + text[insert_at:]
    else:
        text += f"\n{head}\n| Schedule | Job | Log |\n|---|---|---|\n{row}"
    open(reg_path, "w").write(text)


def dirty_before(paths):
    """Files already modified in the working tree before this pass touched them."""
    out = set()
    for line in sh(["git", "-C", MC, "status", "--porcelain"]).splitlines():
        f = line[3:].strip()
        if f in paths:
            out.add(f)
    return out


def _commit(paths, msg, skip=()):
    paths = [p for p in paths if p not in skip]
    if not paths:
        return
    sh(["git", "-C", MC, "add"] + [p for p in paths if os.path.exists(os.path.join(MC, p))])
    if sh(["git", "-C", MC, "diff", "--cached", "--name-only"]):
        sh(["git", "-C", MC, "commit", "-q", "-m", msg + "\n\nCo-Authored-By: Claude Opus 5 "
            "<noreply@anthropic.com>"])
        sh(["git", "-C", MC, "push", "-q", "origin", "master"], timeout=60)


# ---------------------------------------------------------------- brief

def brief(c=None):
    """One local-model paragraph per project that moved today. This is the only part of
    the pass that uses a model, and nothing depends on it: if ollama is down the audit
    still ran, the fixes still landed, and yesterday's summary stays on the card."""
    c = c or load(CENSUS, None) or census()
    status = load(STATUS, {})
    try:
        import models
        from localllm import ask
        models.require("dense", job="back-office brief")
    except SystemExit:
        return status
    except Exception:
        return status
    for name, p in c["projects"].items():
        g = p["git"]
        if not g.get("repo") or not g.get("commits_24h"):
            continue
        log = sh(["git", "-C", p["path"], "log", "--since=24 hours ago",
                  "--pretty=%s", "--stat", "--no-merges"])[:6000]
        if not log.strip():
            continue
        try:
            txt = ask("Below is one day of git activity in a personal project called "
                      f"'{name}'. In at most 2 sentences, plainly state what changed — "
                      "concrete nouns, no praise, no preamble, no bullet list. If it is "
                      "routine upkeep, say so briefly.\n\n" + log, num_predict=140)
        except Exception:
            continue
        if txt:
            status[name] = {"at": now(), "commits_24h": g["commits_24h"],
                            "summary": txt.strip()[:400]}
    save(STATUS, status)
    return status


# ---------------------------------------------------------------- memos

def file_memos(new_findings, dry=False):
    """A finding inside another project is a memo, not an edit. High severity only —
    the bus is for things a session must act on, not a nag feed."""
    filed = []
    for f in new_findings:
        if f.get("fix") != "memo" or f["sev"] != "high" or not f["project"]:
            continue
        target = f["project"].lower().replace("stocks", "stocks")
        inbox = os.path.join(HOME, "memos/inbox", target)
        slug = f"{datetime.now(timezone.utc):%Y-%m-%d}_backoffice-{f['kind']}.md"
        path = os.path.join(inbox, slug)
        if os.path.exists(path) or dry:
            continue
        os.makedirs(inbox, exist_ok=True)
        with open(path, "w") as fh:
            fh.write(f"# {f['title']}\n\n_From: Mission Control back-office pass · "
                     f"{datetime.now(timezone.utc):%Y-%m-%d} · target: {target}_\n\n"
                     f"{f['detail']}\n\nRaised by the daily audit "
                     f"(`~/maintenance/bin/backoffice.py`), rule `{f['kind']}`. "
                     "Fix it in the project, or mute the rule in "
                     "`~/maintenance/config/backoffice_mute.json` if it's an accepted deviation.\n")
        ledger = os.path.join(HOME, "memos/LEDGER.md")
        with open(ledger, "a") as fh:
            fh.write(f"| {datetime.now(timezone.utc):%Y-%m-%d} | backoffice-{f['kind']} | "
                     f"mission-control | {target} | proposed | auto-filed by the daily "
                     f"back-office audit |\n")
        filed.append(f)
    return filed


# ---------------------------------------------------------------- run

def run(dry=False):
    c = census()
    fresh = audit(c)
    new, resolved, open_f = merge_findings(fresh)
    fixed = fix(open_f, dry=dry)
    filed = file_memos(new, dry=dry)
    status = brief(c)
    still_open = [f for f in load(FINDINGS, {}).values() if f.get("state") == "open"]
    line = (f"census {len(c['projects'])} projects / {len(c['crons'])} crons · "
            f"{len(new)} new finding(s) · {len(fixed)} auto-fixed · {len(resolved)} resolved · "
            f"{len(still_open)} open")
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} {line}")
    for f, what in fixed:
        print(f"  fixed: {what}")
    for f in new:
        print(f"  new [{f['sev']}] {f['title']}")
    with open(HISTORY, "a") as fh:
        fh.write(json.dumps({"at": now(), "new": len(new), "fixed": len(fixed),
                             "resolved": len(resolved), "open": len(still_open),
                             "briefed": len(status)}) + "\n")
    # state-change doctrine: push only when something actually changed
    if not dry and (new or fixed or filed):
        head = [f"{f['title']}" for f in sorted(new, key=lambda x: SEV[x["sev"]])[:3]]
        msg = line + ("\n• " + "\n• ".join(head) if head else "")
        if filed:
            msg += f"\n{len(filed)} memo(s) filed to the bus"
        sh([os.path.join(MC, "bin/notify.sh"), "maintenance", "Back office", msg], timeout=30)
    return 0


def show():
    store = load(FINDINGS, {})
    open_f = sorted([f for f in store.values() if f.get("state") == "open"],
                    key=lambda x: (SEV[x["sev"]], x["kind"]))
    print(f"{len(open_f)} open finding(s)\n")
    for f in open_f:
        age = (now() - f.get("first_seen", now())) / 86400
        print(f"  [{f['sev']:>4}] {f['title']}")
        print(f"         {f['detail'][:110]}")
        print(f"         {f['kind']} · {f['project'] or 'box'} · open {age:.0f}d · fix={f['fix']}")
    fixed = [f for f in store.values() if f.get("state") == "fixed"]
    if fixed:
        print(f"\n{len(fixed)} auto-fixed to date")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    dry = "--dry" in sys.argv
    if cmd == "census":
        c = census(); print(json.dumps({k: len(v) if isinstance(v, (list, dict)) else v
                                        for k, v in c.items()}, indent=1))
    elif cmd == "audit":
        for f in audit():
            print(f"[{f['sev']:>4}] {f['kind']:<18} {f['title']}")
    elif cmd == "fix":
        _, _, open_f = merge_findings(audit())
        for f, what in fix(open_f, dry=dry):
            print(("would fix: " if dry else "fixed: ") + what)
    elif cmd == "brief":
        for k, v in brief().items():
            print(f"{k}: {v['summary']}")
    elif cmd == "show":
        show()
    else:
        sys.exit(run(dry=dry))
