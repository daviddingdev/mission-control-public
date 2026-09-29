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

    decide   David's answers from the dashboard that the janitor finishes: a mute goes
             into config/backoffice_mute.json with its reason, an in-dev label's new expiry
             into config/dev.json, and a "I'll do it myself" answer is checked and closed

CLI: backoffice.py [census|audit|fix|brief|run|show|selftest] [--dry]
"""
import json
import os
import re
import shutil
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


# The publish scanner's hit shape, `path:line  [credential from ~/.secrets/<f>] <match>`: what
# follows the tag is the secret itself, even one already rotated out of ~/.secrets.
_CRED_TAG = re.compile(r"(\[credential[^\]]*\])\s+(?!<redacted>)[^\s;|,]+")


def _secret_values():
    try:
        from publish import _secret_values as sv
        return sorted({v for _, v in sv()}, key=len, reverse=True)
    except Exception:
        return []


def _scrub_secrets(o, vals=None):
    """Every string with the ~/.secrets values, and whatever follows a scanner credential tag,
    replaced by <redacted>. Run on census.json and findings.json at every write (2026-09-24):
    the public-leak finding had stored the sudo password verbatim, resolved records keep their
    detail forever, and backup.py archives state/ nightly — so the stored copies are cleaned
    the next time the janitor writes them, not only the new ones."""
    if vals is None:
        vals = _secret_values()
    if isinstance(o, str):
        for v in vals:
            if v in o:
                o = o.replace(v, "<redacted>")
        return _CRED_TAG.sub(r"\1 <redacted>", o) if "[credential" in o else o
    if isinstance(o, list):
        return [_scrub_secrets(x, vals) for x in o]
    if isinstance(o, dict):
        return {k: _scrub_secrets(v, vals) for k, v in o.items()}
    return o


def save(path, obj, indent=1):
    """indent=2 for the hand-edited config files — matching their existing style keeps the
    janitor's diffs readable instead of reformatting the whole file every time. UTF-8 as
    written, not \\u escapes (fix round 2026-09-24): the first answer David gave rewrote every
    '—' in config/backoffice_mute.json — the record of what the box lives with — as \\u2014."""
    if path in (CENSUS, FINDINGS):
        obj = _scrub_secrets(obj)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent, ensure_ascii=False)
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


def _project_dirs(home=None):
    home = home or HOME
    out = []
    for name in sorted(os.listdir(home)):
        p = os.path.join(home, name)
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
    days = _cron_dows(dow)
    if days is not None and len(days) == 7:
        dow = "*"                 # 0-6, */1, sun-sat: every day, the same as *
    m = re.match(r"\*/(\d+)", minute)
    if m and hour == "*" and dom == "*" and dow == "*":
        return max(int(m.group(1)) / 60.0, 0.25)
    gap = 24.0
    if dom != "*":
        gap = 24 * 31
    elif dow != "*":
        # The longest stretch the schedule skips, plus the day it resumes — whatever the
        # hour and minute fields say, the gap is bounded by that stretch. Was a flat 168h
        # that the weekday cap below it could never lower (memo weekday-crons-get-a-week-of-
        # grace, 2026-09-27): 1-5 → Fri→Mon 72h, 1,3,5 → 72h, 0 → 168h.
        gap = _weekday_gap_h(days) if days else 24 * 7
    elif m:                       # */N minutes inside an hour window
        gap = 24.0
    if hour != "*" and "-" in hour and dow in ("1-5", "*"):
        gap = max(gap, 72.0)
    return gap


_DOW_NAMES = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")


def _cron_dows(dow):
    """The weekdays (0=Sun … 6=Sat) a cron day-of-week field allows, or None if it doesn't
    parse. Lists, ranges, steps (`*/2`, `1-5/2`), 0 and 7 both Sunday, three-letter names."""
    def num(tok):
        tok = tok.lower()
        if tok in _DOW_NAMES:
            return _DOW_NAMES.index(tok)
        if tok.isdigit() and int(tok) <= 7:
            return int(tok)
        raise ValueError(tok)
    days = set()
    try:
        for piece in dow.split(","):
            rng, slash, step = piece.partition("/")
            step = int(step) if slash else 1
            if rng == "*":
                lo, hi = 0, 7
            elif "-" in rng:
                lo, hi = map(num, rng.split("-", 1))
            else:
                lo = num(rng)
                hi = 7 if slash else lo      # `N/S` reads as N-7/S
            if step < 1 or lo > hi:
                return None
            days.update(d % 7 for d in range(lo, hi + 1, step))
    except ValueError:
        return None
    return days or None


def _weekday_gap_h(days):
    """(longest run of consecutive disallowed weekdays, circular over the week, + 1) × 24."""
    run = best = 0
    for d in list(range(7)) * 2:  # walked twice so a run across Sat→Sun counts whole
        run = 0 if d in days else run + 1
        best = max(best, run)
    return (min(best, 6) + 1) * 24.0


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


def _port_owners():
    """{port: {pid, cwd, cmd, started, all_ifaces}} for the listening sockets this user owns (`ss -tlnp`
    shows the pid of our own processes without sudo). What turns "port 8911 is listening" into "an
    hbs worktree's test dashboard has listened on every interface since 09-21" — and names the
    project, so the finding reaches its inbox instead of waiting on Mission Control's list."""
    out = {}
    for line in (sh(["ss", "-tlnp"]) or "").splitlines()[1:]:
        m = re.search(r"\s(\S+):(\d+)\s", line)
        pm = re.search(r"pid=(\d+)", line)
        if not m or not pm:
            continue
        port, pid = int(m.group(2)), int(pm.group(1))
        o = out.setdefault(port, {"pid": pid, "all_ifaces": False})
        o["all_ifaces"] = o["all_ifaces"] or m.group(1) in ("0.0.0.0", "*", "[::]")
        try:
            o["cwd"] = os.readlink(f"/proc/{pid}/cwd")
            o["cmd"] = " ".join(open(f"/proc/{pid}/cmdline", "rb").read().decode("utf-8", "replace")
                                .split("\0")).split("\n")[0].strip()[:90]
            o["started"] = os.stat(f"/proc/{pid}").st_mtime
        except OSError:
            pass
    return out


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


# A CALL, never a mention. `runner.launch` bare matched any file that merely NAMES the
# launcher — including this one, once its own rules started quoting it (2026-09-20). The
# trailing paren is what makes it a spawn rather than a sentence.
_CLAUDE_SPAWN = re.compile(
    r'subprocess\.\w+\(\s*\[\s*["\']claude'
    r'|runner\.launch\s*\('
    r'|"claude",\s*"-p"')


def _script_path(job, name):
    for tok in re.findall(r"[\w./~-]+\.py", job.get("cmd", "")):
        if os.path.basename(tok) != name:
            continue
        p = tok.replace("~", HOME)
        if os.path.exists(p):
            return p
        cd = re.search(r"cd\s+(\S+)", job.get("cmd", ""))
        cand = os.path.join((cd.group(1) if cd else HOME).replace("~", HOME), tok)
        if os.path.exists(cand):
            return cand
    return None


def _declared_zero(cmd):
    """True when config/job_weights.json declares this invocation as spending no tokens.

    The mechanism `ops.py verify` already documents ("0 keeps the spawner-aware audit
    honest"): a Claude-CAPABLE script invoked in a code-only mode is declared with a 0
    weight rather than special-cased in a rule. Honoured here so the window rules stop
    flagging dispatchers — the box-wide claudeq tick names runner.launch and runs every
    5 minutes, which would otherwise read as a Claude job in both protected hours and the
    trade lookback, every single day."""
    try:
        w = load(os.path.join(CONFIG, "job_weights.json"), {}) or {}
        for x in w.get("weights", []):
            if x.get("match") and x["match"] in cmd:
                return not x.get("tokens_per_run")
    except Exception:
        pass
    return False


def _spawns_claude(path):
    try:
        return bool(_CLAUDE_SPAWN.search(open(path, errors="replace").read()))
    except OSError:
        return False


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
            # A file can name an ollama endpoint without ever calling one — a test fixture,
            # a docstring, a denylist pattern. Require evidence of a request alongside it,
            # or the rule reports every mention of a URL as an unmanaged caller.
            calls = re.search(r"urlopen|requests\.(get|post)|httpx|curl\s+-|Request\(", text)
            if not calls:
                continue
            managed = ("import gpu" in text or "from gpu import" in text
                       or "localllm" in text or os.path.basename(f) in ("gpu.py", "models.py"))
            out.append({"file": os.path.relpath(f, HOME), "project": name, "managed": managed})
    return out


def _short(title):
    return re.sub(r"^[\d-]+\s*·\s*", "", title)[:44]


def _pid_alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, TypeError, ValueError):
        return False


def _experiments_state():
    """The frontier-technique queue, checked against what actually happened to each item.

    The queue is a ladder — scan appends, a research-only session writes a memo, David
    reads it, implementation is a separate explicit ask. It had no last rung: nothing
    noticed when a technique reached `verified` in the memo ledger, so two adopted-and-
    running techniques sat under "## Queue" for ten days while "## Adopted" said "nothing
    yet", and the state file still claimed their long-dead research processes were running.

    A queue that never empties stops being read, which quietly disables the intake it
    exists to gate.
    """
    out = {"queue": [], "dead_pids": [], "landed": [], "memo_waiting": []}
    try:
        txt = open(os.path.join(MC, "experiments.md"), errors="replace").read()
        ledger = ""
        lp = os.path.join(HOME, "memos/LEDGER.md")
        if os.path.exists(lp):
            ledger = open(lp, errors="replace").read()
        state = load(os.path.join(STATE, "experiments.json"), {})
        queue = txt.split("## Queue", 1)[-1].split("## Adopted", 1)[0]
        for block in re.split(r"\n(?=### )", queue):
            m = re.match(r"### ([^\n]+)", block.strip())
            if not m:
                continue
            title = m.group(1).strip()
            slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:40]
            memo = re.search(r"\*\*Memo:\*\*\s*(\S+)", block)
            st = next((v for k, v in state.items() if k in slug or slug.startswith(k)), {})
            item = {"title": title, "slug": slug, "memo": memo.group(1) if memo else None,
                    "state": st.get("status"), "started": st.get("started")}
            out["queue"].append(item)
            if st.get("status") == "running" and not _pid_alive(st.get("pid", -1)):
                out["dead_pids"].append(item)
            # has this technique already shipped? the memo ledger is the record of truth
            if item["memo"]:
                stem = os.path.basename(item["memo"]).replace(".md", "")
                stem = re.sub(r"^\d{4}-\d{2}-\d{2}_", "", stem)
                for row in ledger.splitlines():
                    if stem and stem in row and re.search(r"\*\*(implemented|verified)", row):
                        item["evidence"] = row.strip()[:400]
                        out["landed"].append(item)
                        break
            if item["memo"] and item not in out["landed"]:
                age = (time.time() - (st.get("started") or time.time())) / 86400
                out["memo_waiting"].append({**item, "days": round(age, 1)})
    except Exception as e:
        out["error"] = str(e)[:120]
    return out


def _diagram_state():
    """Architecture diagrams are AUTHORED, so nothing regenerates them — which means the
    only thing standing between "explains the system" and "quietly lies about it" is a
    check. Auto-generating them instead was rejected: an import graph drawn by a machine
    shows what calls what and never why, which is the entire value of these.

    So the diagrams stay hand-written and the machine checks two things it can actually
    know: does the diagram name a file that no longer exists (it is now wrong), and has a
    .d2 source been edited without re-rendering its .svg (mechanical, so it gets fixed).
    """
    out = {"ghosts": [], "unrendered": [], "diagrams": 0}
    FILE_RE = re.compile(r"[\w][\w.-]*\.(?:py|js|sh|mjs|json|html)")
    try:
        cfg = load(os.path.join(CONFIG, "public_repos.json"), {})
        for name, spec in cfg.get("projects", {}).items():
            src = os.path.join(MC, "public", spec["repo"])
            if not os.path.isdir(src):
                continue
            # everything the project actually contains, by basename
            real = set()
            for base, dirs, files in os.walk(os.path.join(HOME, name)):
                dirs[:] = [d for d in dirs if d not in
                           (".git", "node_modules", ".venv", "__pycache__", "worktrees")]
                real.update(files)
            for base, _, files in os.walk(src):
                for fn in files:
                    if not fn.endswith(".md"):
                        continue
                    text = open(os.path.join(base, fn), errors="replace").read()
                    for block in re.findall(r"```mermaid\n(.*?)```", text, re.S):
                        out["diagrams"] += 1
                        for ref in set(FILE_RE.findall(block)):
                            if ref not in real:
                                out["ghosts"].append({"repo": spec["repo"], "doc": fn,
                                                      "ref": ref, "project": name})
        # A diagram can be perfectly valid — every file it names still exists — and still
        # describe a system that has moved on. Nothing mechanical can see that, so the
        # signal is "the project has changed a lot since anyone touched the picture".
        out["outdated"] = []
        arch = os.path.join(MC, "architecture")
        proj_of = {"stocks": "Stocks", "clientco": "clientco-db", "poker": "poker",
                   "mission-control": "maintenance"}
        # architecture/_style.d2 (2026-09-24) is the shared look a diagram spread-imports with a
        # line reading exactly `...@_style`. Files starting with `_` are imports, never diagrams
        # (no .svg of their own, so they would read as unrendered forever); and a styled
        # diagram's .svg is stale when the style changes, not only when its own source does.
        style = os.path.join(arch, "_style.d2")
        style_mt = os.path.getmtime(style) if os.path.exists(style) else 0
        for f in sorted(os.listdir(arch)) if os.path.isdir(arch) else []:
            if not f.endswith(".d2") or f.startswith("_"):
                continue
            d2 = os.path.join(arch, f)
            svg = d2[:-3] + ".svg"
            out["diagrams"] += 1
            try:
                styled = any(l.strip() == "...@_style" for l in open(d2, errors="replace"))
            except OSError:
                styled = False
            if (not os.path.exists(svg) or os.path.getmtime(svg) < os.path.getmtime(d2)
                    or (styled and os.path.getmtime(svg) < style_mt)):
                out["unrendered"].append(f)
            proj = next((v for k, v in proj_of.items() if k in f), None)
            if proj and os.path.isdir(os.path.join(HOME, proj, ".git")):
                since = dt_from(os.path.getmtime(d2))
                n = len(sh(["git", "-C", os.path.join(HOME, proj), "log",
                            f"--since={since}", "--oneline"]).splitlines())
                if n >= 40:
                    out["outdated"].append({"file": f, "project": proj, "commits": n,
                                            "days": round((time.time() - os.path.getmtime(d2)) / 86400, 1)})
    except Exception as e:
        out["error"] = str(e)[:120]
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
            out["leaks"] = _leak_lines(r.stdout)[:10]
    except Exception as e:
        out["error"] = str(e)[:120]
    return out


_LEAK_LINE = re.compile(r"^(\S+:\d+)\s+\[([^\]]+)\]")


def _leak_lines(stdout):
    """The scan's hits as `file:line [why]` — WHERE and WHY, never the matched text. Until
    2026-09-24 the raw lines were kept, and a credential hit put the sudo password into
    census.json, findings.json, /api/status and the Overview. The file and the rule are all
    anyone needs to act on a hit; the summary line ("N leak(s)") is kept as it is."""
    out = []
    for l in (stdout or "").splitlines():
        m = _LEAK_LINE.match(l)
        if m:
            out.append(f"{m.group(1)} [{m.group(2)}]")
        elif re.match(r"^\d+ leak", l.strip()):
            out.append(l.strip())
    return out


def dt_from(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


def census():
    c = {"at": now(), "projects": {}, "crons": _cron_lines(),
         "ports": _listening_ports(), "port_owners": {str(k): v for k, v in _port_owners().items()},
         "declared": _declared(), "logs": {}}
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
    # The data catalog is compiled here for the same reason everything else is: the audit
    # rules read a census, never the live box. Zero Claude tokens, stdlib only.
    try:
        import catalog as _catalog
        c["catalog"] = _catalog.compile_all()["counts"]
    except Exception as e:
        c["catalog"] = {"error": str(e)[:160]}
    _record_cron_seen(c["crons"])
    # the Claude layer — plugins/skills that load into every session on the box.
    # The dashboard's Sessions › New sessions is the live view; the census copy is what the
    # audit rules read, same as everything else here.
    try:
        sys.path.insert(0, os.path.join(MC, "dashboard"))
        import claudecfg
        pl = claudecfg.plugins()
        c["claude_layer"] = {"installed": pl["installed"], "markets": pl["markets"],
                             "global_mcp": claudecfg.floor()["global_mcp"]}
        _record_plugins_seen(pl["installed"])
    except Exception as e:
        c["claude_layer"] = {"error": str(e)[:200]}
    # docker is a blind spot the audit paid for: retired open-webui kept restart=always
    # and served 0.0.0.0:8080 for three weeks after its 2026-08-08 retirement.
    try:
        c["containers"] = [x for x in sh(["docker", "ps", "--format", "{{.Names}}"]
                                         ).splitlines() if x.strip()]
    except Exception:
        c["containers"] = []
    c["public"] = _public_state()
    c["diagrams"] = _diagram_state()
    c["experiments"] = _experiments_state()
    try:
        r = subprocess.run([sys.executable, os.path.join(MC, "bin/models.py"), "check"],
                           capture_output=True, text=True, timeout=30)
        c["models_check"] = {"exit": r.returncode, "out": r.stdout.strip()[-400:]}
    except Exception as e:
        c["models_check"] = {"exit": -1, "out": str(e)[:200]}
    save(CENSUS, c)
    return c


# ---------------------------------------------------------------- audit

SEEN = None            # path set at import; kept module-level so census and audit agree


def _job_key(job):
    return re.sub(r"\s+", " ", (job.get("sched", "") + " " + job.get("cmd", ""))).strip()[:200]


def _record_cron_seen(crons):
    """Stamp every cron line with when this pass first observed it.

    The crontab spool is not readable by its own user, so "was this job added recently?"
    cannot come from a file date. A record of what we have seen is better anyway — it
    survives a crontab rewrite that touches an unrelated line.

    Stamped for EVERY line during census, not lazily when a check needs it: recording on
    first *failure* would mark a job that has genuinely gone silent as brand new and
    suppress the alarm for a whole cycle. On the very first run the file does not exist
    yet, so every line is backdated to the epoch — they all predate the record, and none
    of them should look new because we only just started writing it down.
    """
    path = os.path.join(STATE, "cron_seen.json")
    seen = load(path, None)
    first_ever = seen is None
    seen = seen or {}
    stamp = 0 if first_ever else now()
    changed = False
    for job in crons:
        k = _job_key(job)
        if k not in seen:
            seen[k] = stamp
            changed = True
    live = {_job_key(j) for j in crons}
    for k in list(seen):                       # forget retired jobs so the file stays honest
        if k not in live:
            del seen[k]
            changed = True
    if changed:
        save(path, seen)
    return seen


def _first_seen(job):
    return load(os.path.join(STATE, "cron_seen.json"), {}).get(_job_key(job))


def _record_plugins_seen(installed):
    """When did each Claude plugin first appear? Unlike cron_seen, everything present
    when this record starts gets stamped NOW, not the epoch: the rule downstream is
    about idleness over time, and backdating would nag every plugin on day one."""
    path = os.path.join(STATE, "claude_plugins_seen.json")
    seen = load(path, {})
    live = {f"{p['name']}@{p['market']}" for p in installed}
    changed = False
    for pid in live:
        if pid not in seen:
            seen[pid] = now()
            changed = True
    for pid in list(seen):
        if pid not in live:
            del seen[pid]
            changed = True
    if changed:
        save(path, seen)


def _key(s):
    """The status layer's key normaliser (dashboard/tt_now.py `_key`), so a finding's key and
    the key David's answer is stored under are the same string."""
    s = re.sub(r"[^a-z0-9._@:+-]+", "-", str(s or "").lower()).strip("-")
    return re.sub(r"-{2,}", "-", s)[:200] or "item"


def _digitless(fid):
    return re.sub(r"\d+", "#", fid or "")


def _legacy_of(f, fid):
    """True when a record stored BEFORE findings had keys (2026-09-24), under `fid`, is the same
    problem as the fresh finding `f` — its id differs only in the numbers. Only a finding with
    an explicit key qualifies: those are exactly the ones whose titles carry a count or an age
    ("12d old" -> "13d old"); a finding keyed by its id (a port number, a unit) never merges
    with another number. Used for the one transition pass, and for an answer David gave in the
    hours between deploy and that pass (keyed on the old id)."""
    return bool(f.get("key") and f["key"] != _key(f["id"]) and fid and fid != f["id"]
                and _digitless(fid) == _digitless(f["id"]))


def _finding(out, kind, sev, title, detail, project="", fix="human", key=None):
    """`id` is unchanged (mutes by id keep working). `key` (2026-09-24) is what stays the SAME
    when a title carries a count or an age — "has not run in 89h" / "96h" were two findings, a
    new one every morning — so a mute or an answer from the dashboard outlives the number. No
    `key` given: the id is already stable, and the key is the id, normalised."""
    fid = f"{kind}:{project}:{re.sub(r'[^a-z0-9]+', '-', title.lower())[:60]}"
    k = _key(":".join(p for p in (kind, str(project).lower(), str(key)) if p)) if key is not None else _key(fid)
    out.append({"id": fid, "key": k, "kind": kind, "sev": sev, "title": title, "detail": detail,
                "project": project, "fix": fix})


# The armed-check registry (2026-09-23): what rule 16 (`guardrail-inert`) proves, as data.
# One row per control the box relies on, the check that proves it is live, and the exact
# finding titles that check files when it is NOT ("{}" stands for an f-string field). A row
# with no titles is a control nothing proves yet: a declared gap, drawn as a gap. Read by the
# dashboard's Box › Guardrails (dashboard/tt_system.py), which calls a control armed when the last
# pass left none of its titles open. audit() below does not read this table, so an armed-check
# added there needs its row here in the same commit; `dashboard/tt_system.py selftest` parses
# this file and fails on a guardrail-inert title with no row, or a row whose title is gone.
# `requires` are paths under ~/maintenance the check executes (a missing one is reported).
ARMED_CHECKS = (
    {"id": "claude-headless", "control": "claude-headless wrapper", "layer": "preventive",
     "standard": "§2a.3", "check": "headless token present · `claude-headless --selftest defaults`",
     "requires": ("bin/claude-headless",),
     "titles": ("claude-headless is installed but has no token — it is a no-op",
                "claude-headless no longer applies the default model/effort/advisor")},
    {"id": "spawn-guard", "control": "PreToolUse claude-spawn guard", "layer": "preventive",
     "standard": "§2a.3", "check": "listed under hooks.PreToolUse · executable · flags a bare `claude -p`, passes claude-headless",
     "requires": ("bin/hook-guard-claude.py",),
     "titles": ("the claude-spawn guard is not armed",)},
    {"id": "claudeq", "control": "claudeq box slot", "layer": "preventive",
     "standard": "§2a.3", "check": "`claudeq.py audit` (C26): every Claude spawn point goes through the slot",
     "requires": ("bin/claudeq.py",),
     "titles": ("a Claude spawn point goes around the box slot",)},
    {"id": "notify", "control": "notify.sh + notify_policy.json", "layer": "preventive",
     "standard": "§1", "check": "policy parses · `notify_policy.py selftest`",
     "requires": ("bin/notify.sh", "bin/notify_policy.py", "config/notify_policy.json"),
     "titles": ("notify_policy.json is unreadable — push tiering is off",
                "notify_policy selftest fails — a tiering rule no longer does what was agreed")},
    {"id": "memo-inbox", "control": "SessionStart memo-inbox hook", "layer": "preventive",
     "standard": "§6", "check": "listed under hooks.SessionStart · executable",
     "requires": ("bin/hook-memo-inbox.py",),
     "titles": ("the memo-inbox SessionStart hook is not armed",)},
    {"id": "schedule-check", "control": "PreToolUse schedule-check hook", "layer": "preventive",
     "standard": "§2a", "check": "listed under hooks.PreToolUse · executable · `schedule-check.py --audit` runs",
     "requires": ("bin/hook-guard-schedule.py", "bin/schedule-check.py"),
     "titles": ("the schedule-check PreToolUse hook is not armed",
                "the schedule-check hook misreads commands — its selftest fails",
                "schedule-check.py does not run — the crontab hook is calling a broken checker")},
    {"id": "session-ledger", "control": "SessionStart/End session ledger", "layer": "preventive",
     "standard": "—", "check": "both hooks listed · executable · a ledger row in the last 26 h",
     "requires": ("bin/claude-session-notify.sh",),
     "titles": ("the session ledger is not armed",)},
    {"id": "models-require", "control": "models.require() roles", "layer": "preventive",
     "standard": "§3", "check": "`models.py check`: every role resolves to an installed model",
     "requires": ("bin/models.py",),
     "titles": ("models.py check fails — a local-model role cannot be served",)},
    {"id": "gpu-slot", "control": "gpu.slot() priority queue", "layer": "preventive",
     "standard": "§3a", "check": "`gpu.py selftest`", "requires": ("bin/gpu.py",),
     "titles": ("gpu.py selftest fails — the GPU queue's ordering rules no longer hold",)},
    {"id": "publish-scan", "control": "publish.py leak scanner", "layer": "preventive",
     "standard": "§5", "check": "`publish.py selftest`", "requires": ("bin/publish.py",),
     "titles": ("publish.py selftest fails — the leak scanner could let a secret through",)},
    {"id": "backup-refusal", "control": "backup.py credential refusal", "layer": "preventive",
     "standard": "—", "check": "`backup.py selftest`", "requires": ("bin/backup.py",),
     "titles": ("backup selftest fails — a credential could reach an archive",)},
    {"id": "sudoers", "control": "update-path sudoers grant", "layer": "preventive",
     "standard": "—", "check": "config/91-spark-updates verbs appear in `sudo -n -l`",
     "requires": ("config/91-spark-updates",),
     "titles": ("the sudoers grant on disk is not the one installed",)},
    {"id": "catalog", "control": "data-catalog rules", "layer": "detective",
     "standard": "§7", "check": "catalog compiles · `catalog.py selftest`",
     "requires": ("bin/catalog.py",),
     "titles": ("the data catalog did not compile — every catalog rule is off",
                "catalog selftest fails — a data-catalog rule no longer fires")},
    {"id": "dashboard", "control": "dashboard tab tests + check.py", "layer": "detective",
     "standard": "—", "check": "`node test_*_tab.js` · `dashboard/check.py`",
     "requires": ("dashboard/test_overview_tab.js", "dashboard/test_claude_tab.js",
                  "dashboard/test_catalog_tab.js", "dashboard/check.py"),
     "titles": ("dashboard {} tab test fails", "a dashboard check is failing")},
    {"id": "deadunits", "control": "dead-unit rule", "layer": "detective",
     "standard": "—", "check": "`deadunits.py selftest`", "requires": ("bin/deadunits.py",),
     "titles": ("deadunits selftest fails — the dead-unit rule no longer fires",)},
    {"id": "crew-namer", "control": "the crew namer (one name per agent everywhere)", "layer": "detective",
     "standard": "—", "check": "`tt_crew.py selftest` (resolver, tools-rule and badge fixtures) · every live job, scripts too, claimed once",
     "requires": ("dashboard/tt_crew.py", "config/crew.json"),
     "titles": ("crew selftest fails — the page can name the wrong agent",)},
    {"id": "dashboard-unit", "control": "systemd unit maintenance-dashboard (Restart=always, MemoryMax=2G, MemorySwapMax=256M)",
     "layer": "preventive", "standard": "—",
     "check": "the unit is enabled · active · MemoryMax and MemorySwapMax are byte counts, not infinity",
     "requires": ("dashboard/maintenance-dashboard.service", "dashboard/serve.sh"),
     "titles": ("the dashboard unit is not armed — :8900 has no supervisor or no memory cap",)},
    {"id": "smb-fruit", "control": "SMB share vfs_fruit (no Finder ._ files)", "layer": "preventive",
     "standard": "—", "check": "`testparm -s` loads fruit + streams_xattr · no AppleDouble since",
     "requires": (),
     "titles": ("the SMB share no longer loads vfs_fruit — Finder copies leave ._ files again",)},
    {"id": "sentinel", "control": "sentinel page recheck", "layer": "detective",
     "standard": "—", "check": "`sentinel.py selftest`", "requires": ("bin/sentinel.py",),
     "titles": ("sentinel selftest fails — it can page on a job that already recovered",)},
    {"id": "rc-sign-in", "control": "phone sign-in canary (healthcheck remote-control-auth) + login-mode re-sign-in",
     "layer": "detective", "standard": "—",
     "check": "`claude-relogin.py selftest` (credential fixtures, login-mode markers, the RC-host pgrep gotcha, healthcheck wiring)",
     "requires": ("bin/claude-relogin.py", "bin/healthcheck.sh"),
     "titles": ("claude-relogin selftest fails — the phone sign-in canary or its re-sign-in can misread",)},
    {"id": "mdreader-drift", "control": "shared markdown reader: every vendored copy is the current build",
     "layer": "detective", "standard": "—",
     "check": "the canonical dist/ VERSION reads · `backoffice.py selftest` (the mdreader-drift fixtures)",
     "requires": ("shared/mdreader/dist/mdreader.js",),
     "titles": ("the mdreader drift rule cannot read the shared reader's VERSION — it checks nothing",)},
)


SMB_FRUIT_SINCE = 1790374800          # 2026-09-25 22:20Z: vfs_fruit live on the user-home share (smb.conf written 22:20:11Z)
_AD_SKIP = {"archive", "backups", "model-vault", "snap", "poker-data", "node_modules", ".venv",
            "venv", ".git", "__pycache__"}


def _appledouble_since(since, root=HOME):
    """-> paths of AppleDouble (`._*`, magic 00 05 16 07) files under ~ changed after `since`.
    Skips the big trees nobody drops files into and the hidden folders at ~ (except .claude).
    ~0.2 s over ~100k files."""
    out = []
    for d, subdirs, files in os.walk(root):
        subdirs[:] = [s for s in subdirs if s not in _AD_SKIP
                      and not (d == root and s.startswith(".") and s != ".claude")]
        for fn in files:
            if not fn.startswith("._"):
                continue
            p = os.path.join(d, fn)
            try:
                if os.stat(p).st_mtime > since:
                    with open(p, "rb") as fh:
                        if fh.read(4) == b"\x00\x05\x16\x07":
                            out.append(p)
            except OSError:
                pass
    return sorted(out)


# The box's one markdown reader (2026-09-27, David: "can we code some md reader to make things clean? this applies
# to every dashboard we have"). Mission Control owns the source; every dashboard serves a COPY of its dist/ (a
# project never reads another project's path), so a copy can be left behind by a rebuild. Rule `mdreader-drift`.
MDREADER_DIST = os.path.join(MC, "shared", "mdreader", "dist")
_MDR_NAME = re.compile(r"^mdreader(?:[.-][0-9A-Za-z][0-9A-Za-z.+-]*)?\.(?:js|py|css)$")   # also mdreader.1.3.0-7baad46b.js
_MDR_SKIP = {"node_modules", ".git", "__pycache__", "venv", ".venv", "site-packages", "dist-packages", "archive",
             ".mypy_cache", ".pytest_cache", ".claude"}   # .claude/worktrees = other sessions' scratch checkouts
_MDR_STAMP = re.compile(r"""(?:\bVERSION\s*=\s*["']|/\*! mdreader )(\d+\.\d+\.\d+(?:\+[0-9a-f]{8})?)""")


def _mdr_version(path):
    """The VERSION a built mdreader file carries in its first 4 KB ("1.3.0+7baad46b": the JS `var VERSION`, the
    Python `VERSION =`, the CSS banner), or None — no file, or no stamp (a hand-made or patched copy)."""
    try:
        with open(path, errors="replace") as fh:
            m = _MDR_STAMP.search(fh.read(4096))
    except OSError:
        return None
    return m.group(1) if m else None


def _mdreader_copies(home=None, canon=None, projects=None):
    """-> sorted [(project, path, version|None)]: every vendored copy of the reader under ~/<project>/ — a file
    named mdreader.js / .py / .css, or a versioned name like mdreader.1.3.0-7baad46b.js — outside the canonical
    folder, skipping dependency trees, virtualenvs (any folder holding pyvenv.cfg) and archives. The roster is
    the disk's (_project_dirs); ~0.1 s over the box's ~50k project files."""
    home = home or HOME
    canon = os.path.realpath(canon or os.path.dirname(MDREADER_DIST))
    out = []
    for name in (projects if projects is not None else _project_dirs(home)):
        for d, subdirs, files in os.walk(os.path.join(home, name)):
            subdirs[:] = [x for x in subdirs if x not in _MDR_SKIP and os.path.realpath(os.path.join(d, x)) != canon
                          and not os.path.exists(os.path.join(d, x, "pyvenv.cfg"))]
            out += [(name, os.path.join(d, fn), _mdr_version(os.path.join(d, fn))) for fn in files if _MDR_NAME.match(fn)]
    return sorted(out, key=lambda r: (r[0], r[1]))


def _mdreader_drift(copies, current):
    """{project: [(path, version|None, why)]} for every copy that is not the `current` build: an older version, the
    same version number built from other source (the hash differs), a stamp AHEAD of the source (edited in place),
    or no stamp at all. A copy is a build artifact, never a fork (shared/mdreader/README.md, the drift rule)."""
    num = lambda v: tuple(int(x) for x in re.match(r"(\d+)\.(\d+)\.(\d+)", v).groups())
    out = {}
    for proj, path, v in copies:
        if v == current:
            continue
        why = ("carries no VERSION stamp" if not v else "is an older version" if num(v) < num(current)
               else "is another build of the same version" if num(v) == num(current)
               else "is ahead of its source — edited in place?")
        out.setdefault(proj, []).append((path, v, why))
    return out


def _catalog_key(r):
    """Stable keys for the catalog rules whose titles carry a count or an age (bin/catalog.py
    builds the rows; the key is derived here so that file needs no change): the dataset id for
    a stale feed, the project for the per-project counts. None = the id is already stable."""
    k, t = r.get("key"), r.get("title") or ""
    if k:
        return k
    if r["kind"] == "catalog-stale":
        m = re.match(r"^(\S+) is \d+d old", t)
        return m.group(1) if m else None
    if r["kind"] in ("catalog-source-uncategorised", "catalog-undeclared", "catalog-undeclared-cold",
                     "catalog-orphan-empty", "catalog-conflict"):
        return ""
    return None


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
    # A row can exist and lie. The VP sweep was documented as "zero Claude tokens" for five
    # days after a Claude review stage landed in it — the registry had an entry, so every
    # existing rule was satisfied. Check the claim, not just the presence.
    for row in re.findall(r"^\|.*$", d["registry_text"], re.M):
        if "zero claude" not in row.lower():
            continue
        for scr in re.findall(r"`?([\w./-]+\.py)", row):
            job = next((j for j in c["crons"] if os.path.basename(scr) in j["scripts"]), None)
            path = _script_path(job, os.path.basename(scr)) if job else None
            if path and _spawns_claude(path):
                _finding(f, "registry-false-claim", "med",
                         f"CRON_REGISTRY says {os.path.basename(scr)} costs zero Claude tokens, "
                         "but it spawns a session",
                         "the row was true when written and stopped being true — a registry "
                         "entry that exists is not the same as one that is correct",
                         job.get("project", ""))

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
            # A job added mid-cycle has not reached its first run yet — that is not drift.
            # Two ways to tell: the script itself is newer than one cycle, or the crontab
            # was edited within one cycle (which covers jobs with no script of their own,
            # like a monthly `claude -p` line — the case that first produced this false
            # positive, on the day its own cron line was written).
            src = next((os.path.join(MC, "bin", n) for n in job["scripts"]
                        if os.path.exists(os.path.join(MC, "bin", n))), None)
            if src and (time.time() - os.path.getmtime(src)) / 3600 < gap:
                continue
            first = _first_seen(job)
            if first and (time.time() - first) / 3600 < gap:
                continue
            _finding(f, "log-missing", "med",
                     f"{name} ({job['sched']}) has never written its log",
                     f"expected at {job['log']}", job["project"], key=f"{name}:{job['sched']}")
        elif age > max(gap * 3, gap + 24):
            _finding(f, "job-silent", "high", f"{name} has not run in {age:.0f}h",
                     f"schedule `{job['sched']}` expects output every ~{gap:.0f}h — "
                     f"{job['log']}", job["project"], key=f"{name}:{job['sched']}")

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
            # who is listening (2026-09-26): an owner makes it the owner's finding — a memo when it
            # listens on every interface — instead of a line on Mission Control's list that no one
            # reads (8797 and 8911 were two forgotten test dashboards, open 5 days, project "")
            o = (c.get("port_owners") or {}).get(str(port)) or {}
            proj = _project_of((o.get("cwd") or "") + "/") if o.get("cwd") else ""
            if proj in ("maintenance", "home"):
                proj = ""
            who = (f"{o['cmd']} (pid {o['pid']}, in {o['cwd'].replace(HOME, '~')}"
                   + (f", running since {dt_from(o['started'])[:10]}" if o.get("started") else "") + ")"
                   if o.get("cmd") and o.get("cwd") else "")
            wide = bool(o.get("all_ifaces"))
            _finding(f, "port-undeclared", "high" if wide and proj else "med",
                     f"port {port} is listening but undeclared",
                     (f"{who}. " if who else "")
                     + ("It listens on every interface, not just the tailnet or localhost. " if wide else "")
                     + "Missing from " + " and ".join(where) + " (PROJECT_STANDARDS §4): stop it if "
                     "it is a leftover test server, or declare it in all three places.",
                     proj, fix="memo" if proj else "human", key=f"port:{port}")

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
                     f"branch {g.get('branch', '?')} — the offsite copy is behind", name, key="")
        if g.get("dirty", 0) >= 20:
            _finding(f, "dirty-tree", "low", f"{name} has {g['dirty']} uncommitted files",
                     "long-lived working tree — either commit it or add it to .gitignore", name, key="")

    # 8. local-model roles
    if c.get("models_check", {}).get("exit", 0) >= 2:
        _finding(f, "model-role-dead", "high", "a local-model role cannot run",
                 c["models_check"]["out"][:200], "maintenance")
    elif c.get("models_check", {}).get("exit", 0) == 1:
        _finding(f, "model-fallback", "low", "a local-model role fell back to a weaker model",
                 c["models_check"]["out"][:200], "maintenance")

    # 9. an AI cron with no display name / no load weight is invisible in the dashboard's
    #    cost view — Agents › Usage silently under-reports the box.
    for job in c["crons"]:
        # "spawns Claude" includes python launchers (ops.py, loop.py, vp.py …) — the
        # crontab-literal `claude -` test missed all of them, which is how six trading
        # roles (incl. a 2M-token COO) ran unweighted until 2026-08-29.
        spawns = (re.search(r"(^|[|&;\s])claude\s+-", job["cmd"])
                  or "claude-headless" in job["cmd"]
                  or any(_spawns_claude(_script_path(job, s)) for s in job["scripts"]
                         if s.endswith(".py") and _script_path(job, s)))
        if not spawns:
            continue
        if not any(w and w in job["cmd"] for w in d["weights"]):
            _finding(f, "weight-missing", "low",
                     f"Claude job has no token weight: {(job['scripts'] or [job['cmd'][:30]])[0]}",
                     "config/job_weights.json — measure it from Agents › Usage, then add it",
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
    # 11b. the experiments ladder's missing last rung. David does not check this often, so
    #      the queue must clear itself and speak up only when it actually needs him.
    exp = c.get("experiments", {})
    for x in exp.get("landed", []):
        _finding(f, "experiment-landed", "low",
                 f"'{_short(x['title'])}' has shipped but is still queued",
                 "the memo ledger records it implemented/verified — moving it to Adopted "
                 "so the queue reflects what is actually open", "maintenance", fix="auto")
    for x in exp.get("dead_pids", []):
        _finding(f, "experiment-dead-pid", "low",
                 f"'{_short(x['title'])}' is marked running but its process is gone",
                 "state/experiments.json outlived the research session — the dashboard "
                 "would show it as in-flight forever", "maintenance", fix="auto")
    for x in exp.get("memo_waiting", []):
        if x["days"] >= 0:
            _finding(f, "experiment-memo-waiting", "med",
                     f"a design memo is ready to read"
                     + (f" (waiting {x['days']:.0f}d)" if x["days"] >= 1 else ""),
                     f"{x['memo']} — the ladder is stalled at the one rung that needs David: "
                     "read it in Sessions › Memos, then either ask a session to build it or "
                     "let it drop to the graveyard", "maintenance", key=x["memo"])

    # 11a. diagrams that have gone out of date with the code they describe
    dia = c.get("diagrams", {})
    for g in dia.get("ghosts", [])[:6]:
        _finding(f, "diagram-ghost", "med",
                 f"{g['repo']} diagram references {g['ref']}, which no longer exists",
                 f"{g['doc']} draws a component the project does not have any more — the "
                 "diagram is now describing a system that is gone", g["project"])
    # Deliberately NOT a finding (David, 2026-08-19): "stale diagrams should just be clear
    # when these were last drawn." A drawing is a point-in-time statement, so the honest fix
    # is to date it and review on a schedule, not to nag daily about a judgement call. The
    # date rides on the diagram itself; the monthly session is what acts on it.
    if dia.get("unrendered"):
        _finding(f, "diagram-unrendered", "low",
                 f"{len(dia['unrendered'])} diagram source(s) edited but not re-rendered",
                 ", ".join(dia["unrendered"]) + " — the .svg the dashboard serves is older "
                 "than its .d2 source (or than architecture/_style.d2, for a diagram that imports "
                 "it)", "maintenance", fix="auto", key="")

    pub = c.get("public", {})
    q = load(os.path.join(STATE, "publish_refresh.json"), {}).get("quarantined", [])
    if q:
        _finding(f, "public-quarantine", "high",
                 f"{len(q)} published file(s) started leaking and were pulled",
                 "; ".join(f"{x['file']} [{x['why']}]" for x in q[:3])[:200]
                 + " — the daily refresh removed them rather than republishing", "maintenance", key="")
    if pub.get("leaks"):
        # file:line [why] only — _leak_lines() never keeps the matched text
        _finding(f, "public-leak", "high", "a public repo would leak private content",
                 "publish.py scan: " + "; ".join(pub["leaks"][:3])[:200], "maintenance")
    for r in pub.get("repos", []):
        if not r["publish"]:
            continue
        if not r["built"]:
            _finding(f, "public-missing", "med", f"{r['project']} has no public counterpart built",
                     f"config/public_repos.json declares {r['repo']} but nothing is rendered",
                     r["project"])
        elif r["commits_since_readme"] >= 25:
            _finding(f, "public-stale", "low",
                     f"{r['repo']} is {r['commits_since_readme']} commits behind the project",
                     "the public overview describes work that has moved on — refresh the "
                     "authored README (it is written, not mirrored, so this is a judgement call)",
                     r["project"], key=r["repo"])

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
                     name, key="")

    # 13. the Claude layer. A plugin loads into EVERY session — one that never fires
    #     is context tax plus attack surface for nothing. First-seen stamps give a new
    #     install 60 days to prove itself; an accepted keeper gets muted, not ignored.
    pseen = load(os.path.join(STATE, "claude_plugins_seen.json"), {})
    for p in (c.get("claude_layer") or {}).get("installed", []):
        pid = f"{p['name']}@{p['market']}"
        active = max(p.get("last_used") or 0, pseen.get(pid) or now())
        idle_d = (now() - active) / 86400
        if idle_d > 60:
            _finding(f, "claude-plugin-unused", "low",
                     f"Claude plugin {pid} idle for {idle_d:.0f} days",
                     f"{p.get('uses', 0)} recorded fires; it still loads into every "
                     "session (context + surface) — uninstall it or mute this", key=pid)
    # 14. the PROTECTED window (PROJECT_STANDARDS §2, David 2026-08-28): 20:00-04:00 UTC
    #     is HBS prep — no scheduled Claude session may START in it. The trigger engine
    #     is exempt (event-driven, market-hours). Checks the cron HOUR field only; a
    #     range or list that touches the window counts.
    def _hours_of(sched):
        parts = sched.split()
        if len(parts) != 5 or parts[1] == "@":
            return set()
        hrs = set()
        for piece in parts[1].split(","):
            piece = piece.split("/")[0]
            if piece == "*":
                return set(range(24))
            if "-" in piece:
                a, b = piece.split("-")
                hrs |= set(range(int(a), int(b) + 1))
            elif piece.isdigit():
                hrs.add(int(piece))
        return hrs
    PROTECTED = {20, 21, 22, 23, 0, 1, 2, 3}
    for job in c["crons"]:
        if "triggers.py" in job["cmd"] or _declared_zero(job["cmd"]):
            continue
        is_claude = "claude -p" in job["cmd"] or any(
            _spawns_claude(_script_path(job, s)) for s in job["scripts"]
            if s.endswith(".py") and _script_path(job, s))
        if is_claude and _hours_of(job["sched"]) & PROTECTED:
            _finding(f, "claude-protected-window", "med",
                     f"Claude cron starts inside the protected HBS window: {job['sched']}",
                     f"{job['cmd'][:110]} — 20:00-04:00 UTC is David's prep time "
                     "(PROJECT_STANDARDS §2); move it to the quiet window, or mute if "
                     "David pinned it here", job.get("project", ""))

    # 14b. the USAGE-WINDOW band (Stocks CLAUDE.md, David 2026-08-31; memo usage-window-cron-moves
    #      2026-09-01): the 14:05 UTC trade session shares a rolling 5-hour usage limit with
    #      everything that started after 09:05, and it died on that limit on 08-31 behind the
    #      morning cluster. So no Claude-weighted cron may START in 09:05-14:04 Mon-Fri. The one
    #      authorized exception (the Monday 10:45 board) is muted by id, which is the record.
    def _mins_of(sched):
        parts = sched.split()
        if len(parts) != 5:
            return set()
        mins = set()
        for piece in parts[0].split(","):
            piece = piece.split("/")[0]
            if piece == "*":
                return set(range(60))
            if "-" in piece:
                a, b = piece.split("-")
                mins |= set(range(int(a), int(b) + 1))
            elif piece.isdigit():
                mins.add(int(piece))
        return mins

    def _weekday_possible(sched):
        parts = sched.split()
        if len(parts) != 5:
            return False
        dow = parts[4]
        if dow == "*":
            return True                       # incl. day-of-month jobs — the 1st lands on weekdays too
        days = set()
        for piece in dow.split(","):
            piece = piece.split("/")[0]
            if "-" in piece:
                a, b = piece.split("-")
                days |= set(range(int(a), int(b) + 1))
            elif piece.isdigit():
                days.add(int(piece))
        return bool(days & {1, 2, 3, 4, 5})

    BAND_LO, BAND_HI = 9 * 60 + 5, 14 * 60 + 5          # [09:05, 14:05) — 14:05 IS the trade session
    for job in c["crons"]:
        if ("triggers.py" in job["cmd"] or "loop.py trade" in job["cmd"]
                or _declared_zero(job["cmd"])):
            continue
        is_claude = "claude -p" in job["cmd"] or "claude-headless" in job["cmd"] or any(
            _spawns_claude(_script_path(job, s)) for s in job["scripts"]
            if s.endswith(".py") and _script_path(job, s))
        if not is_claude or not _weekday_possible(job["sched"]):
            continue
        starts = {h * 60 + m for h in _hours_of(job["sched"]) for m in _mins_of(job["sched"])}
        if any(BAND_LO <= t < BAND_HI for t in starts):
            _finding(f, "usage-window", "med",
                     f"Claude cron in the usage-window band: {job['sched']} {job['scripts'][0] if job.get('scripts') else ''}".strip(),
                     f"{job['cmd'][:110]} — 09:05-14:05 UTC Mon-Fri is the 5h window the 14:05 "
                     "BrokerB PM session draws on (it died on the shared limit 2026-08-31); "
                     "start it before 09:00 or after 15:00, or mute here if David authorized it",
                     job.get("project", ""))

    # A user-scope MCP server rides into every session on the box, and every session
    # re-pays it on every step (the 2026-08-19 BrokerB lesson: ~8M tokens/session of
    # floor re-reads, most of it tools the session could never use). Project tools
    # live at project scope; a genuinely box-wide server gets muted here, not ignored.
    for name in (c.get("claude_layer") or {}).get("global_mcp", []):
        _finding(f, "claude-global-mcp", "med",
                 f"MCP server '{name}' is user-scope (global)",
                 "every session on the box carries it — move it to the project that "
                 "uses it (`claude mcp add --scope local` there, then remove the "
                 "user-scope entry), or mute if it is truly box-wide")

    # 15. a running docker container nobody sanctioned. Containers dodge every other
    #     census (ports table knows the port, not the owner; cron knows nothing), and
    #     restart=always resurrects them across reboots forever.
    sanctioned = set(load(os.path.join(CONFIG, "containers.json"), {}).get("sanctioned", []))
    for name in c.get("containers", []):
        if name not in sanctioned:
            _finding(f, "container-unsanctioned", "med",
                     f"docker container '{name}' is running but not sanctioned",
                     "add it to config/containers.json with why, or stop it and set "
                     "--restart=no (a retired service kept resurrecting itself for "
                     "three weeks this way)")

    # 16. GUARDRAIL ARMED CHECKS (2026-08-29). Every preventive control on this box gets a
    #     rule here proving it is actually live. Born from a real miss: claude-headless was
    #     mandated by §2a.3, used by all seven Claude cron lines, and inert for a month
    #     because ~/.claude/headless-token was never written — so every "compliant" job ran
    #     on the rotating credential that killed auth box-wide three times. A guardrail that
    #     silently no-ops is worse than none: it manufactures confidence. If you add a
    #     preventive control, add its armed-check here in the same commit.
    tok = os.path.expanduser("~/.claude/headless-token")
    if not (os.path.exists(tok) and os.path.getsize(tok) > 0):
        _finding(f, "guardrail-inert", "high",
                 "claude-headless is installed but has no token — it is a no-op",
                 "bin/claude-headless falls through to plain `claude` when "
                 "~/.claude/headless-token is missing, putting every headless job back on "
                 "the rotating credential (the 08-27/28/29 box-wide auth deaths). Re-auth "
                 "from the phone (Mission Control -> Claude -> Headless auth) writes it.")

    # claude-headless's default --model/--effort (2026-09-23, David: every automated Claude
    # job on Opus 5.5 at high effort) and --advisor (2026-09-25, Fable 5.1). If the wrapper
    # stops appending them nothing fails: the jobs that pass no flag quietly go back to Sonnet
    # at medium, with no advisor. So the wrapper's own argv selftest runs here every morning
    # (<1s, zero tokens).
    _hl_st = sh([os.path.join(MC, "bin", "claude-headless"), "--selftest", "defaults"],
                timeout=20) or ""
    if "FAIL" in _hl_st or "OK" not in _hl_st:
        _finding(f, "guardrail-inert", "high",
                 "claude-headless no longer applies the default model/effort/advisor",
                 "run `~/maintenance/bin/claude-headless --selftest defaults`; each FAIL line "
                 "shows the argv a headless job would hand the CLI. Without the defaults, a job "
                 "that passes no --model runs on Sonnet at medium effort, not Opus 5.5 at high: "
                 + _hl_st.strip().replace("\n", " | ")[:300])

    # The sudoers grant the update path depends on: config/91-spark-updates is the SOURCE,
    # /etc/sudoers.d/ is what actually applies. They drifted (2026-08-29): the repo file had
    # granted /usr/sbin/reboot for weeks while the installed copy had not, so the scheduled
    # update could never have rebooted. Compare the declared verbs against `sudo -n -l`.
    try:
        declared = open(os.path.join(CONFIG, "91-spark-updates")).read()
        effective = sh(["sudo", "-n", "-l"], timeout=10) or ""
        want = [v for v in ("full-upgrade", "/usr/sbin/reboot") if v in declared]
        gap = [v for v in want if v not in effective]
        if gap:
            _finding(f, "guardrail-inert", "high",
                     "the sudoers grant on disk is not the one installed",
                     f"config/91-spark-updates declares {', '.join(gap)} but `sudo -n -l` does "
                     f"not show it, so the scheduled update aborts instead of running. Install: "
                     f"sudo cp ~/maintenance/config/91-spark-updates /etc/sudoers.d/91-spark-updates "
                     f"&& sudo chmod 440 /etc/sudoers.d/91-spark-updates")
    except Exception:
        pass

    # The six controls the guardrail ledger listed as declared gaps (2026-09-26, David: "fix all
    # the stuff that needs attention ... i want this to be perfect"). Each check is plain code,
    # under a second, zero tokens — the same "prove it is armed" rule as claude-headless above.
    try:
        _st = load(os.path.expanduser("~/.claude/settings.json"), {}) or {}
        _hk = lambda ev: [h.get("command", "") for grp in (_st.get("hooks", {}).get(ev) or [])
                          for h in grp.get("hooks", [])]
        _g = os.path.join(MC, "bin", "hook-guard-claude.py")
        # one string, not an argv list: the hook's --test joins its arguments anyway, and the
        # list form reads as a Claude spawn to _CLAUDE_SPAWN (it made this file "spawn a session")
        _bad = sh(["python3", _g, "--test", "claude -p hi"], timeout=10) or ""
        _ok = sh(["python3", _g, "--test", os.path.join(MC, "bin", "claude-headless"), "-p", "hi"], timeout=10) or ""
        if (not any("hook-guard-claude" in c for c in _hk("PreToolUse")) or not os.access(_g, os.X_OK)
                or not _bad.startswith("VIOLATION") or _ok.strip() != "ok"):
            _finding(f, "guardrail-inert", "high", "the claude-spawn guard is not armed",
                     "bin/hook-guard-claude.py must be listed under hooks.PreToolUse in ~/.claude/"
                     "settings.json, be executable, flag a bare `claude -p` and pass claude-headless "
                     f"(got: {_bad.strip()[:60]!r} / {_ok.strip()[:30]!r}). Without it a session can "
                     "spawn on the rotating credential that killed auth box-wide three times.")
        _n = os.path.join(MC, "bin", "claude-session-notify.sh")
        _led = os.path.join(MC, "state", "claude_sessions.jsonl")
        _age = time.time() - os.path.getmtime(_led) if os.path.exists(_led) else None
        if (not any("claude-session-notify" in c for c in _hk("SessionStart"))
                or not any("claude-session-notify" in c for c in _hk("SessionEnd"))
                or not os.access(_n, os.X_OK) or _age is None or _age > 26 * 3600):
            _finding(f, "guardrail-inert", "high", "the session ledger is not armed",
                     "bin/claude-session-notify.sh must be listed under hooks.SessionStart AND "
                     "hooks.SessionEnd and be executable, and state/claude_sessions.jsonl must have a "
                     f"row from the last 26 h (newest: {'none' if _age is None else f'{_age / 3600:.0f} h ago'}). "
                     "Every headless-run count, push and usage attribution reads it.")
    except Exception:
        pass
    def _selfcheck(cmd, want=None):
        """-> (ok, output) for a zero-token checker in bin/: exit 0, no FAIL line, and `want` present."""
        try:
            r = subprocess.run(["python3", os.path.join(MC, "bin", cmd[0]), *cmd[1:]],
                               capture_output=True, text=True, timeout=60)
            out = (r.stdout or "") + (r.stderr or "")
            return (r.returncode == 0 and "FAIL" not in out and (not want or want in out),
                    out.strip().replace("\n", " | ")[-300:])
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"
    _ok, _out = _selfcheck(["claudeq.py", "audit"], "every Claude spawn point goes through claudeq")
    if not _ok:
        _finding(f, "guardrail-inert", "high", "a Claude spawn point goes around the box slot",
                 "`claudeq.py audit` (C26) found a launch that does not take the box slot — a "
                 "reservation every other project has to schedule around. Its output: " + _out)
    _ok, _out = _selfcheck(["models.py", "check"])
    if not _ok:
        _finding(f, "guardrail-inert", "high", "models.py check fails — a local-model role cannot be served",
                 "a job asking models.require() for that role exits 75. Its output: " + _out)
    _ok, _out = _selfcheck(["gpu.py", "selftest"], "passed")
    if not _ok:
        _finding(f, "guardrail-inert", "high",
                 "gpu.py selftest fails — the GPU queue's ordering rules no longer hold",
                 "Stocks-first ordering on the one GPU keeps local jobs out of the trading path. "
                 "Its output: " + _out)
    _ok, _out = _selfcheck(["publish.py", "selftest"], "passed")
    if not _ok:
        _finding(f, "guardrail-inert", "high",
                 "publish.py selftest fails — the leak scanner could let a secret through",
                 "the scanner is the only gate between the private repos and GitHub. Its output: " + _out)
    # The phone's Remote Control sign-in died 09-24 → 09-28 with every canary green (memo
    # 2026-09-28 from home). healthcheck's remote-control-auth item and the login-mode re-sign-in
    # both run on claude-relogin.py; its selftest is their proof (fixtures only, <10 s, no push).
    _ok, _out = _selfcheck(["claude-relogin.py", "selftest"], "ALL PASS")
    if not _ok:
        _finding(f, "guardrail-inert", "high",
                 "claude-relogin selftest fails — the phone sign-in canary or its re-sign-in can misread",
                 "run `python3 ~/maintenance/bin/claude-relogin.py selftest`; each FAIL line names the "
                 "case. Until it passes, a dead phone sign-in can sit unseen again. Its output: " + _out)

    # :8900 under systemd (2026-09-28, proposals/2026-09-28_always-on-under-systemd.md). The user
    # unit maintenance-dashboard is what brings the dashboard back, and its MemoryMax (+ a swap cap)
    # is what stops a runaway dashboard eating the memory pool the GPU, Ollama and the Stocks model
    # jobs share. Either one gone (unit disabled, a drop-in or an edit that lost the cap) is this
    # rule's case — and so is a rollback to the cron keepalive, which has no cap at all: that is a
    # deliberate deviation, muted in config/backoffice_mute.json with its reason (rule 7), never a
    # check that quietly stands down and leaves Box › Guardrails calling the unit armed. Cron has no
    # XDG_RUNTIME_DIR, so it is passed: without it `systemctl --user` answers nothing.
    try:
        _env = dict(os.environ, XDG_RUNTIME_DIR=os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}")
        _u = dict(ln.split("=", 1) for ln in subprocess.run(
            ["systemctl", "--user", "show", "maintenance-dashboard",
             "-p", "UnitFileState", "-p", "ActiveState", "-p", "MemoryMax", "-p", "MemorySwapMax"],
            capture_output=True, text=True, timeout=15, env=_env).stdout.splitlines() if "=" in ln)
    except Exception as e:
        _u = {"error": f"{type(e).__name__}: {e}"}
    if not (_u.get("UnitFileState") == "enabled" and _u.get("ActiveState") == "active"
            and (_u.get("MemoryMax") or "").isdigit() and (_u.get("MemorySwapMax") or "").isdigit()):
        _cron = re.search(r"^[^#\n]*maintenance/dashboard/serve\.sh\s+(?:ensure|start)", sh(["crontab", "-l"]), re.M)
        _finding(f, "guardrail-inert", "high",
                 "the dashboard unit is not armed — :8900 has no supervisor or no memory cap",
                 f"the user unit maintenance-dashboard must be enabled and active, with MemoryMax and "
                 f"MemorySwapMax byte counts. It reads {_u or 'nothing (no user manager answered)'}"
                 f"{' — and a crontab keepalive owns :8900 (rolled back?), which has no cap at all' if _cron else ''}. "
                 f"`systemctl --user status maintenance-dashboard`; the unit's source is "
                 f"dashboard/maintenance-dashboard.service, installed with `systemctl --user link` — never "
                 f"copied, so an edit there plus daemon-reload is the fix. A deliberate rollback is muted "
                 f"with its reason: CRON_REGISTRY.md, Always-on.")

    # SMB share hygiene (2026-09-25, memo from stocks): without vfs_fruit, every Finder copy onto
    # the user-home share left a `._<name>` AppleDouble sidecar beside the file, and every
    # glob, rglob and the catalog's undeclared-file sweep read it as a real file. The fix is
    # Samba config (/etc/samba/smb.conf [global]: vfs objects = fruit streams_xattr), which a
    # package upgrade or a hand edit can undo without a sound — so prove it every morning:
    # fruit is still loaded, and no AppleDouble file has appeared on the box since the fix.
    _tp = sh(["testparm", "-s"], timeout=15) or ""
    if _tp and not re.search(r"^\s*vfs objects = .*\bfruit\b.*\bstreams_xattr\b", _tp, re.M):
        _finding(f, "guardrail-inert", "med",
                 "the SMB share no longer loads vfs_fruit — Finder copies leave ._ files again",
                 "`testparm -s` shows no `vfs objects = fruit streams_xattr`. Restore the block in "
                 "/etc/samba/smb.conf (the pre-change copy and the reasons are in "
                 "~/backups/maintenance/smb.conf.2026-09-25-pre-fruit and the [user-home] "
                 "comment), check package samba-vfs-modules is installed, then "
                 "`sudo smbcontrol smbd reload-config`.")
    for p in _appledouble_since(SMB_FRUIT_SINCE):
        rel = os.path.relpath(p, HOME)
        _finding(f, "appledouble-litter", "low", f"a Finder ._ file appeared: {rel}",
                 "An AppleDouble sidecar written after vfs_fruit went live (2026-09-25 22:20Z), so "
                 "a Mac copied onto the share without Apple's SMB extensions — most likely a "
                 "connection opened before the change (eject and reconnect the share in Finder). "
                 "It is metadata only; delete it once `file` says AppleDouble.",
                 _project_of("~/" + rel), key=f"appledouble:{rel}")

    # The memo-inbox SessionStart hook (2026-09-01) is how a directive reaches a session that
    # never read CLAUDE.md. If it is unregistered or not executable, memos go back to being a
    # sentence nobody obeys — and the daily triage nudges David instead of the session.
    try:
        _st = load(os.path.expanduser("~/.claude/settings.json"), {}) or {}
        _cmds = [h.get("command", "") for grp in (_st.get("hooks", {}).get("SessionStart") or [])
                 for h in grp.get("hooks", [])]
        _hook = os.path.join(MC, "bin", "hook-memo-inbox.py")
        if not any("hook-memo-inbox" in c for c in _cmds) or not os.access(_hook, os.X_OK):
            _finding(f, "guardrail-inert", "high",
                     "the memo-inbox SessionStart hook is not armed",
                     "bin/hook-memo-inbox.py must be listed under hooks.SessionStart in "
                     "~/.claude/settings.json and be executable; without it sessions only see "
                     "their memos if they happen to read the sentence in CLAUDE.md")
    except Exception:
        pass

    # The schedule preflight (2026-09-20, David: "set up a global rule to check schedules
    # around automated jobs when these are created"). §2a listed what a cron job ships with
    # and this pass audited it the morning after; the hook is what moves the check to the
    # moment of creation. Unregistered or unexecutable, and we are back to finding five
    # undeclared jobs the next day — which is the miss that prompted it.
    try:
        _st = load(os.path.expanduser("~/.claude/settings.json"), {}) or {}
        _cmds = [h.get("command", "") for grp in (_st.get("hooks", {}).get("PreToolUse") or [])
                 for h in grp.get("hooks", [])]
        _hook = os.path.join(MC, "bin", "hook-guard-schedule.py")
        _chk = os.path.join(MC, "bin", "schedule-check.py")
        if not any("hook-guard-schedule" in c for c in _cmds) or not os.access(_hook, os.X_OK):
            _finding(f, "guardrail-inert", "high",
                     "the schedule-check PreToolUse hook is not armed",
                     "bin/hook-guard-schedule.py must be listed under hooks.PreToolUse "
                     "(matcher Bash) in ~/.claude/settings.json and be executable, or a "
                     "crontab install goes in unchecked again")
        elif subprocess.run([sys.executable, _hook, "selftest"], capture_output=True,
                            text=True, timeout=30).returncode != 0:
            # ...and it must tell an install from a mention (2026-09-26: prose in heredocs that
            # said "crontab" was 4 of its 5 BLOCKs — a hook that cries wolf gets turned off)
            _finding(f, "guardrail-inert", "high",
                     "the schedule-check hook misreads commands — its selftest fails",
                     "run `python3 ~/maintenance/bin/hook-guard-schedule.py selftest`: each FAIL is a "
                     "command it would call an install (or miss). A hook that blocks prose gets "
                     "switched off; one that misses an install is not a guard.")
        elif os.path.exists(_chk):
            # ...and the checker it calls must still run. A hook that shells out to a broken
            # script is the guardrail-inert pattern one level down.
            _rc = subprocess.run([sys.executable, _chk, "--audit", "--quiet"],
                                 capture_output=True, text=True, timeout=120)
            if _rc.returncode not in (0, 1, 2):
                _finding(f, "guardrail-inert", "high",
                         "schedule-check.py does not run — the crontab hook is calling a "
                         "broken checker",
                         f"`schedule-check.py --audit` exited {_rc.returncode}: "
                         + (_rc.stderr or "")[:240])
            elif _rc.returncode == 2:
                _finding(f, "schedule-blocker", "med",
                         "the live crontab has a schedule blocker",
                         "`schedule-check.py --audit` found a job that would be refused at "
                         "creation today — fix it, or record the deviation in "
                         "config/backoffice_mute.json with its reason: "
                         + " | ".join(l.strip() for l in (_rc.stdout or "").splitlines()
                                      if "[BLOCK]" in l)[:300])
    except Exception:
        pass

    pol = os.path.join(CONFIG, "notify_policy.json")
    try:
        _p = load(pol, None)
        assert _p and _p.get("rules"), "no rules"
    except Exception as e:
        _finding(f, "guardrail-inert", "high",
                 "notify_policy.json is unreadable — push tiering is off",
                 f"notify.sh fails OPEN by design, so every push goes through unthrottled "
                 f"until this parses again ({e}).")

    # The policy's own expectations table (2026-09-03). The 08-31 maintenance-channel mute
    # silently swallowed every non-Stocks agent session for three days; a rule edit that
    # breaks a pinned expectation now surfaces the next morning instead of when David asks.
    _st_out = sh([sys.executable, os.path.join(MC, "bin", "notify_policy.py"), "selftest"],
                 timeout=20) or ""
    if "FAIL" in _st_out or "expectations hold" not in _st_out:
        _finding(f, "guardrail-inert", "high",
                 "notify_policy selftest fails — a tiering rule no longer does what was agreed",
                 "run `python3 ~/maintenance/bin/notify_policy.py selftest`; each FAIL line "
                 "names the channel/title and the tier David asked for. Fix the rule, not "
                 "the expectation, unless he changed his mind: "
                 + _st_out.strip().replace("\n", " | ")[:300])

    # WRDS credential liveness (2026-08-31, asked for in memo hbs-database-credentials):
    # the Stocks desk's SQL fundamentals layer authenticates with a password that HBS/WRDS
    # can expire without warning — a dead credential should be a finding the day it dies,
    # not a surprise mid-teardown. One auth probe per daily pass, via the consuming stack's
    # own venv (if THAT can't connect, the layer is down in exactly the way that matters).
    # Skipped entirely when Stocks has no wrds credential configured.
    _wrds_cfg = os.path.expanduser("~/Stocks/_engine/config/sources.json")
    try:
        _has_wrds = "wrds" in (load(_wrds_cfg, {}) or {})
    except Exception:
        _has_wrds = False
    if _has_wrds:
        probe = sh([os.path.expanduser("~/Stocks/_engine/.venv/bin/python"), "-c",
                    "import json,os,psycopg2;"
                    "u=json.load(open(os.path.expanduser('~/Stocks/_engine/config/sources.json')))['wrds']['username'];"
                    "psycopg2.connect(host='wrds-pgdata.wharton.upenn.edu',port=9737,dbname='wrds',"
                    "user=u,sslmode='require',connect_timeout=25).close();print('OK')"],
                   timeout=45)
        if (probe or "").strip() != "OK":
            _finding(f, "credential-dead", "med",
                     "WRDS credential no longer authenticates",
                     "the Stocks desk's wrds_source.py adapter will fail on next use. "
                     "Password may have expired or the account lapsed (Masters accounts "
                     "pause over summer). Fix: David logs in at wrds-www.wharton.upenn.edu "
                     "to check the account, then re-runs "
                     "`~/Stocks/_engine/.venv/bin/python ~/Stocks/_engine/sources/wrds_source.py setup`.",
                     project="stocks")

    # 17. pushes that dodge the choke point entirely. notify.sh is where tiering, the daily
    #     cap and the ledger live; a direct ntfy.sh post is invisible to all three.
    ALLOWED_DIRECT = {
        "maintenance/bin/healthcheck.sh":  "deliberate — the outage alarm must not route "
                                           "through code that can throttle it",
        "maintenance/bin/notify.sh":       "is the choke point",
        "maintenance/dashboard/server.py": "reads the ntfy poll, does not push",
        "Stocks/_engine/advised/desk.py":  "pushes to another person's topic",
        "Stocks/_engine/dashboard/advised_page.py": "pushes to another person's topic",
        "clientco-db/scripts/ntfy.py":      "project-local helper; reads the same ntfy.json",
        "maintenance/bin/backoffice.py":   "this rule quotes the pattern it looks for",
        "Stocks/_engine/agent/journal/ops/_sandbox/sitecustomize.py":
            "the proof sandbox — it names ntfy.sh to BLOCK every push a proof run attempts",
        "Stocks/_engine/agent/journal/ops/2026-09-12_hunt_proof_sandbox.py":
            "the proof that the sandbox blocks a raw ntfy.sh post (to a fake topic)",
    }
    # Copies are not call sites. ~/public/ is generated FROM the private repos, worktrees are
    # throwaway checkouts, and vendored/venv trees are not ours — flagging them would make the
    # rule cry wolf every day and get muted, which is how a good rule dies.
    SKIP = ("/.claude/", "/worktrees/", "/.venv/", "/node_modules/", "/site-packages/")
    hits = sh(["grep", "-rIl", "--include=*.py", "--include=*.sh", "--include=*.js",
               "ntfy.sh/", os.path.expanduser("~")], timeout=30) or ""
    for line in hits.splitlines():
        rel = line.replace(os.path.expanduser("~") + "/", "")
        if (not rel or rel in ALLOWED_DIRECT
                or rel.startswith((".", "archive/", "backups/", "public/", "poker-data/",
                                   "model-vault/", "memos/"))
                or any(k in "/" + rel for k in SKIP)):
            continue
        _finding(f, "notify-bypass", "med",
                 f"{rel} pushes to ntfy directly",
                 "route it through ~/maintenance/bin/notify.sh so box-wide tiering, the "
                 "per-channel daily cap and the notifications ledger can see it "
                 "(PROJECT_STANDARDS §1). If the bypass is deliberate, add it to "
                 "ALLOWED_DIRECT in backoffice.py with the reason.",
                 project=rel.split("/")[0])

    # 18. a project whose CLAUDE.md never points at the standards. The CLAUDE.md chain is
    #     the ONLY governance a session is guaranteed to read; doctrine nothing links to is
    #     doctrine nobody follows.
    for name in _project_dirs():
        cm = os.path.join(HOME, name, "CLAUDE.md")
        if os.path.exists(cm):
            try:
                if "PROJECT_STANDARDS" not in open(cm, errors="replace").read():
                    _finding(f, "standards-unlinked", "med",
                             f"{name}/CLAUDE.md does not reference PROJECT_STANDARDS",
                             "add the box-rules line so a session working here is bound to "
                             "the same guardrails as every other project",
                             project=name)
            except Exception:
                pass

    # 19. an in-dev label that has expired. config/dev.json makes a WIP service invisible to
    #     the watchdog and to every notification tier — which is right while it is being built
    #     and dangerous forever. The expiry is what keeps it a pause rather than an erasure.
    try:
        import importlib.util as _ilu
        _sp = _ilu.spec_from_file_location("_dev", os.path.join(HOME, "maintenance/bin/dev.py"))
        _dev = _ilu.module_from_spec(_sp); _sp.loader.exec_module(_dev)
        for e in _dev.expired():
            _finding(f, "dev-stale", "med",
                     f"{e.get('label')} is still labelled in-dev (expired {e.get('expires')})",
                     "ship it and delete the entry from config/dev.json so it can alert again, "
                     "or extend the date with a reason. While it is listed, the watchdog will "
                     "not page on it and none of its notifications can reach the phone.",
                     project=e.get("project", ""))
    except Exception:
        pass

    # 20. THE DATA CATALOG (2026-09-20). Every dataset on the box is declared by the project
    #     that owns it; these rules are what make "continuously updated" a mechanism rather
    #     than goodwill — a project that ignores the memo is a finding every morning until it
    #     declares. Rule logic and its fixture tests live in bin/catalog.py, so the guardrail
    #     can be proven to fire without running a whole daily pass.
    try:
        import catalog as _catalog
        for r in _catalog.audit():
            _finding(f, r["kind"], r["sev"], r["title"], r["detail"], r["project"],
                     fix=r.get("fix", "human"), key=_catalog_key(r))
    except Exception as e:
        _finding(f, "guardrail-inert", "high",
                 "the data catalog did not compile — every catalog rule is off",
                 f"bin/catalog.py audit raised {type(e).__name__}: {e}. While this is broken "
                 "nothing checks that projects declare their data, that declarations still "
                 "resolve, or that a feed has gone stale.")

    # …and the armed-check for it, the same shape as the notify_policy one: a rule nobody has
    # seen fire is a rule nobody should trust (box rule: `guardrail-inert`).
    _cat_st = sh([sys.executable, os.path.join(MC, "bin", "catalog.py"), "selftest"],
                 timeout=40) or ""
    if "FAIL" in _cat_st or not re.search(r"^(\d+)/\1 passed", _cat_st.strip().splitlines()[-1]
                                          if _cat_st.strip() else ""):
        _finding(f, "guardrail-inert", "high",
                 "catalog selftest fails — a data-catalog rule no longer fires",
                 "run `python3 ~/maintenance/bin/catalog.py selftest`; each FAIL line names "
                 "the contract or rule that broke. The adoption guardrail, the staleness rule "
                 "and the irreplaceable-and-unbacked rule are all proven by that fixture "
                 "suite: " + _cat_st.strip().replace("\n", " | ")[-300:])

    # The dashboard's own tab tests. They are shim-DOM renders against the LIVE :8900
    # payloads, so they catch the thing a syntax check cannot: a field renamed in
    # server.py that leaves a panel blank on David's phone. Nothing ran them until now
    # and test_claude_tab.js had been throwing for weeks unnoticed — a guardrail nobody
    # executes is the `guardrail-inert` case applied to a test.
    _dash = os.path.join(MC, "dashboard")
    _spid, _snewer = _dashboard_stale(_dash)
    if _snewer:
        _finding(f, "dashboard-broken", "high",
                 "the dashboard is running older code than its files",
                 f"{', '.join(_snewer)} changed after the server started (pid {_spid}), and the "
                 f"server reads {'it' if len(_snewer) == 1 else 'them'} only at start — while the page "
                 f"itself is read fresh on every request, so the phone gets the new page against "
                 f"the old API. Restart it: `~/maintenance/dashboard/serve.sh start`. The tab tests "
                 f"and dashboard/check.py were skipped this pass: against a half-updated server they "
                 f"report only this mismatch, under the wrong names.", project="maintenance")
    if shutil.which("node") and not _snewer:
        for _t, _lbl in (("test_overview_tab.js", "Overview"), ("test_claude_tab.js", "Claude"),
                         ("test_catalog_tab.js", "Catalog")):
            _tp = os.path.join(_dash, _t)
            if not os.path.exists(_tp):
                continue
            try:
                _r = subprocess.run(["node", _tp], cwd=_dash, capture_output=True,
                                    text=True, timeout=90)
                _ok, _out = _r.returncode == 0, (_r.stdout + _r.stderr)
            except Exception as _e:
                _ok, _out = False, f"{type(_e).__name__}: {_e}"
            if not _ok:
                _finding(f, "guardrail-inert", "high",
                         f"dashboard {_lbl} tab test fails",
                         f"`node dashboard/{_t}` does not pass. The tab renders from the live "
                         f"payload, so this is a panel that is blank or wrong on the dashboard "
                         f"right now, not a style complaint: "
                         + " | ".join(l for l in _out.splitlines()
                                      if "FAIL" in l or "THREW" in l or "Error" in l)[-300:],
                         project="maintenance")

    # 21. the dashboard itself. "If it isn't on the dashboard, it isn't real" cuts both ways:
    #     a panel that silently renders nothing is worse than a missing one, because the page
    #     still looks alive. dashboard/check.py executes the SERVED page and walks the catalog
    #     click path; a syntax check cannot catch a name collision or a dead render.
    _dash = ("ALL GREEN (skipped: server restart pending)" if _snewer else
             sh([sys.executable, os.path.join(MC, "dashboard", "check.py")], timeout=180) or "")
    if "ALL GREEN" not in _dash:
        _finding(f, "dashboard-broken", "high",
                 "a dashboard check is failing",
                 "run `python3 ~/maintenance/dashboard/check.py`. It executes the served page "
                 "(a top-level throw blanks everything), renders the catalog with no network, "
                 "walks the click path, and refuses duplicate function names — the collision "
                 "that silently swallowed every catalog render on 2026-09-20. Output: "
                 + (_dash.strip().replace("\n", " | ")[-400:] or "no output — is the "
                    "dashboard running? dashboard/serve.sh start"))

    # 21c. the backup layer's credential refusal. ~/.claude was backed up for the first time
    #      on 2026-09-21 and it sits beside .credentials.json and headless-token, so the
    #      source is a NARROW include list — safe only while nobody widens it, and a widening
    #      looks harmless in review. backup.py scans each archive's listing and discards
    #      anything credential-shaped unless the source opted in BY NAME. Proven, not assumed.
    _bk_st = sh([sys.executable, os.path.join(MC, "bin", "backup.py"), "selftest"],
                timeout=120) or ""
    if "ALL PASS" not in _bk_st:
        _finding(f, "guardrail-inert", "high",
                 "backup selftest fails — a credential could reach an archive",
                 "run `python3 ~/maintenance/bin/backup.py selftest`. Its fixtures assert "
                 "that an unnamed credential is refused, that a `secrets_ok` name is "
                 "honoured (clientco-db's .env is a deliberate one), and that a NEW secret "
                 "beside an opted-in one still trips. Output: "
                 + _bk_st.strip().replace("\n", " | ")[-300:])

    # 22. systemd units that can never work, and units stuck in a restart storm (2026-09-21).
    #     Found by hand twice and by a check never: five USER units at ~143,000 restarts each
    #     after their folders were archived (09-08, 3.8G of journal), then a SYSTEM unit left
    #     behind by the same family-vault retirement still looping 190,608 times on 09-21 —
    #     because the 09-08 sweep had only looked at ~/.config/systemd/user/. Retiring a
    #     project reliably removes the folder and the cron lines; the unit file is the step
    #     that gets forgotten, and Restart=always makes forgetting expensive. Both scopes.
    _du = sh([sys.executable, os.path.join(MC, "bin", "deadunits.py"), "scan", "--json"],
             timeout=90)
    try:
        for h in json.loads(_du or "[]"):
            _finding(f, "unit-dead", "med",
                     f"{h['unit']} ({h['scope']}) is looping or cannot start",
                     f"{h['why']}. Either the unit outlived what it ran (retire it: "
                     "`systemctl [--user] disable --now <unit>`, move the unit file to "
                     "~/archive/<project>/systemd/ and add restart notes to ARCHIVE.md), or "
                     "the thing it needs is genuinely absent and the retry cadence should be "
                     "backed off with a drop-in rather than left hammering. Not a finding you "
                     "mute without deciding which.")
    except Exception:
        pass

    # 22b. and the check above must itself be provable — rule `guardrail-inert`.
    _du_st = sh([sys.executable, os.path.join(MC, "bin", "deadunits.py"), "selftest"],
                timeout=60) or ""
    if "ALL PASS" not in _du_st:
        _finding(f, "guardrail-inert", "high",
                 "deadunits selftest fails — the dead-unit rule no longer fires",
                 "run `python3 ~/maintenance/bin/deadunits.py selftest`; each FAIL line names "
                 "the fixture that stopped being detected. Its fixtures are the two real "
                 "misses (09-08 user units, 09-21 system unit) plus the WorkingDirectory "
                 "'!'/'-' prefixes that false-positived 14 healthy desktop units on the first "
                 "draft. Output: " + _du_st.strip().replace("\n", " | ")[-300:])

    # 22c. the sentinel re-reads a flagged job's newest run before paging (memo 2026-09-23,
    #      stocks): its snapshot is taken before a GPU wait that ran 1905 s on 09-23, and it
    #      paged CRITICAL on a mcp_sync DNS failure two clean runs after it had recovered.
    #      The selftest replays that incident; if the recheck stops firing, this says so.
    _sn_st = sh([sys.executable, os.path.join(MC, "bin", "sentinel.py"), "selftest"],
                timeout=60) or ""
    if "ALL PASS" not in _sn_st:
        _finding(f, "guardrail-inert", "high",
                 "sentinel selftest fails — it can page on a job that already recovered",
                 "run `python3 ~/maintenance/bin/sentinel.py selftest`; it replays the 09-23 "
                 "mcp_sync page (14:15 failure read at 14:20, paged 14:51 after clean 14:30/14:45 "
                 "runs) and two cases that must still page. Output: "
                 + _sn_st.strip().replace("\n", " | ")[-300:])

    # 23. crew-unclaimed (v2.5, 2026-09-26; v2.7 scripts too): every scheduled job belongs to exactly
    #     one named agent in config/crew.json (David 2026-09-26: "make all automated jobs assigned to
    #     an agent"), or the Overview's crew and every "who ran this" line on the page quietly miss it.
    #     A job no agent claims (or two do), a declared agent with no job that is not on demand, and an
    #     agent whose `does` is not a badge family are findings; an accepted one is muted in
    #     backoffice_mute.json.
    _cw = sh([sys.executable, os.path.join(MC, "dashboard", "tt_crew.py"), "check"], timeout=90)
    try:
        for h in json.loads(_cw or "null") or []:
            _finding(f, "crew-unclaimed", "low", h["text"],
                     "config/crew.json is Mission Control's display layer over the scheduled jobs "
                     "(dashboard/tt_crew.py). Claim the job with a `cmd` substring on the agent whose "
                     "work it feeds or keeps running (the longest match wins; a script too), add a "
                     "new agent for it, or mark an agent "
                     "`on_demand`. `python3 dashboard/tt_crew.py selftest` proves the whole fleet "
                     "is claimed once.", project="Mission Control", fix="human", key=h["id"])
    except Exception:
        pass
    # 23b. and the namer itself must be provable — rule `guardrail-inert`
    _cw_st = sh([sys.executable, os.path.join(MC, "dashboard", "tt_crew.py"), "selftest"], timeout=120) or ""
    if not re.search(r"^0 failure\(s\)$", _cw_st, re.M):
        _finding(f, "guardrail-inert", "med",
                 "crew selftest fails — the page can name the wrong agent",
                 "run `python3 ~/maintenance/dashboard/tt_crew.py selftest`; it resolves the real "
                 "queue names (ops:signals, research:COO, the Bench in the VP's window), the tools "
                 "rule and every agent's badge family, and checks every live job is claimed once. Output: " + _cw_st.strip().replace("\n", " | ")[-300:])

    # 24. mdreader-drift (2026-09-27): the box's one markdown reader lives in shared/mdreader and every dashboard
    #     serves a COPY of its dist/. A copy left behind by a rebuild reads documents the old way on that
    #     dashboard, and nothing else would notice. One low finding per project, naming each stale copy. The rule
    #     is armed only while the canonical VERSION reads — without it every copy would pass: guardrail-inert.
    _mdr_cur = _mdr_version(os.path.join(MDREADER_DIST, "mdreader.js"))
    if not _mdr_cur:
        _finding(f, "guardrail-inert", "med",
                 "the mdreader drift rule cannot read the shared reader's VERSION — it checks nothing",
                 "~/maintenance/shared/mdreader/dist/mdreader.js is missing or carries no `var VERSION = \"…\"` stamp, "
                 "so no vendored copy can be compared with it. Rebuild it: `python3 ~/maintenance/shared/mdreader/build.py` "
                 "then `./run_tests.sh` there.", project="maintenance")
    else:
        for _proj, _rows in _mdreader_drift(_mdreader_copies(), _mdr_cur).items():
            _finding(f, "mdreader-drift", "low",
                     f"{_proj} serves an out-of-date copy of the shared markdown reader",
                     "; ".join(f"`{p.replace(HOME, '~', 1)}` {why}" + (f" ({v})" if v else "") for p, v, why in _rows)
                     + f". The current build is {_mdr_cur}. Fix: vendor the current ~/maintenance/shared/mdreader/dist "
                     "copy (a copy is never patched in place: change src/ there, rebuild, run its tests, re-vendor), "
                     "and where the page names the file by version (Mission Control's vendor/mdreader.<version>.js) "
                     "rename it and its <script> tag together.",
                     project=_proj, fix="memo" if _proj != "maintenance" else "human", key=_proj)

    uniq = {}
    for x in f:
        uniq.setdefault(x["id"], x)
    return sorted(uniq.values(), key=lambda x: SEV[x["sev"]])


def expire_mutes(today=None):
    """A mute can carry an end date: `_until: {entry: "YYYY-MM-DD"}` (2026-09-26). Past it, the
    entry leaves `muted` for `_expired` (with its reason and the date), so the finding is back the
    next morning — a deferral ("stop pinging and try again next month") must not become a mute
    nobody remembers, the same rule config/dev.json keeps for in-development labels. Done here,
    in the file, so every reader of the mute list (this pass, schedule-check, the dashboard) sees
    the same active list. -> the entries that expired."""
    cfg = load(MUTE, {})
    until = cfg.get("_until") or {}
    today = today or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    gone = [k for k, d in until.items() if str(d) < today]
    if not gone:
        return []
    for k in gone:
        if k in cfg.get("muted", []):
            cfg["muted"].remove(k)
        cfg.setdefault("_expired", {})[k] = {"until": until.pop(k),
                                             "reason": (cfg.get("_reasons") or {}).pop(k, None)}
    save(MUTE, cfg, indent=1)
    return gone


def merge_findings(fresh):
    """Fold today's findings into the durable store. Fingerprints make 'new' meaningful:
    a finding that has been open for a week must not push again, and one that disappears
    is recorded as resolved rather than forgotten."""
    store = load(FINDINGS, {})
    expire_mutes()
    muted = set(load(MUTE, {}).get("muted", []))
    # an open record under the same KEY is the same problem whose title's number moved on
    # ("89h" -> "96h"): it is updated in place, keeping its id and first_seen, instead of
    # resolving and re-opening as "new" — which pushed the same finding every morning.
    open_by_key = {}
    for fid, x in store.items():
        if x.get("state") == "open":
            open_by_key.setdefault(x.get("key") or _key(fid), fid)
    # records written before keys existed, and not found under their own id today: the first
    # keyed pass matches each (once) to today's finding whose id differs only in its numbers,
    # instead of resolving it and re-opening the same problem as "new" (_legacy_of)
    fresh_ids = {f["id"] for f in fresh}
    legacy = [fid for fid, x in store.items()
              if x.get("state") == "open" and not x.get("key") and fid not in fresh_ids]
    seen, new = set(), []
    for f in fresh:
        if f["id"] in muted or f["kind"] in muted or f.get("key") in muted:
            continue
        sid = f["id"] if (store.get(f["id"]) or {}).get("state") == "open" else \
            open_by_key.get(f.get("key"), f["id"])
        if sid == f["id"] and (store.get(sid) or {}).get("state") != "open":
            old_id = next((x for x in legacy if _legacy_of(f, x)), None)
            if old_id:
                legacy.remove(old_id)
                sid = old_id
        seen.add(sid)
        old = store.get(sid)
        if old and old.get("state") == "open":
            old.update(title=f["title"], detail=f["detail"], sev=f["sev"], last_seen=now(),
                       key=f.get("key") or old.get("key"))
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

        elif f["kind"] == "experiment-landed":
            if not dry and _adopt_experiment(f["title"]):
                done.append((f, f"moved a shipped technique to Adopted in experiments.md"))
            elif dry:
                done.append((f, "would move a shipped technique to Adopted"))

        elif f["kind"] == "experiment-dead-pid":
            st_path = os.path.join(STATE, "experiments.json")
            st = load(st_path, {})
            changed = False
            for k, v in st.items():
                if v.get("status") == "running" and not _pid_alive(v.get("pid", -1)):
                    v["status"] = "finished"
                    changed = True
            if changed and not dry:
                save(st_path, st)
            if changed:
                done.append((f, "reaped a finished research process's stale 'running' state"))

        elif f["kind"] == "diagram-unrendered":
            # A repair is claimed only when it happened (critic C5, 2026-09-24): this used to
            # record "re-rendered" whatever the render did, so a diagram d2 refused stayed
            # unrendered while the janitor reported a fix every morning.
            if dry:
                done.append((f, "would re-render the architecture diagrams"))
                continue
            try:
                rr = subprocess.run([os.path.join(MC, "bin/render-diagrams.sh")],
                                    capture_output=True, text=True, timeout=180)
                rc, tail = rr.returncode, (rr.stdout + rr.stderr).strip().splitlines()[-1:]
            except Exception as e:
                rc, tail = -1, [f"{type(e).__name__}: {e}"]
            left = _diagram_state().get("unrendered", [])
            if rc == 0 and not left:
                done.append((f, "re-rendered the architecture diagrams"))
            else:
                print(f"  render did not repair it (exit {rc}; still unrendered: "
                      f"{', '.join(left) or 'none'}) {' '.join(tail)[:160]}")

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


def _adopt_experiment(finding_title):
    """Move a shipped technique from Queue to Adopted, carrying its evidence.

    Mission Control's own file, mechanical to do, and the thing that keeps the queue
    honest — so the janitor does it rather than asking. The entry keeps its memo link and
    gains the ledger row that proves it landed.
    """
    path = os.path.join(MC, "experiments.md")
    cen = load(CENSUS, {}).get("experiments", {})
    name = finding_title.split("'")[1] if "'" in finding_title else ""
    item = next((x for x in cen.get("landed", []) if _short(x["title"]) == name), None)
    if not item:
        return False
    text = open(path).read()
    block = None
    for b in re.split(r"\n(?=### )", text.split("## Queue", 1)[-1].split("## Adopted", 1)[0]):
        if b.strip().startswith("### ") and item["title"] in b:
            block = b.rstrip() + "\n"
            break
    if not block or block not in text:
        return False
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    adopted = block.rstrip() + f"\n- **Adopted {stamp}** — shipped and recorded in the memo ledger.\n"
    text = text.replace(block, "")
    text = text.replace("## Adopted\n(nothing yet)\n", "## Adopted\n")
    head, sep, rest = text.partition("## Adopted")
    if sep and "### " not in head.split("## Queue", 1)[-1]:
        head = re.sub(r"(## Queue\n)\s*", r"\1(empty — the Friday frontier scan refills it)\n\n", head)
        text = head + sep + rest
    idx = text.index("## Adopted") + len("## Adopted\n")
    text = text[:idx] + "\n" + adopted + text[idx:]
    open(path, "w").write(text)
    return True


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
    # sh() strips its output, which took the leading space off the FIRST porcelain line
    # (" M CRON_REGISTRY.md" -> "M CRON_REGISTRY.md"), so line[3:] cut a letter off that path
    # and another session's edit to it went unnoticed (2026-09-24): parse the status code off
    # instead of slicing a fixed width.
    for line in sh(["git", "-C", MC, "status", "--porcelain"]).splitlines():
        f = re.sub(r"^\s*\S{1,2}\s+", "", line).split(" -> ")[-1].strip().strip('"')
        if f in paths:
            out.add(f)
    return out


def _commit(paths, msg, skip=()):
    """-> the new commit's short sha, or "" when nothing was committed.

    Commits ONLY `paths` (`git commit -- <paths>`, 2026-09-24): a bare `git commit` takes
    whatever else is staged, and other sessions share this working tree."""
    paths = [p for p in paths if p not in skip and os.path.exists(os.path.join(MC, p))]
    if not paths:
        return ""
    sh(["git", "-C", MC, "add", "--"] + paths)
    if sh(["git", "-C", MC, "diff", "--cached", "--name-only", "--"] + paths):
        before = sh(["git", "-C", MC, "rev-parse", "--short", "HEAD"])
        sh(["git", "-C", MC, "commit", "-q", "-m", msg + "\n\nCo-Authored-By: Claude Opus 5 "
            "<noreply@anthropic.com>", "--"] + paths)
        sha = sh(["git", "-C", MC, "rev-parse", "--short", "HEAD"])
        if not sha or sha == before:        # the commit failed: HEAD did not move — claim no sha
            return ""
        sh(["git", "-C", MC, "push", "-q", "origin", "master"], timeout=60)
        return sha
    return ""


# ---------------------------------------------------------------- David's answers

def _decide_mod():
    """dashboard/tt_decide.py, the one reader/writer of state/decisions.jsonl."""
    d = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")
    if d not in sys.path:
        sys.path.insert(0, d)
    import tt_decide
    return tt_decide


def _verify_david(e, fresh, store):
    """A "I'll do it myself" answer, checked against the box. -> a result sentence when it is
    verified, else None (it stays queued; the daily check reports it)."""
    rec = store.get(e.get("finding_id") or "") or next(
        (x for x in store.values() if x.get("key") == e["key"]), {})
    if e.get("option") == "rotated":
        m = re.search(r"~/\.secrets/([\w.-]+)", rec.get("detail") or "")
        p = os.path.join(HOME, ".secrets", m.group(1)) if m else None
        # rewritten AFTER HE ANSWERED — what the option's consequence and the daily check's
        # prompt both promise — not merely after the leak was found: on 09-24 the password was
        # changed and then restored to the leaked value (~/.secrets/sudo rewritten at 17:00Z,
        # after the finding's 11:35Z first_seen), which that test would have closed as rotated
        base = int(e.get("at") or 0)
        if p and base and os.path.exists(p) and os.path.getmtime(p) > base:
            return (f"verified: ~/.secrets/{m.group(1)} was rewritten "
                    f"{dt_from(os.path.getmtime(p))}Z, after your answer")
        return None
    if e.get("option") == "purge":
        # a status item that is not a finding (the rejected publish push, 2026-09-25) has no
        # record here, so the repo comes from the option's own words the answer stored
        m = (re.search(r"([\w-]+-public)/", rec.get("detail") or "")
             or re.search(r"~/public/([\w-]+-public)\b", e.get("consequence") or ""))
        d = os.path.join(HOME, "public", m.group(1)) if m else None
        if d and os.path.isdir(os.path.join(d, ".git")):
            local = sh(["git", "-C", d, "rev-parse", "HEAD"])
            remote = (sh(["git", "-C", d, "ls-remote", "origin", "refs/heads/main"], timeout=30) or "").split()
            if local and remote and remote[0] == local:
                return f"verified: {m.group(1)} on GitHub matches the cleaned local history"
        return None
    # gone = found today under neither its key, nor its id, nor (an answer given before findings
    # had keys) the id that differs from today's only in its numbers — keyed on the key alone,
    # every answer from before the first keyed pass read as "no longer found"
    fid = e.get("finding_id")
    if fid and not any(f.get("key") == e["key"] or f["id"] == fid or _legacy_of(f, fid) for f in fresh):
        return "verified: the janitor no longer finds it"
    return None


def apply_decisions(fresh, dry=False):
    """Finish what David answered on the dashboard that belongs to the janitor.

    * mute (code): the item was hidden the moment he tapped; here it becomes a line in
      config/backoffice_mute.json with a `_reasons` entry quoting his answer — rule 7: the mute
      list, not a code path, is the record of what the box lives with. Keyed on the finding's
      stable key, so it outlives a number in the title.
    * extend-dev (code): the in-dev label's new expiry goes into config/dev.json.
    * david: an answer that says he will do it himself is checked against the box (the file
      changed, the remote matches, the finding is gone) and closed when it holds.
    Claude answers are not touched: they belong to the daily check (bin/decisions.py).
    -> [(event, what happened)]"""
    try:
        td = _decide_mod()
        lat = td.latest()
    except Exception as e:
        print(f"  decisions unreadable: {type(e).__name__}: {e}")
        return []
    store = load(FINDINGS, {})
    dev_path = os.path.join(CONFIG, "dev.json")
    mute_cfg, dev_cfg = load(MUTE, {}), load(dev_path, {})
    touched, out, file_of = set(), [], {}
    # whatever another session already had open stays theirs to commit — asked BEFORE this
    # pass writes anything (2026-09-24: asked after, our own write read as "another session's",
    # so the mute list and dev.json were never committed and every answer claimed otherwise)
    theirs = set() if dry else dirty_before({"config/backoffice_mute.json", "config/dev.json"})
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    vals = _secret_values()
    for key, e in sorted(lat.items(), key=lambda kv: kv[1].get("at") or 0):
        st, ex, act = e.get("status"), e.get("exec"), e.get("action")
        note = _scrub_secrets((e.get("note") or "").replace("\n", " "), vals)[:300]
        if ex == "code" and st == "applied" and act == "mute":
            f = next((x for x in fresh if x.get("key") == key or x["id"] == e.get("finding_id")
                      or x["id"].lower() == key), None) or next(
                (x for x in fresh if _legacy_of(x, e.get("finding_id"))), None)
            entry = (f or {}).get("key") or key
            mute_cfg.setdefault("muted", [])
            if entry not in mute_cfg["muted"]:
                mute_cfg["muted"].append(entry)
            mute_cfg.setdefault("_reasons", {})[entry] = (
                f"David via Mission Control {stamp} (decision:{e['id']}): {e.get('label') or 'mute'} — "
                + (note or "no reason given") + f" [{e.get('title') or key}]")
            touched.add("config/backoffice_mute.json")
            file_of[e["id"]] = "config/backoffice_mute.json"
            out.append((e, f"muted {entry} in config/backoffice_mute.json"))
        elif ex == "code" and st == "applied" and act == "extend-dev":
            ent = next((x for x in dev_cfg.get("entries", []) if x.get("label") == e.get("dev_label")), None)
            if not ent or not e.get("expires"):
                out.append((e, None))
                continue
            ent["expires"] = e["expires"]
            ent["why"] = (ent.get("why") or "").rstrip() + (
                f" Extended to {e['expires']} by David via Mission Control {stamp} (decision:{e['id']})"
                + (f": {note}" if note else "") + ".")
            touched.add("config/dev.json")
            file_of[e["id"]] = "config/dev.json"
            out.append((e, f"in-dev label {e.get('dev_label')} now expires {e['expires']} (config/dev.json)"))
        elif ex == "david" and st == "queued":
            res = _verify_david(e, fresh, store)
            if res:
                out.append((e, res))
    if dry or not out:
        return out
    if "config/backoffice_mute.json" in touched:
        save(MUTE, mute_cfg, indent=1)
    if "config/dev.json" in touched:
        save(dev_path, dev_cfg, indent=2)
    sha = _commit(sorted(touched), "Back office: David's answers from Mission Control — "
                  + "; ".join(w for _, w in out if w)[:160], skip=theirs) if touched else ""
    if theirs & touched:
        print("  left uncommitted (another session has them open): " + ", ".join(sorted(theirs & touched)))
    for e, what in out:
        if what is None:
            td.set_status(e["id"], "failed", "the in-dev entry is gone from config/dev.json", by="janitor")
        elif file_of.get(e["id"]) in theirs or (file_of.get(e["id"]) and not sha):
            td.set_status(e["id"], "done", what + " (written, not committed: "
                          + ("another session has the file open)" if file_of[e["id"]] in theirs
                             else "the commit did not happen)"), by="janitor")
        else:
            td.set_status(e["id"], "done", what, sha if file_of.get(e["id"]) else "", by="janitor")
    return out


# The dashboard files the running server reads ONCE, at start (fix round 2026-09-24). index.html
# is read on every request and tt_*.py hot-reload, so after a merge without a restart the phone
# gets the NEW page against the OLD API: the tab tests and check.py then fail with 404s, and the
# janitor filed two high findings (and a push) blaming the tabs instead of the missing restart.
_READ_AT_START = ("server.py", "usage.py", "claudecfg.py")


def _proc_start(pid):
    """Epoch the process started (/proc/<pid>/stat field 22 + btime), or None."""
    try:
        with open(f"/proc/{int(pid)}/stat") as f:
            ticks = int(f.read().rsplit(")", 1)[1].split()[19])
        with open("/proc/stat") as f:
            btime = next(int(ln.split()[1]) for ln in f if ln.startswith("btime"))
        return btime + ticks / os.sysconf("SC_CLK_TCK")
    except Exception:
        return None


def _dashboard_stale(dash=None):
    """(pid, [files changed since that process started]) for the running dashboard — found the
    way dashboard/serve.sh finds it — or (None, []) when it is not running or cannot be read."""
    dash = dash or os.path.join(MC, "dashboard")
    app = os.path.join(dash, "server.py")
    pids = [int(x) for x in sh(["pgrep", "-f", f"python3 {app}"]).split() if x.isdigit()]

    def is_server(p):
        # the server itself — `python3 <app>` — not a shell whose command line merely mentions it
        try:
            argv = open(f"/proc/{p}/cmdline", "rb").read().decode(errors="replace").split("\0")
        except OSError:
            return False
        return len(argv) >= 2 and "python" in os.path.basename(argv[0]) and argv[1] == app
    started = [(p, _proc_start(p)) for p in pids if is_server(p)]
    started = [(p, t) for p, t in started if t]
    if not started:
        return None, []
    pid, t0 = max(started, key=lambda x: x[1])
    newer = []
    for n in _READ_AT_START:
        try:
            if os.path.getmtime(os.path.join(dash, n)) > t0 + 2:
                newer.append(n)
        except OSError:
            pass
    return pid, newer


def _clean_stale_pyc(dirs=None):
    """Delete byte-code caches whose source has changed since they were compiled — exactly the
    test CPython itself applies (the source mtime recorded in the .pyc header), so nothing is
    removed that Python would not rewrite on the next import anyway. 2026-09-24: a stale
    bin/__pycache__/publish.cpython-312.pyc still held the pre-09-23 publish.py, credential
    literal included. -> the removed paths."""
    import struct
    gone = []
    for d in dirs or (os.path.join(MC, "bin"), os.path.join(MC, "dashboard")):
        cache = os.path.join(d, "__pycache__")
        for fn in (sorted(os.listdir(cache)) if os.path.isdir(cache) else []):
            if not fn.endswith(".pyc"):
                continue
            src, pyc = os.path.join(d, fn.split(".")[0] + ".py"), os.path.join(cache, fn)
            try:
                with open(pyc, "rb") as fh:
                    head = fh.read(16)
                flags = struct.unpack("<I", head[4:8])[0]
                if flags:                 # hash-based pyc: not the mtime scheme, leave it alone
                    continue
                mt = struct.unpack("<I", head[8:12])[0]
                if not os.path.exists(src) or int(os.path.getmtime(src)) & 0xFFFFFFFF != mt:
                    os.unlink(pyc)
                    gone.append(pyc)
            except (OSError, struct.error):
                continue
    return gone


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
    # David's dashboard answers first, so a mute he chose skips this very pass's merge
    decided = apply_decisions(fresh, dry=dry)
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
    for e, what in decided:
        print(f"  decided: {what or 'could not apply'} (decision:{e['id']})")
    if not dry:
        for p in _clean_stale_pyc():
            print(f"  removed a stale byte-code cache: {os.path.relpath(p, MC)}")
    for f in new:
        print(f"  new [{f['sev']}] {f['title']}")
    with open(HISTORY, "a") as fh:
        fh.write(json.dumps({"at": now(), "new": len(new), "fixed": len(fixed),
                             "resolved": len(resolved), "open": len(still_open),
                             "briefed": len(status), "decided": len(decided)}) + "\n")
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
        print(f"         {_scrub_secrets(f['detail'])[:110]}")      # the monthly sweep runs this
        print(f"         {f['kind']} · {f['project'] or 'box'} · open {age:.0f}d · fix={f['fix']}")
    fixed = [f for f in store.values() if f.get("state") == "fixed"]
    if fixed:
        print(f"\n{len(fixed)} auto-fixed to date")


def selftest():
    """The 2026-09-24 paths, on throwaway fixtures: stable keys, merge and mute by key, David's
    answers (mute / extend-dev / verified), the leak lines that never carry the match, the
    stored-copy scrub, the diagram rules and the render that must not claim a false repair.
    Nothing here touches the live state, config or git."""
    import tempfile
    global FINDINGS, MUTE, CONFIG, MC, _commit
    ok = True

    def check(name, cond, info=""):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name + (f"  — {info}" if info and not cond else ""))
        ok = ok and bool(cond)

    saved = (FINDINGS, MUTE, CONFIG, MC, _commit, os.environ.get("MC_DECISIONS_FILE"))
    t = tempfile.mkdtemp(prefix="backoffice-selftest.")
    try:
        FINDINGS, MUTE, CONFIG, MC = f"{t}/findings.json", f"{t}/config/mute.json", f"{t}/config", t
        os.makedirs(f"{t}/config")
        os.makedirs(f"{t}/bin")
        os.environ["MC_DECISIONS_FILE"] = f"{t}/decisions.jsonl"
        commits = []
        _commit = lambda paths, msg, skip=(): commits.append((tuple(paths), msg)) or "abc1234"
        save(MUTE, {"muted": []})

        # expected gap from the weekdays a line allows (memo weekday-crons-get-a-week-of-grace,
        # 2026-09-27): the longest skipped stretch + 1 day, never a flat week for a weekday job
        for sched, want in (("5 14 * * 1-5", 72), ("*/30 12-21 * * 1-5", 72), ("*/15 * * * 1-5", 72),
                            ("0 7 * * 0", 168), ("0 7 * * 7", 168), ("0 7 * * 1,3,5", 72),
                            ("0 7 * * 2,6", 96), ("5 4 * * 2-6", 72), ("0 7 * * mon-fri", 72),
                            ("0 7 * * 1-5/2", 72), ("0 7 * * */2", 48), ("0 7 * * 5-7", 120),
                            ("0 7 * * *", 24), ("0 7 * * 0-6", 24), ("*/30 12-21 * * *", 72),
                            ("*/15 * * * *", 0.25), ("*/15 * * * 0-7", 0.25), ("35 9 1 * *", 744),
                            ("0 7 * * L", 168), ("@reboot x", None)):
            got = expected_gap_h(sched)
            check(f"expected gap: `{sched}` → {'none' if want is None else f'{want}h'}", got == want, got)

        # AppleDouble litter (2026-09-25): only real `._` AppleDouble files newer than the fix, and
        # never inside the skipped trees or hidden folders at the root
        ad = f"{t}/home"
        for rel, body, age in (("proj/._new.pdf", b"\x00\x05\x16\x07rest", 0),
                               ("proj/._old.pdf", b"\x00\x05\x16\x07rest", 3 * 86400),
                               ("proj/._notad.txt", b"hello", 0),
                               ("archive/._x.pdf", b"\x00\x05\x16\x07", 0),
                               (".hidden/._y.pdf", b"\x00\x05\x16\x07", 0)):
            os.makedirs(os.path.dirname(f"{ad}/{rel}"), exist_ok=True)
            with open(f"{ad}/{rel}", "wb") as fh:
                fh.write(body)
            os.utime(f"{ad}/{rel}", (time.time() - age, time.time() - age))
        got = [os.path.relpath(p, ad) for p in _appledouble_since(time.time() - 86400, root=ad)]
        check("appledouble: a new AppleDouble file is found; old, non-AppleDouble, archived and "
              "hidden-root ones are not", got == ["proj/._new.pdf"], got)

        # mdreader-drift (2026-09-27): every vendored copy of the shared reader is found by name (a versioned
        # file name too), its stamp read from any of the three built files, the canonical folder and the
        # dependency / venv / archive trees skipped; a copy that is not the current build is drift, per project
        mh, cur = f"{t}/mdr", "1.3.0+7baad46b"
        for rel, body in (("mc/shared/mdreader/dist/mdreader.js", f'var VERSION = "{cur}";'),
                          ("mc/shared/mdreader/src/mdreader.js", "var VERSION = /*@VERSION*/'0.0.0-dev';"),
                          ("mc/dashboard/vendor/mdreader.1.3.0-7baad46b.js", f'/*! mdreader {cur} */\nvar VERSION = "{cur}";'),
                          ("alpha/dash/vendor/mdreader.py", "VERSION = '1.2.0+0123abcd'  # built"),
                          ("alpha/dash/static/mdreader.css", f"/*! mdreader {cur} — generated */"),
                          ("beta/web/mdreader.js", "// patched by hand"),
                          ("beta/web/node_modules/x/mdreader.js", "var VERSION = \"0.1.0\";"),
                          ("beta/.venv/lib/mdreader.py", "VERSION = '0.1.0'"),
                          ("beta/env/pyvenv.cfg", "home = /usr"), ("beta/env/lib/mdreader.py", "VERSION = '0.1.0'"),
                          ("beta/web/test_mdreader.js", "var VERSION = \"0.1.0\";"),
                          ("beta/.claude/worktrees/wt1/web/mdreader.js", "var VERSION = \"0.1.0\";"),
                          ("gamma/CLAUDE.md", "# gamma"), ("gamma/ui/mdreader.js", 'var VERSION = "1.3.0+ffffffff";'),
                          ("delta/ui/mdreader.js", 'var VERSION = "2.0.0+00000000";')):
            os.makedirs(os.path.dirname(f"{mh}/{rel}"), exist_ok=True)
            with open(f"{mh}/{rel}", "w") as fh:
                fh.write(body)
        for proj in ("mc", "alpha", "beta", "delta"):
            os.makedirs(f"{mh}/{proj}/.git", exist_ok=True)
        got = [(p, os.path.relpath(x, mh), v) for p, x, v in _mdreader_copies(home=mh, canon=f"{mh}/mc/shared/mdreader")]
        check("mdreader-drift: finds each vendored copy (a versioned name too) and reads its stamp; skips the "
              "canonical folder, node_modules, venvs, session worktrees and test files", got == [
                  ("alpha", "alpha/dash/static/mdreader.css", cur), ("alpha", "alpha/dash/vendor/mdreader.py", "1.2.0+0123abcd"),
                  ("beta", "beta/web/mdreader.js", None), ("delta", "delta/ui/mdreader.js", "2.0.0+00000000"),
                  ("gamma", "gamma/ui/mdreader.js", "1.3.0+ffffffff"),
                  ("mc", "mc/dashboard/vendor/mdreader.1.3.0-7baad46b.js", cur)], got)
        dr = _mdreader_drift(_mdreader_copies(home=mh, canon=f"{mh}/mc/shared/mdreader"), cur)
        check("mdreader-drift: one entry per project with a stale copy — older, unstamped, another build, ahead — "
              "and none for a project whose copies are all current",
              sorted(dr) == ["alpha", "beta", "delta", "gamma"] and [w for _, _, w in dr["alpha"]] == ["is an older version"]
              and dr["beta"][0][2] == "carries no VERSION stamp" and dr["gamma"][0][2] == "is another build of the same version"
              and dr["delta"][0][2].startswith("is ahead"), dr)
        check("mdreader-drift: the stamp reads from the real built files (the canonical dist/ carries one)",
              all(_mdr_version(os.path.join(MDREADER_DIST, n)) for n in ("mdreader.js", "mdreader.py", "mdreader.css"))
              and len({_mdr_version(os.path.join(MDREADER_DIST, n)) for n in ("mdreader.js", "mdreader.py", "mdreader.css")}) == 1,
              [_mdr_version(os.path.join(MDREADER_DIST, n)) for n in ("mdreader.js", "mdreader.py", "mdreader.css")])

        # a mute with an end date leaves `muted` the day after it, with its reason kept
        save(MUTE, {"muted": ["k-old", "k-new", "k-plain"], "_reasons": {"k-old": "why", "k-new": "why2"},
                    "_until": {"k-old": "2026-09-01", "k-new": "2099-01-01"}})
        gone = expire_mutes(today="2026-09-26")
        mc = load(MUTE, {})
        check("mute expiry: a past _until leaves the list with its reason; a future one and a plain mute stay",
              gone == ["k-old"] and mc["muted"] == ["k-new", "k-plain"]
              and mc["_expired"]["k-old"] == {"until": "2026-09-01", "reason": "why"}
              and "k-old" not in mc["_until"] and "k-old" not in mc["_reasons"], mc)
        save(MUTE, {"muted": []})

        # keys
        a, b = [], []
        job = {"sched": "20 8 * * *", "scripts": ["memo-process.py"], "log": "x.log", "project": "maintenance"}
        _finding(a, "job-silent", "high", "memo-process.py has not run in 89h", "d", "maintenance",
                 key="memo-process.py:20 8 * * *")
        _finding(b, "job-silent", "high", "memo-process.py has not run in 96h", "d", "maintenance",
                 key="memo-process.py:20 8 * * *")
        check("key: a number in the title changes the id, never the key",
              a[0]["id"] != b[0]["id"] and a[0]["key"] == b[0]["key"] and not re.search(r"\d+h", a[0]["key"]),
              (a[0]["key"], b[0]["key"]))
        c = []
        _finding(c, "port-undeclared", "med", "port 8797 is listening but undeclared", "d")
        check("key: with no key given it is the id, normalised the way the status layer does it",
              c[0]["key"] == _key(c[0]["id"]) and c[0]["key"] == c[0]["id"].lower())
        check("key: a catalog-stale key is the dataset, not its age",
              _catalog_key({"kind": "catalog-stale", "title": "stocks/nav_snapshots is 12d old"})
              == _catalog_key({"kind": "catalog-stale", "title": "stocks/nav_snapshots is 13d old"})
              == "stocks/nav_snapshots")

        # merge by key: the same problem a day later is not "new"
        n1, _, _ = merge_findings(a)
        n2, r2, o2 = merge_findings(b)
        st = load(FINDINGS, {})
        check("merge: the same key a day later updates the open record instead of a new one",
              len(n1) == 1 and n2 == [] and r2 == [] and len(o2) == 1
              and o2[0]["title"].endswith("96h") and o2[0]["id"] == a[0]["id"], (n2, r2))
        save(MUTE, {"muted": [a[0]["key"]]})
        n3, r3, o3 = merge_findings(b)
        check("mute: a mute by key silences it (and the open record resolves)",
              n3 == [] and len(r3) == 1 and o3 == [], (n3, r3, o3))
        save(MUTE, {"muted": []})

        # David's answers
        td = _decide_mod()
        leak = []
        _finding(leak, "public-leak", "high", "a public repo would leak private content",
                 "publish.py scan: r-public/code/p.py:91 [credential from ~/.secrets/nope]", "maintenance")
        cat = []
        _finding(cat, "catalog-unbacked", "high", "hbs/x is written here and backed up nowhere", "d", "hbs")
        merge_findings(leak + cat)
        now_ = now()
        td.append({"id": "m0000001", "at": now_, "key": cat[0]["key"], "kind": "catalog-unbacked",
                   "title": cat[0]["title"], "option": "mute", "label": "Accept it — mute with a reason",
                   "exec": "code", "action": "mute", "status": "applied", "by": "david",
                   "note": "regenerable; losing it is fine", "finding_id": cat[0]["id"]})
        save(f"{CONFIG}/dev.json", {"entries": [{"label": "JustinDesk", "expires": "2026-09-29", "why": "WIP."}]}, 2)
        td.append({"id": "x0000002", "at": now_, "key": "service-dev:8790", "kind": "service-dev",
                   "option": "extend-14", "label": "Extend the in-dev label 2 weeks", "exec": "code",
                   "action": "extend-dev", "dev_label": "JustinDesk", "expires": "2026-10-13",
                   "until": now_ + 20 * 86400, "status": "applied", "by": "david"})
        gone = []
        _finding(gone, "unit-dead", "med", "x.service (system) is looping or cannot start", "d")
        td.append({"id": "v0000003", "at": now_, "key": gone[0]["key"], "kind": "unit-dead",
                   "option": "disable", "label": "I'll disable the unit", "exec": "david",
                   "status": "queued", "by": "david", "finding_id": gone[0]["id"]})
        td.append({"id": "c0000004", "at": now_, "key": "job-failed:1234abcd", "kind": "job-failed",
                   "option": "fix", "label": "Fix it", "exec": "claude", "status": "queued", "by": "david"})
        dry = apply_decisions(leak + cat, dry=True)
        check("decide --dry: reports, writes nothing", len(dry) == 3 and load(MUTE, {}).get("muted") == []
              and not commits, [w for _, w in dry])
        got = apply_decisions(leak + cat)
        mc = load(MUTE, {})
        lat = td.latest()
        check("decide: a mute answer becomes a mute-list line, keyed, with his note as the reason",
              cat[0]["key"] in mc["muted"] and "regenerable; losing it is fine" in mc["_reasons"][cat[0]["key"]]
              and "decision:m0000001" in mc["_reasons"][cat[0]["key"]])
        check("decide: the in-dev label moves in config/dev.json with the reason",
              load(f"{CONFIG}/dev.json", {})["entries"][0]["expires"] == "2026-10-13"
              and "decision:x0000002" in load(f"{CONFIG}/dev.json", {})["entries"][0]["why"])
        check("decide: one commit, of those two files only",
              len(commits) == 1 and set(commits[0][0]) == {"config/backoffice_mute.json", "config/dev.json"},
              commits)
        check("decide: both code answers are marked done with the commit",
              lat[cat[0]["key"]]["status"] == "done" and lat[cat[0]["key"]].get("commit") == "abc1234"
              and lat["service-dev:8790"]["status"] == "done")
        check("decide: 'I'll do it myself' closes once the finding is gone; Claude answers are left alone",
              lat[gone[0]["key"]]["status"] == "done" and lat["job-failed:1234abcd"]["status"] == "queued")
        n4, r4, _ = merge_findings(leak + cat)
        check("decide: the muted finding resolves on the same pass",
              all(x["id"] != cat[0]["id"] for x in load(FINDINGS, {}).values() if x.get("state") == "open"))

        # fix round 2026-09-24: hand-edited config keeps its UTF-8 through a janitor write
        hand = '{\n "_doc": "what we live with \u2014 put the reason next to it",\n "muted": []\n}\n'
        open(MUTE, "w", encoding="utf-8").write(hand)
        save(MUTE, load(MUTE, {}), indent=1)
        check("save: a hand-edited file with '\u2014' round-trips byte for byte (no \\u escapes)",
              open(MUTE, encoding="utf-8").read() == hand, open(MUTE, encoding="utf-8").read()[:80])
        save(MUTE, {"muted": []})

        # fix round 2026-09-24: a merge without a restart is named as that, not as broken tabs
        import subprocess as _sp
        sd = f"{t}/dash"
        os.makedirs(sd)
        for n in ("server.py", "usage.py"):
            open(f"{sd}/{n}", "w").write("import time\ntime.sleep(60)\n")
        old_t = time.time() - 600
        for n in ("server.py", "usage.py"):
            os.utime(f"{sd}/{n}", (old_t, old_t))
        proc = _sp.Popen(["python3", f"{sd}/server.py"], stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
        try:
            time.sleep(0.3)
            decoy = _sp.Popen(["bash", "-c", f"sleep 30 # python3 {sd}/server.py"], stdout=_sp.DEVNULL)
            time.sleep(0.2)
            pid0, nw0 = _dashboard_stale(sd)
            os.utime(f"{sd}/server.py", (time.time() + 60, time.time() + 60))
            pid1, nw1 = _dashboard_stale(sd)
            check("stale server: the server's own process (not a shell naming it); files older than it are "
                  "fine; a newer server.py is named",
                  pid0 == proc.pid and nw0 == [] and pid1 == proc.pid and nw1 == ["server.py"], (pid0, nw0, pid1, nw1))
        finally:
            proc.kill()
            proc.wait()
            decoy.kill()
            decoy.wait()
        check("stale server: no process, no claim", _dashboard_stale(sd) == (None, []))

        # leak lines and the stored-copy scrub
        out = _leak_lines("mc-public/code/p.py:91  [credential from ~/.secrets/sudo] s3cretVALUE\n"
                          "   x = 's3cretVALUE'\nmc-public/README.md:3  [absolute home path] /home/q\n1 leak(s)\n")
        check("leaks: file:line [why] only, never the match or the text",
              out == ["mc-public/code/p.py:91 [credential from ~/.secrets/sudo]",
                      "mc-public/README.md:3 [absolute home path]", "1 leak(s)"], out)
        old_store = {"public-leak:maintenance:x": {"state": "resolved", "detail":
                     "publish.py scan: r/code/p.py:91 [credential from ~/.secrets/sudo] oldValue77; 1 leak(s)",
                     "nested": ["token planted-VALUE-42 here"]}}
        save(FINDINGS, _scrub_secrets(old_store, ["planted-VALUE-42"]))
        raw = open(FINDINGS).read()
        check("scrub: a stored detail loses a live value and a rotated one after the scanner's tag",
              "oldValue77" not in raw and "planted-VALUE-42" not in raw and raw.count("<redacted>") == 2, raw)
        sv = _secret_values()
        if sv:
            save(FINDINGS, {"x": {"detail": "a " + sv[0] + " b"}})
            check("scrub: save() of findings.json redacts a live ~/.secrets value on its own",
                  sv[0] not in open(FINDINGS).read())

        # diagrams
        arch = f"{t}/architecture"
        os.makedirs(arch)
        for n, body in (("_style.d2", "vars: {}\n"), ("a.d2", "...@_style\nx -> y\n"), ("b.d2", "x -> y\n")):
            open(f"{arch}/{n}", "w").write(body)
        for n in ("a.svg", "b.svg"):
            open(f"{arch}/{n}", "w").write("<svg/>")
        tt = time.time()
        for n, m in (("a.d2", tt - 300), ("b.d2", tt - 300), ("a.svg", tt - 200), ("b.svg", tt - 200),
                     ("_style.d2", tt - 100)):
            os.utime(f"{arch}/{n}", (m, m))
        ds = _diagram_state()
        check("diagrams: _*.d2 is an import, never a diagram; a styled svg older than _style.d2 is stale",
              ds["unrendered"] == ["a.d2"] and ds["diagrams"] == 2, ds)
        open(f"{t}/bin/render-diagrams.sh", "w").write("#!/bin/sh\necho refused >&2\nexit 1\n")
        os.chmod(f"{t}/bin/render-diagrams.sh", 0o755)
        dfix = []
        _finding(dfix, "diagram-unrendered", "low", "1 diagram source(s) edited but not re-rendered", "a.d2",
                 "maintenance", fix="auto", key="")
        done = fix([dict(dfix[0], state="open")])
        check("fix: a render that fails claims no repair", done == [], done)
        open(f"{t}/bin/render-diagrams.sh", "w").write(f"#!/bin/sh\ntouch {arch}/a.svg\nexit 0\n")
        done = fix([dict(dfix[0], state="open")])
        check("fix: a render that works and leaves nothing stale is claimed",
              [w for _, w in done] == ["re-rendered the architecture diagrams"], done)

        # stale byte-code
        import py_compile
        pk = f"{t}/pyc"
        os.makedirs(pk)
        open(f"{pk}/m.py", "w").write("X = 1\n")
        open(f"{pk}/n.py", "w").write("Y = 1\n")
        py_compile.compile(f"{pk}/m.py", cfile=f"{pk}/__pycache__/m.cpython-312.pyc")
        py_compile.compile(f"{pk}/n.py", cfile=f"{pk}/__pycache__/n.cpython-312.pyc")
        os.utime(f"{pk}/m.py", (tt + 5, tt + 5))
        gone_p = _clean_stale_pyc([pk])
        check("pyc: a cache compiled from an older source is removed, a current one kept",
              [os.path.basename(x) for x in gone_p] == ["m.cpython-312.pyc"]
              and os.path.exists(f"{pk}/__pycache__/n.cpython-312.pyc"), gone_p)

        # the transition: records stored before findings had keys (verifier, 2026-09-24)
        save(MUTE, {"muted": []})
        store = {"catalog-stale:stocks:stocks-nav-snapshots-is-12d-old": {
                     "id": "catalog-stale:stocks:stocks-nav-snapshots-is-12d-old", "kind": "catalog-stale",
                     "sev": "med", "title": "stocks/nav_snapshots is 12d old", "detail": "d", "project": "stocks",
                     "fix": "human", "state": "open", "first_seen": 1000},
                 "port-undeclared::port-8797-is-listening-but-undeclared": {
                     "id": "port-undeclared::port-8797-is-listening-but-undeclared", "kind": "port-undeclared",
                     "sev": "med", "title": "port 8797 is listening but undeclared", "detail": "d", "project": "",
                     "fix": "human", "state": "open", "first_seen": 1000}}
        save(FINDINGS, store)
        fr = []
        _finding(fr, "catalog-stale", "med", "stocks/nav_snapshots is 13d old", "d", "stocks",
                 key=_catalog_key({"kind": "catalog-stale", "title": "stocks/nav_snapshots is 13d old"}))
        _finding(fr, "port-undeclared", "med", "port 9000 is listening but undeclared", "d")
        n5, r5, o5 = merge_findings(fr)
        st5 = load(FINDINGS, {})
        leg = st5["catalog-stale:stocks:stocks-nav-snapshots-is-12d-old"]
        check("legacy: a keyless record whose id differs only in its numbers is updated in place, not "
              "re-opened as new",
              leg["state"] == "open" and leg["key"] == fr[0]["key"] and leg["title"].endswith("13d old")
              and [x["id"] for x in n5] == [fr[1]["id"]], ([x["id"] for x in n5], leg))
        check("legacy: a finding keyed by its own id (a port) never merges with another number",
              st5["port-undeclared::port-8797-is-listening-but-undeclared"]["state"] == "resolved"
              and st5[fr[1]["id"]]["state"] == "open")
        td.append({"id": "l0000005", "at": now_, "key": _key("catalog-stale:stocks:stocks-nav-snapshots-is-12d-old"),
                   "kind": "catalog-stale", "option": "mute", "label": "Accept it", "exec": "code",
                   "action": "mute", "status": "applied", "by": "david",
                   "finding_id": "catalog-stale:stocks:stocks-nav-snapshots-is-12d-old"})
        td.append({"id": "l0000006", "at": now_, "key": "unpushed:stocks:stocks-has-5-unpushed-commits",
                   "kind": "unpushed", "option": "later", "label": "I'll push it", "exec": "david",
                   "status": "queued", "by": "david", "finding_id": "unpushed:stocks:stocks-has-5-unpushed-commits"})
        up = []
        _finding(up, "unpushed", "med", "stocks has 6 unpushed commits", "d", "stocks", key="")
        commits.clear()
        got = dict((e["id"], w) for e, w in apply_decisions(fr + up))
        check("legacy: a mute answered on the old id lands in the mute list under today's stable key",
              fr[0]["key"] in load(MUTE, {}).get("muted", []), load(MUTE, {}).get("muted"))
        check("legacy: 'I'll do it myself' on an old id is NOT closed while its finding is still found",
              "l0000006" not in got and td.latest()["unpushed:stocks:stocks-has-5-unpushed-commits"]["status"]
              == "queued", got)

        # "I rotated it": the secret file must change AFTER he answered, not merely after the leak
        # was found (09-24: changed, then restored to the leaked value)
        sec = f"{t}/secret-file"
        open(sec, "w").write("x")
        rec = {"public-leak:maintenance:x": {"detail": "publish.py scan: r/p.py:9 [credential from ~/.secrets/zz]",
                                             "first_seen": int(tt) - 3600}}
        saved_home = HOME
        try:
            globals()["HOME"] = t
            os.makedirs(f"{t}/.secrets", exist_ok=True)
            open(f"{t}/.secrets/zz", "w").write("x")
            os.utime(f"{t}/.secrets/zz", (tt - 60, tt - 60))
            ev = {"id": "r0000007", "key": "public-leak:maintenance:x", "option": "rotated", "exec": "david",
                  "finding_id": "public-leak:maintenance:x", "at": int(tt) - 30}
            check("rotated: a secret rewritten before the answer (after the leak) is not verified",
                  _verify_david(ev, [], rec) is None)
            os.utime(f"{t}/.secrets/zz", (tt + 5, tt + 5))
            check("rotated: rewritten after the answer is verified", bool(_verify_david(ev, [], rec)))
            # "I'll push the cleaned history" on the rejected-publish row (2026-09-25): a status
            # item, not a finding, so no record — the repo comes from the answer's consequence
            g = lambda *a, cwd=None: subprocess.run(["git", *a], cwd=cwd, capture_output=True)
            g("init", "-q", "--bare", "-b", "main", f"{t}/remote.git")
            g("clone", "-q", f"{t}/remote.git", f"{t}/public/x-public")
            g("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "c",
              cwd=f"{t}/public/x-public")
            pev = {"id": "r0000008", "key": "job-failed:abc", "option": "purge", "exec": "david",
                   "at": int(tt), "consequence": "Public-facing, so you run it: git -C ~/public/x-public "
                   "push --force-with-lease origin main. Nothing runs from this answer; …"}
            check("purge: not yet pushed stays queued", _verify_david(pev, [], {}) is None)
            g("push", "-q", "origin", "main", cwd=f"{t}/public/x-public")
            check("purge: with no record, the repo is read from the consequence and the push verifies",
                  "x-public" in (_verify_david(pev, [], {}) or ""))
        finally:
            globals()["HOME"] = saved_home

        # a real git tree: our own writes are not "another session's", and another session's
        # dirty file — even on the first porcelain line, which sh() strips — is left alone
        g = f"{t}/git"
        os.makedirs(f"{g}/config")
        MC, CONFIG, MUTE = g, f"{g}/config", f"{g}/config/backoffice_mute.json"
        _commit = saved[4]
        genv = {k: os.environ.get(k) for k in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME",
                                                "GIT_COMMITTER_EMAIL")}
        os.environ.update(GIT_AUTHOR_NAME="selftest", GIT_AUTHOR_EMAIL="selftest@localhost",
                          GIT_COMMITTER_NAME="selftest", GIT_COMMITTER_EMAIL="selftest@localhost")
        try:
            save(MUTE, {"muted": []})
            save(f"{CONFIG}/dev.json", {"entries": [{"label": "JustinDesk", "expires": "2026-09-29", "why": "WIP."}]}, 2)
            open(f"{g}/AAA.md", "w").write("a\n")
            sh(["git", "-C", g, "init", "-q"])
            sh(["git", "-C", g, "add", "-A"])
            sh(["git", "-C", g, "commit", "-qm", "init"])
            open(f"{g}/AAA.md", "a").write("another session\n")          # sorts first: " M AAA.md"
            check("dirty_before: the first porcelain line is read whole", dirty_before({"AAA.md"}) == {"AAA.md"})
            os.environ["MC_DECISIONS_FILE"] = f"{t}/decisions-git.jsonl"
            td.append({"id": "g0000008", "at": now_, "key": cat[0]["key"], "kind": "catalog-unbacked",
                       "title": cat[0]["title"], "option": "mute", "label": "Accept it", "exec": "code",
                       "action": "mute", "status": "applied", "by": "david", "finding_id": cat[0]["id"]})
            td.append({"id": "g0000009", "at": now_, "key": "service-dev:8790", "kind": "service-dev",
                       "option": "extend-14", "label": "Extend", "exec": "code", "action": "extend-dev",
                       "dev_label": "JustinDesk", "expires": "2026-10-13", "until": now_ + 20 * 86400,
                       "status": "applied", "by": "david"})
            apply_decisions(cat)
            lg = sh(["git", "-C", g, "show", "--stat", "--format=%s", "HEAD"])
            dirty = sh(["git", "-C", g, "status", "--porcelain"])
            lat = td.latest()
            check("decide: our own two writes are committed together; the other session's file is not",
                  "backoffice_mute.json" in lg and "dev.json" in lg and "AAA.md" not in lg
                  and dirty.strip() == "M AAA.md", (lg, dirty))
            check("decide: each answer records the real commit, and none claims 'left uncommitted'",
                  all(lat[k].get("commit") and "not committed" not in lat[k].get("result", "")
                      for k in (cat[0]["key"], "service-dev:8790"))
                  and lat[cat[0]["key"]]["commit"] == sh(["git", "-C", g, "rev-parse", "--short", "HEAD"]),
                  [(lat[k].get("commit"), lat[k].get("result")) for k in (cat[0]["key"], "service-dev:8790")])
        finally:
            for k, v in genv.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
    finally:
        FINDINGS, MUTE, CONFIG, MC, _commit = saved[:5]
        if saved[5] is None:
            os.environ.pop("MC_DECISIONS_FILE", None)
        else:
            os.environ["MC_DECISIONS_FILE"] = saved[5]
    print("ALL PASS" if ok else "SOME FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
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
    elif cmd == "decide":
        # a preview against the last pass's open findings (the real run uses a fresh audit)
        last = [f for f in load(FINDINGS, {}).values() if f.get("state") == "open"]
        for e, what in apply_decisions(last, dry=dry):
            print(("would: " if dry else "done: ") + (what or "could not apply") + f" (decision:{e['id']})")
    elif cmd == "selftest":
        sys.exit(selftest())
    else:
        sys.exit(run(dry=dry))
