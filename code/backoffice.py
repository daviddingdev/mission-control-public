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
    history-backfill  one-time seed of each finding's first_ever_seen / reopen_count / history
             from state/decisions.jsonl and findings.json (--dry prints, writes nothing)
    experiments  rule 46 (experiment-no-driver) alone: the oldest Queue item older than 7 days with
             no design memo gets one design-only session (prompts/experiment_design.md) through
             `claudeq.py run --kind frontier`, at most 3 a week; --dry says what it would launch

CLI: backoffice.py [census|audit|fix|brief|run|show|decide|selftest|history-backfill|experiments] [--dry|--dry-run]
     (no command = run; --dry and --dry-run are the same; any other argument exits 2 and runs
     nothing — 2026-10-03: until then `--dry-run`, or any typo, ran the live pass)
"""
import json
import os
import re
import shlex
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
MEMOS = os.path.join(HOME, "memos")                      # the bus: inbox/<slug>/ + LEDGER.md (selftest repoints it)
BLOCKERS = os.path.join(CONFIG, "known_blockers.json")   # pending decisions that hold a key (repeat-failure)
REPEAT_ASKED = os.path.join(STATE, "repeat_asked.json")  # key -> the one repeat memo filed for it

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
    return _parse_cron(sh(["crontab", "-l"]))


def _parse_cron(text):
    """_cron_lines()'s parser over any crontab text (rule 44 parses fleet-stop's saved lines with it)."""
    out, comment = [], ""
    for raw in (text or "").splitlines():
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


_HOME_RE = r"(?:~|\$HOME|\$\{HOME\}|" + re.escape(HOME) + r")"


def _project_of(text, names=None):
    """Which project a cron command belongs to, by the path it cds into or runs from.

    In order (2026-09-29, the Data Desk's visibility memo): the folder the command `cd`s into;
    else the project of a script it runs from a project folder, where another project's script
    beats Mission Control's shared tools (claudeq.py, notify.sh, claude-headless wrap other
    projects' jobs); else any project path it names, longest name first. Before this, the last
    rule came first, so `cd ~/Stocks/_engine && … || ~/maintenance/bin/notify.sh …` was filed
    under maintenance (longest name wins) — 10 Stocks/clientco lines on 2026-09-29, and every
    Data Desk line.
    `names` is the project list (default: the folders on disk), for the selftest."""
    names = list(names) if names is not None else _project_dirs()
    known = set(names)
    m = re.search(r"(?:^|&&|;|\|\||[({])\s*cd\s+" + _HOME_RE + r"/([^/\s;&|)]+)", text)
    if m and m.group(1) in known:
        return m.group(1)
    tops = [s.group(1) for s in re.finditer(
        _HOME_RE + r"/([^/\s;&|'\"]+)/[^\s;&|'\"]*?\.(?:py|sh)\b", text) if s.group(1) in known]
    for top in tops:
        if top != "maintenance":
            return top
    if tops:
        return "maintenance"
    for name in sorted(names, key=len, reverse=True):
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
        # api/embed (2026-09-29): ollama's batch embedding endpoint, which the Data Desk is the
        # first to call; the older api/embeddings is a prefix match of it
        hits = sh(["grep", "-rl", "--include=*.py", "-e", "api/chat", "-e", ":11434",
                   "-e", "chat_url(", "-e", "api/generate", "-e", "api/embed",
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
                   "mission-control": "maintenance", "thesis": "thesis", "data-desk": "data-desk"}
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


# A wind-down board's retire step, per project that declares one (`<slug>/handover`). The Data
# Desk's is its own CLI: it files the retirement checklist to the lane's owner as a memo and moves
# the lane to cutover_requested. A board for a project not listed here may name its own step in a
# top-level `retire_cmd` ("{lane}" stands for the lane id).
RETIRE_CMD = {"data-desk": "cd ~/data-desk && .venv/bin/python bin/desk.py handover retire {lane}"}
# no `_`: the answer key (tt_now._key) keeps only [a-z0-9.-] of it, and the daily check reads the
# lane back out of that key
_LANE_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,79}$")


def _handover_state(ids=None, reader=None):
    """Every wind-down board the catalog declares (a dataset `<slug>/handover`; the Data Desk's is
    the first, 2026-09-29), cut to its READY lanes: state `parity_ok`, so the replacement has
    matched the old lane for as many days as the board asks and only David's word is missing.
    Read by catalog id, so the read is logged (box rule 8); only metadata is kept — ids, names,
    counts, states. A lane id goes into a shell command in the finding, so anything but a plain
    slug is dropped and said. `ids` / `reader` (cid -> path) are the selftest's."""
    out = []
    try:
        import catalog as _catalog
        ids = ids if ids is not None else list(_catalog.index())
        reader = reader or (lambda cid: _catalog.path(cid, proj="maintenance"))
    except Exception as e:
        return [{"id": "?", "project": "", "error": f"catalog: {type(e).__name__}: {str(e)[:160]}"}]
    for cid in sorted(i for i in ids if "/" in i and i.split("/", 1)[1] == "handover"):
        slug = cid.split("/", 1)[0]
        try:
            board = json.load(open(reader(cid)))
            lanes = board.get("lanes") or []
        except Exception as e:
            out.append({"id": cid, "project": slug, "error": f"{type(e).__name__}: {str(e)[:160]}"})
            continue
        ready, bad = [], []
        for ln in lanes:
            if not isinstance(ln, dict) or ln.get("state") != "parity_ok":
                continue
            lid = str(ln.get("id") or "")
            if not _LANE_RE.match(lid):
                bad.append(lid[:40])
                continue
            p = ln.get("parity") if isinstance(ln.get("parity"), dict) else {}
            ready.append({"id": lid, "owner": str(ln.get("owner") or "")[:40],
                          "name": str(ln.get("name") or lid)[:120],
                          "replaced_by": [str(x)[:80] for x in (ln.get("replaced_by") or [])][:6],
                          # what moves and what the owner keeps (2026-09-29 review): a lane whose
                          # replacement carries only part of it must not read as a whole handover
                          "carries": re.sub(r"\s+", " ", str(ln.get("carries") or ""))[:160],
                          "keeps": [str(x)[:80] for x in (ln.get("keeps") or []) if isinstance(x, str)][:6],
                          "days_green": ln.get("days_green"), "need_days": ln.get("need_days"),
                          "parity": {k: p.get(k) for k in ("kind", "value", "bar", "n", "covered")},
                          "cron": str((ln.get("cron") or {}).get("state") or "")[:20]})
        cmd = board.get("retire_cmd") if isinstance(board.get("retire_cmd"), str) else None
        out.append({"id": cid, "project": slug, "at": board.get("at"), "lanes": len(lanes),
                    "ready": ready, "bad_ids": bad, "retire_cmd": RETIRE_CMD.get(slug) or cmd})
    return out


def _handover_words(ln):
    """'112/112 legacy items (100%, bar 100%) · 7 of 7 days green' from a ready lane."""
    p = ln.get("parity") or {}
    pct = lambda v: f"{v:.0%}" if isinstance(v, (int, float)) else "?"
    got = (f"{p['covered']}/{p['n']} " if isinstance(p.get("n"), int) and isinstance(p.get("covered"), int)
           else "")
    kind = str(p.get("kind") or "parity").replace("_", " ")
    days = (f" · {ln['days_green']} of {ln['need_days']} days green"
            if ln.get("days_green") is not None and ln.get("need_days") else "")
    return f"{kind} {got}({pct(p.get('value'))}, bar {pct(p.get('bar'))}){days}"


def _handover_findings(f, handovers):
    """Rule 20b's findings from the census' wind-down boards (_handover_state)."""
    for hb in handovers:
        if hb.get("error"):
            _finding(f, "handover-unreadable", "med", f"{hb['id']} cannot be read",
                     f"The wind-down board {hb['id']} is declared but could not be read "
                     f"({hb['error']}), so a lane ready to hand over would never reach Needs "
                     "attention. Its writer is in the project's catalog.json.",
                     hb.get("project", ""), fix="memo", key=hb["id"])
            continue
        if hb.get("bad_ids"):
            _finding(f, "handover-unreadable", "med",
                     f"{hb['id']} has lane ids that are not plain slugs",
                     f"Lane(s) {', '.join(hb['bad_ids'])} are ready but their ids are not "
                     "lowercase slugs, so no retire step is offered for them.",
                     hb["project"], fix="memo", key=hb["id"] + ":ids")
        for ln in hb.get("ready") or []:
            name = re.sub(r"\s*\([^)]*\)\s*$", "", ln["name"]).strip() or ln["id"]
            step = (hb.get("retire_cmd") or "").replace("{lane}", ln["id"])
            keeps = ln.get("keeps") or []
            _finding(f, "handover-ready", "high", f"Ready to hand over: {name[:48]}",
                     f"Lane `{ln['id']}` ({ln['name']}, owned by {ln['owner'] or '?'}): "
                     f"{', '.join(ln['replaced_by']) or 'its replacement'} now carries "
                     f"{ln.get('carries') or 'it'} — {_handover_words(ln)}, read from {hb['id']}"
                     + (f"; {ln['owner'] or 'the owner'} keeps {', '.join(keeps)}, which the replacement "
                        "does not carry" if keeps else "") + ". Its cron line is "
                     f"{ln['cron'] or 'not tracked'}. "
                     + (f"David's go → `{step}` files the retirement memo to {ln['owner']}; "
                        f"{ln['owner']} retires the lane and the board marks it retired."
                        if step else "David's go → the board's retire step files the retirement "
                        f"memo to {ln['owner']}; {hb['project']} names that step."),
                     hb["project"], fix="human", key=ln["id"])


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
    c["handovers"] = _handover_state()
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


def _plural_title(title):
    """"3 job(s)" -> "3 jobs", "1 job(s)" -> "1 job" in a finding's title (2026-10-08: the dashboard
    check fails on "(s)" in a work title, and root-cause memos carry the finding's title)."""
    return re.sub(r"\b(\d+) ((?:[\w'-]+ ){0,3}?[\w'-]+)\(s\)",
                  lambda m: f"{m.group(1)} {m.group(2)}{'' if m.group(1) == '1' else 's'}", title)


def _finding(out, kind, sev, title, detail, project="", fix="human", key=None):
    """`id` is unchanged (mutes by id keep working). `key` (2026-09-24) is what stays the SAME
    when a title carries a count or an age — "has not run in 89h" / "96h" were two findings, a
    new one every morning — so a mute or an answer from the dashboard outlives the number. No
    `key` given: the id is already stable, and the key is the id, normalised."""
    fid = f"{kind}:{project}:{re.sub(r'[^a-z0-9]+', '-', title.lower())[:60]}"
    k = _key(":".join(p for p in (kind, str(project).lower(), str(key)) if p)) if key is not None else _key(fid)
    title = _plural_title(title)       # after the id: an id (and a mute by id) never changes with it
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
     "standard": "§2a.3", "check": "`claudeq.py audit` (C26): every Claude spawn point goes through the slot · "
                                 "`claudeq.py selftest` (the clock, the fit rule, the cron reader and its "
                                 "weekday guards, starvation)",
     "requires": ("bin/claudeq.py",),
     "titles": ("a Claude spawn point goes around the box slot",
                "claudeq selftest fails — the box slot's clock or fit rule no longer does what was agreed")},
    {"id": "notify", "control": "notify.sh + notify_policy.json", "layer": "preventive",
     "standard": "§1", "check": "policy parses · `notify_policy.py selftest` (the tier table, and the Daily "
                                 "rollup's cap_exempt: no channel cap holds it)",
     "requires": ("bin/notify.sh", "bin/notify_policy.py", "config/notify_policy.json"),
     "titles": ("notify_policy.json is unreadable — push tiering is off",
                "notify_policy selftest fails — a tiering rule no longer does what was agreed")},
    {"id": "memo-inbox", "control": "SessionStart memo-inbox hook", "layer": "preventive",
     "standard": "§6", "check": "listed under hooks.SessionStart · executable",
     "requires": ("bin/hook-memo-inbox.py",),
     "titles": ("the memo-inbox SessionStart hook is not armed",)},
    {"id": "schedule-check", "control": "PreToolUse schedule-check hook", "layer": "preventive",
     "standard": "§2a", "check": "listed under hooks.PreToolUse · executable · `schedule-check.py selftest` "
                                 "(the job classifier) · `schedule-check.py --audit` runs",
     "requires": ("bin/hook-guard-schedule.py", "bin/schedule-check.py"),
     "titles": ("the schedule-check PreToolUse hook is not armed",
                "the schedule-check hook misreads commands — its selftest fails",
                "schedule-check.py misreads which jobs run a local model — its selftest fails",
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
     "standard": "—", "check": "`node test_*_tab.js` (every one on disk, as check.py finds them) · "
                               "`dashboard/check.py`",
     "requires": ("dashboard/test_overview_tab.js", "dashboard/test_claude_tab.js",
                  "dashboard/test_catalog_tab.js", "dashboard/check.py"),
     "titles": ("dashboard {} tab test fails", "a dashboard check is failing",
                "some dashboard tab tests did not run (time budget)")},
    {"id": "deadunits", "control": "dead-unit rule", "layer": "detective",
     "standard": "—", "check": "`deadunits.py selftest`", "requires": ("bin/deadunits.py",),
     "titles": ("deadunits selftest fails — the dead-unit rule no longer fires",)},
    {"id": "lead-identity", "control": "lead identity (every lead its own name, role, hue and emblem; a new project "
                                       "is assigned one)", "layer": "detective", "standard": "§5 Day-1",
     "check": "`identity.py selftest` (the assignment fixtures, and the live config/projects.json: no shared hue or "
              "emblem, no project without one) · `tt_team.py selftest` pins the same against the page",
     "requires": ("bin/identity.py", "config/projects.json"),
     "titles": ("identity selftest fails — two leads could share a hue or an emblem, or a project has none",)},
    {"id": "browser-selftest", "control": "the Spark browser's pacing, caps and sign-in reads", "layer": "preventive",
     "standard": "—", "check": "`browser.py selftest --pure` (fixtures only: no Chrome, no site visited)",
     "requires": ("bin/browser.py",),
     "titles": ("browser.py selftest fails — the Spark browser's pacing, caps or sign-in reads may not hold",)},
    {"id": "crew-namer", "control": "the crew namer (one name per agent everywhere)", "layer": "detective",
     "standard": "—", "check": "`tt_crew.py selftest` (resolver, tools-rule and badge fixtures) · every live job, scripts too, claimed once",
     "requires": ("dashboard/tt_crew.py", "config/crew.json"),
     "titles": ("crew selftest fails — the page can name the wrong agent",)},
    {"id": "dashboard-selftests", "control": "the dashboard's own builders (Agents, Sessions › Flow, Box)",
     "layer": "detective", "standard": "—",
     "check": "`tt_fleet.py`, `tt_flow.py` and `tt_system.py selftest` daily (kinds, the upkeep answer, the memo "
              "and read lines, payload budgets); they rotted unseen until 10-08",
     "requires": ("dashboard/tt_fleet.py", "dashboard/tt_flow.py", "dashboard/tt_system.py"),
     "titles": ("a dashboard selftest fails — a page can show a wrong count, kind or line",)},
    {"id": "always-on-units", "control": "always-on user units (systemd, linked from a project repo, memory-capped)",
     "layer": "preventive", "standard": "§4",
     "check": "every enabled user unit linked from ~/<project>/, and maintenance-dashboard always: enabled · active · "
              "MemoryMax and MemorySwapMax are byte counts, not infinity",
     "requires": ("dashboard/maintenance-dashboard.service", "dashboard/serve.sh"),
     "titles": ("an always-on user unit is not armed — {} has no supervisor or no memory cap",
                "always-on unit discovery is inert — it did not see maintenance-dashboard")},
    {"id": "smb-fruit", "control": "SMB share vfs_fruit (no Finder ._ files)", "layer": "preventive",
     "standard": "—", "check": "`testparm -s` loads fruit + streams_xattr · no AppleDouble since",
     "requires": (),
     "titles": ("the SMB share no longer loads vfs_fruit — Finder copies leave ._ files again",)},
    {"id": "sto-owners", "control": "single-threaded owner: every automated thing walks to one lead (rule owner-missing)",
     "layer": "detective", "standard": "box rule 10",
     "check": "rule 28 runs over the crons, 30 days of queue jobs, enabled user units, the crew and the catalog · "
              "`backoffice.py selftest` (the owner-missing fixtures)",
     "requires": ("bin/backoffice.py", "bin/lead.py", "dashboard/tt_crew.py"),
     "titles": ("the owner-missing rule did not run — nothing checks that every job has a lead",)},
    {"id": "patch-guards", "control": "monthly patch guards (GPU and Claude slots held to the reboot, active sessions, sudoers, disk)",
     "layer": "preventive", "standard": "—",
     "check": "`spark-update-run.sh selftest`: each guard seen to refuse, argv exits 2, the pause holds "
              "(no scheduled run before November, or without state/patch/first_pass from a green in-person run)",
     "requires": ("bin/spark-update-run.sh", "bin/spark-postboot-verify.sh"),
     "titles": ("the monthly patch's guards fail their selftest — it could reboot under live work",)},
    {"id": "auto-fix", "control": "auto-fix policy (the never-auto fence, the quiet morning, the 14-day hand-back, "
                                  "the 7-day reversible memo default)",
     "layer": "preventive", "standard": "—",
     "check": "`decisions.py selftest` · `tt_decide.py selftest` · `memo-process.py selftest`: each pins the fence "
              "(public, money, Stocks, deletes, ports, accounts, security never auto-resolve) and its argv",
     "requires": ("bin/decisions.py", "dashboard/tt_decide.py", "bin/memo-process.py"),
     "titles": ("the auto-fix policy's selftests fail — {} could act for David where it must not",)},
    {"id": "deadman", "control": "dead-box alarm (one scheduled ntfy.sh message the watchdog keeps pushing back)",
     "layer": "detective", "standard": "—",
     "check": "`healthcheck.sh selftest` (plan, arm, cancel and the ledger rows on a stub curl) · state/deadman.json "
              "re-armed in the last 75 min",
     "requires": ("bin/healthcheck.sh",),
     "titles": ("the dead-box alarm's selftest fails — a dead box might never page",
                "the dead-box alarm is not armed — {}")},
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
    {"id": "broker-guard", "control": "PreToolUse broker guard (only the PM places BrokerB orders)",
     "layer": "preventive", "standard": "—",
     "check": "listed under hooks.PreToolUse for Bash and mcp__brokerb-trading__.* · executable · "
              "`hook-guard-broker.py selftest`",
     "requires": ("bin/hook-guard-broker.py",),
     "titles": ("the broker guard is not armed — a headless session could place a BrokerB order",)},
    {"id": "lead-guard", "control": "PreToolUse project-lead guard (a lead's bright lines)",
     "layer": "preventive", "standard": "§5",
     "check": "listed under hooks.PreToolUse for Bash and Edit|Write|MultiEdit|NotebookEdit · executable · "
              "`hook-guard-lead.py selftest`",
     "requires": ("bin/hook-guard-lead.py",),
     "titles": ("the project-lead guard is not armed — a lead's bright lines are not enforced",)},
    {"id": "project-leads", "control": "bin/lead.py: the one lead launcher, the 07:02Z pick, the 07:47Z deadline, push safety",
     "layer": "preventive", "standard": "§5",
     "check": "`lead.py selftest` · `lead.py status --json` answers (the lead-* rules read it)",
     "requires": ("bin/lead.py",),
     "titles": ("lead.py selftest fails — the weekly pick, the deadline or the push safety no longer hold",
                "the project-lead rules did not run — `lead.py status` failed")},
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


# PROJECT LEADS (2026-10-02, David: "one main agent per project, let's call it project_lead ...
# some projects like clientco won't have much going on, that is fine as long as the lead is there
# to take memos"). bin/lead.py derives the roster from disk and says, per project, whether its lead
# is declared, valid and keeping its weekly day; these rules read `lead.py status --json`.
LEAD_OVERDUE_FROM = "2026-10-12"     # first-week grace: the leads were installed 2026-10-02
NOTIFICATIONS = os.path.join(STATE, "notifications.jsonl")


def _lead_findings(f, status, today=None):
    """lead-missing / lead-invalid / lead-overdue over `lead.py status --json` rows.
    A project led in its own harness (kind `external` with a `lead`: Stocks, led by the PM since
    2026-10-03) HAS a lead, declared in lead.py's EXTERNAL table, so it files nothing here; that
    declaration replaced the `lead-missing:stocks` mute. An excluded row with no lead still files
    lead-missing."""
    today = today or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for r in status or []:
        name, slug = r.get("dir") or r.get("slug") or "?", r.get("slug") or ""
        lid = slug.replace("-", "_") + "_lead"
        st = r.get("state")
        if r.get("kind") == "external" and r.get("lead"):
            continue
        if st in ("missing", "excluded"):
            _finding(f, "lead-missing", "med", f"{name} has no project lead",
                     (f"lead.py excludes it: {r.get('why')}. " if st == "excluded" else "")
                     + f"PROJECT_STANDARDS §5 Day-1 item 11: `.claude/agents/{lid}.md` (the charter) and "
                     f"`.claude/lead.json` (+ `.claude/lead-memory.md`). Without them nobody owns the "
                     f"project's weekly upkeep, and its memos run one per day on the old path. "
                     f"`python3 ~/maintenance/bin/lead.py roster` shows every project's lead.",
                     name, fix="memo", key="")
        elif st == "invalid":
            _finding(f, "lead-invalid", "med", f"{name}'s project lead is declared but invalid",
                     f"`lead.py check` rejects {lid}, so it is not run, weekly or for memos: "
                     + "; ".join((r.get("problems") or ["no detail"])[:4])[:400],
                     name, fix="memo", key="")
        elif st == "ok" and r.get("overdue") and today >= LEAD_OVERDUE_FROM:
            lr = r.get("last_run") or {}
            _finding(f, "lead-overdue", "med",
                     f"{r.get('lead') or lid} has had no good weekly run in {r.get('days_since_ok') or 0:.0f} days",
                     f"its day is {r.get('weekday') or '?'} 07:02Z; last weekly: {r.get('last_weekly_state') or 'never'}"
                     + (f"; last run {lr.get('mode')} {lr.get('state')}" + (f" ({lr.get('why')})" if lr.get('why') else "")
                        if lr else "")
                     + f"; next: {r.get('next_run') or '?'}. A lead is skipped when the Claude slot is not free "
                     "by the 07:47Z deadline, when another lead used that morning's one session, or when its "
                     "run fails. Read ~/maintenance/logs/lead.log and state/lead/runs.jsonl.",
                     name, fix="human", key="")


# SINGLE-THREADED OWNER (2026-10-03, David: "any automated job should be eventually tied to a lead
# agent if that makes sense ... we should look for single threaded owner as a guiding principle";
# HOME.md box rule 10). Every automated thing on the box has exactly ONE owning lead: a project's
# things are its lead's (Stocks': the PM, `pm`, in Stocks' own harness) and box-wide plumbing is
# maintenance_lead's. Rule owner-missing walks each thing to its lead — a crew claim -> that
# agent's project -> the project's lead; no claim -> the project the line itself names; no
# project -> box-wide -> maintenance_lead — and files one finding per project whose things reach
# no lead, plus one for queue jobs no agent claims and that name no project.
STO_BOX = ("", "box", "mission control", "maintenance")


def _sto_slug(project):
    p = str(project or "").strip().lower()
    return "maintenance" if p in STO_BOX else p


def _sto_leads(status_rows):
    """{project slug: lead id} for every project that has a lead: a valid lead.py lead (state ok),
    or a lead declared in its own harness (kind external: stocks -> pm)."""
    out = {}
    for r in status_rows or []:
        if r.get("lead") and (r.get("state") == "ok" or r.get("kind") == "external"):
            out[str(r.get("slug") or "").lower()] = r["lead"]
    return out


def _owner_findings(f, things, status_rows, agents):
    """owner-missing over [{what, name, agent, project}] (`agent` a crew id or None; `project` a
    project name or slug, "" for box-wide, None for unknown). `agents` is {crew id: agent}; an
    agent's explicit `owner_lead` wins over its project's lead, but only when it names a real lead.
    -> {slug: [thing]} of what reached no lead (the selftest reads it)."""
    leads = _sto_leads(status_rows)
    on_disk = {str(r.get("slug") or "").lower(): r for r in status_rows or []}
    real = set(leads.values())
    miss = {}
    for t in things:
        a = agents.get(t.get("agent")) if t.get("agent") else None
        if a is None and t.get("project") is None:
            miss.setdefault("?", []).append(t)
            continue
        slug = _sto_slug(a.get("project") if a else t.get("project"))
        lead = (a or {}).get("owner_lead") if (a or {}).get("owner_lead") in real else leads.get(slug)
        if not lead:
            miss.setdefault(slug, []).append(t)

    def listing(ts):
        names = [f"{t['what']} {t['name']}" for t in ts[:8]]
        return "; ".join(names) + (f" (+{len(ts) - 8} more)" if len(ts) > 8 else "")
    for slug, ts in sorted(miss.items()):
        if slug == "?":
            _finding(f, "owner-missing", "low", f"{len(ts)} queue job(s) reach no lead: no crew agent claims them",
                     "Single-threaded owner (HOME.md box rule 10): every automated thing walks to exactly one "
                     "lead through the agent that claims it. These names in state/claudeq/events.jsonl (30 days) "
                     "match no `queue` entry in config/crew.json and name no project: " + listing(ts)
                     + ". Claim each on the agent whose work it is (a `queue` entry, exact or a prefix ending "
                     "in ':' or ' '), or add it to `exclude_queue` if it is a probe.",
                     project="maintenance", fix="human", key="unclaimed")
            continue
        r = on_disk.get(slug)
        name = (r or {}).get("dir") or slug
        if r is None and slug != "maintenance":
            _finding(f, "owner-missing", "med", f"{len(ts)} automated thing(s) belong to '{slug}', which is no project on disk",
                     "Single-threaded owner (HOME.md box rule 10): a thing's project must be a top-level project "
                     "folder whose lead owns it. config/crew.json (or the catalog, or a cron line) names a project "
                     f"lead.py does not see: {listing(ts)}. Fix the project name, or retire the thing.",
                     project="maintenance", fix="human", key=slug)
            continue
        st = (r or {}).get("state") or "missing"
        _finding(f, "owner-missing", "med", f"{len(ts)} automated thing(s) in {name} have no owning lead",
                 f"Single-threaded owner (HOME.md box rule 10): every cron job, queue job, service, agent and "
                 f"dataset has exactly one owning lead, David's one gateway into the project. {name}'s lead is "
                 f"{st} (lead-missing / lead-invalid says why), so nobody owns: {listing(ts)}. Declare the lead "
                 f"(PROJECT_STANDARDS §5 Day-1 item 11: the charter + .claude/lead.json), or, for a lead that runs "
                 f"in the project's own harness, add it to EXTERNAL in bin/lead.py (David's call).",
                 project=name, fix="memo" if slug != "maintenance" else "human", key=slug)
    return miss


# THE LEAD-PROJECT LAYOUT (2026-10-04, David: "we need a separate folder for data desk and its own
# claude md etc. any project with a lead should follow this structure"; PROJECT_STANDARDS §5). Rule
# lead-structure walks every project whose lead runs under lead.py (rule 25's `lead.py status` rows,
# state ok or invalid; a lead in its own harness, kind external — Stocks' PM — is lead.py's EXTERNAL
# declaration and is not walked) and files ONE low finding per project listing what its folder lacks.
# fix="human": file_memos() files high findings only, so a memo fix here would send nothing; the
# project's lead sees the finding in its weekly packet (lead.py findings_section).
_PLACEHOLDER_DESC = re.compile(r"^\s*\(auto-added", re.I)


def _worktrees_ignored(root):
    """True when `.claude/worktrees/` is git-ignored in `root` (git's own answer, so a whitelist
    .gitignore counts); no git or git unsure -> a plain read of .gitignore."""
    if os.path.isdir(os.path.join(root, ".git")):
        try:
            r = subprocess.run(["git", "-C", root, "check-ignore", "-q", "--no-index", ".claude/worktrees/probe"],
                               capture_output=True, timeout=10)
            if r.returncode in (0, 1):
                return r.returncode == 0
        except (OSError, subprocess.SubprocessError):
            pass
    try:
        with open(os.path.join(root, ".gitignore")) as fh:
            lines = {ln.strip().lstrip("/").rstrip("/") for ln in fh}
    except OSError:
        return False
    return bool(lines & {".claude/worktrees", ".claude", ".claude/*", ".claude/worktrees/*"})


def _lead_layout_gaps(root, dirname, lead, projects_cfg):
    """What one lead project's folder lacks, [] when the layout is whole."""
    slug, gaps = dirname.lower(), []
    if not os.path.isdir(os.path.join(root, ".git")):
        gaps.append("not a git repo")
    try:
        with open(os.path.join(root, "CLAUDE.md"), errors="replace") as fh:
            cm = fh.read()
    except OSError:
        cm = None
    if cm is None:
        gaps.append("no CLAUDE.md")
    elif not re.search(r"^##\s+Project lead\b", cm, re.M | re.I):
        gaps.append("CLAUDE.md has no '## Project lead' section")
    cat = "config/catalog.json" if slug == "maintenance" else "catalog.json"
    for rel, what in (("README.md", "no README.md"), ("ARCHITECTURE.md", "no ARCHITECTURE.md at the root"),
                      (f".claude/agents/{lead}.md", f"no .claude/agents/{lead}.md (the charter)"),
                      (".claude/lead.json", "no .claude/lead.json"), (".claude/lead-memory.md", "no .claude/lead-memory.md"),
                      (cat, f"no {cat} (box rule 8)")):
        if not os.path.exists(os.path.join(root, *rel.split("/"))):
            gaps.append(what)
    if not _worktrees_ignored(root):
        gaps.append(".gitignore does not cover .claude/worktrees/")
    row = None
    for k, v in ((projects_cfg or {}).get("projects") or {}).items():
        if isinstance(v, dict) and (k.lower() == slug or slug in [str(m).lower() for m in v.get("match") or []]):
            row = v
            break
    desc = str((row or {}).get("desc") or "").strip()
    if row is None:
        gaps.append("no row in config/projects.json")
    elif not desc or desc.lower() in (slug, dirname.lower()) or _PLACEHOLDER_DESC.match(desc):
        gaps.append(f"config/projects.json desc is a placeholder ({desc or 'empty'!s})")
    return gaps


def _lead_structure_findings(f, status_rows, projects_cfg, home=None):
    """lead-structure over `lead.py status --json` rows. -> {slug: gaps} for the projects with any."""
    home = home or HOME
    out = {}
    for r in status_rows or []:
        if r.get("kind") == "external" or r.get("state") not in ("ok", "invalid"):
            continue
        name = r.get("dir") or r.get("slug") or ""
        slug = (r.get("slug") or name).lower()
        root = os.path.join(home, name)
        if not name or not os.path.isdir(root):
            continue
        gaps = _lead_layout_gaps(root, name, r.get("lead") or slug.replace("-", "_") + "_lead", projects_cfg)
        if not gaps:
            continue
        out[slug] = gaps
        _finding(f, "lead-structure", "low", f"{name} does not follow the lead-project layout",
                 "PROJECT_STANDARDS §5, the lead-project layout (David 2026-10-04: \"any project with a lead "
                 "should follow this structure\"). Missing: " + "; ".join(gaps) + ". The project's lead owns "
                 "the fix (it sees this in its weekly packet); Mission Control's own config/projects.json row "
                 "is MC's to set. An accepted deviation is muted in config/backoffice_mute.json with its reason.",
                 name, fix="human", key=slug)
    return out


def _sto_queue_jobs(path=None, days=30):
    """[(job name, kind)] that started in the box Claude queue in the last `days` days."""
    cut, seen = time.time() - days * 86400, {}
    try:
        with open(path or os.path.join(STATE, "claudeq", "events.jsonl"), errors="replace") as fh:
            for line in fh:
                if '"start"' not in line:
                    continue
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("ev") == "start" and (e.get("at") or 0) >= cut and e.get("job"):
                    seen[e["job"]] = e.get("kind")
    except OSError:
        pass
    return sorted(seen.items())


AUTO_FIX_SELFTESTS = (("decisions.py", "bin/decisions.py"), ("tt_decide.py", "dashboard/tt_decide.py"),
                      ("memo-process.py", "bin/memo-process.py"))
DEADMAN_STALE_S = 4500       # healthcheck.sh DM_UNARMED: two missed re-arms (75 min)


def _auto_fix_selftests(run=None):
    """-> [(name, why)] for each auto-fix policy selftest that does not pass (exit 0, an ALL PASS line,
    no FAIL line). `run(path) -> (rc, output)` is the selftest's hook; by default each runs for real:
    fixtures only, scratch stores, nothing pushed (each one's argv takes exactly `selftest`)."""
    def real(path):
        r = subprocess.run([sys.executable, os.path.join(MC, path), "selftest"], capture_output=True,
                           text=True, timeout=120)
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    bad = []
    for name, path in AUTO_FIX_SELFTESTS:
        try:
            rc, out = (run or real)(path)
        except Exception as e:
            rc, out = -1, f"{type(e).__name__}: {e}"
        fails = [ln for ln in out.splitlines() if ln.startswith("FAIL")]
        if rc != 0 or fails or not re.search(r"^ALL PASS$", out, re.M):
            bad.append((name, f"exit {rc}" + (": " + " | ".join(fails)[-200:] if fails else
                                               "" if rc else ": " + out.strip()[-200:])))
    return bad


def _deadman_why(path=None, now=None):
    """'' when the dead-box alarm is armed (healthcheck.sh re-armed it in the last 75 min, or it was
    cancelled for a planned shutdown in that time), else why not, in plain words."""
    now = int(now or time.time())
    try:
        with open(path or os.path.join(STATE, "deadman.json")) as fh:
            d = json.load(fh)
        at, cancelled = int(d.get("at") or 0), int(d.get("cancelled") or 0)
    except (OSError, ValueError, TypeError, AttributeError):
        return "state/deadman.json is missing or unreadable, so no alarm is waiting on ntfy.sh"
    if at and now - at < DEADMAN_STALE_S:
        return ""
    if cancelled and now - cancelled < DEADMAN_STALE_S:
        return ""
    if cancelled and not at:
        return (f"it was cancelled {(now - cancelled) // 60} min ago (healthcheck.sh deadman-cancel) and the "
                "watchdog has not re-armed it since")
    if not at:
        return "it has never been armed"
    return f"its last re-arm was {(now - at) // 60} min ago; the watchdog re-arms it every hour"


def _rollup_held(path=None):
    """-> (row, flags). `row` is the newest "Daily rollup" row in the notifications ledger when it
    says pushed=false, else None; `flags` says, for each of the last 14 rollups, whether it went out."""
    rows = []
    try:
        with open(path or NOTIFICATIONS, errors="replace") as fh:
            for line in fh:
                if '"Daily rollup"' not in line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("title") == "Daily rollup" and r.get("channel") == "maintenance":
                    rows.append(r)
    except OSError:
        return None, []
    flags = [r.get("pushed") is not False for r in rows[-14:]]
    return (rows[-1] if rows and rows[-1].get("pushed") is False else None), flags


def _queue_starved(f, rows, hours=48):
    """Rule queue-starved (2026-10-03): a job in the box Claude queue that has been STARTABLE for
    more than `hours` and never started. `rows` is claudeq.starving() (the selftest passes
    fixtures), which measures from the job's not_before when that is later than its filing, so a
    job deferred to next Saturday is scheduled, not starving. One finding per job, keyed by the job
    key, so it resolves the morning the job runs or is dropped. Filed and never pushed on its own
    (QUIET_KINDS); never a memo — the queue is Mission Control's, and the filer is named."""
    for r in rows or ():
        key = str(r.get("key") or "?")
        _finding(f, "queue-starved", "med", f"a Claude queue job has waited over {hours} h: {key}",
                 f"{key} (tier {r.get('tier')}, ~{r.get('est_min')}m, filed by {r.get('by') or '?'}) has been "
                 f"startable for {r.get('waiting_h')} h (filed {r.get('filed_h')} h ago) and the queue has not "
                 f"started it. Its last refusal: {r.get('skip') or 'none recorded'}. `python3 "
                 "~/maintenance/bin/claudeq.py status` shows what holds it; if a rule can never admit it, fix "
                 f"the rule or the job's estimate, and if nobody wants it any more, `claudeq.py drop {key}`.",
                 project="maintenance", fix="human", key=key)


def _jsonl_rows(path):
    """Every parsable JSON object in a .jsonl file ([] when it is missing); a bad line is skipped."""
    out = []
    try:
        with open(path) as fh:
            for ln in fh:
                try:
                    r = json.loads(ln)
                except ValueError:
                    continue
                if isinstance(r, dict):
                    out.append(r)
    except OSError:
        pass
    return out


def _ver(v):
    """'2.1.280' -> (2, 1, 280); anything unparsable -> None."""
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)", str(v or ""))
    return tuple(int(x) for x in m.groups()) if m else None


def _cli_stale(rows, now_ts=None, days=14, patches=5):
    """Rule cli-stale's core, pure (memo wa-cli-currency ask 3). `rows` are state/claude_sessions.jsonl
    records ({time, headless, cli_version}). Compares the newest headless cli_version seen in the last
    `days` with the newest interactive one in the same window. -> None when headless keeps up, else
    {headless, interactive, behind, lag_d, why}: `behind` is the patch gap (same major.minor; a newer
    minor/major counts as behind by `patches`), `lag_d` the days between the two versions' first
    sighting anywhere in the log. Fires on behind >= patches or lag_d > days."""
    now_ts = time.time() if now_ts is None else now_ts
    first, head, inter = {}, None, None
    for r in rows or ():
        v, t = _ver(r.get("cli_version")), r.get("time")
        if not v or not isinstance(t, (int, float)):
            continue
        first[v] = min(first.get(v, t), t)
        if t < now_ts - days * 86400:
            continue
        if r.get("headless") is True:
            head = max(head or v, v)
        elif r.get("headless") is False:
            inter = max(inter or v, v)
    if not head or not inter or head >= inter:
        return None
    behind = inter[2] - head[2] if inter[:2] == head[:2] else patches
    lag_d = round((first[inter] - first[head]) / 86400, 1)
    why = [w for w, hit in ((f"{behind} patch versions behind", behind >= patches),
                            (f"first seen {lag_d}d before the interactive version", lag_d > days)) if hit]
    if not why:
        return None
    return {"headless": ".".join(map(str, head)), "interactive": ".".join(map(str, inter)),
            "behind": behind, "lag_d": lag_d, "why": " and ".join(why)}


def _cli_stale_finding(f, rows, now_ts=None):
    r = _cli_stale(rows, now_ts)
    if r:
        _finding(f, "cli-stale", "low",
                 f"headless Claude runs an older CLI than David's sessions ({r['headless']} vs {r['interactive']})",
                 f"In the last 14 days the newest headless session ran Claude Code {r['headless']} and the newest "
                 f"interactive one {r['interactive']}: the headless pin is {r['why']}. Headless jobs miss the fixes "
                 "David's sessions already have. `python3 ~/maintenance/bin/cli-update.py status` shows the pin and "
                 "the canary; the canary override (moving the headless pin without a passing canary) is parked "
                 "with David.", project="maintenance", key="cli-stale")
    return r


def _backup_tamper(rows, sources, broot, now_ts=None, hash_max=200_000_000, hasher=None):
    """Rule backup-tamper's core (memo wa-backups-integrity ask 3). `rows` are state/backup_sums.jsonl
    records {ts, source, archive (relative to broot), bytes, sha256}, the last row per archive wins;
    `sources` is config/backups.json's sources (keep_days). -> [(archive, why)] for:
      * a recorded archive whose size is not its recorded bytes (a stat: cheap, every archive);
      * a recorded archive under `hash_max` bytes whose sha256 is not the recorded one (the big ones,
        hbs at ~700 MB each, are left to `backup.py verify`, which hashes everything);
      * a recorded archive gone while younger than its source's keep_days less one day of slack
        (pruning keeps the newest keep_days archives, so a nightly one leaves at ~keep_days old).
    Stray files (in ~/backups with no record) are not judged in this slice. A source no longer in
    the config is skipped (it was retired, not tampered with)."""
    now_ts = time.time() if now_ts is None else now_ts
    if hasher is None:
        import backup as _bk
        hasher = _bk.sha256_file
    last = {}
    for r in rows or ():
        if isinstance(r, dict) and r.get("archive") and r.get("source"):
            last[r["archive"]] = r
    out = []
    for rel, r in sorted(last.items()):
        spec = (sources or {}).get(r["source"])
        if not isinstance(spec, dict) or spec.get("delegated_to"):
            continue
        path = os.path.join(broot, rel)
        try:
            size = os.path.getsize(path)
        except OSError:
            try:
                age_d = (now_ts - datetime.strptime(r.get("ts", ""), "%Y-%m-%dT%H:%M:%SZ")
                         .replace(tzinfo=timezone.utc).timestamp()) / 86400
            except ValueError:
                continue
            keep = int(spec.get("keep_days", 30))
            if age_d < keep - 1:
                out.append((rel, f"disappeared {age_d:.1f}d after it was written; {r['source']} keeps "
                                 f"{keep} days, so nothing should have pruned it yet"))
            continue
        if isinstance(r.get("bytes"), int) and size != r["bytes"]:
            out.append((rel, f"is {size} bytes; {r['bytes']} were recorded when it was written"))
        elif r.get("sha256") and size < hash_max and hasher(path) != r["sha256"]:
            out.append((rel, f"same size, but its sha256 is not the recorded {str(r['sha256'])[:12]}"))
    return out


def _backup_tamper_finding(f, problems):
    for rel, why in problems:
        _finding(f, "backup-tamper", "high", f"a backup archive changed after it was written: {rel}",
                 f"~/backups/{rel} {why}. An archive is written once and never edited, so this is tampering, "
                 "disk trouble or a hand edit. Do not prune or restore over it: run `python3 "
                 "~/maintenance/bin/backup.py verify` (it re-hashes every recorded archive against the sidecar "
                 "and state/backup_sums.jsonl, which lives outside ~/backups) and compare with the .manifest.json.",
                 project="maintenance", key=rel)


# ---------------------------------------------------------------- rules 37-41 (2026-10-05)
# Pure helpers: each takes what it reads as data (or a path), so the selftest drives them on fixtures.

def _iso_ts(s):
    """An ISO-8601 UTC stamp ('...Z' or '+00:00') or epoch number -> epoch seconds, else None."""
    if isinstance(s, (int, float)) and not isinstance(s, bool):
        return float(s)
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _browser_signin_findings(f, state, sites_cfg):
    """Rule browser-signin (memo browser-signin-needs-attention ask 1): a site the Spark browser was
    signed in to once (`signed_in_once`) that `browser.py daily` now reads as logged_out or
    challenge. A registered site David never signed in to stays silent, and so does a signed-out site
    marked `ask: when_needed` (its job asks him with `browser.py ask-signin` when it needs the site)."""
    sites = (state or {}).get("sites") or {}
    cfg = (sites_cfg or {}).get("sites") or {}
    for site, s in sorted(sites.items()):
        if not isinstance(s, dict) or s.get("signed_in_once") is not True:
            continue
        st = s.get("state")
        if st not in ("logged_out", "challenge"):
            continue
        if st == "logged_out" and (cfg.get(site) or {}).get("ask") == "when_needed":
            continue        # 12twenty (2026-10-05): its job asks with `browser.py ask-signin` when it needs him
        label = (cfg.get(site) or {}).get("label") or site
        what = "a bot check (challenge)" if st == "challenge" else "signed out"
        _finding(f, "browser-signin", "med", f"the Spark browser is signed out of {label}",
                 f"{label} ({site}) reads {what} since {s.get('since') or '?'} (probe: {s.get('detail') or '?'}, "
                 f"checked {s.get('checked_at') or '?'}). Every read of it fails until David signs in again at "
                 "https://<host>.<tailnet>.ts.net:8912 (the browser's desktop). If the site is no longer "
                 "wanted, drop it from config/browser_sites.json and the check stops.",
                 project="maintenance", key=site)


def _dash_check_finding(f, d, now_ts=None, days=7):
    """Rule dashboard-check-red (memo wa-attention-kpi-leftovers): serve.sh's post-restart
    check.py run (state/dash_check.json) came back red within `days`. Missing file -> quiet."""
    if not isinstance(d, dict) or d.get("ok") is not False:
        return
    at = _iso_ts(d.get("at")) or _iso_ts(d.get("ts"))
    now_ts = now_ts or time.time()
    if not at or now_ts - at > days * 86400:
        return
    summ = d.get("summary") or [f"FAIL {x}" for x in d.get("items") or []]
    heads = [str(x) for x in summ if not str(x).startswith("FAIL - ")] or [str(x) for x in summ]
    _finding(f, "dashboard-check-red", "med",
             "the dashboard check after the last restart is red",
             f"serve.sh ran dashboard/check.py at {d.get('at') or '?'} ({d.get('trigger') or 'restart'}, rc "
             f"{d.get('rc')}, {d.get('duration_s', '?')} s) and it failed: " + "; ".join(heads)[:300]
             + ". Details: " + " | ".join(str(x) for x in summ)[:600]
             + ". Re-run `python3 ~/maintenance/dashboard/check.py`; logs/dash_check.log has the full run.",
             project="maintenance", key="dash-check")


def _push_undelivered_finding(f, path, now_ts=None, mins=30):
    """Rule push-undelivered (memo wa-alerts-and-boot slice 3): healthcheck.sh's delivery spool
    (state/alerts_pending, one JSON line {ts, title, ...} per undelivered push) holds a push older
    than `mins`. The watchdog retries it every 15 min and drops it after 24 h."""
    now_ts = now_ts or time.time()
    old = [r for r in _jsonl_rows(path)
           if isinstance(r.get("ts"), (int, float)) and now_ts - r["ts"] > mins * 60]
    if not old:
        return
    first = min(old, key=lambda r: r["ts"])
    age = int((now_ts - first["ts"]) // 60)
    _finding(f, "push-undelivered", "high", "a critical push has not reached David's phone",
             f"{len(old)} push(es) in state/alerts_pending have waited over {mins} min; the oldest, "
             f"\"{str(first.get('title') or '?')[:80]}\", for {age} min (first tried {dt_from(first['ts'])}Z). "
             "healthcheck.sh retries the spool every 15 min and drops a line after 24 h, so ntfy.sh has been "
             "unreachable or refusing since. Check the uplink and `tail ~/maintenance/logs/healthcheck.log`.",
             project="maintenance", key="spool")


GUARD_LEDGERS = (("broker", "guard_broker.jsonl", "high"), ("lead", "guard_lead.jsonl", "low"),
                 ("claude", "guard_claude.jsonl", "high"))


def _guard_denials(rows, now_ts=None, hours=24):
    """The DENY rows of one guard ledger in the last `hours` (broker/lead: verdict, ts; the claude
    spawn guard: mode, time — every row there is a firing, `mode` deny is a refusal)."""
    now_ts = now_ts or time.time()
    out = []
    for r in rows or ():
        ts = r.get("ts", r.get("time"))
        if not isinstance(ts, (int, float)) or now_ts - ts > hours * 3600:
            continue
        if (r.get("verdict") or r.get("mode")) == "deny":
            out.append(r)
    return sorted(out, key=lambda r: r.get("ts", r.get("time")))


def _guard_deny_findings(f, by_ledger, now_ts=None):
    """Rule guard-deny (memo wa-security-detection slice 3): any refusal in the last 24 h, one
    finding per ledger. A lead denied by hook-guard-lead is the guard working as designed (med);
    a broker or bare-claude refusal is an unattended session reaching for something it must not
    (high). No command or excerpt is carried: who, which tool, the guard's first reason."""
    for name, fname, sev in GUARD_LEDGERS:
        den = _guard_denials(by_ledger.get(name), now_ts)
        if not den:
            continue
        last = den[-1]
        who = last.get("lead") or last.get("who") or last.get("class") or "a session"
        tool = last.get("tool") or ("Bash" if name == "claude" else "?")
        why = last.get("reason")
        why = (why[0] if isinstance(why, list) and why else why) or (
            "a bare `claude -p` spawn" if name == "claude" else "")
        why = _scrub_secrets(str(why)[:160])
        _finding(f, "guard-deny", sev, f"the {name} guard refused {len(den)} action(s) in 24 h",
                 f"state/{fname}: {len(den)} denial(s) since {dt_from(den[0].get('ts', den[0].get('time')))}Z. "
                 f"Latest {dt_from(last.get('ts', last.get('time')))}Z: session "
                 f"{str(last.get('session') or '?')[:8]}, {who}, tool {tool}"
                 + (f", because {why}" if why else "") + ". "
                 + ("A lead stopped at its fence is the guard working; read the ledger for a lead that keeps "
                    "trying the same thing (it should file a proposal patch or a memo instead)."
                    if name == "lead" else
                    "An unattended session tried something only David (or the Stocks PM) may do: read the "
                    "session's transcript and docs/INCIDENTS.md before anything else."),
                 project="maintenance", key=name)


def _restore_unproven_finding(f, rows, now_ts=None, days=30, sources=None):
    """Rule restore-unproven (memo wa-restore-and-rebuild): the newest green `backup.py drill` row
    (state/restore_drills.jsonl) is older than `days`, or there is none. The pass does NOT run the drill
    itself (slice 4/4, 2026-10-05: a drill writes the ledger and reports/restore-drill-<month>.md outside
    its temp dir, and an hbs archive is ~700 MB to extract and sample, past a 5-minute bound); the detail
    names the exact command for the source drilled least recently (`sources`: config/backups.json's)."""
    now_ts = now_ts or time.time()
    green = [t for t in (_iso_ts(r.get("ts")) for r in rows or () if r.get("ok") is True) if t]
    if green and now_ts - max(green) <= days * 86400:
        return
    when = f"the newest is from {dt_from(max(green))}Z" if green else "none is recorded"
    nxt = _drill_next(rows, sources) if sources else None
    cmd = (f"`python3 ~/maintenance/bin/backup.py drill {nxt}` (the source drilled least recently; "
           "`backup.py drill` with no source does them all)") if nxt else "`python3 ~/maintenance/bin/backup.py drill`"
    _finding(f, "restore-unproven", "med", f"no green restore drill in {days} days",
             f"A backup is proven only by a restore. state/restore_drills.jsonl: {when}. Run "
             f"{cmd}: it extracts into a 0700 temp dir under /tmp, checksums against live, and removes it; nothing "
             "is restored over anything. `--dry` first shows the size. Then read reports/restore-drill-<month>.md.",
             project="maintenance", key="drill")


def _manifest_stale_finding(f, root, now_ts=None, days=2):
    """Rule manifest-stale (memo wa-restore-and-rebuild): the newest ~/backups/manifest/<YYYY-MM-DD>/
    (real directories only, as manifest.py writes them) is older than `days`, or there is none."""
    now_ts = now_ts or time.time()
    try:
        names = [n for n in os.listdir(root) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", n)
                 and os.path.isdir(os.path.join(root, n)) and not os.path.islink(os.path.join(root, n))]
    except OSError:
        names = []
    newest = max(names) if names else None
    t = _iso_ts(newest + "T00:00:00+00:00") if newest else None
    if t and now_ts - t <= (days + 1) * 86400:       # a day folder covers that whole UTC day
        return
    _finding(f, "manifest-stale", "med", "the nightly rebuild manifest is stale",
             f"{root}: " + (f"the newest day folder is {newest}" if newest else "no day folder")
             + f" (want one within {days} days). bin/manifest.py runs from `backup.py run` each night and "
               "never pages on its own; `python3 ~/maintenance/bin/manifest.py write --dry` shows what it would "
               "write, and logs/backup.log has its last `manifest:` line.",
             project="maintenance", key="manifest")


def _ledger_order_finding(f, path):
    """Rule ledger-order (memo review-ledger-hygiene slice 2): ~/memos/LEDGER.md is newest-first, and
    every writer goes through bin/ledger_rows.py prepend. A row dated newer than the row above it is a
    writer that appended (or a hand edit): ONE low finding with the count and the first offender.
    A missing or unreadable ledger is quiet (other rules own the bus)."""
    import ledger_rows
    try:
        with open(path, errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return
    bad = ledger_rows.out_of_order(lines)
    if not bad:
        return
    i, d, prev, memo = bad[0]
    _finding(f, "ledger-order", "low", "the memo ledger has rows out of date order",
             f"{path}: {len(bad)} row(s) dated newer than the row above them; the first is line {i} "
             f"({d} {memo}, under a {prev} row). The ledger is newest-first and every writer prepends through "
             "bin/ledger_rows.py; a row like this came from a writer that appends or a hand edit. "
             "`python3 ~/maintenance/bin/ledger_rows.py check` lists them; move each up to its date.",
             project="maintenance", key="ledger")


# ---------------------------------------------------------------- rules 44-45 (2026-10-05)
# 44 fleet-stop-engaged (memo wa-security-detection slice 4/4): while `paused.py fleet-stop` is engaged the
# Claude cron lines it commented out are paused on purpose, not broken. The job-shaped findings about those
# jobs are held, and ONE finding says since when and why. 45 backup-stray (memo wa-backups-integrity slice
# 4/4): a file or folder in the top two levels of ~/backups that no writer declares. Both pure on fixtures.
FLEET_SUPPRESS_KINDS = {"job-silent", "log-missing", "registry-orphan", "registry-missing", "registry-false-claim",
                        "weight-missing", "name-missing", "owner-missing", "crew-unclaimed", "queue-starved",
                        "lead-overdue", "schedule-blocker", "claude-protected-window"}
FLEET_GENERIC = {"claudeq.py", "claude-headless", "stream_filter.py", "notify.sh"}


def _fleet_state():
    """paused.fleet_state() (PAUSED_FLEET_STATE moves the file): the engaged state dict, or None."""
    import paused
    return paused.fleet_state()


def _fleet_stop_tokens(st, live_scripts=()):
    """What names a paused job in a finding: each `--job "<name>"` of the saved lines, and each script
    they run that no live cron line still runs (a script shared with a live line names that line too)."""
    toks = set()
    for j in _parse_cron("\n".join(st.get("lines") or [])):
        toks |= {x for x in j["scripts"] if x not in FLEET_GENERIC and x not in set(live_scripts)}
        toks |= set(re.findall(r"""--job\s+["']([^"']+)["']""", j["cmd"]))
    return {t for t in toks if len(t) >= 4}


def _fleet_stop_apply(f, st, live_scripts=()):
    """-> the findings to keep. Not engaged: `f` unchanged. Engaged: drop the FLEET_SUPPRESS_KINDS
    findings that name a paused job, and add one high fleet-stop-engaged (quiet: David engaged it)."""
    if not st or not st.get("engaged_at"):
        return f
    toks = _fleet_stop_tokens(st, live_scripts)
    keep, held = [], []
    for x in f:
        blob = " ".join(str(x.get(k, "")) for k in ("title", "detail", "key"))
        (held if x.get("kind") in FLEET_SUPPRESS_KINDS and any(t in blob for t in toks) else keep).append(x)
    n = len(st.get("lines") or [])
    _finding(keep, "fleet-stop-engaged", "high", "fleet-stop is engaged: the scheduled Claude jobs are paused",
             f"Since {st['engaged_at']}: {st.get('reason') or 'no reason given'}. {n} Claude cron line(s) carry "
             f"`# PAUSED fleet-stop` ({', '.join(sorted(toks))[:300] or 'no named jobs'}); "
             f"{len(held)} finding(s) about those jobs are held while it is engaged "
             f"({', '.join(sorted({x['kind'] for x in held})) or 'none today'}). Lines that start Claude inside "
             "their own code (lead.py, memo-process, Stocks' loop.py/ops.py, `claudeq.py tick`) still run: "
             "`python3 ~/maintenance/bin/paused.py fleet-stop --dry` lists them. Release is David's incident action: "
             "`python3 ~/maintenance/bin/paused.py fleet-stop release --yes`.",
             project="maintenance", key="fleet-stop")
    return keep


BACKUP_STRAY_SINCE = "2026-10-05T00:00:00Z"   # the baseline: anything older predates the rule
BACKUP_FIXED = ("manifest", "incident", "crontab", "stocks-worktrees", "family-vault")   # contents not judged
BACKUP_FILES = ("README.md",)


def _backup_written(src, name):
    """A name backup.py writes in a source folder: `<src>_<date>.tar.gz`, its two sidecars, or a
    dot-prefixed atomic temp (`_write_atomic`)."""
    base = re.escape(src) + r"_\d{4}-\d{2}-\d{2}\.tar\.gz(?:\.sha256|\.manifest\.json)?"
    return bool(re.fullmatch(base, name) or re.fullmatch(r"\." + base + r"\.tmp", name))


def _backup_stray(broot, sources, since_ts=None):
    """Rule backup-stray's core: the relative paths at the top two levels of `broot` that no writer
    declares and that are newer than the baseline. Declared: each source folder in config/backups.json
    (lowercased, as backup.py's dest_dir does; a delegated source's folder is opaque, its writer is
    someone else), BACKUP_FIXED folders (opaque: manifest.py, paused.py's incident/, hand copies) and
    BACKUP_FILES. Inside a backup.py source folder only _backup_written names. Stat only, no hashing."""
    since_ts = _iso_ts(BACKUP_STRAY_SINCE) if since_ts is None else since_ts
    srcs = {str(n).lower(): (s or {}) for n, s in (sources or {}).items()}

    def new(p):
        try:
            return os.lstat(p).st_mtime >= since_ts
        except OSError:
            return False
    out = []
    try:
        top = sorted(os.listdir(broot))
    except OSError:
        return out
    for e in top:
        p = os.path.join(broot, e)
        real_dir = os.path.isdir(p) and not os.path.islink(p)
        if e in BACKUP_FILES and os.path.isfile(p):
            continue
        if real_dir and (e in BACKUP_FIXED or (e in srcs and srcs[e].get("delegated_to"))):
            continue
        if real_dir and e in srcs:
            for g in sorted(os.listdir(p)):
                q = os.path.join(p, g)
                if os.path.isfile(q) and not os.path.islink(q) and _backup_written(e, g):
                    continue
                if new(q):
                    out.append(f"{e}/{g}")
            continue
        if new(p):
            out.append(e)
    return out


def _backup_stray_finding(f, stray, limit=10):
    if not stray:
        return
    _finding(f, "backup-stray", "med", f"{len(stray)} undeclared item(s) in ~/backups",
             "Nothing in config/backups.json, the fixed folders (" + ", ".join(BACKUP_FIXED) + ") or "
             "README.md accounts for: " + ", ".join(stray[:limit])
             + (f" and {len(stray) - limit} more" if len(stray) > limit else "")
             + f". Older entries (before {BACKUP_STRAY_SINCE[:10]}) are the baseline. ~/backups is written only "
             "by bin/backup.py, Stocks' backup.sh, manifest.py and paused.py; a file outside them is a hand copy, a "
             "new writer that skipped config/backups.json, or tampering. Declare it, move it out, or mute with the reason.",
             project="maintenance", key="backup-stray")


def _drill_next(rows, sources):
    """Round-robin: the non-delegated source drilled least recently (never drilled first, config order)."""
    last = {}
    for r in rows or ():
        t = _iso_ts(r.get("ts")) if isinstance(r, dict) else None
        if t and r.get("source") and r.get("ok") is not None:
            last[r["source"]] = max(t, last.get(r["source"], 0))
    cands = [n for n, s in (sources or {}).items() if not (s or {}).get("delegated_to")]
    return min(cands, key=lambda n: (last.get(n, 0), cands.index(n))) if cands else None


# ---------------------------------------------------------------- weight-drift (rule 43)
# Memo wa-cost-attribution slice 3/4 (2026-10-05): the daily pass trues config/job_weights.json up from
# measured runs (bin/weight_drift.py, imported: measure + apply_rows + _write), ONCE a UTC day
# (state/weight_drift_applied.json), and commits the file by path. Rows with 1-2 measured runs are not
# rewritten (n < MIN_N); when one is more than half off its declared figure it is a low, quiet finding.
WEIGHT_DRIFT_STAMP = os.path.join(STATE, "weight_drift_applied.json")
WEIGHT_DRIFT_OFF = 0.5


def _weight_drift_findings(f, rows, min_n=3):
    """rows: weight_drift.measure() rows. One low finding listing the n < min_n rows > 50% off."""
    off = []
    for r in rows:
        dec, med, n = r.get("declared"), r.get("median"), r.get("n") or 0
        if not (0 < n < min_n) or not dec or not med:
            continue
        if abs(med - dec) > WEIGHT_DRIFT_OFF * dec:
            off.append(f"{r['match']}: declared {dec // 1000:,}k, measured {med // 1000:,}k (n={n})")
    if not off:
        return
    _finding(f, "weight-drift", "low",
             f"{len(off)} job weight{'s' if len(off) != 1 else ''} more than half off {'their' if len(off) != 1 else 'its'} "
             f"measured runs, too few runs to rewrite",
             "bin/weight_drift.py rewrites a weight only at n >= 3 measured runs in 30 days; these have 1-2 and "
             "differ from config/job_weights.json by more than 50%: " + "; ".join(off)
             + ". Nothing to do unless the figure is plainly wrong: the daily pass rewrites each row on its "
               "third run. `python3 ~/maintenance/bin/weight_drift.py show` has the table.",
             project="maintenance", key="weights")


def weight_drift_pass(dry=False, live=None, weights=None, stamp=None, today=None, commit=None):
    """-> (line, findings). `live` (tests) returns (doc, rows) like weight_drift._live without unattr.
    Applies at most once per UTC day, never on --dry; a failure is a line, never an exception."""
    import weight_drift as wd
    weights = weights or wd.WEIGHTS
    stamp = stamp or WEIGHT_DRIFT_STAMP
    today = today or f"{datetime.now(timezone.utc):%Y-%m-%d}"
    f = []
    try:
        if live:
            doc, rows = live()
        else:
            doc, rows, _u, _n = wd._live(time.time())
    except Exception as e:
        return f"weight drift could not measure: {type(e).__name__}: {str(e)[:160]}", f
    _weight_drift_findings(f, rows, wd.MIN_N)
    if dry:
        n = sum(1 for r in rows if r.get("action") in ("rewrite", "add"))
        return f"weight drift: {n} row(s) measurable (dry: nothing written)", f
    if (load(stamp, {}) or {}).get("day") == today:
        return "", f
    try:
        ch = wd.apply_rows(doc, rows, today)
        ch = [c for c in ch if c[1] != c[2]]          # a row already at its median is not a change
        if ch:
            wd._write(doc, weights)
        save(stamp, {"day": today, "changes": [list(c) for c in ch]})
    except Exception as e:
        return f"weight drift apply failed: {type(e).__name__}: {str(e)[:160]}", f
    if not ch:
        return "weight drift: no weight moved", f
    rel = os.path.relpath(weights, MC)
    sha = (commit or _commit)([rel], f"weights: {len(ch)} row(s) trued up from measured runs (backoffice daily "
                                     "pass, bin/weight_drift.py; memo wa-cost-attribution slice 3/4)\n\n"
                              + "\n".join(f"{m}: {old} -> {new} (n={k})" for m, old, new, k in ch))
    return (f"weight drift: {len(ch)} row(s) rewritten in {rel}"
            + (f", {sha}" if sha else ", not committed")), f


# Finding kinds that are filed (Needs attention, findings.json) but never by themselves make the
# pass push: a slow signal David reads on the page, not on his phone.
# ---------------------------------------------------------------- argv-unsafe (rule 34)
# Memo wa-lessons-propagation ask 1 (2026-10-04): ea7af80's lesson ("an argument a script does not know
# is exit 2, never the live job") carried to every project. STATIC ONLY: each script is parsed with ast
# and never imported or run. Heuristics lean to missing some rather than flagging a fixed script: a
# dispatch with an exit-2 branch, argparse parse_args, or an argv-checking helper counts as guarded.
# Scope (2026-10-05, memo review-argv-scan-scope): every *.py under the project with a `__main__` block,
# not only bin/ and scripts/ (Stocks keeps runnable scripts in _engine/, hbs in pipeline folders). Pruned:
# hidden dirs, vendored/generated trees by name (_ARGV_SKIP), and whatever the project's own .gitignore
# ignores (one `git check-ignore --stdin` per project, over the candidates only). Declared data_roots are
# NOT pruned: Stocks declares a code folder (_engine/sources) as one, and a data file is not a .py, so
# walking them costs a directory listing. Bounded: ARGV_MAX_ENTRIES walked, ARGV_MAX_PY read, ARGV_MAX_BYTES each.
ARGV_OPT_OUT = "# argv: data"
ARGV_CAP = 15
ARGV_ASKED = os.path.join(STATE, "argv_asked.json")   # project -> the argv-unsafe memo filed and the list it named
_ARGV_SKIP = {"__pycache__", "node_modules", ".venv", "venv", "env", "site-packages", "dist-packages", ".git",
              "dist", "build", "worktrees", "backups", "vendor", "third_party", "data", "logs", "cache",
              "tmp", "out", "output", "htmlcov", "egg-info"}
ARGV_MAX_ENTRIES = 200_000     # directory entries walked per project
ARGV_MAX_PY = 4000             # .py files read per project
ARGV_MAX_BYTES = 512 * 1024    # a bigger .py is generated, not a script someone runs by hand
_ARGV_HELPER = re.compile(r"(parse|check|valid|error|guard).*argv|argv.*(error|check|valid|guard)", re.I)
_ARGV_HARMLESS = {"print", "exit", "quit", "_exit", "SystemExit", "usage", "_usage", "print_usage", "print_help",
                  "write", "strip", "splitlines", "split", "join", "format", "lower", "upper", "dumps", "get"}
_ARGV_READONLY = {"list", "status", "show", "report", "help", "usage", "packet", "summary", "check", "ls", "info",
                  "view", "stats", "print", "doctor", "audit", "scan", "selftest", "test", "dry", "--dry", "--dry-run"}


def _is_sys_argv(n):
    import ast
    return isinstance(n, ast.Attribute) and n.attr == "argv" and isinstance(n.value, ast.Name) and n.value.id == "sys"


def _argv_scan_source(src):
    """-> [(line, why)] for one script's source. Empty when it has no `__main__` block, never reads
    sys.argv, opts out with `# argv: data`, or does not parse."""
    import ast
    if ARGV_OPT_OUT in src:
        return []
    try:
        tree = ast.parse(src)
    except (SyntaxError, ValueError):
        return []

    def is_main(t):
        return (isinstance(t, ast.Compare) and isinstance(t.left, ast.Name) and t.left.id == "__name__"
                and any(isinstance(c, ast.Constant) and c.value == "__main__" for c in t.comparators))
    main = [n for n in tree.body if isinstance(n, ast.If) and is_main(n.test)]
    if not main:
        return []
    funcs = {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    scope, seen, argvish, sliced = list(main), set(), set(), set()

    def from_argv(e):
        return _is_sys_argv(e) or (isinstance(e, ast.Subscript) and _is_sys_argv(e.value)
                                   and isinstance(e.slice, ast.Slice))
    # one level into the module functions the main block calls (claudeq's `_cli(sys.argv)` shape)
    for node in [x for m in main for x in ast.walk(m)]:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in funcs \
                and node.func.id not in seen:
            fn = funcs[node.func.id]
            seen.add(fn.name)
            scope.append(fn)
            params = [a.arg for a in fn.args.args]
            for i, a in enumerate(node.args):
                if from_argv(a) and i < len(params):
                    argvish.add(params[i])
                    if not _is_sys_argv(a):
                        sliced.add(params[i])
    nodes = [x for s in scope for x in ast.walk(s)]
    for n in nodes:                                   # `args = sys.argv[1:]`
        if isinstance(n, ast.Assign) and from_argv(n.value):
            argvish.update(t.id for t in n.targets if isinstance(t, ast.Name))
            if not _is_sys_argv(n.value):
                sliced.update(t.id for t in n.targets if isinstance(t, ast.Name))

    def is_argv(e):
        return from_argv(e) or (isinstance(e, ast.Name) and e.id in argvish) or \
            (isinstance(e, ast.Subscript) and isinstance(e.slice, ast.Slice) and is_argv(e.value))
    if not any(is_argv(n) for n in nodes) and not any(
            isinstance(n, ast.Attribute) and n.attr == "parse_known_args" for n in nodes):
        return []

    def call_name(c):
        f = c.func
        return f.id if isinstance(f, ast.Name) else (f.attr if isinstance(f, ast.Attribute) else "")

    def nonzero_int(args):
        return bool(args) and isinstance(args[0], ast.Constant) and isinstance(args[0].value, int) \
            and not isinstance(args[0].value, bool) and args[0].value != 0
    guarded = False
    for n in nodes:
        if isinstance(n, ast.Call):
            nm = call_name(n)
            if nm in ("exit", "_exit", "SystemExit") and nonzero_int(n.args):
                guarded = True
            elif nm == "parse_args" or (nm and _ARGV_HELPER.search(nm)):
                guarded = True
        elif isinstance(n, ast.Return) and isinstance(n.value, ast.Constant) and n.value.value == 2:
            guarded = True                            # gpu.py / lead.py: the dispatch returns 2 to sys.exit
    hits = []
    for n in nodes:
        if isinstance(n, ast.Attribute) and n.attr == "parse_known_args":
            hits.append((n.lineno, "argparse parse_known_args: unknown arguments are ignored"))
    if guarded:
        return sorted(set(hits))
    def data_idx(e):                                  # argv[N] past the command word
        return isinstance(e, ast.Subscript) and is_argv(e.value) and isinstance(e.slice, ast.Constant) \
            and isinstance(e.slice.value, int) and e.slice.value >= (1 if (isinstance(e.value, ast.Name)
                                                                       and e.value.id in sliced) else 2)
    # statements directly in the main block or a dispatch function's body: a script-wide optional
    # positional every command shares (hbs learnings_review: `wk = sys.argv[2] if len(sys.argv) > 2 else None`)
    top = [st for s_ in scope for st in s_.body]
    cmdvars, defaults = set(), {}
    for n in nodes:
        if isinstance(n, ast.Compare) and isinstance(n.left, ast.Constant) and isinstance(n.left.value, str) \
                and "dry" in n.left.value.lower() and n.left.value.startswith("-") \
                and any(isinstance(o, (ast.In, ast.NotIn)) for o in n.ops) and any(is_argv(c) for c in n.comparators):
            hits.append((n.lineno, f"dry flag {n.left.value!r} read by membership: a misspelt dry flag "
                                   "(--dry-run, -n) is ignored and the live job runs"))
        elif isinstance(n, ast.Assign) and isinstance(n.value, ast.IfExp) and isinstance(n.value.orelse, ast.Constant) \
                and isinstance(n.value.orelse.value, str) and isinstance(n.value.body, ast.Subscript) \
                and is_argv(n.value.body.value):
            for t in n.targets:
                if isinstance(t, ast.Name):
                    cmdvars.add(t.id)
                    defaults[t.id] = n.value.orelse.value
    for st in (top if cmdvars else ()):              # only beside a command word: the `run --dry` shape
        if isinstance(st, ast.Assign) and isinstance(st.value, ast.IfExp) and data_idx(st.value.body) \
                and not any(isinstance(c, ast.Call) and not (isinstance(c.func, ast.Name) and c.func.id == "len")
                            for c in ast.walk(st.value.test)):
            hits.append((st.lineno, "an optional second argument is taken as data unchecked "
                                    "(`run --dry` reads '--dry' as that argument and runs live)"))

    def live_calls(stmts):
        out, stack = [], list(stmts)
        while stack:
            x = stack.pop()
            if isinstance(x, ast.Call):
                nm = call_name(x)
                if nm == "print":
                    continue                          # whatever a print formats is not an action
                if nm and nm not in _ARGV_HARMLESS:
                    out.append(nm)
            stack.extend(ast.iter_child_nodes(x))
        return out
    # a command default (`sys.argv[1] if len(sys.argv) > 1 else "run"`) whose if/elif chain ends in a
    # bare `else:` that calls something: an unknown command runs that live path
    chained = set()
    for n in nodes:
        if not isinstance(n, ast.If) or id(n) in chained:
            continue
        t = n.test
        if not (isinstance(t, ast.Compare) and isinstance(t.left, ast.Name) and t.left.id in cmdvars):
            continue
        if defaults.get(t.left.id, "").lower() in _ARGV_READONLY:
            continue                                  # an unknown word falls to a read-only default
        cur = n
        while len(cur.orelse) == 1 and isinstance(cur.orelse[0], ast.If):
            cur = cur.orelse[0]
            chained.add(id(cur))
        if cur is n or not cur.orelse:
            continue
        calls = live_calls(cur.orelse)
        if calls and not any(isinstance(x, ast.Raise) for x in cur.orelse):
            hits.append((cur.orelse[0].lineno, f"an unknown command falls to a live `else:` ({calls[0]}())"))
    return sorted(set(hits))


def _argv_gitignored(root, rels):
    """-> the subset of `rels` (paths relative to `root`) the project's git ignores. Empty when root is
    not a work tree or git fails: a missing check never hides a script."""
    if not rels or not os.path.exists(os.path.join(root, ".git")):
        return set()
    try:
        r = subprocess.run(["git", "-C", root, "check-ignore", "--stdin"], input="\n".join(rels) + "\n",
                           capture_output=True, text=True, timeout=20)
    except Exception:
        return set()
    return set(r.stdout.splitlines()) if r.returncode in (0, 1) else set()


def _argv_candidates(root, stats=None):
    """-> sorted relpaths of the project's runnable scripts: *.py with a `__main__` and an `argv` in the
    source, under `root`, pruned and bounded as above. `stats` (a dict) gets walked/read/capped counts."""
    walked = read = 0
    capped = ""
    cands = []
    for dp, dns, fns in os.walk(root):
        dns[:] = sorted(x for x in dns if x not in _ARGV_SKIP and not x.startswith(".")
                        and not x.endswith(".egg-info"))
        walked += len(dns) + len(fns)
        if walked > ARGV_MAX_ENTRIES:
            capped = f"walked {ARGV_MAX_ENTRIES} entries"
            break
        for fn in sorted(fns):
            if not fn.endswith(".py"):
                continue
            p = os.path.join(dp, fn)
            try:
                if os.path.islink(p) or os.path.getsize(p) > ARGV_MAX_BYTES:
                    continue
                if read >= ARGV_MAX_PY:
                    capped = f"read {ARGV_MAX_PY} .py files"
                    break
                read += 1
                src = open(p, encoding="utf-8", errors="replace").read()
            except OSError:
                continue
            if "__main__" in src and "argv" in src:
                cands.append(os.path.relpath(p, root))
        if capped:
            break
    ign = _argv_gitignored(root, cands)
    if stats is not None:
        stats.update(walked=walked, read=read, candidates=len(cands), ignored=len(ign), capped=capped)
    return sorted(c for c in cands if c not in ign)


def argv_scan(home=None, projects=None, stats=None):
    """-> {project: [(relpath, line, why)]} over every runnable script of every project in the roster
    (see the scope note above). `stats` (a dict) gets {project: _argv_candidates stats}."""
    home = home or HOME
    out = {}
    for proj in (projects if projects is not None else _project_dirs(home)):
        root = os.path.join(home, proj)
        st = {}
        rows = []
        for rel in _argv_candidates(root, st):
            try:
                src = open(os.path.join(root, rel), encoding="utf-8", errors="replace").read()
            except OSError:
                continue
            rows += [(rel, ln, why) for ln, why in _argv_scan_source(src)]
        if stats is not None:
            stats[proj] = st
        if rows:
            out[proj] = rows
    return out


def _argv_findings(f, scan):
    for proj, rows in sorted(scan.items()):
        files = sorted({r[0] for r in rows})
        lines = [f"{r}:{ln} — {why}" for r, ln, why in rows[:ARGV_CAP]]
        more = f"\n…and {len(rows) - ARGV_CAP} more" if len(rows) > ARGV_CAP else ""
        _finding(f, "argv-unsafe", "low",
                 f"{len(files)} script(s) in {proj} can run a live path on an unknown argument",
                 "Read statically, never run: each of these dispatches on sys.argv with no exit-2 branch, so a "
                 "misspelt flag or command runs the live job (Mission Control's 10-03 lesson, commit ea7af80). "
                 "Fix: unknown arguments exit 2 with one usage line, --dry and --dry-run as one flag, argv cases "
                 "in the selftest; or mark a script `# argv: data` and ask maintenance to mute it with a reason.\n"
                 + "\n".join(lines) + more,
                 project=proj, key="scripts")


def _ledger_row_for(name, target):
    try:
        with open(os.path.join(MEMOS, "LEDGER.md")) as fh:
            return any(f"| {name} |" in ln and f"| {target} |" in ln for ln in fh)
    except OSError:
        return False


def argv_memos(scan, dry=False, now_ts=None):
    """ONE argv-unsafe memo per non-Stocks project (ARGV_ASKED), filed again only when its list grows
    (a script:reason not named before). Stocks: the finding only, never a memo, until David reopens it.
    -> [(project, outcome)], outcome filed:<path> | would-file | held-stocks | asked-before."""
    now_ts = time.time() if now_ts is None else now_ts
    asked = load(ARGV_ASKED, {})
    out, changed = [], False
    for proj, rows in sorted(scan.items()):
        target = proj.lower()
        if target == "stocks":
            out.append((proj, "held-stocks"))
            continue
        cur = sorted({f"{r}: {why}" for r, _, why in rows})
        prev = asked.get(target)
        if prev is not None:
            grew = sorted(set(cur) - set(prev.get("hits") or []))
            if not grew:
                out.append((proj, "asked-before"))
                continue
        elif _ledger_row_for("argv-unsafe", target):
            out.append((proj, "asked-before"))
            continue
        if dry:
            out.append((proj, "would-file"))
            continue
        lead = "the PM" if target == "stocks" else f"{target.replace('-', '_')}_lead"
        listing = "\n".join(f"- `{r}:{ln}` — {why}" for r, ln, why in rows[:ARGV_CAP])
        body = (f"# {proj}: scripts that run a live path on an unknown argument\n\n_From: Mission Control back-office "
                f"pass · {datetime.fromtimestamp(now_ts, timezone.utc):%Y-%m-%d} · target: {target} ({lead}) · "
                "rule argv-unsafe_\n\n**Evidence** (static read with ast; nothing was run)\n" + listing +
                (f"\n- …and {len(rows) - ARGV_CAP} more" if len(rows) > ARGV_CAP else "") +
                "\n\n**Why.** On 2026-10-03 a checker ran `memo-process.py --selftest`; nothing parsed the argument, the "
                "live pass ran, and it started a real session in David's evening. Mission Control fixed its own bin/ "
                "in commit ea7af80 (see `git -C ~/maintenance show ea7af80`).\n\n**Ask.** Apply the same pattern to the "
                "scripts above: a `parse_argv()` that accepts only the known commands and flags, `--dry` and "
                "`--dry-run` as one flag, anything else exit 2 with one usage line and nothing run; add argv cases to "
                "the script's selftest. A script whose second argument really is free data can carry `# argv: data` "
                "and ask `maintenance` for a mute with its reason (box rule 7).\n\nFiled once; filed again only if the "
                "list grows. The finding stays on Needs attention until the scan comes back clean.\n")
        path = _write_memo(target, "argv-unsafe", body, "auto-filed by the daily back-office audit (argv-unsafe)")
        if path:
            asked[target] = {"at": datetime.fromtimestamp(now_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                             "path": path, "hits": cur}
            changed = True
        out.append((proj, f"filed:{path}" if path else "asked-before"))
    if changed and not dry:
        save(ARGV_ASKED, asked)
    return out


# ---------------------------------------------------------------- posture (rule 35)
# Memo wa-security-detection ask 2 (2026-10-04): the box's outside posture against a written baseline.
# (a) a GitHub repo turned public, or a new public one; (b) a new SSH key on the GitHub account; (c) a
# listener off loopback whose port the dashboard's KNOWN_PORTS does not declare. gh is polled at most
# hourly (state/posture.json); a gh failure is a note there, never a finding.
EXPECTED_GITHUB = os.path.join(CONFIG, "expected_github.json")
POSTURE = os.path.join(STATE, "posture.json")
POSTURE_GH_EVERY_S = 3600
POSTURE_EPHEMERAL = 30000          # a listener at or above this with no pid of ours is someone else's ephemeral
PORT_IGNORE = {53, 631, 5355, 11000, 19999, 3493, 4317, 8125, 22, 41641, 5353}   # rule 5's quiet list


def _rule5_reports(port, d):
    """True when rule 5 (port-undeclared) already files this port, so rule 35 does not file it twice."""
    return (port not in PORT_IGNORE and 1024 <= port < 32768 and port not in d.get("known_ports", [])
            and port not in d.get("infra_ports", []))


def _gh_bin():
    """gh by absolute path: cron's PATH has no ~/.local/bin, so a bare "gh" never ran under cron
    and the posture rule's GitHub half was inert (2026-10-04 handoff check)."""
    import shutil
    return shutil.which("gh") or next((p for p in (os.path.expanduser("~/.local/bin/gh"),
                                                   "/usr/bin/gh", "/usr/local/bin/gh")
                                       if os.path.exists(p)), "gh")


def _gh_run(cmd):
    if cmd and cmd[0] == "gh":
        cmd = [_gh_bin()] + list(cmd[1:])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return r.returncode, r.stdout, (r.stderr or "").strip()[-200:]
    except Exception as e:
        return 1, "", f"{type(e).__name__}: {e}"[:200]


def _gh_snapshot(owner="userdev", run=None, now_ts=None, cache=None):
    """-> {"at", "repos": [{name, visibility}] | None, "keys": [id] | None, "notes": [..]}; reuses the
    cached snapshot younger than POSTURE_GH_EVERY_S (a failed poll included: at most one try an hour)."""
    run, now_ts, cache = run or _gh_run, time.time() if now_ts is None else now_ts, cache or POSTURE
    old = load(cache, {})
    if isinstance(old, dict) and old.get("at") and now_ts - float(old["at"]) < POSTURE_GH_EVERY_S:
        return old
    snap = {"at": now_ts, "repos": None, "keys": None, "notes": []}
    rc, out, err = run(["gh", "repo", "list", owner, "--limit", "100", "--json", "name,visibility"])
    try:
        if rc != 0:
            raise ValueError(f"rc {rc}: {err}")
        snap["repos"] = [{"name": r["name"], "visibility": str(r["visibility"]).upper()} for r in json.loads(out)]
    except Exception as e:
        snap["notes"].append(f"gh repo list failed: {str(e)[:160]}")
    rc, out, err = run(["gh", "api", "user/keys"])
    try:
        if rc != 0:
            raise ValueError(f"rc {rc}: {err}")
        snap["keys"] = sorted(int(k["id"]) for k in json.loads(out))
    except Exception as e:
        snap["notes"].append(f"gh api user/keys failed: {str(e)[:160]}")
    try:
        save(cache, snap)
    except OSError:
        pass
    return snap


def _ss_listeners(text):
    """`ss -ltnp` -> [(addr, port, pid|None)] for the TCP listeners off loopback."""
    out = []
    for line in (text or "").splitlines()[1:]:
        m = re.search(r"^\S+\s+\d+\s+\d+\s+(\S+):(\d+)\s", line)
        if not m:
            continue
        addr = m.group(1).strip("[]").split("%")[0]
        if addr.startswith("127.") or addr == "::1" or "%lo" in m.group(1):
            continue
        pm = re.search(r"pid=(\d+)", line)
        out.append((addr, int(m.group(2)), int(pm.group(1)) if pm else None))
    return out


def _posture(snap, expected, listeners, known_ports, skip=()):
    """-> [(sev, title, detail, key)]. Pure: the fixtures feed it fake gh and ss outputs."""
    probs = []
    pub, priv = set(expected.get("public") or []), set(expected.get("private") or [])
    for r in snap.get("repos") or []:
        if r["visibility"] != "PUBLIC" or r["name"] in pub:
            continue
        turned = r["name"] in priv
        probs.append(("high", f"GitHub repo {r['name']} is {'now public' if turned else 'a new public repo'}",
                      (f"{r['name']} was private in config/expected_github.json and the account now shows it PUBLIC. "
                       if turned else f"{r['name']} is public and is not in config/expected_github.json. ")
                      + "Every project repo is private; only the authored <repo>-public counterparts may be public. "
                      "If this was not David, make it private now (`gh repo edit userdev/<repo> --visibility "
                      "private --accept-visibility-change-consequences`) and check the audit log; if it was "
                      "intended, add it to the file's public list.", f"repo:{r['name']}"))
    want = {int(k) for k in expected.get("keys") or []}
    for k in snap.get("keys") or []:
        if int(k) not in want:
            probs.append(("high", f"a new SSH key ({k}) is on the GitHub account",
                          f"`gh api user/keys` lists key id {k}, which config/expected_github.json does not expect. "
                          "An unknown key can push to every repo. If David did not add it, delete it "
                          f"(`gh api -X DELETE user/keys/{k}`) and rotate; if he did, add the id to the file.",
                          f"key:{k}"))
    seen = set()
    for addr, port, pid in listeners:
        if port in known_ports or port in skip or port in seen or (port >= POSTURE_EPHEMERAL and pid is None):
            continue
        seen.add(port)
        wide = addr in ("0.0.0.0", "*", "::")
        probs.append(("high" if wide else "med", f"port {port} listens off loopback and is not in KNOWN_PORTS",
                      f"`ss -ltn` shows {addr}:{port}" + (f" (pid {pid})" if pid else "") +
                      (", on every interface (the home LAN included). " if wide else ". ") +
                      "Box rule 5: a listening port is declared in the dashboard's KNOWN_PORTS, healthcheck.sh and "
                      "~/INFRASTRUCTURE.md, tailnet or localhost only. Stop it, or declare it.", f"port:{port}"))
    return probs


def _posture_findings(f, probs):
    for sev, title, detail, key in probs:
        _finding(f, "posture", sev, title, detail, project="maintenance", key=key)


QUIET_KINDS = {"queue-starved", "cli-stale", "argv-unsafe",
               # rules 37-41 (2026-10-05): findings only, never a push of their own (the brief: no pushes)
               "browser-signin", "dashboard-check-red", "push-undelivered", "guard-deny",
               "restore-unproven", "manifest-stale",
               "ledger-order", "weight-drift",  # rules 42-43 (2026-10-05): low, filed, never a push
               "fleet-stop-engaged", "backup-stray",  # rules 44-45: David engaged it / filed, never a push
               "experiment-no-driver"}  # rule 46: the design memo reaches David as memos do today, no new push


def _push_worthy(new):
    """The new findings that may make run() push (everything but QUIET_KINDS)."""
    return [x for x in new if x.get("kind") not in QUIET_KINDS]


def _guard_gaps(settings, script, tools, bindir=None):
    """What keeps a PreToolUse guard from being armed: [] when it is listed for every tool it
    covers (the matcher read as Claude Code reads it: a regex, "" or "*" for all), executable,
    and its own fixture selftest passes. audit() files each guard's title as a literal
    _finding, so the dashboard's guardrail page can read it from source."""
    try:
        gaps = []
        for tool in tools:
            hit = False
            for grp in ((settings or {}).get("hooks", {}).get("PreToolUse") or []):
                mt = grp.get("matcher") or ""
                try:
                    m = mt in ("", "*") or re.fullmatch(mt, tool)
                except re.error:
                    m = mt == tool
                if m and any(script in (h.get("command") or "") for h in grp.get("hooks", [])):
                    hit = True
            if not hit:
                gaps.append(tool)
        out = ["not listed under hooks.PreToolUse for " + ", ".join(gaps)] if gaps else []
        p = os.path.join(bindir or os.path.join(MC, "bin"), script)
        if not os.access(p, os.X_OK):
            return out + ["not executable"]
        r = subprocess.run([p, "selftest"], capture_output=True, text=True, timeout=60)
        o = (r.stdout or "") + (r.stderr or "")
        if r.returncode != 0 or "ALL PASS" not in o:
            out.append(f"`{script} selftest` exits {r.returncode}: "
                       + " | ".join(l for l in o.splitlines() if l.startswith("FAIL"))[-240:])
        return out
    except Exception as e:
        return [f"the armed-check itself raised {type(e).__name__}: {e}"]


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
    for port in c["ports"]:
        if _rule5_reports(port, d):
            where = ["dashboard KNOWN_PORTS", "INFRASTRUCTURE.md"]
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
                 (f"{len(dia['unrendered'])} diagram sources edited but not re-rendered" if len(dia['unrendered']) != 1
                  else "1 diagram source edited but not re-rendered"),
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
    # ...and the slot's own rules (2026-10-03): the clock, the fit rule, the cron reader and its
    # weekday guards. Its selftest sat at 91/98 for days (the pinned config read Stocks' live
    # pm_days) and nothing ran it, so a broken fit rule would have looked the same as a fine one.
    _ok, _out = _selfcheck(["claudeq.py", "selftest"], "passed")
    if not _ok:
        _finding(f, "guardrail-inert", "high",
                 "claudeq selftest fails — the box slot's clock or fit rule no longer does what was agreed",
                 "run `python3 ~/maintenance/bin/claudeq.py selftest`; each FAIL line names the case (the "
                 "evening block, the trade-session band, a reservation read from the crontab, the 5h budget). "
                 "Until it passes, the queue can start a Claude session where it must not, or never start "
                 "one. Its output: " + _out)
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
    # every lead its own name, role, hue and emblem, and a new project assigned one (2026-10-04, David: "there should be a
    # cleaner distinction between each of the project lead UI's. this should be applied for every new project when they
    # come as well"): identity.py's selftest runs the assignment fixtures and the live config/projects.json
    _ok, _out = _selfcheck(["identity.py", "selftest"], "ALL PASS")
    if not _ok:
        _finding(f, "guardrail-inert", "med",
                 "identity selftest fails — two leads could share a hue or an emblem, or a project has none",
                 "run `python3 ~/maintenance/bin/identity.py selftest`; a FAIL on the live registry names the project "
                 "in config/projects.json whose identity is missing or shared (fix it there: identity_palette lists "
                 "the free hues and emblems). Its output: " + _out)
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

    # Always-on user units (2026-09-28 for :8900; every repo-linked unit since 2026-10-01 —
    # proposals/2026-09-28_always-on-under-systemd.md, PROJECT_STANDARDS §4). A user unit is what
    # brings a server back, and its MemoryMax (+ a swap cap) is what stops a runaway one eating
    # the memory pool the GPU, Ollama and the Stocks model jobs share. Every ENABLED user unit
    # whose file resolves into a project folder (~/<project>/…: installed with `systemctl --user
    # link`, so FragmentPath is the ~/.config symlink and realpath is the repo) is checked without
    # an edit here; a unit copied into ~/.config is not seen. maintenance-dashboard is checked
    # whether or not it is enabled: a rollback to the cron keepalive (no cap at all) is a
    # deliberate deviation, muted in config/backoffice_mute.json with its reason (rule 7), never
    # a check that quietly stands down and leaves Box › Guardrails calling it armed. Another
    # project disabling its own unit is its owner's rollback and stands down (healthcheck.sh does
    # the same). Discovery missing an enabled maintenance-dashboard is the check itself gone
    # inert. Cron has no XDG_RUNTIME_DIR, so it is passed: without it `systemctl --user` answers
    # nothing.
    _env = dict(os.environ, XDG_RUNTIME_DIR=os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}")
    _props = ["-p", "Id", "-p", "FragmentPath", "-p", "UnitFileState", "-p", "ActiveState",
              "-p", "MemoryMax", "-p", "MemorySwapMax"]
    _units, _err = {}, ""
    try:
        _en = [ln.split()[0] for ln in subprocess.run(
            ["systemctl", "--user", "list-unit-files", "--type=service", "--state=enabled", "--no-legend"],
            capture_output=True, text=True, timeout=15, env=_env).stdout.splitlines() if ln.split()]
        _out = subprocess.run(["systemctl", "--user", "show", *_props, *sorted(set(_en) | {"maintenance-dashboard.service"})],
                              capture_output=True, text=True, timeout=15, env=_env).stdout
        for _blk in _out.split("\n\n"):
            _u = dict(ln.split("=", 1) for ln in _blk.splitlines() if "=" in ln)
            if _u.get("Id"):
                _units[_u["Id"].removesuffix(".service")] = _u
    except Exception as e:
        _err = f"{type(e).__name__}: {e}"
    _home = os.path.expanduser("~")

    def _repo_linked(u):
        r = os.path.realpath(u.get("FragmentPath") or "/")
        rel = os.path.relpath(r, _home)
        return not rel.startswith(".") and os.sep in rel     # ~/<project>/…, not ~/.config, not ~/x
    _seen = sorted(n for n, u in _units.items() if u.get("UnitFileState") == "enabled" and _repo_linked(u))
    _md = _units.get("maintenance-dashboard") or ({"error": _err} if _err else {})
    if _md.get("UnitFileState") == "enabled" and "maintenance-dashboard" not in _seen:
        _finding(f, "guardrail-inert", "high",
                 "always-on unit discovery is inert — it did not see maintenance-dashboard",
                 f"maintenance-dashboard is enabled, but the always-on check found it among no repo-linked "
                 f"units (found: {', '.join(_seen) or 'none'}; its FragmentPath "
                 f"{_md.get('FragmentPath') or '?'} resolves to {os.path.realpath(_md.get('FragmentPath') or '/')}). "
                 f"Every other project's unit is going unchecked. Was it copied into ~/.config instead of "
                 f"linked? `systemctl --user link ~/maintenance/dashboard/maintenance-dashboard.service`.")
    for _n in sorted(set(_seen) | {"maintenance-dashboard"}):
        _u = _units.get(_n) or ({"error": _err} if _err else {})
        if (_u.get("UnitFileState") == "enabled" and _u.get("ActiveState") == "active"
                and (_u.get("MemoryMax") or "").isdigit() and (_u.get("MemorySwapMax") or "").isdigit()):
            continue
        _show = {k: v for k, v in _u.items() if k != "Id"}
        _src = os.path.relpath(os.path.realpath(_u.get("FragmentPath") or "/"), _home) if _u.get("FragmentPath") else "?"
        _cron = (_n == "maintenance-dashboard" and re.search(
            r"^[^#\n]*maintenance/dashboard/serve\.sh\s+(?:ensure|start)", sh(["crontab", "-l"]), re.M))
        _finding(f, "guardrail-inert", "high",
                 f"an always-on user unit is not armed — {_n} has no supervisor or no memory cap",
                 f"the user unit {_n} must be enabled and active, with MemoryMax and MemorySwapMax byte "
                 f"counts. It reads {_show or 'nothing (no user manager answered)'}"
                 f"{' — and a crontab keepalive owns :8900 (rolled back?), which has no cap at all' if _cron else ''}. "
                 f"`systemctl --user status {_n}`; the unit's source is ~/{_src}, installed with "
                 f"`systemctl --user link` — never copied, so an edit there plus daemon-reload is the fix. "
                 f"A deliberate rollback is muted with its reason (rule 7); another project's own rollback "
                 f"disables its unit, which stands this check down.")

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
        elif os.path.exists(_chk) and subprocess.run([sys.executable, _chk, "selftest"],
                                                     capture_output=True, text=True,
                                                     timeout=60).returncode != 0:
            # ...and the checker must know what kind of job a line is (2026-10-02, memo from
            # data-desk: every `bin/desk.py <sub>` model line read as a code job, so the :35 lane
            # and GPU checks never ran on the desk — a check that runs on the wrong kind is inert)
            _finding(f, "guardrail-inert", "high",
                     "schedule-check.py misreads which jobs run a local model — its selftest fails",
                     "run `python3 ~/maintenance/bin/schedule-check.py selftest`: each FAIL is a cron "
                     "line it would file under the wrong kind, so that kind's window, :35-lane or GPU "
                     "checks never run on it.")
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

    # 20b. A WIND-DOWN LANE READY TO HAND OVER (2026-09-29, the Data Desk). A project that
    #      declares `<slug>/handover` shadows another project's lane until its replacement has
    #      matched it for the days the board asks; from then only David's word is missing. One
    #      high finding per ready lane puts it in Overview › Needs attention, keyed by the lane so
    #      a snooze, a mute or a Claude answer outlives the day count. The answer "retire" goes to
    #      the daily check, which runs the board's own retire step (RETIRE_CMD): that files the
    #      retirement memo to the lane's owner, and the owner retires it (box rule 6). A lane
    #      that leaves parity_ok (retired, requested, fell back) resolves its finding by itself.
    _handover_findings(f, c.get("handovers") or [])

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
        # every dashboard/test_*_tab.js, found on disk the way check.py finds them (2026-10-05,
        # memo review-backoffice-runs-team-tests: a hard-coded three let test_team_tab.js go red
        # with no finding). Per test 90 s, the whole set DASH_TESTS_BUDGET_S; a test the budget
        # leaves unrun is named in one finding rather than silently dropped.
        _t0 = time.time()
        _unrun = []
        for _t, _lbl in dashboard_tab_tests(_dash):
            _tp = os.path.join(_dash, _t)
            _left = DASH_TESTS_BUDGET_S - (time.time() - _t0)
            if _left < 15:
                _unrun.append(_t)
                continue
            try:
                _r = subprocess.run(["node", _tp], cwd=_dash, capture_output=True,
                                    text=True, timeout=min(DASH_TEST_TIMEOUT_S, _left))
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
        if _unrun:
            _finding(f, "guardrail-inert", "med",
                     "some dashboard tab tests did not run (time budget)",
                     f"{len(_unrun)} of the dashboard's test_*_tab.js were left unrun after "
                     f"{DASH_TESTS_BUDGET_S} s: {', '.join(_unrun)}. A slow test is hiding the ones "
                     f"after it; time them with `node dashboard/<test>` and fix the slow one.",
                     project="maintenance", key="dashboard-tests-unrun")

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
    #     backoffice_mute.json. Filed under the slug `maintenance` (2026-09-29): the display name
    #     "Mission Control" put a space in the finding's id, which the answer path could not reach.
    _cw = sh([sys.executable, os.path.join(MC, "dashboard", "tt_crew.py"), "check"], timeout=90)
    try:
        for h in json.loads(_cw or "null") or []:
            _finding(f, "crew-unclaimed", "low", h["text"],
                     "config/crew.json is Mission Control's display layer over the scheduled jobs "
                     "(dashboard/tt_crew.py). Claim the job with a `cmd` substring on the agent whose "
                     "work it feeds or keeps running (the longest match wins; a script too), add a "
                     "new agent for it, or mark an agent "
                     "`on_demand`. `python3 dashboard/tt_crew.py selftest` proves the whole fleet "
                     "is claimed once.", project="maintenance", fix="human", key=h["id"])
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

    # 23c. the dashboard's own builders (memo maintenance_lead-work-selftests-green-and-daily, 2026-10-08):
    #      three selftests nothing ran sat red for days (a stale kind, a payload over budget, a row lost
    #      before noon). One finding names each that fails.
    _dash_bad = []
    for _t in ("tt_fleet", "tt_flow", "tt_system"):
        try:
            _r = subprocess.run([sys.executable, "-B", os.path.join(MC, "dashboard", f"{_t}.py"), "selftest"],
                                capture_output=True, text=True, timeout=240, cwd=MC)
            _o = (_r.stdout or "") + (_r.stderr or "")
            _fl = [l for l in _o.splitlines() if l.startswith("FAIL")]
            if _r.returncode != 0 or _fl:
                _dash_bad.append(f"{_t} (exit {_r.returncode}): " + (" | ".join(_fl)[-200:] or _o.strip()[-200:]))
        except Exception as e:
            _dash_bad.append(f"{_t}: {type(e).__name__}: {e}")
    if _dash_bad:
        _finding(f, "guardrail-inert", "med",
                 "a dashboard selftest fails — a page can show a wrong count, kind or line",
                 "run `python3 -B ~/maintenance/dashboard/<name>.py selftest`; a stale fixture is fixed in the fixture, "
                 "a regression in the code, a test that reads the live clock gets a pinned input. Failing: "
                 + " ; ".join(_dash_bad))

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

    # 25. PROJECT LEADS (2026-10-02): lead-missing, lead-invalid, lead-overdue from `lead.py status
    #     --json` (helpers above audit()). If lead.py cannot answer, every lead rule is off, which is
    #     guardrail-inert.
    _ls = sh([sys.executable, os.path.join(MC, "bin", "lead.py"), "status", "--json"], timeout=90)
    try:
        _st_rows = json.loads(_ls or "null")
        assert isinstance(_st_rows, list), "no JSON list on stdout"
        _lead_findings(f, _st_rows)
    except Exception as e:
        _finding(f, "guardrail-inert", "high", "the project-lead rules did not run — `lead.py status` failed",
                 f"`python3 ~/maintenance/bin/lead.py status --json` gave no roster ({type(e).__name__}: {e}). "
                 "Until it does, nothing checks that each project has a valid lead or that the leads keep "
                 "their weekly day: " + (_ls or "")[:200], project="maintenance")
    # 25b. ...and the armed-checks for what makes a lead safe to run unattended: the 07:02Z pick,
    #      the 07:47Z deadline, the fits() re-check at slot grant and the push safety (lead.py
    #      selftest), and the two PreToolUse guards that hold every unattended session to its bright
    #      lines (David 2026-10-02: "only pm agent can place live brokerb, nothing else (unless
    #      it's in a session i'm directly working with)"). Each guard must be registered for every
    #      tool it covers, executable, and pass its own fixture selftest.
    try:       # not _selfcheck: one PASS line quotes "'0 FAIL'", so a FAIL is a line that starts with it
        _r = subprocess.run([sys.executable, os.path.join(MC, "bin", "lead.py"), "selftest"],
                            capture_output=True, text=True, timeout=120)
        _o = (_r.stdout or "") + (_r.stderr or "")
        _ok = (_r.returncode == 0 and "ALL PASS" in _o
               and not any(l.startswith("FAIL") for l in _o.splitlines()))
        _out = " | ".join(l for l in _o.splitlines() if l.startswith("FAIL"))[-300:] or _o.strip()[-300:]
    except Exception as e:
        _ok, _out = False, f"{type(e).__name__}: {e}"
    if not _ok:
        _finding(f, "guardrail-inert", "high",
                 "lead.py selftest fails — the weekly pick, the deadline or the push safety no longer hold",
                 "run `python3 ~/maintenance/bin/lead.py selftest`; each FAIL line names the case (weekday and "
                 "catch-up pick, blackout days, the 07:47Z deadline, fits() under pm_days Mon-Fri, never pushing "
                 "a -public remote or a red tree, the launch argv). Its output: " + _out, project="maintenance")
    _hs = load(os.path.expanduser("~/.claude/settings.json"), {}) or {}
    _gb = _guard_gaps(_hs, "hook-guard-broker.py", ("Bash", "mcp__brokerb-trading__place_equity_order"))
    if _gb:
        _finding(f, "guardrail-inert", "high",
                 "the broker guard is not armed — a headless session could place a BrokerB order",
                 "bin/hook-guard-broker.py: " + "; ".join(_gb) + " (~/.claude/settings.json). Only the PM's "
                 "trade session, or a session David is working in, may place, cancel or preview a live "
                 "BrokerB order, and this hook is the tool-layer choke point for that.", project="maintenance")
    _gl = _guard_gaps(_hs, "hook-guard-lead.py", ("Bash", "Edit", "Write", "MultiEdit", "NotebookEdit"))
    if _gl:
        _finding(f, "guardrail-inert", "high",
                 "the project-lead guard is not armed — a lead's bright lines are not enforced",
                 "bin/hook-guard-lead.py: " + "; ".join(_gl) + " (~/.claude/settings.json). It keeps every "
                 "unattended project lead inside its own project: no force-push, nothing public, no edits "
                 "to the guardrail files, no deletions or crontab changes in the pilot.", project="maintenance")

    # 26. rollup-held (2026-10-02): the 23:00 Daily rollup is David's one Mission Control push of the
    #     day, and the 1/day maintenance cap held it on 11 of the 12 days 09-20..10-01 with nothing
    #     noticing. notify_policy.json `cap_exempt` fixes the cause; this is the detective rule.
    _rh, _flags = _rollup_held()
    if _rh:
        _finding(f, "rollup-held", "high", "the 23:00 Daily rollup was held, not pushed",
                 f"{datetime.fromtimestamp(_rh.get('time', 0), timezone.utc):%Y-%m-%d %H:%MZ}: "
                 f"{_rh.get('reason') or 'no reason recorded'}. {_flags.count(False)} of the last "
                 f"{len(_flags)} rollups were held. It is the only push that carries everything the policy "
                 "held that day. Check `cap_exempt` in config/notify_policy.json, run "
                 "`python3 ~/maintenance/bin/notify_policy.py selftest`, and look for an in-dev label in "
                 "config/dev.json that matches it.", project="maintenance", fix="human", key="")

    # 27. queue-starved (2026-10-03): a job in the box Claude queue startable for more than 48 h and
    #     never started. `claudeq.py status` showed a Stocks update job "waiting 8078m" and nothing else on the
    #     box would have said so. claudeq.starving() measures from eligibility; the helper above files.
    try:
        import claudeq as _cq
        _queue_starved(f, _cq.starving(), int(_cq.STARVE_H))
    except Exception as e:
        _finding(f, "queue-starved", "med", "the queue-starved rule could not read the Claude queue",
                 f"claudeq.starving() raised {type(e).__name__}: {str(e)[:160]}. Until it reads, a job that "
                 "can never start sits in state/claudeq/pending/ unseen. Run `python3 "
                 "~/maintenance/bin/claudeq.py starving`.", project="maintenance", fix="human", key="")

    # 28. owner-missing (2026-10-03, single-threaded owner; helpers above audit()): every cron job,
    #     queue job (30 days), enabled user service, crew agent and catalog dataset walks to exactly
    #     one lead. Reads rule 25's `lead.py status` rows; if those or the crew namer cannot be read
    #     the rule is off, which is guardrail-inert.
    try:
        _dash = os.path.join(MC, "dashboard")
        if _dash not in sys.path:
            sys.path.insert(0, _dash)
        import tt_crew as _tc
        _ccfg = _tc.load()
        _things = []
        for _j in c["crons"]:
            _cl = _tc.claims(_j.get("cmd"))
            _things.append({"what": "cron job", "name": (" ".join(_j.get("scripts") or []) or _j.get("cmd", "")[:60]),
                            "agent": _cl[0] if len(_cl) == 1 else None, "project": _j.get("project") or ""})
        for _qn, _qk in _sto_queue_jobs():
            if _tc.skip(qjob=_qn):
                continue
            _aid = _tc.agent_for(qjob=_qn)
            _m = re.match(r"(?:lead|memo) ([A-Za-z0-9_.-]+)[: ]", _qn)
            _things.append({"what": "queue job", "name": f"{_qn!r} ({_qk})", "agent": _aid,
                            "project": (_m.group(1) if _m else None) if not _aid else None})
        _hm = os.path.expanduser("~")
        for _n, _u in sorted(_units.items()):
            if _u.get("UnitFileState") != "enabled":
                continue
            _rel = os.path.relpath(os.path.realpath(_u.get("FragmentPath") or "/"), _hm)
            _things.append({"what": "service", "name": _n, "agent": None,
                            "project": _rel.split(os.sep)[0] if _repo_linked(_u) else ""})
        for _a in _ccfg["agents"]:
            _things.append({"what": "agent", "name": f"{_a.get('name')} ({_a['id']})", "agent": _a["id"]})
        for _did in sorted((load(os.path.join(STATE, "catalog.json"), {}) or {}).get("entries") or {}):
            _things.append({"what": "dataset", "name": _did, "agent": None, "project": _did.split("/", 1)[0]})
        if not isinstance(_st_rows, list) or not _st_rows:
            raise RuntimeError("no lead.py status rows (rule 25 failed)")
        _owner_findings(f, _things, _st_rows, _ccfg["by_id"])
    except Exception as e:
        _finding(f, "guardrail-inert", "med", "the owner-missing rule did not run — nothing checks that every job has a lead",
                 f"rule 28 raised {type(e).__name__}: {str(e)[:200]}. It walks every cron job, queue job, "
                 "service, crew agent and dataset to its lead (single-threaded owner, HOME.md box rule 10) "
                 "through dashboard/tt_crew.py and `lead.py status --json`. `python3 ~/maintenance/bin/backoffice.py "
                 "selftest` holds its fixtures.", project="maintenance")

    # 29. the monthly patch's guards (2026-10-03, memo wa-alerts-and-boot): spark-update-run.sh
    #     reboots the box, so each of its guards must be seen to refuse. Its selftest is fixtures
    #     only (scratch slot state, no apt, no push, no reboot).
    _pt = sh(["bash", os.path.join(MC, "bin", "spark-update-run.sh"), "selftest"], timeout=240) or ""
    if not re.search(r"^ALL PASS$", _pt, re.M):
        _finding(f, "guardrail-inert", "high",
                 "the monthly patch's guards fail their selftest — it could reboot under live work",
                 "run `bash ~/maintenance/bin/spark-update-run.sh selftest`: it watches the sudoers, disk, "
                 "headless, Claude-slot, active-session and GPU-slot guards each refuse, the argv contract and "
                 "the pause. Do not run --scheduled or --in-person until it passes. Output: "
                 + " | ".join(l for l in _pt.splitlines() if l.startswith("FAIL"))[-300:], project="maintenance")

    # 30. the auto-fix policy's own proofs (2026-10-03, David: "auto fix is good"): the daily check acts
    #     for David on an unanswered item and the memo pass takes a lead's reversible default after 7
    #     days. Each selftest pins the fence (what may NEVER auto-resolve) and its argv; a fence that
    #     stopped holding would look exactly like one that holds, so all three run every morning.
    _af = _auto_fix_selftests()
    if _af:
        _finding(f, "guardrail-inert", "high",
                 f"the auto-fix policy's selftests fail — {', '.join(n for n, _ in _af)} could act for David where it must not",
                 "The auto-fix policy (dashboard/tt_decide.py's policy section, bin/decisions.py at the 07:50 daily "
                 "check, bin/memo-process.py for the 7-day memo default) carries out the recommended option on an "
                 "item David left unanswered. Until these pass, it may act on a kind it must never touch (public, "
                 "money, Stocks, deletes, ports, accounts, security), or never act. Run each `python3 <script> "
                 "selftest`: " + " · ".join(f"{n}: {w}" for n, w in _af)[:400],
                 project="maintenance", key="auto-fix-selftests")

    # 31. the dead-box alarm (2026-10-03): healthcheck.sh keeps ONE scheduled "Spark silent for 2h" message
    #     on ntfy.sh and pushes it back every hour, so a box that is off or offline still pages. Its
    #     selftest (stub curl) proves the plan, arm and cancel; state/deadman.json proves the live re-arm.
    _hs = sh(["bash", os.path.join(MC, "bin", "healthcheck.sh"), "selftest"], timeout=60) or ""
    if not re.search(r"^ALL PASS$", _hs, re.M):
        _finding(f, "guardrail-inert", "high", "the dead-box alarm's selftest fails — a dead box might never page",
                 "run `bash ~/maintenance/bin/healthcheck.sh selftest` (a stub curl: nothing is sent). It proves "
                 "the alarm's plan (re-arm at 55 min, recovered once its time passed), the arm and the cancel. "
                 "Output: " + " | ".join(l for l in _hs.splitlines() if l.startswith("FAIL"))[-300:],
                 project="maintenance")
    _dw = _deadman_why()
    if _dw:
        _finding(f, "guardrail-inert", "high", f"the dead-box alarm is not armed — {_dw}",
                 "If the box went down now, nothing would page David: the scheduled \"Spark silent for 2h\" message "
                 "on ntfy.sh is what pages for a box that cannot. healthcheck.sh (every 15 min) re-arms it when the "
                 "last arm is 55+ min old and fails its own run (dead-box-alarm-unarmed) at 75. Check "
                 "~/maintenance/logs/healthcheck.log and `bash ~/maintenance/bin/healthcheck.sh --dry`.",
                 project="maintenance", key="deadman-unarmed")

    # 32. cli-stale (2026-10-04, memo wa-cli-currency ask 3): the headless CLI pin lagging David's
    #     interactive sessions. Low and quiet (QUIET_KINDS); the core is _cli_stale(), pure.
    try:
        _cli_stale_finding(f, _jsonl_rows(os.path.join(STATE, "claude_sessions.jsonl")))
    except Exception as e:
        _finding(f, "cli-stale", "low", "the cli-stale rule could not read the session ledger",
                 f"{type(e).__name__}: {str(e)[:160]}. state/claude_sessions.jsonl is what it reads.",
                 project="maintenance", key="cli-stale-unread")

    # 33. backup-tamper (2026-10-04, memo wa-backups-integrity ask 3): a recorded archive whose size (or,
    #     under 200 MB, sha256) is not what backup.py recorded, or that vanished inside its keep window.
    #     Full re-hashing of every archive is `backup.py verify`'s job, not a daily cost here.
    try:
        import backup as _bk
        _backup_tamper_finding(f, _backup_tamper(_jsonl_rows(_bk.SUMS), _bk.cfg().get("sources") or {},
                                                 _bk.root(), hasher=_bk.sha256_file))
    except Exception as e:
        _finding(f, "backup-tamper", "med", "the backup-tamper rule could not run",
                 f"{type(e).__name__}: {str(e)[:160]}. It reads state/backup_sums.jsonl and config/backups.json "
                 "through bin/backup.py; `python3 ~/maintenance/bin/backup.py verify` checks the same by hand.",
                 project="maintenance", key="backup-tamper-unread")

    # 34. argv-unsafe (2026-10-04, memo wa-lessons-propagation ask 1): a script in any project
    #     that runs a live path on an unknown argument. Static (ast), low and quiet; the memos
    #     (one per non-Stocks project, ask-once) are argv_memos(), called from run() only when not dry.
    #     Since 2026-10-05 every runnable *.py in the project, not only bin/ and scripts/.
    try:
        _argv_st = {}
        _argv_findings(f, argv_scan(stats=_argv_st))
        _argv_capped = {p: st["capped"] for p, st in _argv_st.items() if st.get("capped")}
        if _argv_capped:
            _finding(f, "argv-unsafe", "low", "the argv-unsafe scan stopped at its cap",
                     "Scripts past the cap were not read: " + "; ".join(f"{p}: {c}" for p, c in
                     sorted(_argv_capped.items())) + ". A vendored or generated tree the prune list "
                     "misses (_ARGV_SKIP in bin/backoffice.py) is the usual cause.",
                     project="maintenance", key="argv-unsafe-capped")
    except Exception as e:
        _finding(f, "argv-unsafe", "low", "the argv-unsafe rule could not run",
                 f"{type(e).__name__}: {str(e)[:160]}. It parses every project's runnable *.py with ast.",
                 project="maintenance", key="argv-unsafe-unread")

    # 35. posture (2026-10-04, memo wa-security-detection ask 2): a repo turned public, a new GitHub key,
    #     a listener off loopback that KNOWN_PORTS does not declare (one rule 5 already files is skipped).
    try:
        _exp = load(EXPECTED_GITHUB, None)
        _snap = _gh_snapshot() if isinstance(_exp, dict) else {"repos": None, "keys": None}
        _posture_findings(f, _posture(_snap, _exp if isinstance(_exp, dict) else {},
                                      _ss_listeners(sh(["ss", "-ltnp"])), d["known_ports"],
                                      skip={p for p in c["ports"] if _rule5_reports(p, d)}))
    except Exception as e:
        _finding(f, "posture", "med", "the posture rule could not run",
                 f"{type(e).__name__}: {str(e)[:160]}. It reads gh (repo list, user/keys), `ss -ltnp` and "
                 "config/expected_github.json.", project="maintenance", key="posture-unread")

    # 36. lead-structure (2026-10-04, David: "any project with a lead should follow this structure";
    #     helpers above audit()): every lead.py lead's project has the lead-project layout of
    #     PROJECT_STANDARDS §5. Reads rule 25's rows; the selftest fixture is its armed-check.
    try:
        if not isinstance(_st_rows, list) or not _st_rows:
            raise RuntimeError("no lead.py status rows (rule 25 failed)")
        _lead_structure_findings(f, _st_rows, load(os.path.join(CONFIG, "projects.json"), {}))
    except Exception as e:
        _finding(f, "lead-structure", "low", "the lead-structure rule could not run",
                 f"{type(e).__name__}: {str(e)[:160]}. It walks each lead.py lead's project folder against "
                 "PROJECT_STANDARDS §5's lead-project layout, from `lead.py status --json` and config/projects.json.",
                 project="maintenance", key="lead-structure-unread")

    # 37. browser-signin (2026-10-05, memo browser-signin-needs-attention ask 1): a site the Spark browser was
    #     signed in to once that `browser.py daily` now reads logged_out or challenge. Options come from the
    #     kind template in dashboard/tt_decide.py. 37b: its pure selftest beside the other morning ones.
    try:
        _browser_signin_findings(f, load(os.path.join(STATE, "browser.json"), {}),
                                 load(os.path.join(CONFIG, "browser_sites.json"), {}))
    except Exception as e:
        _finding(f, "browser-signin", "low", "the browser-signin rule could not run",
                 f"{type(e).__name__}: {str(e)[:160]}. It reads state/browser.json and config/browser_sites.json.",
                 project="maintenance", key="browser-signin-unread")
    _ok, _out = _selfcheck(["browser.py", "selftest", "--pure"], "PASS")
    if not _ok:
        _finding(f, "guardrail-inert", "med",
                 "browser.py selftest fails — the Spark browser's pacing, caps or sign-in reads may not hold",
                 "run `python3 ~/maintenance/bin/browser.py selftest --pure` (fixtures only: no Chrome, no site "
                 "visited); each FAIL line names the case. Its output: " + _out)

    # 38. dashboard-check-red (2026-10-05, memo wa-attention-kpi-leftovers): serve.sh's post-restart check.py run
    #     came back red in the last 7 days.
    _dash_check_finding(f, load(os.path.join(STATE, "dash_check.json"), None))

    # 39. push-undelivered (2026-10-05, memo wa-alerts-and-boot slice 3): a critical push in healthcheck.sh's
    #     delivery spool for over 30 min.
    _push_undelivered_finding(f, os.path.join(STATE, "alerts_pending"))

    # 40. guard-deny (2026-10-05, memo wa-security-detection slice 3): any refusal in a guard ledger in 24 h.
    _guard_deny_findings(f, {n: _jsonl_rows(os.path.join(STATE, fn)) for n, fn, _ in GUARD_LEDGERS})

    # 41. restore-unproven + manifest-stale (2026-10-05, memo wa-restore-and-rebuild): the last green restore
    #     drill > 30 days old (the pass does not run the drill), and the nightly rebuild manifest > 2 days old.
    try:
        import backup as _bk2
        _drill_srcs = _bk2.cfg().get("sources") or {}
    except Exception:
        _drill_srcs = None
    _restore_unproven_finding(f, _jsonl_rows(os.path.join(STATE, "restore_drills.jsonl")), sources=_drill_srcs)
    _manifest_stale_finding(f, os.path.join(HOME, "backups", "manifest"))

    # 42. ledger-order (2026-10-05, memo review-ledger-hygiene slice 2): a LEDGER.md row dated newer than
    #     the row above it (the ledger is newest-first; every writer prepends through bin/ledger_rows.py).
    _ledger_order_finding(f, os.path.join(MEMOS, "LEDGER.md"))

    # 45. backup-stray (2026-10-05, memo wa-backups-integrity slice 4/4): an undeclared file or folder at the
    #     top two levels of ~/backups, newer than the baseline. Stat only.
    try:
        import backup as _bk3
        _backup_stray_finding(f, _backup_stray(_bk3.root(), _bk3.cfg().get("sources") or {}))
    except Exception as e:
        _finding(f, "backup-stray", "low", "the backup-stray rule could not run",
                 f"{type(e).__name__}: {str(e)[:160]}. It lists ~/backups against config/backups.json.",
                 project="maintenance", key="backup-stray-unread")

    # 44. fleet-stop-engaged (2026-10-05, memo wa-security-detection slice 4/4): LAST, over every finding
    #     above: while fleet-stop is engaged, the job-shaped findings about the lines it paused are held.
    try:
        f = _fleet_stop_apply(f, _fleet_state(), {x for j in c["crons"] for x in j["scripts"]})
    except Exception as e:
        _finding(f, "fleet-stop-engaged", "med", "the fleet-stop rule could not read its state",
                 f"{type(e).__name__}: {str(e)[:160]}. It reads state/fleet_stop.json through bin/paused.py.",
                 project="maintenance", key="fleet-stop-unread")

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


# A key's life across episodes (memo 2026-10-03_wa-repeat-failures, ask 1). first_seen and
# resolved_at still describe the CURRENT episode; these three fields describe every episode and are
# never erased when a closed finding reopens: first_ever_seen (set once), reopen_count (+1 each time
# a closed record comes back) and history (its closed episodes, oldest dropped past HISTORY_CAP).
HISTORY_CAP = 20
CLOSED_STATES = ("resolved", "fixed", "muted")


def _close(f, how, at=None):
    """Close the current episode: state = how (resolved | fixed | muted), one history entry."""
    at = at or now()
    f["state"] = how
    f.setdefault("first_ever_seen", f.get("first_seen") or at)
    f.setdefault("reopen_count", 0)
    if how == "resolved":
        f["resolved_at"] = at
    elif how == "muted":
        f["muted_at"] = at
    h = f.setdefault("history", [])
    h.append({"opened": f.get("first_seen"), "closed": at, "how": how})
    del h[:-HISTORY_CAP]
    return f


def _carry_life(f, old):
    """f opens a new episode of the closed record `old`: carry its lifetime over, count a reopen."""
    if not old:
        f.setdefault("first_ever_seen", f.get("first_seen") or now())
        f.setdefault("reopen_count", 0)
        f.setdefault("history", [])
        return f
    f["first_ever_seen"] = min(x for x in (old.get("first_ever_seen"), old.get("first_seen"),
                                           f.get("first_seen")) if x)
    f["reopen_count"] = int(old.get("reopen_count") or 0) + 1
    f["history"] = list(old.get("history") or [])[-HISTORY_CAP:]
    return f


def _prior_closed(store, f):
    """The closed record a fresh finding re-opens: its own id first, else the newest closed record
    under the same key (a title whose number moved between episodes)."""
    old = store.get(f["id"])
    if old and old.get("state") in CLOSED_STATES and not old.get("reopened_as"):
        return old
    k = f.get("key")
    if not k:
        return None
    cands = [x for x in store.values() if x.get("key") == k and x.get("state") in CLOSED_STATES
             and not x.get("reopened_as")]
    return max(cands, key=lambda x: x.get("last_seen") or x.get("first_seen") or 0) if cands else None


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
            prior = _prior_closed(store, f)
            f.update(first_seen=now(), last_seen=now(), state="open")
            _carry_life(f, prior)
            if prior is not None and prior is not store.get(f["id"]):
                prior["reopened_as"] = f["id"]       # its life moved to the new id; not counted twice
            store[f["id"]] = f
            new.append(f)
    # a record that left the open set: "muted" when its id, kind or key is on the mute list (the
    # problem is still there, we chose to live with it), else "resolved". Both are returned as
    # `resolved` so the pass line and state/backoffice.jsonl keep counting what left the open set.
    resolved = []
    for fid, f in store.items():
        if f.get("state") == "open" and fid not in seen:
            is_muted = fid in muted or f.get("kind") in muted or (f.get("key") or _key(fid)) in muted
            _close(f, "muted" if is_muted else "resolved")
            resolved.append(f)
    save(FINDINGS, store)
    return new, resolved, [f for f in store.values() if f.get("state") == "open"]


def _answer_episodes(evs):
    """{key: [episode]} from decisions.jsonl events (oldest first). An answer is an id's first
    event; it ends at that id's first non-queued event (an `applied` first event ends at once). A
    later answer for the same key starts a NEW episode only once every earlier one had ended (two
    answers to one open item are one episode). episode = {first_at, ids, ended_at, how}."""
    first, ended, how, soft = {}, {}, {}, set()
    for e in evs:
        i, k = e.get("id"), e.get("key")
        if not i or not k or not e.get("at"):
            continue
        if i not in first:
            first[i] = e
            if (e.get("status") or "queued") == "queued":
                continue
        if i not in ended and (e.get("status") or "queued") != "queued":
            ended[i] = e["at"]
            if e.get("action") in ("snooze", "ack", "hide") and e is first[i]:
                soft.add(i)                      # applied at once, but the item stayed open
            how[i] = ("muted" if e.get("action") == "mute" or first[i].get("action") == "mute"
                      else "fixed" if e.get("commit") else "resolved")
    by_key = {}
    for i, e in sorted(first.items(), key=lambda x: x[1]["at"]):
        eps = by_key.setdefault(e["key"], [])
        last = eps[-1] if eps else None
        if last and (last["ended_at"] is None or last["ended_at"] > e["at"]):
            last["ids"].append(i)
            last["ended_at"] = None if (last["ended_at"] is None or i not in ended) \
                else max(last["ended_at"], ended[i])
            last["how"] = how.get(i, last["how"])
        else:
            eps.append({"first_at": e["at"], "ids": [i], "ended_at": ended.get(i),
                        "how": how.get(i, "resolved"), "finding_id": e.get("finding_id"),
                        "soft": i in soft})
    return by_key


def backfill_history(store, evs, at=None):
    """One-time seed of first_ever_seen / reopen_count / history (memo wa-repeat-failures ask 1)
    from David's answers and the store itself. Mutates `store`, returns [(id, {field: value})] for
    what changed. Idempotent: counts only ever rise, history is seeded once (history_backfilled).
    A prior episode is an answer episode that began before the record's current first_seen; its
    `opened` is the answer time (an upper bound, marked approx)."""
    at = at or now()
    eps = _answer_episodes(evs)
    # answers attach to ONE record per key: the one carrying the key's life (newest, not superseded)
    owner = {}
    for fid, r in store.items():
        if not isinstance(r, dict) or r.get("reopened_as"):
            continue
        k = r.get("key") or _key(fid)
        cur = owner.get(k)
        if cur is None or (r.get("last_seen") or 0) > (store[cur].get("last_seen") or 0):
            owner[k] = fid
    changes = []
    for fid, r in store.items():
        if not isinstance(r, dict):
            continue
        k = r.get("key") or _key(fid)
        mine = [x for kk, lst in eps.items() for x in lst
                if kk == k or x.get("finding_id") == fid] if owner.get(k) == fid else []
        fs = r.get("first_seen") or at
        prior = sorted((x for x in mine if x["first_at"] < fs), key=lambda x: x["first_at"])
        before = {f: r.get(f) for f in ("first_ever_seen", "reopen_count", "history")}
        r["first_ever_seen"] = min([v for v in (r.get("first_ever_seen"), fs) if v]
                                   + [x["first_at"] for x in mine])
        r["reopen_count"] = max(int(r.get("reopen_count") or 0), len(prior))
        if not r.get("history_backfilled"):
            h = []
            for n, x in enumerate(prior):
                nxt = prior[n + 1]["first_at"] if n + 1 < len(prior) else fs
                end = nxt if x.get("soft") else min(x["ended_at"] or nxt, nxt)   # a snooze closes nothing
                h.append({"opened": x["first_at"], "closed": end,
                          "how": x["how"], "approx": True, "src": "decisions:" + ",".join(x["ids"])})
            h += list(r.get("history") or [])
            st = r.get("state")
            closed_at = r.get({"resolved": "resolved_at", "fixed": "fixed_at", "muted": "muted_at"}
                              .get(st, ""), None)
            if st in CLOSED_STATES and closed_at and not any(y.get("closed") == closed_at for y in h):
                h.append({"opened": r.get("first_seen"), "closed": closed_at, "how": st})
            r["history"] = h[-HISTORY_CAP:]
            r["history_backfilled"] = at
        diff = {f: r[f] for f in before if before[f] != r[f]}
        if diff:
            changes.append((fid, diff))
    return changes


def history_backfill(dry=False):
    """CLI: `backoffice.py history-backfill [--dry]`. --dry prints and writes nothing."""
    store = load(FINDINGS, {})
    try:
        evs = _decide_mod().events()
    except Exception as e:
        print(f"history-backfill: decisions.jsonl unreadable ({type(e).__name__}: {e}) — nothing written",
              file=sys.stderr)
        return 1
    changes = backfill_history(store, evs)
    fmt = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%MZ") if t else "-"
    rest = 0
    for fid, d in sorted(changes, key=lambda x: -(x[1].get("reopen_count") or 0)):
        r = store[fid]
        if not r.get("reopen_count") and not any(y.get("approx") for y in r["history"]):
            rest += 1                            # defaults only: counted, not listed
            continue
        print(f"{'would set' if dry else 'set'} {fid[:70]}: reopen_count={r['reopen_count']} "
              f"first_ever_seen={fmt(r['first_ever_seen'])} history={len(r['history'])}"
              + (" (from answers)" if any(y.get("approx") for y in r["history"]) else ""))
    if rest:
        print(f"{'would seed' if dry else 'seeded'} defaults (reopen_count 0, first_ever_seen = first_seen, "
              f"the current episode's history) on {rest} more record(s)")
    print(f"{len(changes)} of {len(store)} record(s) {'would change' if dry else 'changed'}; "
          f"{sum(1 for r in store.values() if (r.get('reopen_count') or 0) > 0)} with reopens")
    if not dry and changes:
        save(FINDINGS, store)                    # atomic (tmp + os.replace), secret-scrubbed
    return 0


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


def _new_card(pcfg, name):
    """The config/projects.json card the auto-add writes for a project on disk with none (`roster-missing`). Since
    2026-10-04 it carries the project's identity (David: "there should be a cleaner distinction between each of the
    project lead UI's. this should be applied for every new project when they come as well"): bin/identity.py assign —
    a name from the folder, role "Project lead", the next palette hue and fallback emblem no project holds, no vitals
    yet (its lead's home shows Mission Control's own record of it), `for` its desc. The project's lead then names its
    role and vitals by memo to maintenance (PROJECT_STANDARDS.md Day-1). An identity that cannot be assigned leaves the
    card without one: the dashboard then assigns it at read time, never a blank."""
    desc = _desc_of(name)
    card = {"desc": desc, "match": [name], "activity_file": None,
            "next": "(auto-added by the back-office pass — set the next step)"}
    try:
        import identity
        card["identity"] = identity.assign(pcfg, name, desc)
    except Exception as e:
        print(f"  identity for {name}: {type(e).__name__}: {e}", file=sys.stderr)
    return card


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
            # (a logs/ directory is not an activity file: activity_file stays None for a human to set)
            card = pcfg.setdefault("projects", {})[name] = _new_card(pcfg, name)
            idn = card.get("identity") or {}
            done.append((f, f"added {name} to the dashboard roster"
                         + (f" ({idn.get('name')}: {idn.get('emblem')} on {idn.get('hue')})" if idn else "")))

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
        _close(f, "fixed", f["fixed_at"])
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


# ---------------------------------------------------------------- experiment-no-driver (rule 46)
# Memo wa-handoffs-and-frontier, slice 2/2 (2026-10-07). The Queue had no driver: every graveyard
# reason read "3 weeks without a design memo", and no memo in proposals/ was newer than 08-10. Once
# a pass, the oldest Queue item older than EXP_DRIVE_AGE_D days with no design memo gets a
# design-only session (prompts/experiment_design.md, kind `frontier`) through `claudeq.py run`,
# at most EXP_DRIVE_WEEK_MAX launches in any 7 days, never twice for one item. A typed queue
# filing (claudeq's enqueue) is NOT used: its tick dispatches only Stocks' kinds and would drop a
# frontier job as "unknown job kind"; `claudeq.py run` is the client every non-Stocks job uses.
# A run the queue refused (SKIPPED in its log) never started, so the item is tried again next pass.
# Stocks targets are held (David's while Stocks is paused): no job, no memo, a line saying so.
EXP_DRIVER = "experiment_driver.json"          # under STATE: slug -> {title, attempts: [...]}
EXP_DRIVE_AGE_D = 7
EXP_DRIVE_WEEK_MAX = 3
EXP_DRIVE_PER_PASS = 1                          # one a day keeps three 30-min sessions from stacking
EXP_DRIVE_SKIPS_MAX = 5                         # refused this many times running -> a finding
EXP_DRIVE_EST = 30
# OFF (maintenance_lead 2026-10-07): bin/ is a zero-Claude-token zone, and the janitor must not start a Claude
# session itself. Until the design run has its own cron line (prompts/experiment_design.md through claudeq, after
# the pilot ends 10-16: memo wa-handoffs-and-frontier slice 3/3), the rule only reports what it would launch.
EXP_DRIVE_LAUNCH = False


def _exp_memo_slug(title):
    """The memo's file slug: the dashboard's _slug (server.py) on the title without its date, so a
    memo this writes is the one the dashboard's run_experiment would have written."""
    s = re.sub(r"[^a-z0-9]+", "-", _short_full(title).lower()).strip("-")
    if len(s) > 40:
        s = s[:40].rsplit("-", 1)[0]
    return s


def _short_full(title):
    return re.sub(r"^[\d-]+\s*·\s*", "", title).strip()


def _exp_queue_items(text):
    """The Queue section of experiments.md -> [{title, queued, slug, target, memo, stocks}]."""
    out = []
    queue = text.split("## Queue", 1)[-1].split("## Adopted", 1)[0]
    for block in re.split(r"\n(?=### )", queue):
        m = re.match(r"### ([^\n]+)", block.strip())
        if not m:
            continue
        title = m.group(1).strip()
        d = re.match(r"(\d{4}-\d{2}-\d{2})", title)
        tgt = re.search(r"\*\*Target:\*\*\s*([^\n]+)", block)
        tgt = tgt.group(1) if tgt else ""
        memo = re.search(r"\*\*Memo:\*\*\s*(\S+)", block)
        out.append({"title": title, "queued": d.group(1) if d else "", "slug": _exp_memo_slug(title),
                    "target": tgt, "memo": memo.group(1) if memo else None,
                    "stocks": bool(re.search(r"~/Stocks\b|\bStocks/", tgt))})
    return out


def _exp_attempt_state(a, alive=None, read=None):
    """One launch record -> running | skipped | ran. `alive(pid)` / `read(path)` are the selftest's."""
    alive = alive or _pid_alive
    if a.get("dry"):
        return "dry"
    if a.get("pid") and alive(a["pid"]):
        return "running"
    try:
        txt = (read or (lambda p: open(p, errors="replace").read()))(a.get("log") or "")
    except OSError:
        txt = ""
    tail = txt.split(a.get("marker") or "\x00", 1)[-1] if a.get("marker") and a["marker"] in txt else ""
    return "skipped" if "claudeq run: SKIPPED" in tail else "ran"


def experiment_driver(dry=False, now_ts=None, mc=None, state_dir=None, spawn=None, alive=None, read=None):
    """Rule 46, `experiment-no-driver`. -> (lines, findings). Dry reports what it WOULD launch and
    launches nothing, writes nothing. `spawn(argv, log) -> pid` and the paths are the selftest's."""
    now_ts = int(time.time() if now_ts is None else now_ts)
    mc, state_dir = mc or MC, state_dir or STATE
    today = datetime.fromtimestamp(now_ts, timezone.utc).strftime("%Y-%m-%d")
    lines, f = [], []
    try:
        text = open(os.path.join(mc, "experiments.md"), errors="replace").read()
    except OSError as e:
        return [f"experiment driver: experiments.md unreadable ({e})"], f
    sp = os.path.join(state_dir, EXP_DRIVER)
    st = load(sp, {})
    try:
        props = os.listdir(os.path.join(mc, "proposals"))
    except OSError:
        props = []
    week = [a for v in st.values() for a in v.get("attempts", [])
            if now_ts - a.get("at", 0) < 7 * 86400 and _exp_attempt_state(a, alive, read) != "skipped"]
    budget = max(0, min(EXP_DRIVE_PER_PASS, EXP_DRIVE_WEEK_MAX - len(week)))
    items = sorted(_exp_queue_items(text), key=lambda x: x["queued"] or "9999")
    for it in items:
        slug = it["slug"]
        has_memo = it["memo"] or any(p.endswith(f"_{slug}.md") for p in props)
        if has_memo or not it["queued"]:
            continue
        age = (now_ts - datetime.strptime(it["queued"], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) / 86400
        if age < EXP_DRIVE_AGE_D:
            continue
        name = _short(it["title"])
        if it["stocks"]:
            lines.append(f"experiment driver: held '{name}' (target is Stocks; David's while Stocks is paused)")
            continue
        rec = st.get(slug, {})
        att = rec.get("attempts", [])
        states = [_exp_attempt_state(a, alive, read) for a in att]
        if "running" in states:
            lines.append(f"experiment driver: '{name}' design session is running")
            continue
        if "ran" in states:                      # never twice for one item: the run happened, no memo came
            _finding(f, "experiment-no-driver", "med", f"'{name}' design session ran but wrote no memo",
                     f"launched {datetime.fromtimestamp(att[states.index('ran')]['at'], timezone.utc):%Y-%m-%d} "
                     f"(log {att[states.index('ran')].get('log')}); the driver does not launch it twice. Read the "
                     "log, then re-run by hand or move the item to the Graveyard", "maintenance", key=f"exp-ran:{slug}")
            continue
        skips = states.count("skipped")
        if skips >= EXP_DRIVE_SKIPS_MAX:
            _finding(f, "experiment-no-driver", "low", f"'{name}' design job refused by the Claude queue {skips} times",
                     "claudeq.py run SKIPPED it every pass (5h budget or the clock); the driver keeps trying daily",
                     "maintenance", key=f"exp-skips:{slug}")
        if budget <= 0:
            lines.append(f"experiment driver: '{name}' waits ({age:.0f}d old; "
                         f"{len(week)} of {EXP_DRIVE_WEEK_MAX} this week, {EXP_DRIVE_PER_PASS} per pass)")
            continue
        budget -= 1
        log = os.path.join(mc, "logs", f"experiment_design_{slug}.log")
        rendered = os.path.join(state_dir, "experiment_design", f"{slug}.md")
        job = f"frontier design: {slug}"
        cmd = (f"python3 {mc}/bin/claudeq.py run --kind frontier --est {EXP_DRIVE_EST} --job {shlex.quote(job)} "
               f"--wait 20 --quiet -- timeout 1800 {mc}/bin/claude-headless -p \"$(cat {shlex.quote(rendered)})\" "
               "--dangerously-skip-permissions")
        if dry:
            lines.append(f"experiment driver: would launch '{name}' ({age:.0f}d old, no memo) as claudeq "
                         f"frontier job {job!r} -> proposals/{today}_{slug}.md")
            continue
        try:
            tpl = open(os.path.join(mc, "prompts", "experiment_design.md")).read()
            for k, v in (("{title}", _short_full(it["title"])), ("{queued}", it["queued"]),
                         ("{date}", today), ("{slug}", slug)):
                tpl = tpl.replace(k, v)
            os.makedirs(os.path.dirname(rendered), exist_ok=True)
            os.makedirs(os.path.dirname(log), exist_ok=True)
            with open(rendered, "w") as fh:
                fh.write(tpl)
            marker = f"=== experiment-no-driver {now_ts} ==="
            with open(log, "a") as fh:
                fh.write(f"\n{marker}\n")
            pid = (spawn or _exp_spawn)(["bash", "-c", cmd], log)
        except Exception as e:                   # a launch failure is a line and a finding, never a crash
            lines.append(f"experiment driver: launch of '{name}' failed: {type(e).__name__}: {e}")
            _finding(f, "experiment-no-driver", "med", f"'{name}' design job could not be launched",
                     f"{type(e).__name__}: {e}"[:200], "maintenance", key=f"exp-launch:{slug}")
            continue
        att.append({"at": now_ts, "pid": pid, "log": log, "marker": marker, "job": job})
        st[slug] = {"title": it["title"], "attempts": att}
        save(sp, st)
        lines.append(f"experiment driver: launched '{name}' as claudeq frontier job {job!r} (pid {pid}, log "
                     f"{os.path.relpath(log, mc)})")
    return lines, f


def _exp_spawn(argv, log):
    """Detached: the session outlives the daily pass; claudeq.py run holds the box slot for it."""
    with open(log, "ab") as fh:
        p = subprocess.Popen(argv, stdout=fh, stderr=fh, cwd=MC, start_new_session=True,
                             stdin=subprocess.DEVNULL)
    return p.pid


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


DASH_TEST_TIMEOUT_S = 90       # one tab test
DASH_TESTS_BUDGET_S = 600      # every tab test together, so a hung server cannot stall the pass


def dashboard_tab_tests(dash=None):
    """-> [(file, label)] for every dashboard/test_*_tab.js, sorted: the same discovery as
    dashboard/check.py (its browser tests, test_*_browser.js, and the boot/click/routing helpers
    are not *_tab.js, so neither runs them). check.py skips test_catalog_tab.js only because it
    runs that test's two catalog checks itself; the janitor runs the tab test directly."""
    dash = dash or os.path.join(MC, "dashboard")
    try:
        names = sorted(fn for fn in os.listdir(dash)
                       if fn.startswith("test_") and fn.endswith("_tab.js")
                       and os.path.isfile(os.path.join(dash, fn)))
    except OSError:
        return []
    return [(fn, fn[5:-7].replace("_", " ").title()) for fn in names]


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

def _write_memo(target, name, body, note):
    """Drop memo `<date>_<name>.md` in MEMOS/inbox/<target>/ and append its LEDGER row. -> the path,
    or None when a memo of that name was already filed today (the file is the ask-once for a day)."""
    day = f"{datetime.now(timezone.utc):%Y-%m-%d}"
    inbox = os.path.join(MEMOS, "inbox", target)
    path = os.path.join(inbox, f"{day}_{name}.md")
    if os.path.exists(path):
        return None
    os.makedirs(inbox, exist_ok=True)
    with open(path, "w") as fh:
        fh.write(body)
    # newest-first, first under the table header (bin/ledger_rows.py, memo review-ledger-hygiene);
    # Source is the bus slug "maintenance", never "mission-control"
    import ledger_rows
    ledger_rows.prepend(f"| {day} | {name} | maintenance | {target} | proposed | {note} |",
                        os.path.join(MEMOS, "LEDGER.md"))
    return path


def file_memos(new_findings, dry=False):
    """A finding inside another project is a memo, not an edit. High severity only —
    the bus is for things a session must act on, not a nag feed."""
    filed = []
    for f in new_findings:
        if f.get("fix") != "memo" or f["sev"] != "high" or not f["project"]:
            continue
        target = f["project"].lower()
        if dry:
            continue
        body = (f"# {f['title']}\n\n_From: Mission Control back-office pass · "
                f"{datetime.now(timezone.utc):%Y-%m-%d} · target: {target}_\n\n"
                f"{f['detail']}\n\nRaised by the daily audit "
                f"(`~/maintenance/bin/backoffice.py`), rule `{f['kind']}`. "
                "Fix it in the project, or mute the rule in "
                "`~/maintenance/config/backoffice_mute.json` if it's an accepted deviation.\n")
        if _write_memo(target, f"backoffice-{f['kind']}", body, "auto-filed by the daily back-office audit"):
            filed.append(f)
    return filed


# ---------------------------------------------------------------- repeat-failure

REPEAT_EPISODES, REPEAT_WINDOW_D = 3, 30


def _episodes_in_window(f, now_ts, days=REPEAT_WINDOW_D):
    """Episodes of an open finding inside the window: the open one plus each closed history entry
    (resolved or fixed; a mute is a choice, not a recurrence) that closed inside it."""
    lo = now_ts - days * 86400
    return 1 + sum(1 for h in (f.get("history") or ())
                   if h.get("how") in ("resolved", "fixed") and (h.get("closed") or 0) >= lo)


def _is_erp_host(f):
    """The clientco ERP host (an outside owner's machine): David 09-08, "just stop pinging". A key or
    title naming the ERP / VENDORERP / ERP-A as a word."""
    txt = f"{f.get('key') or ''} {f.get('title') or ''}".lower()
    return bool(re.search(r"(?<![a-z0-9])(erp|vendorerp|erp-a)(?![a-z0-9])", txt))


def _blocker_for(key, blockers):
    for b in blockers or ():
        try:
            if b.get("pattern") and re.search(b["pattern"], key or ""):
                return b
        except re.error:
            continue
    return None


def _ledger_has(name):
    """True when MEMOS/LEDGER.md already carries a row for memo `name` (ask-once survives a lost state file)."""
    try:
        with open(os.path.join(MEMOS, "LEDGER.md")) as fh:
            return any(f"| {name} |" in ln for ln in fh)
    except OSError:
        return False


def _repeat_slug(key):
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", str(key).lower())).strip("-")[:60] or "item"


def repeat_failures(open_findings, dry=False, now_ts=None):
    """Rule repeat-failure (memo wa-repeat-failures asks 2-3): an open finding on its 3rd episode inside
    30 days (reopen_count >= 2) changes form. ONE memo per key, ever (REPEAT_ASKED), to the owner's inbox
    asking for a root-cause slice: fix the cause, or declare it expected and get it muted with a reason
    (rule 7). Never a push. Held instead of filed:
      * a key matching config/known_blockers.json: the detail says "blocked by <row>: answer that";
      * a Stocks-owned key: "held: Stocks is frozen" until David reopens it;
    and the clientco ERP host's memo asks clientco_db_lead to draft the note and park it as needs-david
    (a message to a human is David's to send). Annotates each repeating record (`repeat`, and the
    "×N since" line in its detail). -> [(key, outcome)], outcome one of filed:<path> | held-stocks |
    blocked:<row> | asked-before."""
    now_ts = time.time() if now_ts is None else now_ts
    blockers = load(BLOCKERS, [])
    blockers = blockers if isinstance(blockers, list) else []
    asked = load(REPEAT_ASKED, {})
    out, changed = [], False
    for f in open_findings:
        if int(f.get("reopen_count") or 0) < REPEAT_EPISODES - 1:
            continue
        n = _episodes_in_window(f, now_ts)
        if n < REPEAT_EPISODES:
            continue
        key = f.get("key") or _key(f.get("id"))
        since = datetime.fromtimestamp(f.get("first_ever_seen") or f.get("first_seen") or now_ts,
                                       timezone.utc).strftime("%Y-%m-%d")
        owner = (f.get("project") or "maintenance").lower()
        b = _blocker_for(key, blockers)
        if b:
            outcome, note = f"blocked:{b.get('row')}", (f"blocked by {b.get('row')}: answer that "
                                                         f"({b.get('reason', '')}). No repeat memo is filed.")
        elif owner == "stocks":
            outcome, note = "held-stocks", "held: Stocks is frozen until David reopens it; no repeat memo is filed."
        elif key in asked or _ledger_has(f"repeat-{_repeat_slug(key)}"):
            a = asked.get(key) or {}
            outcome, note = "asked-before", (f"repeat memo filed {a.get('at', '?')[:10]}: {a.get('path', '?')}" if a
                                             else f"repeat memo already on the LEDGER (repeat-{_repeat_slug(key)})")
        else:
            erp = owner.startswith("clientco") and _is_erp_host(f)
            ask = ("**Do not contact the ERP host's owner.** clientco_db_lead: draft the note to the outside owner "
                   "and park it as a `needs-david` LEDGER row. A message to a human is David's to send, and the "
                   "host is never pinged (David 09-08: \"just stop pinging and try again next month\")."
                   if erp else
                   "Take a root-cause slice: fix the cause so it stops coming back, or, if this is expected "
                   "behaviour, say so in a memo to `maintenance` asking for a mute with its reason "
                   "(`config/backoffice_mute.json`, box rule 7).")
            body = (f"# Repeat failure: {f.get('title')}\n\n_From: Mission Control back-office pass · "
                    f"{datetime.fromtimestamp(now_ts, timezone.utc):%Y-%m-%d} · target: {owner} · rule repeat-failure_\n\n"
                    f"`{key}` has opened {n} times in {REPEAT_WINDOW_D} days (×{int(f.get('reopen_count') or 0) + 1} "
                    f"since {since}). Each time it was closed it came back in the same form.\n\n"
                    f"**Latest detail.** {f.get('detail', '')}\n\n**Ask.** {ask}\n\n"
                    "This memo is filed once per key; the finding stays on Needs attention with its count.\n")
            path = None if dry else _write_memo(owner, f"repeat-{_repeat_slug(key)}", body,
                                                "auto-filed by the daily back-office audit (repeat-failure)"
                                                + ("; draft only, never a ping" if erp else ""))
            if path:
                asked[key] = {"at": datetime.fromtimestamp(now_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                              "path": path}
                changed = True
            outcome = f"filed:{path}" if path else ("would-file" if dry else "asked-before")
            note = f"repeat memo filed to {owner}" + (" (ERP host: draft, never a ping)" if erp else "")
        line = f"×{n} in {REPEAT_WINDOW_D} days, first seen {since}: {note}"
        f["repeat"] = {"episodes": n, "since": since, "outcome": outcome}
        base = (f.get("detail") or "").split("\n\nRepeat: ")[0]
        f["detail"] = f"{base}\n\nRepeat: {line}"
        out.append((key, outcome))
    if not dry:
        if changed:
            save(REPEAT_ASKED, asked)
        if out:
            store = load(FINDINGS, {})
            for f in open_findings:
                if f.get("repeat") and f.get("id") in store:
                    store[f["id"]].update(repeat=f["repeat"], detail=f["detail"])
            save(FINDINGS, store)
    return out


# ---------------------------------------------------------------- run

def _sustainability_due(reports_dir=None, today=None, max_age_d=28):
    """-> True when no reports/YYYY-MM-sustainability.md exists or the newest is older than max_age_d
    days (memo wa-sustainability-report, 2026-10-04: monthly, ridden by this daily pass so the pilot
    needs no crontab change)."""
    d = reports_dir or os.path.join(MC, "reports")
    try:
        ms = [os.path.getmtime(os.path.join(d, n)) for n in os.listdir(d)
              if re.match(r"^\d{4}-\d{2}-sustainability\.md$", n)]
    except OSError:
        ms = []
    t = today if today is not None else time.time()
    return not ms or (t - max(ms)) > max_age_d * 86400


def _sustainability_report():
    """Write the month's sustainability report (bin/sustainability.py: zero tokens, read-only, deletes
    nothing) when due, and commit the report by path. -> a line for the pass, or ""."""
    if not _sustainability_due():
        return ""
    r = subprocess.run([sys.executable, os.path.join(MC, "bin", "sustainability.py"), "report"],
                       capture_output=True, text=True, timeout=1200)
    if r.returncode != 0:
        return f"sustainability report failed (rc {r.returncode}): {(r.stderr or r.stdout).strip()[-160:]}"
    rep = f"reports/{datetime.now(timezone.utc):%Y-%m}-sustainability.md"
    sha = _commit([rep], f"sustainability report {datetime.now(timezone.utc):%Y-%m} (backoffice daily pass, monthly)")
    return f"sustainability report written ({rep}{', ' + sha if sha else ', not committed'})"


def _ledger_verify_apply(run=None):
    """Flip the LEDGER rows whose verify-when clause now passes (`bin/ledger_verify.py apply`, memo
    wa-lessons-propagation ask 2; zero tokens, its own selftest pins it). -> its summary line. A
    failure is a line, never an exception: the pass goes on."""
    run = run or subprocess.run
    try:
        r = run([sys.executable, os.path.join(MC, "bin", "ledger_verify.py"), "apply"],
                capture_output=True, text=True, timeout=60)
    except Exception as e:
        return f"ledger verify failed: {type(e).__name__}: {str(e)[:160]}"
    out = (r.stdout or "").strip().splitlines()
    if r.returncode != 0:
        return f"ledger verify failed (rc {r.returncode}): {((r.stderr or '') + ' ' + (out[-1] if out else '')).strip()[-160:]}"
    return f"ledger verify: {out[-1] if out else 'no output'}"


ATTENTION_WEEKS = os.path.join(STATE, "attention_weeks.jsonl")
ATTENTION_LIMIT = 25        # model calls per week, largest sessions first (~2 s each); the rest heuristic


def _attention_due(today, rows):
    """-> the Monday of the ISO week this pass should measure, or None (memo wa-attention-kpi ask 2:
    weekly, ridden by this daily pass, no crontab line). Only on a Sunday (UTC); the week is the last
    FULL one (Mon..Sun ending a week before today: the current week still runs at 11:35); and once
    that day: a history row for that week generated today means this Sunday's run already happened.
    A backfilled row from an earlier day does not count, so the Sunday run re-measures it in place."""
    from datetime import timedelta
    if today.weekday() != 6:
        return None
    mon = today - timedelta(days=13)
    y, w, _ = mon.isocalendar()
    label = f"{y}-W{w:02d}"
    if any(r.get("week") == label and str(r.get("generated") or "")[:10] == today.isoformat()
           for r in rows):
        return None
    return mon


def _attention_weekly(today=None, rows=None, run=None, dry=False):
    """On a Sunday, `attention.py week` for the last full week: state/attention.json + one row in
    state/attention_weeks.jsonl (local model, zero Claude tokens, HBS text stays on the box).
    -> a line for the pass, or "" on any other day. Never raises; a failure is a line, and a missed
    week surfaces as catalog-stale on maintenance/attention_weeks."""
    try:
        today = today or datetime.now(timezone.utc).date()
        if rows is None:
            from attention import read_weeks
            rows = read_weeks(ATTENTION_WEEKS)
        mon = _attention_due(today, rows)
        if mon is None:
            return ""
        argv = [sys.executable, os.path.join(MC, "bin", "attention.py"), "week",
                "--week", mon.isoformat(), "--limit", str(ATTENTION_LIMIT)]
        if dry:
            return f"attention: would measure the week of {mon} (dry: {' '.join(argv[2:])})"
        r = (run or subprocess.run)(argv, capture_output=True, text=True, timeout=1800)
        out = (r.stdout or "").strip().splitlines()
        if r.returncode != 0:
            return (f"attention week failed (rc {r.returncode}): "
                    f"{((r.stderr or '').strip() + ' ' + (out[-1] if out else '')).strip()[-160:]}")
        return "attention: " + (out[0] if out else "no output")
    except Exception as e:
        return f"attention week failed: {type(e).__name__}: {str(e)[:160]}"


def run(dry=False):
    c = census()
    fresh = audit(c)
    # rule 43 (memo wa-cost-attribution slice 3/4): true the weights up once a day, file weight-drift
    try:
        _wd_line, _wd_f = weight_drift_pass(dry=dry)
    except Exception as e:                       # the weight pass must never break the pass
        _wd_line, _wd_f = f"weight drift failed: {type(e).__name__}: {e}", []
    fresh += _wd_f
    # rule 46 (memo wa-handoffs-and-frontier slice 2/2): a stale Queue item gets its design session
    try:
        _ed_lines, _ed_f = experiment_driver(dry=dry or not EXP_DRIVE_LAUNCH)
    except Exception as e:                       # the driver must never break the pass
        _ed_lines, _ed_f = [f"experiment driver failed: {type(e).__name__}: {e}"], []
    fresh += _ed_f
    # David's dashboard answers first, so a mute he chose skips this very pass's merge
    decided = apply_decisions(fresh, dry=dry)
    new, resolved, open_f = merge_findings(fresh)
    fixed = fix(open_f, dry=dry)
    filed = file_memos(new, dry=dry)
    try:
        repeats = repeat_failures(open_f, dry=dry)
    except Exception as e:                       # the repeat rule must never break the pass
        repeats = []
        print(f"  repeat-failure rule failed: {type(e).__name__}: {e}")
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
    for k, outcome in repeats:
        print(f"  repeat: {k} -> {outcome}")
    if _wd_line:
        print(f"  {_wd_line}")
    for _ln in _ed_lines:
        print(f"  {_ln}")
    if not dry:
        for p in _clean_stale_pyc():
            print(f"  removed a stale byte-code cache: {os.path.relpath(p, MC)}")
        try:
            _sr = _sustainability_report()
        except Exception as e:                   # a report must never break the pass
            _sr = f"sustainability report failed: {e}"
        if _sr:
            print(f"  {_sr}")
        try:
            print(f"  {_ledger_verify_apply()}")
        except Exception as e:                   # the ledger flip must never break the pass
            print(f"  ledger verify failed: {e}")
        try:                                     # rule 34's memos: one per non-Stocks project, ask-once
            for proj, outcome in argv_memos(argv_scan()):
                if outcome.startswith("filed:"):
                    print(f"  argv-unsafe: memo to {proj.lower()} -> {outcome[6:]}")
        except Exception as e:                   # the memo filing must never break the pass
            print(f"  argv-unsafe memos failed: {type(e).__name__}: {e}")
    try:                                         # Sundays only: David's decide+learn KPI for the last week
        _at = _attention_weekly(dry=dry)
    except Exception as e:                       # the KPI must never break the pass
        _at = f"attention week failed: {type(e).__name__}: {e}"
    if _at:
        print(f"  {_at}")
    for f in new:
        print(f"  new [{f['sev']}] {f['title']}")
    with open(HISTORY, "a") as fh:
        fh.write(json.dumps({"at": now(), "new": len(new), "fixed": len(fixed),
                             "resolved": len(resolved), "open": len(still_open),
                             "briefed": len(status), "decided": len(decided)}) + "\n")
    # state-change doctrine: push only when something actually changed
    loud = _push_worthy(new)                     # QUIET_KINDS are filed, never pushed on their own
    if not dry and (loud or fixed or filed):
        head = [f"{f['title']}" for f in sorted(loud, key=lambda x: SEV[x["sev"]])[:3]]
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
    global FINDINGS, MUTE, CONFIG, MC, _commit, MEMOS, BLOCKERS, REPEAT_ASKED, ARGV_ASKED, ARGV_MAX_PY
    ok = True

    def check(name, cond, info=""):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name + (f"  — {info}" if info and not cond else ""))
        ok = ok and bool(cond)

    pt = []
    _finding(pt, "k", "low", "3 queue job(s) reach no lead", "d")
    _finding(pt, "k", "low", "the x guard refused 1 action(s) in 24 h", "d")
    check("a finding title says 3 jobs / 1 action, never (s); its id keeps the raw title",
          [x["title"] for x in pt] == ["3 queue jobs reach no lead", "the x guard refused 1 action in 24 h"]
          and pt[0]["id"] == "k::3-queue-job-s-reach-no-lead", [(x["id"], x["title"]) for x in pt])
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

        # cron attribution (2026-09-29): the folder a line cds into wins, then another project's
        # script over Mission Control's shared tools, then the old longest-name match
        pn = ["Stocks", "data-desk", "clientco-db", "hbs", "maintenance", "poker", "poker-appstore", "thesis"]
        for cmd, want in (
                ("cd ~/Stocks/_engine && ./.venv/bin/python research/closes.py refresh >> logs/c.log 2>&1 "
                 f"|| {HOME}/maintenance/bin/notify.sh --tier actionable alerts x y", "Stocks"),
                ("cd ~/data-desk && .venv/bin/python bin/desk.py predmkt >> logs/predmkt.log 2>&1 "
                 "|| ~/maintenance/bin/notify.sh alerts x y", "data-desk"),
                (f"cd {HOME}/hbs && python3 {HOME}/maintenance/bin/claudeq.py run --kind k -- x", "hbs"),
                (f"python3 {HOME}/maintenance/bin/claudeq.py run --kind vp --job vp -- "
                 f"{HOME}/Stocks/_engine/agent/runner.py vp >> /tmp/x.log 2>&1", "Stocks"),
                ("cd ~/maintenance && python3 bin/catalog.py compile ~/Stocks/x", "maintenance"),
                ("python3 ~/maintenance/bin/sentinel.py >> ~/maintenance/logs/sentinel.log 2>&1", "maintenance"),
                ("cd ~/poker-appstore && npm run build >> logs/b.log 2>&1", "poker-appstore"),
                ("cd /tmp && python3 ~/poker/tools/x.py", "poker"),
                ("~/.claude/remote-control/keepalive", "maintenance"), ("echo hi", "")):
            got = _project_of(cmd, names=pn)
            check(f"cron attribution: {cmd[:48]}… → {want or 'nobody'}", got == want, got)

        # project leads (2026-10-02): the three lead rules over `lead.py status --json` rows. Stocks is
        # led by the PM in its own harness (kind external, 2026-10-03) and files nothing; an excluded
        # row with no lead still files lead-missing, and a mute by key quiets it; lead-overdue waits
        # out the first-week grace
        st_rows = [{"slug": "stocks", "dir": "Stocks", "lead": "pm", "lead_name": "Stocks PM", "kind": "external",
                    "harness": "Stocks loop.py/ops.py", "state": "excluded", "why": "led by the PM"},
                   {"slug": "oldproj", "dir": "oldproj", "lead": None, "state": "excluded", "why": "David: not now"},
                   {"slug": "newproj", "dir": "newproj", "lead": "newproj_lead", "state": "missing",
                    "problems": ["no charter and no lead.json yet"]},
                   {"slug": "poker", "dir": "poker", "lead": "poker_lead", "state": "invalid",
                    "problems": ["lead.json: weekday 'Tues' is not one of Mon, Tue, …"]},
                   {"slug": "hbs", "dir": "hbs", "lead": "hbs_lead", "state": "ok", "overdue": True,
                    "days_since_ok": 12.4, "weekday": "Wed", "last_weekly_state": "skipped",
                    "last_run": {"mode": "weekly", "state": "skipped", "why": "a Claude lead session already ran"},
                    "next_run": "2026-10-14T07:02+00:00"},
                   {"slug": "thesis", "dir": "thesis", "lead": "thesis_lead", "state": "ok", "overdue": False}]
        lf = []
        _lead_findings(lf, st_rows, today="2026-10-13")
        kinds = sorted((x["kind"], x["project"], x["key"]) for x in lf)
        check("leads: missing (an excluded project with no lead too), invalid and overdue, one each, keyed by "
              "project; Stocks, led by the PM in its own harness, files nothing", kinds == [
            ("lead-invalid", "poker", "lead-invalid:poker"), ("lead-missing", "newproj", "lead-missing:newproj"),
            ("lead-missing", "oldproj", "lead-missing:oldproj"), ("lead-overdue", "hbs", "lead-overdue:hbs")], kinds)
        check("leads: the invalid finding carries lead.py's problem",
              any("weekday 'Tues'" in x["detail"] for x in lf if x["kind"] == "lead-invalid"))
        lf2 = []
        _lead_findings(lf2, st_rows, today="2026-10-11")
        check("leads: no lead-overdue inside the first-week grace (before 2026-10-12)",
              not any(x["kind"] == "lead-overdue" for x in lf2), [x["kind"] for x in lf2])
        _fsave = FINDINGS
        FINDINGS = f"{t}/findings-leads.json"
        save(MUTE, {"muted": ["lead-missing:oldproj"]})
        new, _, _ = merge_findings(lf)
        check("leads: a mute by key keeps one lead-missing off the list; the rest stay",
              sorted(x["project"] for x in new) == ["hbs", "newproj", "poker"], [x["id"] for x in new])
        FINDINGS = _fsave
        save(MUTE, {"muted": []})

        # owner-missing (2026-10-03, single-threaded owner): every thing walks to one lead
        ag = {"pm": {"id": "pm", "project": "Stocks"}, "janitor": {"id": "janitor", "project": "Mission Control"},
              "collector": {"id": "collector", "project": "newproj"}, "ghost": {"id": "ghost", "project": "Atlantis"},
              "scout": {"id": "scout", "project": "newproj", "owner_lead": "pm"},
              "liar": {"id": "liar", "project": "newproj", "owner_lead": "nobody_lead"}}
        th = [{"what": "cron job", "name": "loop.py trade", "agent": "pm", "project": "Stocks"},
              {"what": "cron job", "name": "backoffice.py", "agent": "janitor", "project": "maintenance"},
              {"what": "cron job", "name": "healthcheck.sh", "agent": None, "project": ""},
              {"what": "cron job", "name": "run.py", "agent": None, "project": "newproj"},
              {"what": "agent", "name": "Collector (collector)", "agent": "collector"},
              {"what": "agent", "name": "Ghost (ghost)", "agent": "ghost"},
              {"what": "agent", "name": "Scout (scout)", "agent": "scout"},
              {"what": "agent", "name": "Liar (liar)", "agent": "liar"},
              {"what": "queue job", "name": "'pe firms' (pe)", "agent": None, "project": None},
              {"what": "queue job", "name": "'lead hbs:inbox' (lead)", "agent": None, "project": "hbs"},
              {"what": "service", "name": "maintenance-dashboard", "agent": None, "project": "maintenance"},
              {"what": "service", "name": "stocks-thing", "agent": None, "project": "Stocks"},
              {"what": "dataset", "name": "box/memo_inbox", "agent": None, "project": "box"},
              {"what": "dataset", "name": "stocks/desk", "agent": None, "project": "stocks"}]
        sto_rows = st_rows + [{"slug": "maintenance", "dir": "maintenance", "lead": "maintenance_lead", "state": "ok"}]
        of = []
        miss = _owner_findings(of, th, sto_rows, ag)
        check("owner-missing: Stocks' things (cron, service, dataset, the PM itself) walk to pm; box-wide "
              "things and Mission Control's agents walk to maintenance_lead; an agent's owner_lead naming a "
              "real lead wins", "stocks" not in miss and "maintenance" not in miss and "box" not in miss
              and not any(t["name"].startswith("Scout") for ts in miss.values() for t in ts), sorted(miss))
        check("owner-missing: a project without a lead, a project not on disk, a fake owner_lead and an "
              "unclaimed queue job each file, one finding per project; a queue job naming a project with a "
              "lead (lead hbs:inbox) is owned",
              sorted((x["project"], x["key"]) for x in of) == [
                  ("maintenance", "owner-missing:maintenance:atlantis"),
                  ("maintenance", "owner-missing:maintenance:unclaimed"), ("newproj", "owner-missing:newproj:newproj")]
              and len(miss["newproj"]) == 3, sorted((x["project"], x["key"]) for x in of))
        of2 = []
        _owner_findings(of2, th, [r for r in sto_rows if r["slug"] != "maintenance"]
                        + [{"slug": "maintenance", "dir": "maintenance", "lead": "maintenance_lead", "state": "invalid"}], ag)
        check("owner-missing: with maintenance_lead invalid, the box-wide plumbing has no owner either",
              any(x["key"] == "owner-missing:maintenance:maintenance" and "healthcheck.sh" in x["detail"] for x in of2),
              [x["key"] for x in of2])
        qp = f"{t}/cq_events.jsonl"
        with open(qp, "w") as fh:
            fh.write(json.dumps({"at": int(time.time()) - 3600, "ev": "start", "job": "pe firms", "kind": "pe"}) + "\n"
                     + json.dumps({"at": int(time.time()) - 40 * 86400, "ev": "start", "job": "old one", "kind": "x"}) + "\n"
                     + json.dumps({"at": int(time.time()) - 60, "ev": "release", "job": "rel", "kind": "x"}) + "\n")
        check("owner-missing: queue jobs are the starts of the last 30 days",
              _sto_queue_jobs(qp) == [("pe firms", "pe")], _sto_queue_jobs(qp))

        # lead-structure (2026-10-04, David: "any project with a lead should follow this structure"):
        # the armed-check is this fixture — a complete lead project passes, one missing README trips,
        # a placeholder projects.json desc trips, an external lead (Stocks) is never walked
        lh = f"{t}/leadhome"

        def lead_proj(name, skip=()):
            r = f"{lh}/{name}"
            os.makedirs(f"{r}/.claude/agents", exist_ok=True)
            subprocess.run(["git", "init", "-q", r], check=False, capture_output=True)
            files = {"CLAUDE.md": f"# {name}\n\n## Project lead\n\n{name}_lead owns it.\n", "README.md": "x\n",
                     "ARCHITECTURE.md": "x\n", f".claude/agents/{name}_lead.md": "---\nname: x\n---\n",
                     ".claude/lead.json": "{}\n", ".claude/lead-memory.md": "x\n", "catalog.json": "{}\n",
                     ".gitignore": ".claude/worktrees/\n"}
            for rel, body in files.items():
                if rel not in skip:
                    with open(f"{r}/{rel}", "w") as fh:
                        fh.write(body)
        lead_proj("fullp")
        lead_proj("nordme", skip=("README.md",))
        lead_proj("bare", skip=("CLAUDE.md", "ARCHITECTURE.md", ".gitignore"))
        with open(f"{lh}/bare/CLAUDE.md", "w") as fh:
            fh.write("# bare\n\nno lead section here\n")
        lrows = [{"slug": "fullp", "dir": "fullp", "lead": "fullp_lead", "state": "ok"},
                 {"slug": "nordme", "dir": "nordme", "lead": "nordme_lead", "state": "ok"},
                 {"slug": "bare", "dir": "bare", "lead": "bare_lead", "state": "invalid"},
                 {"slug": "stocks", "dir": "Stocks", "lead": "pm", "state": "excluded", "kind": "external"},
                 {"slug": "nolead", "dir": "nolead", "lead": "nolead_lead", "state": "missing"}]
        lcfg = {"projects": {"Full P": {"desc": "A real one-liner", "match": ["fullp"]},
                             "nordme": {"desc": "Another real one", "match": ["nordme"]},
                             "bare": {"desc": "bare", "match": ["bare"]}}}
        lf = []
        lgaps = _lead_structure_findings(lf, lrows, lcfg, home=lh)
        check("lead-structure: a complete lead project passes; an external lead (Stocks) and a project "
              "with no lead are not walked", "fullp" not in lgaps and "stocks" not in lgaps
              and "nolead" not in lgaps, lgaps)
        check("lead-structure: a lead project missing its README trips, one low finding naming only that",
              lgaps.get("nordme") == ["no README.md"]
              and [(x["kind"], x["sev"], x["project"]) for x in lf if x["project"] == "nordme"]
              == [("lead-structure", "low", "nordme")], (lgaps.get("nordme"), lf))
        check("lead-structure: no lead section, no ARCHITECTURE.md, worktrees not ignored and a placeholder "
              "desc are each a gap, still one finding for the project",
              len([x for x in lf if x["project"] == "bare"]) == 1
              and any("Project lead" in g for g in lgaps.get("bare", []))
              and any("ARCHITECTURE" in g for g in lgaps.get("bare", []))
              and any("worktrees" in g for g in lgaps.get("bare", []))
              and any("projects.json" in g for g in lgaps.get("bare", [])), lgaps.get("bare"))
        lcfg2 = {"projects": {"fullp": {"desc": "(auto-added by the back-office pass)", "match": ["fullp"]},
                              "nordme": {"desc": "x", "match": ["nordme"]}}}
        lg2 = _lead_structure_findings([], lrows[:1], lcfg2, home=lh)
        lg3 = _lead_structure_findings([], lrows[:1], {"projects": {}}, home=lh)
        check("lead-structure: an '(auto-added ...)' desc and a missing projects.json row are gaps",
              len(lg2.get("fullp", [])) == 1 and len(lg3.get("fullp", [])) == 1, (lg2, lg3))

        # armed: the auto-fix policy's three selftests and the dead-box alarm (2026-10-03)
        outs = {"bin/decisions.py": (0, "PASS a\nALL PASS\n"), "dashboard/tt_decide.py": (0, "PASS b\nALL PASS"),
                "bin/memo-process.py": (0, "PASS c\nALL PASS")}
        _af0 = _auto_fix_selftests(lambda p: outs[p])
        check("auto-fix armed: all three pass -> nothing", _af0 == [], _af0)
        outs["dashboard/tt_decide.py"] = (1, "PASS b\nFAIL policy: never auto: money\n1 FAILED")
        outs["bin/memo-process.py"] = (0, "PASS c")                # no ALL PASS line: it did not finish
        bad = _auto_fix_selftests(lambda p: outs[p])
        check("auto-fix armed: a FAIL line or a missing ALL PASS names the script and why",
              [n for n, _ in bad] == ["tt_decide.py", "memo-process.py"] and "never auto: money" in bad[0][1], bad)
        dm, now0 = f"{t}/deadman.json", 1791100000
        save(dm, {"at": now0 - 3600, "due": now0 + 3600, "seq": "spark-deadman"})
        fresh = _deadman_why(dm, now0)
        save(dm, {"at": now0 - 4600, "due": now0 + 2600})
        stale = _deadman_why(dm, now0)
        save(dm, {"at": 0, "due": 0, "cancelled": now0 - 600})
        cancel_new = _deadman_why(dm, now0)
        save(dm, {"at": 0, "due": 0, "cancelled": now0 - 9000})
        cancel_old = _deadman_why(dm, now0)
        check("deadman armed: re-armed 60 min ago or cancelled 10 min ago -> armed; 76 min, a cancel left 2.5 h "
              "and a missing file -> not, in plain words",
              (fresh, cancel_new, "76 min ago" in stale, "cancelled 150 min ago" in cancel_old,
               "missing" in _deadman_why(f"{t}/nope.json", now0)) == ("", "", True, True, True),
              (fresh, stale, cancel_new, cancel_old))

        # rollup-held (2026-10-02): only the NEWEST Daily rollup row counts
        nl = f"{t}/notifications.jsonl"
        roll = lambda ts, pushed: {"time": ts, "channel": "maintenance", "title": "Daily rollup", "tier": "actionable",
                                   "pushed": pushed, "reason": "maintenance at its 1/day cap — held" if not pushed else "sent"}
        with open(nl, "w") as fh:
            for r in (roll(1, False), roll(2, True), {"time": 3, "channel": "maintenance", "title": "Memo needs your call",
                                                       "pushed": True}):
                fh.write(json.dumps(r) + "\n")
        check("rollup-held: quiet when the newest rollup went out", _rollup_held(nl) == (None, [False, True]),
              _rollup_held(nl))
        with open(nl, "a") as fh:
            fh.write(json.dumps(roll(4, False)) + "\n")
        row, fl = _rollup_held(nl)
        check("rollup-held: fires on a held newest rollup, with the 14-day count",
              row and row["time"] == 4 and fl == [False, True, False], (row, fl))
        check("rollup-held: no ledger, nothing to say", _rollup_held(f"{t}/missing.jsonl") == (None, []))

        # the two guard hooks' armed-check (2026-10-02): listed for every tool they cover (matchers
        # read as regexes), executable, own selftest passes
        gb = f"{t}/guardbin"
        os.makedirs(gb)
        for name, body, mode in (("good.py", "print('PASS a')\nprint('ALL PASS')\n", 0o755),
                                 ("bad.py", "import sys\nprint('FAIL deny path')\nsys.exit(1)\n", 0o755),
                                 ("noexec.py", "print('ALL PASS')\n", 0o644)):
            with open(f"{gb}/{name}", "w") as fh:
                fh.write("#!/usr/bin/env python3\n" + body)
            os.chmod(f"{gb}/{name}", mode)
        cmd = lambda n: {"type": "command", "command": f"{gb}/{n}", "timeout": 10}
        hs = {"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [cmd("good.py"), cmd("bad.py")]},
                                       {"matcher": "Edit|Write|MultiEdit|NotebookEdit", "hooks": [cmd("good.py")]},
                                       {"matcher": "mcp__brokerb-trading__.*", "hooks": [cmd("bad.py")]}]}}
        check("guard armed-check: registered for every tool, executable, selftest passes -> armed",
              _guard_gaps(hs, "good.py", ("Bash", "Edit", "Write", "NotebookEdit"), gb) == [],
              _guard_gaps(hs, "good.py", ("Bash", "Edit", "Write", "NotebookEdit"), gb))
        g = _guard_gaps(hs, "good.py", ("Bash", "mcp__brokerb-trading__place_equity_order"), gb)
        check("guard armed-check: a tool the matchers do not route to the hook is named",
              g == ["not listed under hooks.PreToolUse for mcp__brokerb-trading__place_equity_order"], g)
        g = _guard_gaps(hs, "bad.py", ("Bash", "mcp__brokerb-trading__cancel_equity_order"), gb)
        check("guard armed-check: a failing selftest is named with its FAIL line",
              len(g) == 1 and "exits 1" in g[0] and "FAIL deny path" in g[0], g)
        g = _guard_gaps({"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [cmd("noexec.py")]}]}},
                        "noexec.py", ("Bash",), gb)
        check("guard armed-check: not executable", g == ["not executable"], g)
        check("guard armed-check: no hooks at all", _guard_gaps({}, "good.py", ("Bash",), gb)
              == ["not listed under hooks.PreToolUse for Bash"])

        # handover-ready (2026-09-29): one high finding per lane at parity_ok on a declared
        # `<slug>/handover` board, keyed by the lane; nothing for shadow/stays/requested lanes,
        # a lane id that is not a slug never reaches the shell command, an unreadable board says so
        hv = f"{t}/handover.json"
        lane = lambda i, st, **k: dict({"id": i, "owner": "stocks", "name": f"Lane {i} (feeds.py)", "state": st,
                                        "days_green": 7, "need_days": 7, "replaced_by": ["data-desk/filings_radar"],
                                        "parity": {"kind": "accession_superset", "value": 1.0, "bar": 1.0,
                                                   "n": 112, "covered": 112, "missing_sample": ["0000-secret"]},
                                        "cron": {"state": "live", "line": 16}, "consumers": ["x"]}, **k)
        save(hv, {"status": "ready", "at": "2026-09-29T08:15:00Z", "ready": ["a-lane"],
                  "lanes": [lane("a-lane", "parity_ok"), lane("b-lane", "shadow"), lane("c-lane", "stays"),
                            lane("d-lane", "cutover_requested"), lane("x; rm -rf ~", "parity_ok")]})
        hs = _handover_state(ids=["data-desk/handover", "data-desk/health", "stocks/feed", "zz/handover"],
                             reader=lambda cid: hv if cid == "data-desk/handover" else f"{t}/nope.json")
        hf = []
        _handover_findings(hf, hs)
        ready = [x for x in hf if x["kind"] == "handover-ready"]
        check("handover-ready: only the declared */handover ids are read, the parity_ok lane is the one finding",
              [h["id"] for h in hs] == ["data-desk/handover", "zz/handover"] and len(ready) == 1
              and ready[0]["key"] == "handover-ready:data-desk:a-lane" and ready[0]["sev"] == "high"
              and ready[0]["project"] == "data-desk" and ready[0]["title"] == "Ready to hand over: Lane a-lane", [hs, hf])
        d0 = ready[0]["detail"] if ready else ""
        check("handover-ready: the detail names the lane, its replacement, the parity and David's step",
              all(w in d0 for w in ("`a-lane`", "owned by stocks", "data-desk/filings_radar", "112/112", "100%",
                                    "7 of 7 days green", "cron line is live",
                                    "`cd ~/data-desk && .venv/bin/python bin/desk.py handover retire a-lane`",
                                    "retirement memo to stocks"))
              and "0000-secret" not in json.dumps(hs), d0)
        check("handover-ready: without carries/keeps the detail still reads 'now carries it'",
              "data-desk/filings_radar now carries it — accession superset" in d0 and "keeps" not in d0, d0)
        hs3 = [dict(hs[0], ready=[dict(hs[0]["ready"][0], carries="the EDGAR daily-index download",
                                       keeps=["stocks/sc13d_subjects", "stocks/spin_parents"])])]
        hf3 = []
        _handover_findings(hf3, hs3)
        d3 = next((x["detail"] for x in hf3 if x["kind"] == "handover-ready"), "")
        check("handover-ready: a partial handover names what moves and what the owner keeps",
              "now carries the EDGAR daily-index download — accession superset" in d3
              and "stocks keeps stocks/sc13d_subjects, stocks/spin_parents, which the replacement does not carry" in d3, d3)
        sv = f"{t}/handover_partial.json"
        save(sv, {"lanes": [lane("p-lane", "parity_ok", carries="the index\n download " + "x" * 300,
                                 keeps=["stocks/a", 7, "stocks/b"])]})
        hp = _handover_state(ids=["data-desk/handover"], reader=lambda cid: sv)
        rp = (hp[0].get("ready") or [{}])[0]
        check("handover-ready: carries is one line of at most 160 chars, keeps only strings",
              len(rp.get("carries", "")) == 160 and "\n" not in rp["carries"] and rp.get("keeps") == ["stocks/a", "stocks/b"], rp)
        check("handover-ready: a lane id that is not a slug never reaches a command; an unreadable board is a finding",
              not any("rm -rf" in x["detail"] and "retire x" in x["detail"] for x in hf)
              and sorted(x["key"] for x in hf if x["kind"] == "handover-unreadable")
              == ["handover-unreadable:data-desk:data-desk-handover:ids", "handover-unreadable:zz:zz-handover"], hf)
        hf2 = []
        _handover_findings(hf2, [dict(hs[0], ready=[dict(hs[0]["ready"][0], days_green=9, need_days=7)])])
        check("handover-ready: the key stays the same when the day count moves, so an answer sticks",
              [x["key"] for x in hf2 if x["kind"] == "handover-ready"] == ["handover-ready:data-desk:a-lane"], hf2)

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

        # the tab tests come from the folder, the way check.py finds them (2026-10-05, memo
        # review-backoffice-runs-team-tests): a new test_<view>_tab.js is run with no edit here;
        # browser tests and the boot/click helpers are not
        for n in ("test_team_tab.js", "test_overview_tab.js", "test_world_browser.js",
                  "test_catalog_boot.js", "test_x_tab.js.bak", "team_tab.js"):
            open(f"{sd}/{n}", "w").write("")
        os.makedirs(f"{sd}/test_dir_tab.js")
        check("dashboard tab tests: every test_*_tab.js on disk, labelled, browser and helper tests left out",
              dashboard_tab_tests(sd) == [("test_overview_tab.js", "Overview"), ("test_team_tab.js", "Team")],
              dashboard_tab_tests(sd))
        _live = [x for x, _ in dashboard_tab_tests(os.path.join(HOME, "maintenance", "dashboard"))]
        check("dashboard tab tests: the live folder's set includes the three once hard-coded and test_team_tab.js",
              {"test_overview_tab.js", "test_claude_tab.js", "test_catalog_tab.js", "test_team_tab.js"} <= set(_live),
              _live)

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
        # the auto-add (roster-missing) writes the new project's identity with no edit (2026-10-04): a free hue and a
        # free fallback emblem, a name from its folder, no vitals; the existing cards keep theirs
        save(f"{CONFIG}/projects.json", {"identity_palette": {
            "hues": [["project-a", "#111111"], ["project-b", "#222222"], ["project-c", "#333333"]],
            "emblems": ["alpha"], "fallback_emblems": ["flag", "compass"]},
            "projects": {"alpha": {"desc": "x", "match": ["alpha"], "identity": {
                "name": "Alpha", "role": "Chief", "emblem": "alpha", "hue": "project-a", "for": "", "vitals": []}}}})
        rfx = []
        _finding(rfx, "roster-missing", "high", "brand-new-proj is not on the dashboard", "3 commits", "brand-new-proj", fix="auto")
        _finding(rfx, "roster-missing", "high", "second-one is not on the dashboard", "1 commit", "second-one", fix="auto")
        rdone = fix([dict(x, state="open") for x in rfx])
        rc = load(f"{CONFIG}/projects.json", {}).get("projects") or {}
        i1, i2 = (rc.get("brand-new-proj") or {}).get("identity") or {}, (rc.get("second-one") or {}).get("identity") or {}
        check("roster-missing: the auto-add writes each new project's identity, a hue and an emblem of its own, with no edit",
              len(rdone) == 2 and i1.get("name") == "Brand New Proj" and i1.get("role") == "Project lead"
              and (i1.get("hue"), i1.get("emblem")) == ("project-b", "flag") and (i2.get("hue"), i2.get("emblem")) == ("project-c", "compass")
              and i1.get("vitals") == [] and rc["alpha"]["identity"]["hue"] == "project-a"
              and "flag on project-b" in rdone[0][1], (rdone, rc))
        dfix = []
        _finding(dfix, "diagram-unrendered", "low", "1 diagram source edited but not re-rendered", "a.d2",
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
    # queue-starved (2026-10-03): one med finding per starving job, keyed by the job; filed, not pushed
    qf = []
    _queue_starved(qf, [{"key": "update:OLD", "tier": 20, "est_min": 15, "by": "cli", "waiting_h": 49.0,
                         "filed_h": 49.0, "skip": "15m would run into pe_claude.py at 05:40Z"}])
    _queue_starved(qf, [])
    check("queue-starved: one med finding for the starving job, keyed by it, naming filer and refusal",
          [(x["kind"], x["sev"], x["project"], x["fix"]) for x in qf] == [("queue-starved", "med", "maintenance", "human")]
          and "update:OLD" in qf[0]["title"] and qf[0]["key"] == _key("queue-starved:maintenance:update:OLD")
          and "by cli" in qf[0]["detail"] and "pe_claude" in qf[0]["detail"], qf)
    check("queue-starved: filed, never pushed on its own; any other new finding still pushes",
          _push_worthy(qf) == [] and len(_push_worthy(qf + [{"kind": "rollup-held"}])) == 1)
    try:
        import claudeq as _cq
        _t0 = datetime(2026, 10, 3, 6, 10, tzinfo=timezone.utc)
        _deferred = {"key": "update:TICKER", "since": 1790523096.9, "not_before": 1791000300.0, "by": "plane-rescore"}
        _late = dict(_deferred, key="update:LATE", not_before=None, since=_t0.timestamp() - 50 * 3600)
        check("queue-starved: claudeq.starving() skips a job deferred by not_before (a Stocks update, filed 09-27, "
              "held to 10-03 04:05Z) and returns one startable 50 h",
              [r["key"] for r in _cq.starving(_t0, jobs=[_deferred, _late])] == ["update:LATE"])
    except Exception as e:
        check("queue-starved: claudeq.starving() is importable", False, f"{type(e).__name__}: {e}")

    # the monthly sustainability report rides this pass (memo wa-sustainability-report, 2026-10-04)
    ts_ = tempfile.mkdtemp(prefix="backoffice-sust.")
    try:
        _n0 = time.time()
        _due0 = _sustainability_due(ts_, _n0)
        open(f"{ts_}/2026-10-sustainability.md", "w").write("x")
        open(f"{ts_}/2026-10.md", "w").write("x")        # the sweep's report is not this one
        _due1 = _sustainability_due(ts_, _n0)
        _due2 = _sustainability_due(ts_, _n0 + 29 * 86400)
        check("sustainability: due with no report, not due with a fresh one, due again after 28 days",
              (_due0, _due1, _due2) == (True, False, True), (_due0, _due1, _due2))
    finally:
        shutil.rmtree(ts_, ignore_errors=True)

    # a key's life across episodes (memo 2026-10-03_wa-repeat-failures, ask 1): a reopen keeps
    # first_ever_seen / reopen_count / history; a mute is recorded as muted, not resolved
    saved_fm = (FINDINGS, MUTE)
    th = tempfile.mkdtemp(prefix="backoffice-history.")
    try:
        FINDINGS, MUTE = f"{th}/findings.json", f"{th}/mute.json"
        save(MUTE, {"muted": []})
        save(FINDINGS, {})
        base = []
        _finding(base, "dashboard-broken", "high", "a fixture check is failing", "d")
        fid = base[0]["id"]
        fresh = lambda: [dict(base[0])]
        n_h, _, _ = merge_findings(fresh())
        first0 = load(FINDINGS, {})[fid]["first_ever_seen"]
        reopens = []
        for _ in range(3):
            _, r_h, _ = merge_findings([])           # gone -> resolved
            n_h, _, _ = merge_findings(fresh())      # back -> reopened
            reopens.append(bool(n_h) and n_h[0]["id"] == fid)
        rec = load(FINDINGS, {})[fid]
        check("history: a finding that reopens 3 times has reopen_count 3, first_ever_seen unchanged, "
              "3 closed episodes in its history, and is new (pushed) each time it reopens",
              rec["reopen_count"] == 3 and rec["first_ever_seen"] == first0 and len(rec["history"]) >= 3
              and all(y["how"] == "resolved" and y["closed"] for y in rec["history"])
              and rec["state"] == "open" and all(reopens), rec)
        save(MUTE, {"muted": [base[0]["key"]]})
        _, r_m, _ = merge_findings(fresh())
        rec = load(FINDINGS, {})[fid]
        check("history: a muted finding is recorded as state muted (not resolved), still counted as "
              "leaving the open set, its history noting how=muted",
              rec["state"] == "muted" and rec.get("muted_at")
              and [x["id"] for x in r_m] == [fid] and rec["history"][-1]["how"] == "muted", rec)
        save(MUTE, {"muted": []})
        merge_findings(fresh())
        rec = load(FINDINGS, {})[fid]
        check("history: unmuted and found again is a 4th reopen, the lifetime carried over",
              rec["state"] == "open" and rec["reopen_count"] == 4 and rec["first_ever_seen"] == first0
              and len(rec["history"]) == 4, rec)
        big = {"history": [{"opened": i, "closed": i, "how": "resolved"} for i in range(HISTORY_CAP)],
               "first_seen": 5}
        _close(big, "fixed", 99)
        check("history: capped at HISTORY_CAP, the oldest dropped",
              len(big["history"]) == HISTORY_CAP and big["history"][0]["opened"] == 1
              and big["history"][-1] == {"opened": 5, "closed": 99, "how": "fixed"} and big["state"] == "fixed")
        # moved key: a title whose number changed between episodes still carries the life
        st_k = {"x:p:has-1-thing": {"id": "x:p:has-1-thing", "key": "x:p:thing", "kind": "x", "state": "resolved",
                                    "first_seen": 10, "first_ever_seen": 5, "reopen_count": 2, "last_seen": 20,
                                    "history": [{"opened": 10, "closed": 30, "how": "resolved"}]}}
        save(FINDINGS, st_k)
        merge_findings([{"id": "x:p:has-2-thing", "key": "x:p:thing", "kind": "x", "sev": "low",
                         "title": "has 2 thing", "detail": "d", "project": "p", "fix": "human"}])
        st_k = load(FINDINGS, {})
        check("history: a reopen under a new id (same key) carries first_ever_seen/reopen_count/history and "
              "marks the old record reopened_as",
              st_k["x:p:has-2-thing"]["reopen_count"] == 3 and st_k["x:p:has-2-thing"]["first_ever_seen"] == 5
              and len(st_k["x:p:has-2-thing"]["history"]) == 1
              and st_k["x:p:has-1-thing"].get("reopened_as") == "x:p:has-2-thing", st_k)
        # the one-time backfill, on the 09-26 → 10-01 shape of dashboard-broken (decisions.jsonl)
        K = "dashboard-broken::a-dashboard-check-is-failing"
        bf = {K: {"id": K, "key": K, "kind": "dashboard-broken", "state": "resolved", "first_seen": 1000,
                  "last_seen": 1100, "resolved_at": 1200},
              "other::x": {"id": "other::x", "key": "other::x", "state": "open", "first_seen": 50}}
        evs = [{"id": "a1", "at": 100, "key": K, "finding_id": K, "status": "queued", "by": "david"},
               {"id": "a1", "at": 150, "key": K, "status": "done", "by": "daily check", "commit": "abc"},
               {"id": "a2", "at": 300, "key": K, "finding_id": K, "status": "queued", "by": "david"},
               {"id": "a2", "at": 350, "key": K, "status": "done", "by": "daily check"},
               {"id": "a3", "at": 600, "key": K, "status": "applied", "action": "snooze", "by": "david"},
               {"id": "a3b", "at": 610, "key": K, "status": "queued", "by": "david"},   # a snooze ends its answer: new episode
               {"id": "a4", "at": 1050, "key": K, "status": "queued", "by": "david"},  # the current episode
               {"id": "a4", "at": 1150, "key": K, "status": "done", "by": "daily check"}]
        ch = backfill_history(bf, evs, at=2000)
        r = bf[K]
        check("backfill: answers before the current episode seed reopen_count, first_ever_seen is the first "
              "answer, history holds the prior episodes (approx) and the current one",
              r["reopen_count"] == 4 and r["first_ever_seen"] == 100 and len(r["history"]) == 5
              and r["history"][0] == {"opened": 100, "closed": 150, "how": "fixed", "approx": True, "src": "decisions:a1"}
              and r["history"][2]["closed"] == 610 and r["history"][-1] == {"opened": 1000, "closed": 1200,
                                                                           "how": "resolved"}
              and bf["other::x"]["reopen_count"] == 0 and bf["other::x"]["first_ever_seen"] == 50
              and bf["other::x"]["history"] == [] and {c[0] for c in ch} == {K, "other::x"}, r)
        check("backfill: a second run changes nothing (counts only rise, history seeded once)",
              backfill_history(bf, evs, at=3000) == [] and bf[K]["reopen_count"] == 4)
        live = os.path.join(STATE, "findings.json")
        check("history: every fixture write went to the temp store, never the live findings.json",
              FINDINGS.startswith(th) and os.path.realpath(FINDINGS) != os.path.realpath(live))
    finally:
        FINDINGS, MUTE = saved_fm
        shutil.rmtree(th, ignore_errors=True)

    # cli-stale (2026-10-04): the headless CLI pin against David's interactive sessions, pure core
    D, T0 = 86400, 2_000_000_000
    cs = lambda h, i, hf=None, inf=None: ([{"time": hf or T0 - D, "headless": True, "cli_version": h},
                                           {"time": inf or T0 - D, "headless": False, "cli_version": i}])
    r6 = _cli_stale(cs("2.1.280", "2.1.286"), T0)
    check("cli-stale: 6 patches behind fires and names both versions",
          bool(r6) and r6["headless"] == "2.1.280" and r6["interactive"] == "2.1.286" and r6["behind"] == 6, r6)
    check("cli-stale: 4 patches behind with close first sightings is quiet; headless ahead or equal is quiet",
          _cli_stale(cs("2.1.282", "2.1.286"), T0) is None and _cli_stale(cs("2.1.286", "2.1.286"), T0) is None
          and _cli_stale(cs("2.1.290", "2.1.286"), T0) is None, _cli_stale(cs("2.1.282", "2.1.286"), T0))
    old_first = [{"time": T0 - 40 * D, "headless": True, "cli_version": "2.1.282"}]
    r_lag = _cli_stale(old_first + cs("2.1.282", "2.1.284", inf=T0 - 2 * D), T0)
    check("cli-stale: 2 patches behind fires when the headless version was first seen >14d before the interactive one",
          bool(r_lag) and r_lag["behind"] == 2 and r_lag["lag_d"] == 38.0, r_lag)
    check("cli-stale: rows older than 14 days do not count; no interactive row is quiet; a newer minor counts as behind",
          _cli_stale(cs("2.1.280", "2.1.286", hf=T0 - 20 * D, inf=T0 - 20 * D), T0) is None
          and _cli_stale(cs("2.1.280", "x"), T0) is None
          and (_cli_stale(cs("2.1.299", "2.2.0"), T0) or {}).get("behind") == 5)
    fc = []
    _cli_stale_finding(fc, cs("2.1.280", "2.1.286"), T0)
    check("cli-stale: one low finding keyed cli-stale, quiet (never pushed), detail names cli-update.py status and the parked canary",
          len(fc) == 1 and fc[0]["sev"] == "low" and fc[0]["key"] == "cli-stale:maintenance:cli-stale"
          and "cli-update.py status" in fc[0]["detail"] and "parked" in fc[0]["detail"]
          and _push_worthy(fc) == [], fc)

    # backup-tamper (2026-10-04): a recorded archive that changed size or hash, or vanished in its keep window
    import hashlib as _hl
    tb = tempfile.mkdtemp(prefix="backoffice-bt.")
    try:
        os.makedirs(f"{tb}/poker")
        stamp = lambda d: datetime.fromtimestamp(T0 - d * D, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        brows = []
        for n, body, age in (("a", b"alpha", 1), ("b", b"bravo", 2), ("c", b"charlie", 3), ("old", b"o", 40)):
            with open(f"{tb}/poker/{n}.tar.gz", "wb") as fh:
                fh.write(body)
            brows.append({"ts": stamp(age), "source": "poker", "archive": f"poker/{n}.tar.gz",
                          "bytes": len(body), "sha256": _hl.sha256(body).hexdigest()})
        os.unlink(f"{tb}/poker/old.tar.gz")      # pruned at 40 days: fine
        srcs = {"poker": {"keep_days": 21}}
        hz = lambda p: _hl.sha256(open(p, "rb").read()).hexdigest()
        check("backup-tamper: a clean recorded set (and an archive pruned past keep_days) files nothing",
              _backup_tamper(brows, srcs, tb, T0, hasher=hz) == [], _backup_tamper(brows, srcs, tb, T0, hasher=hz))
        with open(f"{tb}/poker/a.tar.gz", "ab") as fh:
            fh.write(b"x")
        os.unlink(f"{tb}/poker/b.tar.gz")
        with open(f"{tb}/poker/c.tar.gz", "wb") as fh:
            fh.write(b"CHARLIE")             # same size, different bytes
        bt = _backup_tamper(brows, srcs, tb, T0, hasher=hz)
        fb = []
        _backup_tamper_finding(fb, bt)
        check("backup-tamper: a size change, a deleted young archive and a same-size edit each file one high finding",
              [r for r, _ in bt] == ["poker/a.tar.gz", "poker/b.tar.gz", "poker/c.tar.gz"]
              and len(fb) == 3 and all(x["sev"] == "high" for x in fb) and "disappeared" in bt[1][1], bt)
        check("backup-tamper: over the hash limit only the size is judged; a delegated or unknown source is skipped",
              [r for r, _ in _backup_tamper(brows, srcs, tb, T0, hash_max=3, hasher=hz)] == ["poker/a.tar.gz", "poker/b.tar.gz"]
              and _backup_tamper(brows, {"poker": {"delegated_to": "x"}}, tb, T0, hasher=hz) == []
              and _backup_tamper(brows, {}, tb, T0, hasher=hz) == [])
    finally:
        shutil.rmtree(tb, ignore_errors=True)

    # repeat-failure (2026-10-04): the 3rd episode in 30 days files ONE memo to the owner; Stocks is
    # held, a known blocker names its row, the ERP host gets a draft-and-park memo, never a ping
    saved_rp = (MEMOS, BLOCKERS, REPEAT_ASKED, FINDINGS)
    tr = tempfile.mkdtemp(prefix="backoffice-repeat.")
    try:
        MEMOS, BLOCKERS = f"{tr}/memos", f"{tr}/known_blockers.json"
        REPEAT_ASKED, FINDINGS = f"{tr}/repeat_asked.json", f"{tr}/findings.json"
        os.makedirs(f"{tr}/memos/inbox")
        open(f"{tr}/memos/LEDGER.md", "w").write("| date | memo |\n")
        save(BLOCKERS, [{"pattern": "^job-failed:00ab0ae3$", "row": "reopen-stocks-for-broker-gate",
                         "reason": "Stocks is closed", "since": "2026-10-03"}])
        hist = lambda *ages: [{"opened": T0 - (a + 1) * D, "closed": T0 - a * D, "how": "resolved"} for a in ages]
        rec = lambda key, proj, rc, h, title="a check is failing": {
            "id": key, "key": key, "kind": key.split(":")[0], "sev": "med", "title": title, "detail": "it failed.",
            "project": proj, "state": "open", "first_seen": T0 - 3600, "first_ever_seen": T0 - 20 * D,
            "reopen_count": rc, "history": h}
        recs = [rec("dashboard-broken::x", "", 2, hist(10, 3)),                 # 3rd episode: one memo
                rec("unit-dead:Stocks:y", "Stocks", 2, hist(9, 2)),             # Stocks: held
                rec("job-failed:00ab0ae3", "maintenance", 3, hist(8, 5, 1)),    # blocked
                rec("dashboard-broken::old", "", 2, hist(60, 45)),              # episodes older than 30 days
                rec("unit-dead:clientco-db:erp-host-down", "clientco-db", 2, hist(7, 4), "the ERP host is down"),
                rec("dashboard-broken::twice", "", 1, hist(3))]                 # only the 2nd episode
        save(FINDINGS, {r["id"]: dict(r) for r in recs})
        rp = dict(repeat_failures(recs, now_ts=T0))
        inbox = lambda slug: sorted(os.listdir(f"{tr}/memos/inbox/{slug}")) if os.path.isdir(f"{tr}/memos/inbox/{slug}") else []
        led = open(f"{tr}/memos/LEDGER.md").read()
        check("repeat-failure: a key on its 3rd episode in 30 days files exactly one memo to its owner, plus one LEDGER row",
              rp.get("dashboard-broken::x", "").startswith("filed:") and len(inbox("maintenance")) == 1
              and inbox("maintenance")[0].endswith("_repeat-dashboard-broken-x.md")
              and led.count("| repeat-dashboard-broken-x |") == 1, (rp, inbox("maintenance")))
        check("repeat-failure: a Stocks key files nothing and its detail says held: Stocks is frozen",
              rp.get("unit-dead:Stocks:y") == "held-stocks" and inbox("stocks") == []
              and "held: Stocks is frozen" in recs[1]["detail"], recs[1]["detail"])
        check("repeat-failure: a key under a known blocker files nothing and names the blocker",
              rp.get("job-failed:00ab0ae3") == "blocked:reopen-stocks-for-broker-gate"
              and "blocked by reopen-stocks-for-broker-gate: answer that" in recs[2]["detail"]
              and len(inbox("maintenance")) == 1, recs[2]["detail"])
        erp_memo = inbox("clientco-db")
        check("repeat-failure: the clientco ERP host files one memo asking clientco_db_lead to draft and park the note, never a ping",
              len(erp_memo) == 1 and "clientco_db_lead" in open(f"{tr}/memos/inbox/clientco-db/{erp_memo[0]}").read()
              and "needs-david" in open(f"{tr}/memos/inbox/clientco-db/{erp_memo[0]}").read()
              and "never a ping" in led, erp_memo)
        check("repeat-failure: episodes older than 30 days and a 2nd episode file nothing",
              "dashboard-broken::old" not in rp and "dashboard-broken::twice" not in rp, rp)
        st = load(FINDINGS, {})
        check("repeat-failure: the store keeps the count (×3) on the record",
              st["dashboard-broken::x"]["repeat"]["episodes"] == 3 and "×3 in 30 days" in st["dashboard-broken::x"]["detail"],
              st["dashboard-broken::x"].get("detail"))
        for r in recs:
            r["detail"] = "it failed."               # merge_findings rewrites the detail from the fresh finding
        rp2 = dict(repeat_failures(recs, now_ts=T0 + D))
        check("repeat-failure: a second pass (next day) files nothing more and says it asked before",
              rp2.get("dashboard-broken::x") == "asked-before" and len(inbox("maintenance")) == 1
              and len(inbox("clientco-db")) == 1 and open(f"{tr}/memos/LEDGER.md").read() == led
              and recs[0]["detail"].count("Repeat: ") == 1, rp2)
        os.unlink(REPEAT_ASKED)
        check("repeat-failure: with the state file lost, the LEDGER row still stops a second memo",
              dict(repeat_failures(recs, now_ts=T0 + 2 * D)).get("dashboard-broken::x") == "asked-before"
              and len(inbox("maintenance")) == 1)
        check("repeat-failure: a dry pass files nothing", all(not o.startswith("filed:") for _, o in
              repeat_failures([rec("dashboard-broken::z", "", 2, hist(5, 2))], dry=True, now_ts=T0)))
    finally:
        MEMOS, BLOCKERS, REPEAT_ASKED, FINDINGS = saved_rp
        shutil.rmtree(tr, ignore_errors=True)

    # argv-unsafe (2026-10-04, rule 34): ea7af80's pre-fix scripts are flagged, the fixed ones are not;
    # one memo per non-Stocks project, ask-once, again only when the list grows; Stocks a finding only
    _mc_real = os.path.join(HOME, "maintenance")
    _git_old = lambda f: subprocess.run(["git", "-C", _mc_real, "show", f"ea7af80^:bin/{f}"],
                                        capture_output=True, text=True).stdout
    _olds = {f: _git_old(f) for f in ("backoffice.py", "publish.py", "sentinel.py")}
    check("argv-unsafe: ea7af80^ backoffice.py, publish.py and sentinel.py are flagged (dry flag by membership, live else:)",
          all(_olds.values()) and all(_argv_scan_source(v) for v in _olds.values())
          and any("live `else:`" in w for _, w in _argv_scan_source(_olds["backoffice.py"])),
          {f: _argv_scan_source(v) for f, v in _olds.items()})
    _cur = {f: open(os.path.join(_mc_real, "bin", f)).read() for f in
            ("backoffice.py", "publish.py", "sentinel.py", "memo-process.py", "lead.py", "claudeq.py", "gpu.py")}
    check("argv-unsafe: the fixed versions (and lead.py, claudeq.py, gpu.py, memo-process.py) are not flagged",
          not any(_argv_scan_source(v) for v in _cur.values()),
          {f: _argv_scan_source(v) for f, v in _cur.items() if _argv_scan_source(v)})
    _hbs = ('import sys\nif __name__ == "__main__":\n    cmd = sys.argv[1] if len(sys.argv) > 1 else "packet"\n'
            '    wk = sys.argv[2] if len(sys.argv) > 2 else None\n    if cmd == "packet":\n        print(wk)\n'
            '    elif cmd == "run":\n        sys.exit(run(wk))\n    else:\n        sys.exit(__doc__)\n')
    _ap = lambda m: ('import argparse\nif __name__ == "__main__":\n    p = argparse.ArgumentParser()\n'
                     f'    a = p.{m}()\n    run(a)\n')
    check("argv-unsafe: an optional second argument beside a command word is flagged (hbs learnings_review shape), "
          "`# argv: data` opts out, parse_args is clean, parse_known_args is flagged, no __main__ is clean",
          [ln for ln, _ in _argv_scan_source(_hbs)] == [4] and _argv_scan_source("# argv: data\n" + _hbs) == []
          and _argv_scan_source(_ap("parse_args")) == [] and len(_argv_scan_source(_ap("parse_known_args"))) == 1
          and _argv_scan_source("import sys\nprint(sys.argv[2])\n") == [] and _argv_scan_source("def (:") == [],
          _argv_scan_source(_hbs))
    saved_av = (MEMOS, ARGV_ASKED)
    ta = tempfile.mkdtemp(prefix="backoffice-argv.")
    try:
        for proj, src in (("hbs", _hbs), ("Stocks", _olds["publish.py"]), ("clean", _cur["backoffice.py"])):
            os.makedirs(f"{ta}/home/{proj}/.git")
            os.makedirs(f"{ta}/home/{proj}/bin")
            open(f"{ta}/home/{proj}/bin/job.py", "w").write(src)
        os.makedirs(f"{ta}/home/hbs/scripts/__pycache__")
        open(f"{ta}/home/hbs/scripts/__pycache__/x.py", "w").write(_hbs)
        open(f"{ta}/home/notaproject.py", "w").write(_hbs)
        sc = argv_scan(home=f"{ta}/home")
        check("argv-unsafe: the scan walks every project's bin/ (skips caches) and names file:line",
              sorted(sc) == ["Stocks", "hbs"] and sc["hbs"] == [("bin/job.py", 4, sc["hbs"][0][2])], sc)
        # 2026-10-05 (memo review-argv-scan-scope): the whole project, not only bin/ and scripts/ — a
        # script deep in a pipeline folder is found; vendored, generated, hidden, gitignored and oversize
        # files are not, and a declared data root that holds code (Stocks' _engine/sources) is still read
        _deep = {"pipeline/stage/run.py": _hbs, "_engine/sources/fetch.py": _hbs,
                 "node_modules/pkg/cli.py": _hbs, ".venv/lib/x.py": _hbs, "build/lib/x.py": _hbs,
                 "gen/made.py": _hbs, "huge.py": _hbs + "#" * (ARGV_MAX_BYTES + 1)}
        _gp = f"{ta}/gitproj"
        os.makedirs(_gp)
        subprocess.run(["git", "init", "-q", _gp], capture_output=True)
        open(f"{_gp}/.gitignore", "w").write("gen/\n")
        for rel, src in _deep.items():
            os.makedirs(os.path.dirname(f"{_gp}/{rel}"), exist_ok=True)
            open(f"{_gp}/{rel}", "w").write(src)
        _dst = {}
        _dsc = argv_scan(home=ta, projects=["gitproj"], stats=_dst)
        check("argv-unsafe: a script outside bin/ is found (and one in a declared data root); node_modules, "
              ".venv, build/, a gitignored folder and an oversize file are not",
              sorted({r[0] for r in _dsc.get("gitproj", [])}) == ["_engine/sources/fetch.py", "pipeline/stage/run.py"]
              and _dst["gitproj"]["ignored"] == 1 and not _dst["gitproj"]["capped"], (_dsc, _dst))
        _cap = ARGV_MAX_PY
        try:
            ARGV_MAX_PY = 1
            _cst = {}
            argv_scan(home=ta, projects=["gitproj"], stats=_cst)
        finally:
            ARGV_MAX_PY = _cap
        check("argv-unsafe: the per-project read cap holds and says so", _cst["gitproj"]["read"] == 1
              and _cst["gitproj"]["capped"].startswith("read 1"), _cst)
        fa = []
        _argv_findings(fa, sc)
        check("argv-unsafe: one low, quiet finding per project (Stocks included), stable key",
              len(fa) == 2 and all(x["sev"] == "low" and x["kind"] in QUIET_KINDS for x in fa)
              and {x["key"] for x in fa} == {"argv-unsafe:hbs:scripts", "argv-unsafe:stocks:scripts"}
              and "bin/job.py:4" in [x for x in fa if x["project"] == "hbs"][0]["detail"], fa)
        MEMOS, ARGV_ASKED = f"{ta}/memos", f"{ta}/argv_asked.json"
        os.makedirs(f"{ta}/memos/inbox")
        open(f"{ta}/memos/LEDGER.md", "w").write("| date | memo |\n")
        ainbox = lambda slug: sorted(os.listdir(f"{ta}/memos/inbox/{slug}")) if os.path.isdir(f"{ta}/memos/inbox/{slug}") else []
        check("argv-unsafe: a dry pass files nothing", dict(argv_memos(sc, dry=True)) ==
              {"Stocks": "held-stocks", "hbs": "would-file"} and ainbox("hbs") == [] and not os.path.exists(ARGV_ASKED))
        am = dict(argv_memos(sc))
        led = open(f"{ta}/memos/LEDGER.md").read()
        body = open(f"{ta}/memos/inbox/hbs/{ainbox('hbs')[0]}").read() if ainbox("hbs") else ""
        check("argv-unsafe: ONE memo to hbs (asks hbs_lead for the ea7af80 pattern) plus a LEDGER row; Stocks gets none",
              am["hbs"].startswith("filed:") and am["Stocks"] == "held-stocks" and len(ainbox("hbs")) == 1
              and ainbox("hbs")[0].endswith("_argv-unsafe.md") and "ea7af80" in body and "hbs_lead" in body
              and ainbox("stocks") == [] and ainbox("Stocks") == [] and led.count("| argv-unsafe |") == 1, (am, led))
        check("argv-unsafe: the next pass with the same list files nothing more",
              dict(argv_memos(sc))["hbs"] == "asked-before" and len(ainbox("hbs")) == 1
              and open(f"{ta}/memos/LEDGER.md").read() == led)
        os.rename(f"{ta}/memos/inbox/hbs/{ainbox('hbs')[0]}", f"{ta}/memos/inbox/hbs/2000-01-01_argv-unsafe.md")
        grown = {"hbs": sc["hbs"] + [("bin/other.py", 9, "a dry flag '--dry' read by membership")]}
        check("argv-unsafe: a list that grows files again (next day)", dict(argv_memos(grown))["hbs"].startswith("filed:")
              and len(ainbox("hbs")) == 2 and load(ARGV_ASKED, {})["hbs"]["hits"][0].startswith("bin/job.py"))
        os.unlink(ARGV_ASKED)
        check("argv-unsafe: with the state file lost, the LEDGER row still stops a repeat memo",
              dict(argv_memos(sc))["hbs"] == "asked-before")
    finally:
        MEMOS, ARGV_ASKED = saved_av
        shutil.rmtree(ta, ignore_errors=True)

    # posture (2026-10-04, rule 35): a repo turned public + a new GitHub key + an undeclared off-loopback
    # listener are 3 findings; the baseline is 0; gh is polled at most hourly and a gh failure is a note only
    tp = tempfile.mkdtemp(prefix="backoffice-posture.")
    try:
        exp = {"public": ["stocks-public"], "private": ["stocks", "hbs"], "keys": [11, 22]}
        base_repos = [{"name": "stocks-public", "visibility": "PUBLIC"}, {"name": "stocks", "visibility": "PRIVATE"},
                      {"name": "hbs", "visibility": "PRIVATE"}]
        ss_txt = ("State Recv-Q Send-Q Local Address:Port Peer Address:PortProcess\n"
                  "LISTEN 0 4096 0.0.0.0:22 0.0.0.0:*\n"
                  "LISTEN 0 5 <host-ip>:8088 0.0.0.0:*\n"
                  "LISTEN 0 4096 127.0.0.1:11434 0.0.0.0:*\n"
                  "LISTEN 0 4096 127.0.0.53%lo:53 0.0.0.0:*\n"
                  "LISTEN 0 16 [::1]:3493 [::]:*\n"
                  "LISTEN 0 4096 [fd7a:115c:a1e0::9b01:ed99]:38244 [::]:*\n")
        calls = []

        def fake_gh(repos, keys, rc=0):
            def run(cmd):
                calls.append(cmd)
                if rc:
                    return rc, "", "HTTP 401: Bad credentials"
                return 0, json.dumps(repos if "repo" in cmd else [{"id": k} for k in keys]), ""
            return run
        cache = f"{tp}/posture.json"
        snap0 = _gh_snapshot(run=fake_gh(base_repos, [11, 22]), now_ts=T0, cache=cache)
        lst0 = _ss_listeners(ss_txt)
        check("posture: the baseline (gh as expected, listeners declared, loopback and a foreign ephemeral ignored) is 0",
              _posture(snap0, exp, lst0, [22, 8088]) == [] and {p for _, p, _ in lst0} == {22, 8088, 38244}, lst0)
        flipped = [dict(r, visibility="PUBLIC") if r["name"] == "hbs" else r for r in base_repos]
        snap1 = _gh_snapshot(run=fake_gh(flipped, [11, 22, 33]), now_ts=T0 + 4000, cache=cache)
        pr = _posture(snap1, exp, _ss_listeners(ss_txt + "LISTEN 0 5 0.0.0.0:9555 0.0.0.0:* users:((\"x\",pid=7,fd=3))\n"),
                      [22, 8088])
        fp = []
        _posture_findings(fp, pr)
        check("posture: a flipped repo + an extra key + an extra port are 3 high findings with stable keys",
              len(fp) == 3 and all(x["sev"] == "high" and x["kind"] == "posture" for x in fp)
              and {x["key"] for x in fp} == {"posture:maintenance:repo:hbs", "posture:maintenance:key:33",
                                            "posture:maintenance:port:9555"}
              and "now public" in [x for x in fp if "hbs" in x["title"]][0]["title"], [x["key"] for x in fp])
        check("posture: a port rule 5 already files is skipped; a new public repo not on either list is flagged",
              _posture(snap0, exp, [("0.0.0.0", 9555, 7)], [22], skip={9555}) == []
              and "a new public repo" in _posture({"repos": [{"name": "x", "visibility": "PUBLIC"}]}, exp, [], [])[0][1])
        n = len(calls)
        again = _gh_snapshot(run=fake_gh([], []), now_ts=T0 + 4000 + 1800, cache=cache)
        check("posture: gh is polled at most hourly (a second call inside the hour reuses the cache)",
              len(calls) == n and again["keys"] == [11, 22, 33])
        bad = _gh_snapshot(run=fake_gh([], [], rc=1), now_ts=T0 + 9000, cache=cache)
        check("posture: a gh failure is a note, never a finding",
              bad["repos"] is None and bad["keys"] is None and len(bad["notes"]) == 2
              and _posture(bad, exp, [], []) == [], bad)
        check("posture: rule 5's predicate (the shared quiet list, below 1024 and ephemeral skipped)",
              _rule5_reports(9555, {"known_ports": [], "infra_ports": []})
              and not _rule5_reports(22, {}) and not _rule5_reports(40000, {})
              and not _rule5_reports(8088, {"known_ports": [8088], "infra_ports": []}))
    finally:
        shutil.rmtree(tp, ignore_errors=True)

    # rules 37-41 (2026-10-05): browser-signin, dashboard-check-red, push-undelivered, guard-deny,
    # restore-unproven, manifest-stale — pure helpers on fixtures, findings only, none of them pushes
    N = 1791200000.0                                                     # 2026-10-05 11:33Z
    t7 = tempfile.mkdtemp(prefix="backoffice-r37.")
    try:
        check("rules 37-41: every new kind is filed but never pushes on its own (QUIET_KINDS)",
              {"browser-signin", "dashboard-check-red", "push-undelivered", "guard-deny", "restore-unproven",
               "manifest-stale"} <= QUIET_KINDS)
        fbr = []
        _browser_signin_findings(fbr, {"sites": {
            "canvas": {"state": "logged_out", "signed_in_once": True, "since": "x"},
            "wsj": {"state": "challenge", "signed_in_once": True},
            "ft": {"state": "logged_in", "signed_in_once": True},
            "onefinnet": {"state": "logged_out", "signed_in_once": False},
            "vii": {"state": "unknown"}}}, {"sites": {"canvas": {"label": "Canvas (HBS)"}}})
        check("browser-signin: a site signed in once and now logged_out or challenge -> one med finding each, keyed by site",
              sorted((x["key"], x["sev"]) for x in fbr) == [("browser-signin:maintenance:canvas", "med"),
                                                           ("browser-signin:maintenance:wsj", "med")]
              and any(x["title"] == "the Spark browser is signed out of Canvas (HBS)" for x in fbr)
              and any(x["title"].endswith("signed out of wsj") for x in fbr), [(x["key"], x["title"]) for x in fbr])
        fbr = []
        _browser_signin_findings(fbr, {"sites": {"onefinnet": {"state": "logged_out", "signed_in_once": False}}}, {})
        _browser_signin_findings(fbr, {}, {})
        check("browser-signin: a registered site never signed in, and no state file, stay quiet", fbr == [], fbr)
        fdc = []
        _dash_check_finding(fdc, {"ok": False, "at": "2026-10-05T08:29:47Z", "rc": 1,
                                  "summary": ["FAIL overview tab", "FAIL - a check :: detail"]}, N)
        check("dashboard-check-red: a red check within 7 days -> one med finding naming the failing tab",
              len(fdc) == 1 and fdc[0]["sev"] == "med" and fdc[0]["key"] == "dashboard-check-red:maintenance:dash-check"
              and "FAIL overview tab" in fdc[0]["detail"], fdc)
        fdc = []
        _dash_check_finding(fdc, None, N)
        _dash_check_finding(fdc, {"ok": True, "at": "2026-10-05T08:29:47Z"}, N)
        _dash_check_finding(fdc, {"ok": False, "at": "2026-09-20T08:00:00Z"}, N)
        check("dashboard-check-red: missing file, a green check, or a red one older than 7 days -> quiet", fdc == [], fdc)
        sp = f"{t7}/alerts_pending"
        with open(sp, "w") as fh:
            fh.write(json.dumps({"ts": int(N) - 600, "title": "fresh", "body": "b", "state": "s"}) + "\n")
        fpu = []
        _push_undelivered_finding(fpu, sp, N)
        _push_undelivered_finding(fpu, f"{t7}/no_spool", N)
        check("push-undelivered: a missing spool or a push spooled 10 min ago -> quiet", fpu == [], fpu)
        with open(sp, "a") as fh:
            fh.write("not json\n" + json.dumps({"ts": int(N) - 3600, "title": "Spark: ollama DOWN"}) + "\n")
        _push_undelivered_finding(fpu, sp, N)
        check("push-undelivered: a push spooled over 30 min -> one high finding naming the oldest",
              len(fpu) == 1 and fpu[0]["sev"] == "high" and "ollama DOWN" in fpu[0]["detail"]
              and "60 min" in fpu[0]["detail"], fpu)
        gl = {"broker": [{"ts": N - 100, "verdict": "allow", "session": "s1"},
                         {"ts": N - 200, "verdict": "deny", "session": "abcdef123456", "who": "headless cron session",
                          "tool": "mcp__fixture_broker__order", "reason": ["an order tool"],
                          "excerpt": "SECRET-EXCERPT"}],
              "lead": [{"ts": N - 50, "verdict": "deny", "session": "l1", "lead": "poker_lead", "tool": "Edit",
                        "reason": ["outside its project"]},
                       {"ts": N - 90000, "verdict": "deny", "session": "old", "lead": "x_lead"}],
              "claude": [{"time": N - 30, "mode": "warn", "command": "claude -p hi"},
                         {"time": N - 99999, "mode": "deny", "command": "claude -p old"}]}
        fgd = []
        _guard_deny_findings(fgd, gl, N)
        by = {x["key"]: x for x in fgd}
        check("guard-deny: a broker deny in 24 h -> high; a lead deny -> low (the guard working as designed); a warn or a deny older than 24 h -> nothing",
              sorted((k, v["sev"]) for k, v in by.items()) == [("guard-deny:maintenance:broker", "high"),
                                                                ("guard-deny:maintenance:lead", "low")], by)
        check("guard-deny: names the count, session, agent and tool, never the excerpt or command",
              "1 denial" in by["guard-deny:maintenance:broker"]["detail"]
              and "abcdef12" in by["guard-deny:maintenance:broker"]["detail"]
              and "mcp__fixture_broker__order" in by["guard-deny:maintenance:broker"]["detail"]
              and "poker_lead" in by["guard-deny:maintenance:lead"]["detail"]
              and not any("SECRET-EXCERPT" in x["detail"] or "claude -p" in x["detail"] for x in fgd), fgd)
        fgd = []
        _guard_deny_findings(fgd, {"claude": [{"time": N - 30, "mode": "deny", "session": "c1",
                                               "command": "claude -p x"}]}, N)
        check("guard-deny: a bare-claude deny -> high, keyed claude", [(x["key"], x["sev"]) for x in fgd]
              == [("guard-deny:maintenance:claude", "high")], fgd)
        fru = []
        _restore_unproven_finding(fru, [{"ts": "2026-10-04T08:51:49Z", "ok": True}], N)
        check("restore-unproven: a green drill yesterday -> quiet", fru == [], fru)
        _restore_unproven_finding(fru, [{"ts": "2026-08-01T00:00:00Z", "ok": True},
                                        {"ts": "2026-10-04T00:00:00Z", "ok": False}], N)
        _restore_unproven_finding(fru, [], N)
        check("restore-unproven: newest GREEN drill 65 days old (a red one since does not count), or none -> med",
              len(fru) == 2 and all(x["sev"] == "med" for x in fru) and "2026-08-01" in fru[0]["detail"]
              and "none is recorded" in fru[1]["detail"], fru)
        mr = f"{t7}/manifest"
        os.makedirs(f"{mr}/2026-10-01")
        os.makedirs(f"{mr}/notadate")
        fms = []
        _manifest_stale_finding(fms, mr, N)
        _manifest_stale_finding(fms, f"{t7}/no_manifest", N)
        check("manifest-stale: newest day folder 4 days old, or no folder at all -> med each",
              len(fms) == 2 and all(x["sev"] == "med" for x in fms) and "2026-10-01" in fms[0]["detail"], fms)
        os.makedirs(f"{mr}/2026-10-03")
        os.symlink(f"{mr}/2026-10-03", f"{mr}/2026-10-09")
        fms = []
        _manifest_stale_finding(fms, mr, N)
        check("manifest-stale: a folder from 2 days ago is fresh (a symlinked future 'day' is ignored)", fms == [], fms)
        # restore-unproven slice 4/4: the detail names the exact command for the least-recently drilled source
        _srcs = {"poker": {}, "Stocks": {"delegated_to": "x"}, "hbs": {}, "memos": {}}
        _dr = [{"ts": "2026-08-01T00:00:00Z", "source": "poker", "ok": True},
               {"ts": "2026-08-02T00:00:00Z", "source": "memos", "ok": False},
               {"ts": "2026-08-03T00:00:00Z", "source": "Stocks", "ok": None}]
        check("restore-unproven: round-robin picks a never-drilled source first, then the oldest; a delegated one never",
              _drill_next(_dr, _srcs) == "hbs" and _drill_next(_dr + [{"ts": "2026-08-04T00:00:00Z", "source": "hbs",
                                                                        "ok": True}], _srcs) == "poker"
              and _drill_next([], {"Stocks": {"delegated_to": "x"}}) is None, _drill_next(_dr, _srcs))
        fru = []
        _restore_unproven_finding(fru, _dr, N, sources=_srcs)
        check("restore-unproven: the pass does not drill; the finding names `backup.py drill hbs` as the exact command",
              len(fru) == 1 and "`python3 ~/maintenance/bin/backup.py drill hbs`" in fru[0]["detail"], fru)
        # rule 45: backup-stray — the top two levels against the declared writers; older than the baseline is allowed
        br = f"{t7}/backups"
        for d_ in ("poker", "stocks", "manifest/2026-10-05", "incident/2026-10-05T01", "crontab", "family-vault/x"):
            os.makedirs(f"{br}/{d_}")
        for fn in ("README.md", "poker/poker_2026-10-05.tar.gz", "poker/poker_2026-10-05.tar.gz.sha256",
                   "poker/poker_2026-10-05.tar.gz.manifest.json", "poker/.poker_2026-10-05.tar.gz.sha256.tmp",
                   "stocks/anything.db", "crontab/x.txt", "incident/2026-10-05T01/ss.txt", "poker/old-handcopy.conf"):
            open(f"{br}/{fn}", "w").close()
        os.utime(f"{br}/poker/old-handcopy.conf", (N - 86400 * 10, N - 86400 * 10))
        _bs = {"poker": {"keep_days": 21}, "Stocks": {"delegated_to": "~/Stocks/_engine/backup.sh"}}
        check("backup-stray: declared sources, sidecars, temps, fixed folders, README and a pre-baseline file -> nothing",
              _backup_stray(br, _bs, N - 86400) == [], _backup_stray(br, _bs, N - 86400))
        os.makedirs(f"{br}/newdump")
        for fn in ("loose.tar.gz", "poker/evil.sh", "poker/hbs_2026-10-05.tar.gz"):
            open(f"{br}/{fn}", "w").close()
        os.symlink("/etc", f"{br}/crontab2")
        _st = _backup_stray(br, _bs, N - 86400)
        fbs = []
        _backup_stray_finding(fbs, _st)
        check("backup-stray: a new top-level folder, file and symlink, and a foreign name in a source folder -> one med",
              _st == ["crontab2", "loose.tar.gz", "newdump", "poker/evil.sh", "poker/hbs_2026-10-05.tar.gz"]
              and len(fbs) == 1 and fbs[0]["sev"] == "med" and fbs[0]["key"] == "backup-stray:maintenance:backup-stray"
              and "poker/evil.sh" in fbs[0]["detail"], (_st, fbs))
        fbs = []
        _backup_stray_finding(fbs, [f"s{i}" for i in range(14)])
        check("backup-stray: lists at most 10, then the count; a missing root is quiet",
              "s9" in fbs[0]["detail"] and "s10" not in fbs[0]["detail"] and "and 4 more" in fbs[0]["detail"]
              and _backup_stray(f"{t7}/no_backups", _bs) == [], fbs)
        # rule 44: fleet-stop-engaged — PAUSED_FLEET_STATE points paused.py at a temp file
        _fs = f"{t7}/fleet_stop.json"
        _pf = os.environ.get("PAUSED_FLEET_STATE")
        os.environ["PAUSED_FLEET_STATE"] = _fs
        try:
            check("fleet-stop: no state file -> not engaged, findings untouched",
                  _fleet_state() is None and _fleet_stop_apply([{"kind": "job-silent"}], _fleet_state()) == [{"kind": "job-silent"}])
            with open(_fs, "w") as fh:
                json.dump({"engaged_at": "2026-10-05T12:00:00Z", "reason": "suspected token leak", "lines": [
                    '50 7 * * * cd ~/maintenance && python3 bin/decisions.py pending >> logs/decisions.log 2>&1 && '
                    'python3 /h/maintenance/bin/claudeq.py run --kind decisions --est 20 --job "daily check" -- x',
                    '15 15 5 * * cd ~/clientco-db && ./.venv/bin/python scripts/ingest_gate.py >> logs/ingest.log 2>&1 '
                    '&& python3 /h/maintenance/bin/claudeq.py run --kind clientco --job "clientco ingest" -- y']}, fh)
            _fl = []
            _finding(_fl, "registry-orphan", "low", "ingest_gate.py is documented but no longer scheduled", "d")
            _finding(_fl, "registry-orphan", "low", "decisions.py is documented but no longer scheduled", "d")
            _finding(_fl, "queue-starved", "low", "queue job 'daily check' startable 60h", "d")
            _finding(_fl, "job-silent", "high", "memo-process.py has not run in 90h", "d")
            _finding(_fl, "guardrail-inert", "med", "gpu.py selftest fails — the GPU queue's ordering rules no longer hold", "d")  # a REGISTERED title: tt_system reads every guardrail-inert title in this file
            _finding(_fl, "posture", "high", "a repo turned public", "d")
            _kept = _fleet_stop_apply(_fl, _fleet_state(), {"decisions.py", "memo-process.py", "claudeq.py"})
            _ks = sorted((x["kind"], x["title"][:22]) for x in _kept)
            _fe = [x for x in _kept if x["kind"] == "fleet-stop-engaged"]
            check("fleet-stop: engaged -> the paused jobs' registry/queue findings held; a live job, a guardrail and posture kept",
                  _ks == sorted([("registry-orphan", "decisions.py is docume"), ("job-silent", "memo-process.py has no"),
                                 ("guardrail-inert", "gpu.py selftest fails "), ("posture", "a repo turned public"),
                                 ("fleet-stop-engaged", "fleet-stop is engaged:")]), _ks)
            check("fleet-stop: ONE high quiet finding, owner maintenance, with since when, why and the held count",
                  len(_fe) == 1 and _fe[0]["sev"] == "high" and _fe[0]["project"] == "maintenance"
                  and "fleet-stop-engaged" in QUIET_KINDS and "2026-10-05T12:00:00Z" in _fe[0]["detail"]
                  and "suspected token leak" in _fe[0]["detail"] and "2 finding(s)" in _fe[0]["detail"]
                  and _fe[0]["key"] == "fleet-stop-engaged:maintenance:fleet-stop", _fe)
        finally:
            if _pf is None:
                os.environ.pop("PAUSED_FLEET_STATE", None)
            else:
                os.environ["PAUSED_FLEET_STATE"] = _pf
        # rule 42: ledger-order, and _write_memo's row lands first under the header with Source maintenance
        lg = f"{t7}/LEDGER.md"
        hdr = "# l\n\n| Date | Memo | Source | Target | Status | Evidence |\n|---|---|---|---|---|---|\n"
        with open(lg, "w") as fh:
            fh.write(hdr + "| 2026-10-04 | b | x | y | p | e |\n| 2026-10-01 | a | x | y | p | e |\n")
        flo = []
        _ledger_order_finding(flo, lg)
        _ledger_order_finding(flo, f"{t7}/no_ledger.md")
        check("ledger-order: a newest-first ledger, or none, is quiet", flo == [], flo)
        with open(lg, "a") as fh:
            fh.write("| 2026-10-05 | c | x | y | p | e |\n| 2026-10-03 | d | x | y | p | e |\n"
                     "| 2026-10-04 | e | x | y | p | e |\n")
        _ledger_order_finding(flo, lg)
        check("ledger-order: two appended rows -> ONE low quiet finding with the count and the first offender",
              len(flo) == 1 and flo[0]["sev"] == "low" and flo[0]["kind"] in QUIET_KINDS
              and flo[0]["key"] == "ledger-order:maintenance:ledger" and "2 row(s)" in flo[0]["detail"]
              and "line 7 (2026-10-05 c, under a 2026-10-01 row)" in flo[0]["detail"], flo)
        _mm = MEMOS
        try:
            MEMOS = f"{t7}/memos"
            os.makedirs(MEMOS)
            with open(f"{MEMOS}/LEDGER.md", "w") as fh:
                fh.write(hdr + "| 2026-10-01 | a | x | y | p | e |\n")
            _wp = _write_memo("poker", "backoffice-x", "body", "note")
            _ll = open(f"{MEMOS}/LEDGER.md").read().splitlines()
            check("_write_memo: its row is first under the header (newest-first), Source maintenance",
                  _wp and _ll[4].endswith(f"| backoffice-x | maintenance | poker | proposed | note |")
                  and _ll[5] == "| 2026-10-01 | a | x | y | p | e |" and _ll[:4] == hdr.splitlines(), _ll)
        finally:
            MEMOS = _mm
        # rule 43: weight-drift on fixtures (a temp weights file and stamp; job_weights.json never touched)
        _wf = f"{t7}/job_weights.json"
        _doc = {"weights": [{"match": "a", "tokens_per_run": 2_000_000}, {"match": "b", "tokens_per_run": 100_000},
                            {"match": "c", "tokens_per_run": 100_000}, {"match": "d", "tokens_per_run": 0}]}
        with open(_wf, "w") as fh:
            json.dump(_doc, fh)
        _rows = [{"match": "a", "declared": 2_000_000, "n": 4, "median": 330_400, "usd_run": 1.5, "action": "rewrite"},
                 {"match": "b", "declared": 100_000, "n": 2, "median": 260_000, "usd_run": 0.5,
                  "action": "report only (n=2 < 3)"},
                 {"match": "c", "declared": 100_000, "n": 1, "median": 130_000, "usd_run": 0.3,
                  "action": "report only (n=1 < 3)"},
                 {"match": "d", "declared": 0, "n": 0, "median": None, "usd_run": None, "action": "skip: declared 0"}]
        _live = lambda: (json.load(open(_wf)), [dict(r) for r in _rows])
        _cm = []
        _st = f"{t7}/wd_stamp.json"
        _ln, _wff = weight_drift_pass(dry=True, live=_live, weights=_wf, stamp=_st, today="2026-10-05",
                                      commit=lambda p, m: _cm.append(p) or "x")
        check("weight-drift: --dry writes nothing (no file change, no stamp, no commit) and still files",
              json.load(open(_wf)) == _doc and not os.path.exists(_st) and not _cm and len(_wff) == 1
              and "dry" in _ln, (_ln, _cm))
        check("weight-drift: ONE low quiet finding naming only the n<3 row more than 50% off",
              len(_wff) == 1 and _wff[0]["sev"] == "low" and _wff[0]["kind"] in QUIET_KINDS
              and _wff[0]["key"] == "weight-drift:maintenance:weights" and "b: declared 100k, measured 260k (n=2)"
              in _wff[0]["detail"] and "c:" not in _wff[0]["detail"] and "a:" not in _wff[0]["detail"], _wff)
        _ln, _ = weight_drift_pass(live=_live, weights=_wf, stamp=_st, today="2026-10-05",
                                   commit=lambda p, m: _cm.append(p) or "x")
        _w = {w["match"]: w for w in json.load(open(_wf))["weights"]}
        check("weight-drift: the live pass rewrites n>=3 only (330k), leaves n<3 and a declared 0, commits by path",
              _w["a"]["tokens_per_run"] == 330_000 and _w["b"]["tokens_per_run"] == 100_000
              and _w["d"]["tokens_per_run"] == 0 and _cm == [[os.path.relpath(_wf, MC)]]
              and "1 row(s) rewritten" in _ln, (_ln, _cm, _w))
        _ln, _ = weight_drift_pass(live=_live, weights=_wf, stamp=_st, today="2026-10-05",
                                   commit=lambda p, m: _cm.append(p) or "x")
        check("weight-drift: once a day (a second pass the same day applies nothing)", _ln == "" and len(_cm) == 1, _ln)
        _ln, _ = weight_drift_pass(live=lambda: (_ for _ in ()).throw(OSError("no ledger")), weights=_wf, stamp=_st)
        check("weight-drift: a measuring failure is a line, never an exception", "could not measure" in _ln, _ln)
    finally:
        shutil.rmtree(t7, ignore_errors=True)

    # the attention KPI rides this pass on Sundays (memo wa-attention-kpi ask 2): fires only on a
    # Sunday, for the last full ISO week, once that day; a failure is a line, never an exception
    from datetime import date as _date
    _week = [_date(2026, 10, d) for d in range(5, 12)]                  # Mon 10-05 .. Sun 10-11
    check("attention: not due Monday..Saturday",
          [_attention_due(d, []) for d in _week[:6]] == [None] * 6)
    check("attention: due on Sunday, for the last FULL week (Mon 09-28 .. Sun 10-04)",
          _attention_due(_week[6], []) == _date(2026, 9, 28))
    check("attention: a row for that week generated earlier (the backfill) does not stop it",
          _attention_due(_week[6], [{"week": "2026-W40", "generated": "2026-10-05T02:00:00Z"}])
          == _date(2026, 9, 28))
    check("attention: once that Sunday (a row generated today -> not due again)",
          _attention_due(_week[6], [{"week": "2026-W40", "generated": "2026-10-11T11:40:00Z"}]) is None)
    check("attention: a year-end Sunday names the right ISO week (Sun 2027-01-10 -> W53 of 2026)",
          _attention_due(_date(2027, 1, 10), []) == _date(2026, 12, 28)
          and _attention_due(_date(2027, 1, 10), [{"week": "2026-W53", "generated": "2027-01-10T11:40Z"}])
          is None)
    _calls = []

    class _R:
        def __init__(self, rc, out="", err=""):
            self.returncode, self.stdout, self.stderr = rc, out, err

    def _fake(rc=0, exc=None):
        def go(argv, **kw):
            _calls.append(argv)
            if exc:
                raise exc
            return _R(rc, "attention week 2026-09-28..2026-10-04: KPI (DECIDE+LEARN) = 41.0%\nwrote x", "boom")
        return go
    check("attention: a weekday pass runs nothing and says nothing",
          _attention_weekly(today=_week[0], rows=[], run=_fake()) == "" and not _calls)
    _line = _attention_weekly(today=_week[6], rows=[], run=_fake())
    check("attention: the Sunday pass runs `attention.py week --week <Mon> --limit N` once, reports its KPI line",
          len(_calls) == 1 and _calls[0][2:] == ["week", "--week", "2026-09-28", "--limit", str(ATTENTION_LIMIT)]
          and _line.startswith("attention: attention week 2026-09-28") and "41.0%" in _line, (_calls, _line))
    check("attention: dry names the run and runs nothing",
          "would measure the week of 2026-09-28" in _attention_weekly(today=_week[6], rows=[], run=_fake(), dry=True)
          and len(_calls) == 1)
    check("attention: a failing run (rc 1) or a raising one is a line, never an exception",
          _attention_weekly(today=_week[6], rows=[], run=_fake(rc=1)).startswith("attention week failed (rc 1)")
          and _attention_weekly(today=_week[6], rows=[], run=_fake(exc=TimeoutError("t")))
          .startswith("attention week failed: TimeoutError"))

    # rule 46 experiment-no-driver (memo wa-handoffs-and-frontier slice 2/2), on a scratch tree
    _ex = tempfile.mkdtemp(prefix="backoffice-expdrv.")
    try:
        os.makedirs(f"{_ex}/prompts"); os.makedirs(f"{_ex}/proposals"); os.makedirs(f"{_ex}/state")
        open(f"{_ex}/prompts/experiment_design.md", "w").write("design {title} queued {queued} -> {date}_{slug}.md")
        open(f"{_ex}/experiments.md", "w").write(
            "# q\n\n## Queue\n\n### 2026-09-18 · Stale poker corpus\n- **Target:** `~/poker/tools/`\n\n"
            "### 2026-09-18 · Stale Stocks trim\n- **Target:** `~/Stocks/_engine/agent/loop.py`\n\n"
            "### 2026-09-20 · Has memo already\n- **Target:** `~/hbs/x`\n- **Memo:** proposals/x.md — SKIP\n\n"
            "### 2026-09-21 · Second stale item\n- **Target:** `~/maintenance/bin/x.py`\n\n"
            "### 2026-09-22 · Third stale item\n- **Target:** `~/hbs/bin/ingest.py`\n\n"
            "### 2026-09-23 · Fourth stale item\n- **Target:** `~/poker/x`\n\n"
            "### 2026-10-05 · Fresh item\n- **Target:** `~/poker/x`\n\n## Adopted\n\n## Graveyard\n")
        T = int(datetime(2026, 10, 7, 11, 36, tzinfo=timezone.utc).timestamp())
        spawned, live, logs = [], set(), {}
        def _spawn(argv, log):
            spawned.append((argv, log)); return 1000 + len(spawned)
        _alive = lambda pid: pid in live
        _read = lambda p: open(p, errors="replace").read() + logs.get(p, "")
        ED = lambda dry=False, ts=T: experiment_driver(dry=dry, now_ts=ts, mc=_ex, state_dir=f"{_ex}/state",
                                                      spawn=_spawn, alive=_alive, read=_read)
        _l, _f = ED(dry=True)
        check("experiment-no-driver: dry names the oldest stale item, launches nothing, writes no state",
              not spawned and not os.path.exists(f"{_ex}/state/{EXP_DRIVER}")
              and any("would launch 'Stale poker corpus'" in x for x in _l), _l)
        check("experiment-no-driver: a Stocks-target item is held with a line, never launched",
              any("held 'Stale Stocks trim'" in x for x in _l) and not any("would launch 'Stale Stocks" in x for x in _l))
        _l, _f = ED()
        _st = load(f"{_ex}/state/{EXP_DRIVER}", {})
        check("experiment-no-driver: a stale item enqueues exactly ONE claudeq run --kind frontier job",
              len(spawned) == 1 and "claudeq.py run --kind frontier" in spawned[0][0][2]
              and "claude-headless" in spawned[0][0][2] and list(_st) == ["stale-poker-corpus"], (spawned, _st))
        check("experiment-no-driver: the rendered prompt carries the item, the date and the memo slug",
              open(f"{_ex}/state/experiment_design/stale-poker-corpus.md").read()
              == "design Stale poker corpus queued 2026-09-18 -> 2026-10-07_stale-poker-corpus.md")
        live.add(1001)
        ED(ts=T + 86400)
        check("experiment-no-driver: next pass, the running item is not relaunched; the next stale one goes",
              [os.path.basename(s[1]) for s in spawned] == ["experiment_design_stale-poker-corpus.log",
                                                             "experiment_design_second-stale-item.log"],
              [s[1] for s in spawned])
        live.discard(1001)
        _l, _f = ED(ts=T + 2 * 86400)
        check("experiment-no-driver: an item whose session ran and wrote no memo is never launched twice "
              "(a finding instead)", "stale-poker-corpus" not in spawned[-1][1]
              and any(x["kind"] == "experiment-no-driver" and "Stale poker corpus" in x["title"] for x in _f), _f)
        with open(spawned[1][1], "a") as _fh:
            _fh.write("claudeq run: SKIPPED frontier design: second-stale-item — 5h budget\n")
        n = len(spawned)
        _l, _f = ED(ts=T + 3 * 86400)
        check("experiment-no-driver: a run the queue SKIPPED never started; it is retried, inside the cap",
              len(spawned) == n + 1 and "second-stale-item" in spawned[-1][1], [s[1] for s in spawned])
        _l, _f = ED(ts=T + 4 * 86400)
        check("experiment-no-driver: at most 3 started launches in 7 days (the skipped one not counted)",
              len(spawned) == 4 and any("Fourth stale item' waits" in x and "3 of 3" in x for x in _l), _l)
        open(f"{_ex}/proposals/2026-10-01_fourth-stale-item.md", "w").write("# memo")
        check("experiment-no-driver: a memo file in proposals/ counts as a memo; fresh items wait their week",
              not any("Fourth stale" in x or "Fresh item" in x for x in ED(dry=True, ts=T)[0]))
    finally:
        shutil.rmtree(_ex, ignore_errors=True)

    # the argv contract (2026-10-03): an argument this does not know is exit 2, never the live pass
    P = parse_argv
    check("argv: `experiments` and `experiments --dry` parse; `experiments --now` is exit 2",
          P(["experiments", "--dry"]) == ("experiments", True, "") and P(["experiments"]) == ("experiments", False, "")
          and bool(P(["experiments", "--now"])[2]))
    check("argv: history-backfill takes --dry / --dry-run, and an unknown flag after it is exit 2",
          P(["history-backfill", "--dry"]) == ("history-backfill", True, "")
          and P(["history-backfill"]) == ("history-backfill", False, "")
          and bool(P(["history-backfill", "--force"])[2]))
    check("argv: no command is run; --dry and --dry-run are the same dry run; commands keep their flag",
          [P(a) for a in ([], ["--dry"], ["--dry-run"], ["run"], ["fix", "--dry-run"], ["selftest"])]
          == [("run", False, ""), ("run", True, ""), ("run", True, ""), ("run", False, ""), ("fix", True, ""),
              ("selftest", False, "")], [P(a) for a in ([], ["--dry-run"], ["fix", "--dry-run"])])
    check("argv: --selftest, --bogus, -n, a misspelt command, run --force are errors (exit 2)",
          all(P(a)[2] for a in (["--selftest"], ["--bogus"], ["-n"], ["rnu"], ["run", "--force"])),
          [P(a) for a in (["--selftest"], ["rnu"])])
    print("ALL PASS" if ok else "SOME FAILED")
    return 0 if ok else 1


COMMANDS = ("census", "audit", "fix", "brief", "run", "show", "decide", "selftest", "history-backfill",
            "experiments")
USAGE = ("usage: backoffice.py [census|audit|fix|brief|run|show|decide|selftest|history-backfill|experiments] "
         "[--dry|--dry-run] | --help")


def parse_argv(argv):
    """-> (cmd, dry, error). No command is `run`; --dry and --dry-run are one flag. Anything else
    is an error the caller turns into exit 2 — never the live pass (2026-10-03)."""
    cmd, dry, rest = "run", False, list(argv)
    if rest and not rest[0].startswith("-"):
        cmd = rest.pop(0)
        if cmd not in COMMANDS:
            return cmd, dry, f"unknown command {cmd!r}"
    for a in rest:
        if a in ("--dry", "--dry-run"):
            dry = True
        else:
            return cmd, dry, f"unknown argument {a!r}"
    return cmd, dry, ""


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    cmd, dry, _err = parse_argv(sys.argv[1:])
    if _err:
        print(f"backoffice.py: {_err} — nothing run. {USAGE}", file=sys.stderr)
        sys.exit(2)
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
    elif cmd == "history-backfill":
        sys.exit(history_backfill(dry=dry))
    elif cmd == "experiments":                   # rule 46 alone; --dry launches nothing, writes nothing
        _ls, _fs = experiment_driver(dry=dry or not EXP_DRIVE_LAUNCH)
        for _ln in _ls or ["experiment driver: nothing to drive"]:
            print(_ln)
        for _f in _fs:
            print(f"[{_f['sev']:>4}] {_f['kind']:<18} {_f['title']}")
    elif cmd == "run":
        sys.exit(run(dry=dry))
