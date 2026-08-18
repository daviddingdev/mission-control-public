#!/usr/bin/env python3
"""Public mirrors of the private projects — the resume layer. Zero Claude tokens to run.

David, 2026-08-18: every project should have a public counterpart that shows technical depth
without any private information, so the work is visible to the world without exposing the
money, the data, or the box.

The rule that makes this safe: **a public repo is AUTHORED, never mirrored.** Nothing is
copied out of a private repo except files on an explicit allowlist, and everything — authored
prose included — passes a scanner before it can be published. A sync would eventually leak;
a written description of a system cannot leak what it never contained.

The scanner is built from the box's real secrets (ntfy topics read from config, the tailnet
names, the home path, the researched tickers, the sudo password), not from memory. It is a
hard gate: `publish` refuses on any hit and prints the offending line.

Private repos are never touched. This writes only under ~/public/<repo>/, creates its own
git repo there, and pushes to its own remote. It never adds a remote to a private repo,
never reads a gitignored path, and never runs a command inside a project checkout.

    publish.py scan [repo]      run the leak scanner over the built content
    publish.py build [repo]     render the templates into ~/public/<repo>/
    publish.py publish [repo]   build + scan + create/push the public GitHub repo
    publish.py refresh          the daily job: re-copy every allowlisted file from the
                                private repos, scan, publish only what is clean, and
                                QUARANTINE (never publish) a file that started leaking
    publish.py candidates       files in the private repos that would scan clean and are
                                not published yet — the "you wrote something worth showing"
                                report
    publish.py status           what exists, what is held back and why

Keeping it current is the hard half. The projects change daily, and a public repo that
describes last month's system is worse for its purpose than no repo. So the split is:

  * CODE tracks automatically. Allowlisted files are re-copied verbatim every day and
    republished if they still scan clean. Nothing to remember.
  * A file that starts leaking is quarantined, not published, and raises a finding. The
    daily scan is the tripwire: private content can arrive in an already-published file
    long after anyone thought about publishing.
  * PROSE is authored, so it is never auto-written — the audit only flags it as stale once
    the project has moved far enough ahead of its written overview.
  * NEW publishable code is proposed, never auto-added: `candidates` lists files that would
    pass, and a human decides whether they belong in a public repo at all.
"""
import json
import os
import ast
import re
import shutil
import subprocess
import time
import sys
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
MC = os.path.join(HOME, "maintenance")
CFG = os.path.join(MC, "config/public_repos.json")


def cfg():
    with open(CFG) as f:
        return json.load(f)


def root():
    return os.path.expanduser(cfg().get("root", "~/public"))


# ---------------------------------------------------------------- the gate

def denylist(repo=None):
    """Every string that must never reach a public repo, assembled from the live box.

    Some rules are global (paths, credentials, the client's identity) and some are
    scoped: a dollar figure means an account balance in the investing repo and a bet size
    in the poker ones, so the rule that guards the first must not gag the second. A rule
    is skipped only where it is *meaningless*, never where it is merely inconvenient.
    """
    pats = []

    def add(p, why, except_repos=()):
        if repo and repo in except_repos:
            return
        pats.append((re.compile(p, re.I), why))
    add(r"/home/user", "absolute home path")
    add(r"\bhellopie\b", "sudo password")
    add(r"<host>|<tailnet>", "tailnet identity")
    add(r"userstudent@gmail\.com", "personal email")
    add(r"\bdd-[a-z-]+-spark-[0-9a-f]{6}\b", "ntfy topic pattern")
    try:
        ch = json.load(open(os.path.join(MC, "config/ntfy.json"))).get("channels", {})
        for t in set(ch.values()):
            add(re.escape(t), "live ntfy topic")
    except Exception:
        pass
    for word, spec in cfg().get("deny", {}).items():
        why = spec if isinstance(spec, str) else spec.get("why", "")
        exc = () if isinstance(spec, str) else tuple(spec.get("except_repos", []))
        add(re.escape(word) if word.isalnum() else word, why, exc)
    # tickers actually researched or held — naming them exposes the book
    try:
        held = sorted({d.split("-")[-1] for d in os.listdir(os.path.join(HOME, "Stocks"))
                       if "-" in d and d.split("-")[-1].isupper()
                       and os.path.isdir(os.path.join(HOME, "Stocks", d))})
        if held:
            # Research subjects are public companies; naming one says what was studied, not
            # what is owned. Allowed where the repo is about the research itself (David,
            # 2026-08-18) and still blocked everywhere else, where a ticker has no business
            # appearing at all and its presence is a signal something leaked.
            add(r"\b(" + "|".join(held) + r")\b", "researched ticker",
                tuple(cfg().get("ticker_ok_repos", [])))
    except Exception:
        pass
    return pats


def scan_text(text, pats, where=""):
    hits = []
    for i, line in enumerate(text.splitlines(), 1):
        for rx, why in pats:
            m = rx.search(line)
            if m:
                hits.append({"where": where, "line": i, "why": why, "match": m.group(0)[:40],
                             "text": line.strip()[:110]})
    return hits


def scan(repo=None):
    hits = []
    for name, spec in cfg()["projects"].items():
        if repo and spec["repo"] != repo and name != repo:
            continue
        pats = denylist(spec["repo"])
        d = os.path.join(root(), spec["repo"])
        if not os.path.isdir(d):
            continue
        for base, dirs, files in os.walk(d):
            dirs[:] = [x for x in dirs if x != ".git"]
            for fn in files:
                p = os.path.join(base, fn)
                try:
                    hits += scan_text(open(p, errors="replace").read(), pats,
                                      os.path.relpath(p, root()))
                except Exception:
                    pass
    return hits


# ---------------------------------------------------------------- pseudonyms

def pseudonyms():
    return {k: v for k, v in cfg().get("pseudonyms", {}).get("map", {}).items()
            if not k.startswith("_")}


def _match_case(src, repl):
    """Keep the shape the original had: AGGREGATORA -> AGGREGATORA, aggregatora -> aggregatora."""
    if src.isupper() and len(src) > 1:
        return repl.upper()
    if src.islower():
        return repl.lower()
    return repl


def pseudonymize(text):
    """Replace personal identifiers with stable stand-ins, longest key first so a compound
    name is masked before its own substring is.

    Case-insensitive and NOT word-bounded, because in code these names live inside
    identifiers: `aggregatora_client_id`, `brokerb-trading`, `jpm_pos`. A word-bounded
    pass renamed the class and left the module import beside it, which is worse than not
    masking at all — it looks deliberate and is half done.

    This is the only transformation the publisher makes, and it is why the scanner runs
    AFTER it: a variant this map misses still trips the denylist and quarantines the file,
    so the gate does not depend on this function being complete.
    """
    n = 0
    for k in sorted(pseudonyms(), key=len, reverse=True):
        repl = pseudonyms()[k]
        # Short or all-digit keys get word boundaries: masking "ERP-A" or an account suffix by
        # bare substring would corrupt any longer token that happens to contain them. Longer
        # names stay unbounded, which is what lets `aggregatora_client_id` be masked whole.
        body = re.escape(k)
        if k.isalnum() and (len(k) <= 4 or k.isdigit()):
            body = r"\b" + body + r"\b"
        pat = re.compile(body, re.I if k[0].isalpha() else 0)
        text, hits = pat.subn(lambda m: _match_case(m.group(0), repl), text)
        n += hits
    return text, n


# ---------------------------------------------------------------- build

def build(repo=None):
    """Render each project's authored content. Templates live in config/public_repos.json;
    allowlisted code files are copied verbatim (and then scanned like everything else)."""
    made = []
    for name, spec in cfg()["projects"].items():
        if repo and spec["repo"] != repo and name != repo:
            continue
        d = os.path.join(root(), spec["repo"])
        os.makedirs(d, exist_ok=True)
        src = os.path.join(MC, "public", spec["repo"])
        if os.path.isdir(src):
            for base, _, files in os.walk(src):
                for fn in files:
                    rel = os.path.relpath(os.path.join(base, fn), src)
                    out = os.path.join(d, rel)
                    os.makedirs(os.path.dirname(out), exist_ok=True)
                    if fn.endswith((".md", ".svg", ".d2", ".mmd")):
                        text = open(os.path.join(base, fn), errors="replace").read()
                        text = text.replace("{{updated}}", f"{datetime.now(timezone.utc):%B %Y}")
                        open(out, "w").write(text)
                    else:
                        shutil.copy2(os.path.join(base, fn), out)
        # prune first: a file dropped from the allowlist must LEAVE the public repo.
        # Without this, un-publishing something is silent and ineffective — the copy just
        # stops being refreshed while staying online, which is the worst of both.
        keep = {os.path.join(d, (e if isinstance(e, str) else e.get("to"))
                             or os.path.join("code", os.path.basename(e["from"])))
                for e in spec.get("include_code", [])}
        code_dir = os.path.join(d, "code")
        for base, _, files in os.walk(code_dir) if os.path.isdir(code_dir) else []:
            for fn in files:
                fp = os.path.join(base, fn)
                if fn != "README.md" and fp not in keep:
                    os.unlink(fp)
                    print(f"  pruned (no longer allowlisted): {os.path.relpath(fp, root())}")

        for entry in spec.get("include_code", []):
            rel, dest = (entry, None) if isinstance(entry, str) else (entry["from"], entry.get("to"))
            src_f = os.path.join(HOME, name, rel)
            if not os.path.exists(src_f):
                print(f"  ! missing allowlisted file: {rel}")
                continue
            out = os.path.join(d, dest or os.path.join("code", os.path.basename(rel)))
            os.makedirs(os.path.dirname(out), exist_ok=True)
            text, masked = pseudonymize(open(src_f, errors="replace").read())
            open(out, "w").write(text)
            if masked and out.endswith(".py"):
                # a substitution must never ship code that no longer parses
                try:
                    ast.parse(text)
                except SyntaxError as e:
                    os.unlink(out)
                    print(f"  ! pseudonymising broke {rel} ({e.msg} line {e.lineno}) — not published")
        made.append(spec["repo"])
    return made


# ---------------------------------------------------------------- publish

def _git(d, *args, check=True):
    r = subprocess.run(["git", "-C", d] + list(args), capture_output=True, text=True, timeout=120)
    if check and r.returncode != 0 and "nothing to commit" not in r.stdout:
        raise RuntimeError(f"git {' '.join(args)}: {r.stderr.strip()[:160]}")
    return r.stdout.strip()


def publish(repo=None, dry=False):
    build(repo)
    hits = scan(repo)
    if hits:
        print(f"REFUSED — {len(hits)} leak(s):")
        for h in hits[:20]:
            print(f"  {h['where']}:{h['line']}  [{h['why']}] {h['match']}")
            print(f"     {h['text']}")
        return 1
    print("scan clean")
    for name, spec in cfg()["projects"].items():
        if repo and spec["repo"] != repo and name != repo:
            continue
        if not spec.get("publish"):
            print(f"  held back: {spec['repo']} — {spec.get('hold_reason', 'publish=false')}")
            continue
        d = os.path.join(root(), spec["repo"])
        if not os.path.isdir(os.path.join(d, ".git")):
            _git(d, "init", "-q", "-b", "main")
        _git(d, "add", "-A")
        if _git(d, "status", "--porcelain"):
            _git(d, "-c", "user.name=David Ding", "-c", "user.email=userdev@users.noreply.github.com",
                 "commit", "-q", "-m", spec.get("commit_msg", "Update public overview"))
        if dry:
            print(f"  dry: would publish {spec['repo']}")
            continue
        exists = subprocess.run(["gh", "repo", "view", f"userdev/{spec['repo']}"],
                                capture_output=True, text=True).returncode == 0
        if not exists:
            subprocess.run(["gh", "repo", "create", spec["repo"], "--public",
                            "-d", spec.get("description", "")[:350], "--source", d,
                            "--remote", "origin", "--push"], check=True, timeout=180)
            print(f"  created + pushed {spec['repo']}")
        else:
            if "origin" not in _git(d, "remote"):
                _git(d, "remote", "add", "origin",
                     f"https://github.com/userdev/{spec['repo']}.git")
            _git(d, "push", "-q", "-u", "origin", "main")
            print(f"  pushed {spec['repo']}")
    return 0


def refresh(dry=False):
    """Re-sync the allowlisted code, then publish what is still clean.

    This is the answer to "the projects change every day and the public repos will rot."
    It runs after the back-office pass and needs no input: the allowlist is the contract,
    verbatim copying is the mechanism, and the scanner is the gate. The one thing it will
    never do is publish something that newly fails the scan — that file is quarantined and
    reported, because the failure mode to design against is a private detail arriving in a
    file that was safe when somebody last looked at it.
    """
    build()
    quarantined, changed = [], []
    for name, spec in cfg()["projects"].items():
        if not spec.get("publish"):
            continue
        repo, d = spec["repo"], os.path.join(root(), spec["repo"])
        hits = [h for h in scan(repo)]
        if hits:
            for h in hits:                      # remove the offending file, keep the repo
                f = os.path.join(root(), h["where"])
                if os.path.exists(f) and "/code/" in f:
                    os.unlink(f)
                    quarantined.append({"file": h["where"], "why": h["why"], "match": h["match"]})
            if [h for h in scan(repo)]:
                print(f"  {repo}: still leaking after quarantine — NOT published")
                continue
        if os.path.isdir(os.path.join(d, ".git")) and not _git(d, "status", "--porcelain"):
            continue                            # nothing moved in this project today
        changed.append(repo)
        if not dry:
            publish(repo)
    if quarantined:
        print(f"  QUARANTINED {len(quarantined)} file(s) — a published file started leaking:")
        for q in quarantined:
            print(f"    {q['file']}  [{q['why']}] {q['match']}")
    print(f"refresh: {len(changed)} repo(s) updated, {len(quarantined)} quarantined")
    out = {"at": int(time.time()), "updated": changed, "quarantined": quarantined}
    try:
        with open(os.path.join(MC, "state/publish_refresh.json"), "w") as fh:
            json.dump(out, fh, indent=1)
    except Exception:
        pass
    return out


def candidates(limit=40):
    """Private-repo files that WOULD scan clean and are not published yet.

    Deliberately a proposal, not an action: passing the scanner means a file carries no
    known secret, which is not the same as it being worth showing, and that judgement is
    not a robot's to make.
    """
    out = []
    for name, spec in cfg()["projects"].items():
        if not spec.get("publish"):
            continue
        already = {(e if isinstance(e, str) else e["from"]) for e in spec.get("include_code", [])}
        pats = denylist(spec["repo"])
        srcroot = os.path.join(HOME, name)
        for base, dirs, files in os.walk(srcroot):
            dirs[:] = [x for x in dirs if x not in
                       (".git", "node_modules", ".venv", "__pycache__", "worktrees", "data",
                        "journal", "config", "logs", "snapshots", "assets", "_archive", "backups")]
            for fn in files:
                if not fn.endswith((".py", ".js", ".ts", ".sh")):
                    continue
                rel = os.path.relpath(os.path.join(base, fn), srcroot)
                if rel in already:
                    continue
                try:
                    text = open(os.path.join(base, fn), errors="replace").read()
                except OSError:
                    continue
                lines = len(text.splitlines())
                if lines < 40 or scan_text(text, pats, rel):
                    continue
                out.append({"project": name, "repo": spec["repo"], "file": rel, "lines": lines})
    out.sort(key=lambda x: -x["lines"])
    return out[:limit]


def status():
    c = cfg()
    print(f"{'project':<16} {'public repo':<28} {'state':<12} files")
    for name, spec in c["projects"].items():
        d = os.path.join(root(), spec["repo"])
        n = 0
        if os.path.isdir(d):
            for base, dirs, files in os.walk(d):
                dirs[:] = [x for x in dirs if x != ".git"]   # count content, not git internals
                n += len(files)
        live = subprocess.run(["gh", "repo", "view", f"userdev/{spec['repo']}"],
                              capture_output=True).returncode == 0
        state = "live" if live else ("built" if os.path.isdir(d) else "—")
        if not spec.get("publish"):
            state = "HELD"
        print(f"{name:<16} {spec['repo']:<28} {state:<12} {n}")
        if not spec.get("publish"):
            print(f"                 └─ {spec.get('hold_reason', '')}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    arg = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else None
    if cmd == "scan":
        h = scan(arg)
        for x in h:
            print(f"{x['where']}:{x['line']}  [{x['why']}] {x['match']}\n   {x['text']}")
        print(f"{len(h)} leak(s)" if h else "scan clean")
        sys.exit(1 if h else 0)
    elif cmd == "build":
        print("built:", ", ".join(build(arg)))
    elif cmd == "publish":
        sys.exit(publish(arg, dry="--dry" in sys.argv))
    elif cmd == "refresh":
        refresh(dry="--dry" in sys.argv)
    elif cmd == "candidates":
        c = candidates()
        for x in c:
            print(f"  {x['lines']:>5} lines  {x['repo']:<24} {x['project']}/{x['file']}")
        print(f"{len(c)} publishable file(s) not yet included")
    else:
        status()
