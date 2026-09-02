#!/usr/bin/env python3
"""Nightly backups for everything gitignored-and-unrecoverable. Zero Claude tokens.

One declaration (config/backups.json), three consumers: this writer, healthcheck.sh's
freshness assertion, and the back-office audit's coverage rule. Before this existed the
only backup on the box was Stocks' own script, so the poker app's live database — every
hand David has ever logged, gitignored by design — sat as a single file on a single disk.

What earns a backup: gitignored AND unrecoverable. If git has it, GitHub is already the
backup; if a build produces it, the build is the backup. A project with nothing that
qualifies goes in `exempt` with the reason, which is also what stops the audit asking again.

    backup.py run [project]     write the snapshots (cron: 03:35 daily)
    backup.py check             freshness only — exit 1 if anything is stale
    backup.py list              what exists on disk today

Every archive is verified by reading it back before the old ones are pruned: a backup you
have never restored is a hypothesis, and `tar tzf` is the cheapest possible test of it.
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
MC = os.path.join(HOME, "maintenance")
CFG = os.path.join(MC, "config/backups.json")


def cfg():
    with open(CFG) as f:
        return json.load(f)


def root():
    return os.path.expanduser(cfg().get("root", "~/backups"))


def dest_dir(name):
    return os.path.join(root(), name.lower())


def newest(name):
    d = dest_dir(name)
    if not os.path.isdir(d):
        return None
    files = [os.path.join(d, f) for f in os.listdir(d) if not f.startswith(".")]
    files = [f for f in files if os.path.isfile(f)]
    return max(files, key=os.path.getmtime) if files else None


def age_h(path):
    return round((time.time() - os.path.getmtime(path)) / 3600, 1) if path else None


def write_one(name, spec):
    """tar the declared paths, verify the archive, then prune. Returns (ok, message)."""
    src = os.path.join(HOME, name)
    paths = [p for p in spec.get("paths", []) if os.path.exists(os.path.join(src, p))]
    if not paths:
        return False, f"{name}: none of the declared paths exist"
    d = dest_dir(name)
    os.makedirs(d, exist_ok=True)
    out = os.path.join(d, f"{name.lower()}_{datetime.now(timezone.utc):%Y-%m-%d}.tar.gz")
    r = subprocess.run(["tar", "czf", out, "-C", src] + paths,
                       capture_output=True, text=True, timeout=1800)
    if r.returncode != 0 or not os.path.exists(out):
        return False, f"{name}: tar failed — {r.stderr.strip()[:120]}"
    # verify before pruning: an unreadable archive must not be allowed to age out a good one
    v = subprocess.run(["tar", "tzf", out], capture_output=True, text=True, timeout=1800)
    if v.returncode != 0:
        os.unlink(out)
        return False, f"{name}: archive unreadable, discarded — {v.stderr.strip()[:120]}"
    entries = len(v.stdout.splitlines())
    keep = int(spec.get("keep_days", 30))
    olds = sorted((os.path.join(d, f) for f in os.listdir(d) if f.endswith(".tar.gz")),
                  key=os.path.getmtime, reverse=True)
    for f in olds[keep:]:
        os.unlink(f)
    mb = os.path.getsize(out) / 1e6
    return True, f"{name}: {mb:.1f} MB, {entries} entries -> {os.path.basename(out)}"


def run(only=None):
    c = cfg()
    ok, fails = [], []
    for name, spec in c["sources"].items():
        if only and name != only:
            continue
        if spec.get("delegated_to"):
            continue                      # that project writes its own; we only assert freshness
        good, msg = write_one(name, spec)
        (ok if good else fails).append(msg)
        print(("  ok   " if good else "  FAIL ") + msg)
    stale = check(quiet=True)
    if fails or stale:
        body = "; ".join(fails + [f"{n} stale ({a}h)" for n, a in stale])
        subprocess.run([os.path.join(MC, "bin/notify.sh"), "alerts", "Backup problem", body],
                       capture_output=True, timeout=30)
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} backup: {len(ok)} ok, "
          f"{len(fails)} failed, {len(stale)} stale")
    return 1 if (fails or stale) else 0


def check(quiet=False):
    """Every declared source must have a recent archive — including the delegated ones."""
    stale = []
    for name, spec in cfg()["sources"].items():
        n = newest(name)
        a = age_h(n)
        limit = spec.get("max_gap_h", 30)
        if n is None or a > limit:
            stale.append((name, a if a is not None else -1))
            if not quiet:
                print(f"  STALE {name}: " + (f"{a}h old (limit {limit}h)" if n else "no backup at all"))
        elif not quiet:
            print(f"  ok    {name}: {a}h old, {os.path.getsize(n) / 1e6:.1f} MB")
    return stale


def _list():
    c = cfg()
    for name in list(c["sources"]) + list(c.get("exempt", {})):
        n = newest(name)
        if name in c.get("exempt", {}):
            print(f"  {name:<16} exempt — {c['exempt'][name][:70]}")
        elif n:
            print(f"  {name:<16} {age_h(n):>5.1f}h  {os.path.getsize(n) / 1e6:>8.1f} MB  "
                  f"{os.path.basename(n)}")
        else:
            print(f"  {name:<16} —      no backup yet")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        sys.exit(run(sys.argv[2] if len(sys.argv) > 2 else None))
    elif cmd == "check":
        sys.exit(1 if check() else 0)
    elif cmd == "list":
        _list()
    else:
        sys.exit("usage: backup.py run [project] | check | list")
