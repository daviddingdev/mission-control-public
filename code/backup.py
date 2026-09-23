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
import glob
import json
import fnmatch
import os
import re
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


# Names that mean "this is a live secret". Matched against the archive listing, not the
# filesystem, so it costs nothing on a 734MB tree.
SECRETISH = re.compile(
    r"(^|/)\.?(credentials?|secrets?)(\.json|\.yaml|\.yml|/|$)"
    r"|(^|/)(headless-token|\.credentials\.json|id_rsa|id_ed25519)(/|$)"
    r"|\.pem$|\.p12$|\.pfx$|(^|/)\.env$|(^|/)\.netrc$|(^|/)\.pgpass$",
    re.I)


def write_one(name, spec):
    """tar the declared paths, verify the archive, then prune. Returns (ok, message)."""
    # `src` lets a source live somewhere other than ~/<name>. Needed for the Claude layer:
    # naming the source `.claude` would produce `.claude_<date>.tar.gz`, a HIDDEN file, and
    # newest() skips dotfiles — so every archive would be written and then invisible, and
    # the freshness check would fail forever against backups that were in fact being made.
    src = os.path.expanduser(spec.get("src") or os.path.join(HOME, name))
    paths = sorted({
        os.path.relpath(m, src)
        for p in spec.get("paths", [])
        for m in glob.glob(os.path.join(src, p))
    })
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
    # A credential must never enter a backup. `claude-layer` is declared as a narrow include
    # list precisely because ~/.claude also holds .credentials.json and headless-token — but
    # a narrow include list is only safe until someone widens it, and the widening would look
    # harmless in review. So the archive LISTING (already computed for the readability check
    # above, so this is nearly free) is scanned, and a hit discards the archive rather than
    # shipping it to ~/backups and, later, offsite.
    # A source MAY deliberately archive a secret — clientco-db's whole backup is its .env,
    # because the ERP data itself is in git and the credential is the only unrecoverable
    # piece. It opts in BY NAME (`secrets_ok: [".env"]`), not with a blanket boolean, so
    # widening the paths later still trips on anything that was not named.
    allowed = spec.get("secrets_ok") or []
    leaked = [ln for ln in v.stdout.splitlines()
              if SECRETISH.search(ln)
              and not any(fnmatch.fnmatch(ln.rstrip("/"), a) or ln.rstrip("/") == a
                          for a in allowed)]
    if leaked:
        os.unlink(out)
        return False, (f"{name}: REFUSED — archive contained {len(leaked)} credential-ish "
                       f"path(s), e.g. {leaked[0][:60]}. Narrow the `paths` in "
                       f"config/backups.json; do not add an exclude and hope.")
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


def selftest():
    """Prove the credential refusal actually fires.

    The `claude-layer` source is declared as a narrow include list because ~/.claude also
    holds .credentials.json and headless-token. A narrow list is only safe until somebody
    widens it, and the widening looks harmless in review — so write_one() scans the archive
    listing and discards anything credential-shaped. A guard nobody has watched refuse is a
    guard nobody should trust (box rule: `guardrail-inert`), hence these fixtures.
    """
    import shutil
    import tempfile
    tmp = tempfile.mkdtemp(prefix="backup-selftest-")
    src_dir = os.path.join(tmp, "fakeproj")
    os.makedirs(os.path.join(src_dir, "state"))
    open(os.path.join(src_dir, "state", "data.json"), "w").write("{}")
    open(os.path.join(src_dir, ".credentials.json"), "w").write('{"oauth": "secret"}')
    open(os.path.join(src_dir, "id_ed25519"), "w").write("KEY")
    global root
    _real_root, root = root, (lambda: os.path.join(tmp, "dest"))
    cases = [
        ({"paths": ["state"]}, True, "ordinary data archives fine"),
        ({"paths": ["state", ".credentials.json"]}, False,
         "an UNNAMED credential is refused"),
        ({"paths": ["state", ".credentials.json"], "secrets_ok": [".credentials.json"]},
         True, "…and allowed when opted in BY NAME"),
        ({"paths": ["state", ".credentials.json", "id_ed25519"],
          "secrets_ok": [".credentials.json"]}, False,
         "a NEW secret beside an opted-in one still trips"),
    ]
    bad = 0
    for spec, want, label in cases:
        ok, msg = write_one("fakeproj", {**spec, "src": src_dir})
        good = ok is want
        bad += not good
        print(f"  {'ok  ' if good else 'FAIL'} {label:<48} -> {'archived' if ok else 'refused'}")
    root = _real_root
    shutil.rmtree(tmp, ignore_errors=True)
    print("ALL PASS" if not bad else f"SELFTEST FAILED ({bad})")
    return 1 if bad else 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "selftest":
        sys.exit(selftest())
    if cmd == "run":
        sys.exit(run(sys.argv[2] if len(sys.argv) > 2 else None))
    elif cmd == "check":
        sys.exit(1 if check() else 0)
    elif cmd == "list":
        _list()
    else:
        sys.exit("usage: backup.py run [project] | check | list | selftest")
