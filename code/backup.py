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
    backup.py paths [source]    read-only: what each source WOULD archive, sizes, and the
                                declared globs matching nothing (exit 1 if any)
    backup.py check             freshness only — exit 1 if anything is stale
    backup.py list              what exists on disk today
    backup.py verify [source]   read-only: re-hash every archive that has a recorded sum and
                                compare; exit 1 on any mismatch. Archives with no record
                                (written before sums existed) are listed, never hashed, and
                                do not fail. Delegated sources (Stocks) are skipped.
    backup.py drill [source] [--dry]
                                restore drill: extract the newest archive of each source into
                                a 0700 temp dir under /tmp (always removed), then check it (see
                                drill() below); one row per source to state/restore_drills.jsonl
                                and reports/restore-drill-<YYYY-MM>.md. --dry (= --dry-run) lists
                                what it would check and extracts and writes nothing. Exit 1 when
                                any source fails. Stocks (delegated) is never drilled.
    backup.py selftest          fixtures for the credential refusal, the sums, verify, the drill

Every archive is verified by reading it back before the old ones are pruned: a backup you
have never restored is a hypothesis, and `tar tzf` is the cheapest possible test of it.

Checksums at write time (memo wa-backups-integrity, 2026-10-03). Once an archive passes the
readback and the credential scan, write_one() records:
  <archive>.sha256          `sha256sum` format, so `sha256sum -c` works from that folder
  <archive>.manifest.json   path, size, sha256 of every regular file, computed by streaming
                            the ARCHIVE (never the live tree, which may have moved on)
  state/backup_sums.jsonl   one row {ts, source, archive, bytes, sha256, members,
                            manifest_sha256}; it lives in ~/maintenance, outside ~/backups, so
                            one `rm -rf ~/backups` (or a rewritten sidecar) cannot take the
                            evidence with it. `archive` is relative to the backups root.
Only `*.tar.gz` counts as an archive: freshness, `list` and keep-count pruning never see the
sidecars, and pruning removes exactly the two sibling names with the archive it prunes.
"""
import glob
import hashlib
import json
import fnmatch
import os
import random
import re
import shutil
import sqlite3
import stat
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
MC = os.path.join(HOME, "maintenance")
CFG = os.path.join(MC, "config/backups.json")
# Outside ~/backups on purpose. Module-level so the selftest can point it at a temp file.
SUMS = os.path.join(MC, "state/backup_sums.jsonl")
SIDECARS = (".sha256", ".manifest.json")
# The restore drill (memo wa-restore-and-rebuild ask 1). Module-level so the selftest can
# point every write, and the extraction parent, into its own temp dir.
DRILLS = os.path.join(MC, "state/restore_drills.jsonl")
DRILL_REPORTS = os.path.join(MC, "reports")
DRILL_TMPDIR = "/tmp"
DRILL_SAMPLE = 200                     # live-vs-archive sha256 comparisons per source, at most
DRILL_MAX_FILE = 200 * 1024 * 1024     # never hash a file bigger than this in the comparison
DRILL_DROP = 0.30                      # fail when the newest archive is >30% under the median
DRILL_PREV = 7                         # ... of at most this many previous archives
DELEGATED_NOTE = "delegated, not drilled (Stocks frozen)"


def cfg():
    with open(CFG) as f:
        return json.load(f)


def root():
    return os.path.expanduser(cfg().get("root", "~/backups"))


def dest_dir(name):
    return os.path.join(root(), name.lower())


def is_archive(fn):
    """An archive is a non-hidden `*.tar.gz`. Sidecars (`.sha256`, `.manifest.json`), the
    hand-parked config copies in maintenance/ and atomic-write temp files are not."""
    return fn.endswith(".tar.gz") and not fn.startswith(".")


def archives(d):
    """Full paths of the archives in folder d (none if it does not exist)."""
    if not os.path.isdir(d):
        return []
    return [os.path.join(d, f) for f in os.listdir(d)
            if is_archive(f) and os.path.isfile(os.path.join(d, f))]


def newest(name):
    files = archives(dest_dir(name))
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


def expand(name, spec):
    """(src, the relative paths tar would archive, the declared globs that match nothing).

    One expansion shared by write_one() and `paths`, so "is it backed up?" is answered by
    the same code that does the backing up (data-desk memo, 2026-10-03)."""
    # `src` lets a source live somewhere other than ~/<name>. Needed for the Claude layer:
    # naming the source `.claude` would produce `.claude_<date>.tar.gz`, a HIDDEN file, and
    # newest() skips dotfiles — so every archive would be written and then invisible, and
    # the freshness check would fail forever against backups that were in fact being made.
    src = os.path.expanduser(spec.get("src") or os.path.join(HOME, name))
    found, missing = set(), []
    for p in spec.get("paths", []):
        hits = glob.glob(os.path.join(src, p))
        if not hits:
            missing.append(p)
        found.update(os.path.relpath(m, src) for m in hits)
    return src, sorted(found), missing


def _du(path):
    if os.path.isfile(path) or os.path.islink(path):
        return os.path.getsize(path) if os.path.isfile(path) else 0
    total = 0
    for dp, _dn, fns in os.walk(path):
        for fn in fns:
            fp = os.path.join(dp, fn)
            try:
                if os.path.isfile(fp) and not os.path.islink(fp):
                    total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def paths_report(only=None):
    """Read-only: what each source WOULD archive, with sizes, and the globs matching nothing.

    Exit 1 when a declared path matches nothing (write_one() skips it silently unless ALL
    are missing), 2 on an unknown source. Writes nothing."""
    c = cfg()
    if only and only not in c["sources"]:
        print(f"backup.py paths: unknown source {only!r} "
              f"(known: {', '.join(sorted(c['sources']))})", file=sys.stderr)
        return 2
    gaps = 0
    for name, spec in c["sources"].items():
        if only and name != only:
            continue
        if spec.get("delegated_to"):
            print(f"{name}: delegated to {spec['delegated_to']} (not expanded here)")
            continue
        src, paths, missing = expand(name, spec)
        sizes = [(p, _du(os.path.join(src, p))) for p in paths]
        tot = sum(b for _p, b in sizes)
        print(f"{name}: would tar {len(paths)} path(s), {tot / 1e6:.1f} MB from {src}")
        for p, b in sizes:
            print(f"   {b / 1e6:>9.1f} MB  {p}")
        for m in missing:
            print(f"   MISSING      {m}  (declared, matches nothing)")
        gaps += len(missing)
    return 1 if gaps else 0


_CHUNK = 1 << 20


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(_CHUNK), b""):
            h.update(b)
    return h.hexdigest()


class _HashReader:
    """A read-only file wrapper that hashes every byte it hands out, so the archive's own
    sha256 and its member manifest come from ONE pass over the file."""

    def __init__(self, f):
        self.f, self.h = f, hashlib.sha256()

    def read(self, n=-1):
        b = self.f.read(n)
        self.h.update(b)
        return b


def scan_archive(path):
    """(archive sha256, [{path, size, sha256} per regular file]) by streaming the tar.

    Reads the archive, never the live tree: the manifest has to describe what a restore will
    actually get back, and the source may have changed since tar ran."""
    members = []
    with open(path, "rb") as raw:
        hr = _HashReader(raw)
        with tarfile.open(fileobj=hr, mode="r|gz") as tf:
            for m in tf:
                if not m.isfile():
                    continue
                fh, h, size = tf.extractfile(m), hashlib.sha256(), 0
                for b in iter(lambda: fh.read(_CHUNK), b""):
                    h.update(b)
                    size += len(b)
                members.append({"path": m.name, "size": size, "sha256": h.hexdigest()})
        while hr.read(_CHUNK):            # tar stops at its end blocks; hash the tail too
            pass
    return hr.h.hexdigest(), members


def _write_atomic(path, text):
    d, base = os.path.split(path)
    tmp = os.path.join(d, f".{base}.tmp")   # dot-prefixed: never mistaken for anything
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def record_sums(name, out):
    """Write the two sidecars and append the sums row. Returns (sha256, members)."""
    sha, members = scan_archive(out)
    base = os.path.basename(out)
    _write_atomic(out + ".sha256", f"{sha}  {base}\n")
    man = json.dumps({"archive": base, "sha256": sha, "members": members}, indent=1) + "\n"
    _write_atomic(out + ".manifest.json", man)
    row = {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "source": name,
           "archive": os.path.relpath(out, root()), "bytes": os.path.getsize(out),
           "sha256": sha, "members": len(members),
           "manifest_sha256": hashlib.sha256(man.encode()).hexdigest()}
    os.makedirs(os.path.dirname(SUMS), exist_ok=True)
    with open(SUMS, "a") as f:
        f.write(json.dumps(row) + "\n")
    return sha, members


def unlink_with_sidecars(path):
    """Remove an archive and exactly its two sibling sidecars, nothing else."""
    for p in [path] + [path + s for s in SIDECARS]:
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass


def write_one(name, spec):
    """tar the declared paths, verify the archive, then prune. Returns (ok, message)."""
    src, paths, _missing = expand(name, spec)
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
        unlink_with_sidecars(out)         # any sidecars there describe a same-day archive tar overwrote
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
        unlink_with_sidecars(out)
        return False, (f"{name}: REFUSED — archive contained {len(leaked)} credential-ish "
                       f"path(s), e.g. {leaked[0][:60]}. Narrow the `paths` in "
                       f"config/backups.json; do not add an exclude and hope.")
    # Record the sums before pruning: an archive nobody can prove unchanged must not be the
    # reason an older, recorded one ages out. The archive itself is good, so it is kept.
    try:
        sha, _members = record_sums(name, out)
    except (OSError, tarfile.TarError, ValueError) as e:
        return False, (f"{name}: archive kept but checksum NOT recorded, nothing pruned — "
                       f"{type(e).__name__}: {str(e)[:100]}")
    keep = int(spec.get("keep_days", 30))
    olds = sorted(archives(d), key=os.path.getmtime, reverse=True)   # archives only, never sidecars
    for f in olds[keep:]:
        unlink_with_sidecars(f)
    mb = os.path.getsize(out) / 1e6
    return True, (f"{name}: {mb:.1f} MB, {entries} entries -> {os.path.basename(out)} "
                  f"sha256 {sha[:12]}")


def _manifest():
    """The nightly rebuild manifest (bin/manifest.py, memo wa-restore-and-rebuild ask 3), on a
    full run only. Its outcome is printed in this run's output and NEVER changes the backups'
    exit code or alert: a broken manifest must not page as a backup failure. Staleness is the
    back office's `manifest-stale` rule's to catch."""
    try:
        sys.path.insert(0, os.path.join(MC, "bin"))
        import manifest
        rc = manifest.write(log=lambda s: print("       " + s))
        print(("  ok   " if rc == 0 else "  WARN ") + f"manifest: exit {rc}")
    except Exception as e:                       # never let the record fail the backups
        print(f"  WARN manifest: {type(e).__name__}: {str(e)[:160]}")


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
    if not only:
        _manifest()
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


def _ledger():
    """The last sums row per archive (relative to the root). A bad line is skipped, not fatal."""
    recs = {}
    try:
        with open(SUMS) as f:
            for ln in f:
                try:
                    r = json.loads(ln)
                    recs[r["archive"]] = r
                except (ValueError, KeyError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    return recs


def _sidecar_sum(path):
    try:
        with open(path + ".sha256") as f:
            tok = f.read().split()
        return tok[0].lower() if tok else ""
    except FileNotFoundError:
        return None


def _records(a, ledger):
    rel = os.path.relpath(a, root())
    row, side = ledger.get(rel), _sidecar_sum(a)
    recs = {k: v for k, v in (("sidecar", side), ("ledger", row and row.get("sha256")))
            if v is not None}
    return row, recs


def record_check(a, ledger, hash_it=True):
    """(state, actual sha256 or None, bad records, gone records) for one archive, shared by
    verify and the drill. state: ok | mismatch | missing-record | unrecorded, or `recorded`
    when hash_it is False (the drill's --dry: it says a record exists and hashes nothing)."""
    row, recs = _records(a, ledger)
    if not recs:
        return "unrecorded", None, [], []
    if not hash_it:
        return "recorded", None, [], []
    actual = sha256_file(a)
    bad = [k for k, v in recs.items() if v != actual]
    gone = [k for k in ("sidecar", "ledger") if k not in recs]
    man = a + ".manifest.json"
    if not os.path.exists(man):
        gone.append("manifest")
    elif row and row.get("manifest_sha256") and sha256_file(man) != row["manifest_sha256"]:
        bad.append("manifest")
    return ("mismatch" if bad else "missing-record" if gone else "ok"), actual, bad, gone


def verify(only=None, sources=None):
    """Read-only: re-hash each archive that has a recorded sum and compare.

    A record is the `.sha256` sidecar, the backup_sums.jsonl row, or both; an archive must
    match every record it has (a sidecar rewritten to match a changed archive still loses to
    the ledger outside ~/backups). An archive with NO record predates the sums: it is listed
    and never hashed, so a live run over ~25 GB of older archives costs nothing. Exit 1 on any
    mismatch, else 0. `missing-record` (one of the two records gone) is reported, not failed:
    the drill or backoffice rule decides what that means."""
    if sources is None:
        c = cfg()["sources"]
        if only and only not in c:
            print(f"backup.py verify: unknown source {only!r} "
                  f"(known: {', '.join(sorted(c))})", file=sys.stderr)
            return 2
        sources = c
    ledger = _ledger()
    n_ok = n_bad = n_miss = n_unrec = 0
    for name, spec in sources.items():
        if only and name != only:
            continue
        if spec.get("delegated_to"):
            print(f"{name}: delegated to {spec['delegated_to']} (no sums recorded here)")
            continue
        found = sorted(archives(dest_dir(name)))
        unrec = []
        for a in found:
            rel = os.path.relpath(a, root())
            state, actual, bad, gone = record_check(a, ledger)
            if state == "unrecorded":
                unrec.append(os.path.basename(a))
                continue
            if bad:
                n_bad += 1
                print(f"  MISMATCH {rel}: sha256 {actual[:12]} differs from the "
                      f"{' + '.join(bad)} record")
            elif gone:
                n_miss += 1
                print(f"  missing-record {rel}: hash ok, no {' / '.join(gone)}")
            else:
                n_ok += 1
                print(f"  ok       {rel}")
        n_unrec += len(unrec)
        if unrec:
            print(f"  unrecorded (predates sums) {name}: {len(unrec)} archive(s), "
                  f"{unrec[0]} .. {unrec[-1]}")
        if not found:
            print(f"  {name}: no archives")
    print(f"verify: {n_ok} ok, {n_bad} mismatched, {n_miss} missing-record, "
          f"{n_unrec} unrecorded (predates sums)")
    return 1 if n_bad else 0


# ---- the restore drill (memo wa-restore-and-rebuild ask 1, 2026-10-04) ----------------------
# Before this the only restore on record was poker on 2026-08-18, and the nightly check was
# `tar tzf`: proof the archive lists, not that it restores. The drill restores for real, into
# a throwaway 0700 dir, and checks what came back against what was declared and what is live.

def _member_name(m):
    n = m.name
    while n.startswith("./"):
        n = n[2:]
    return n.rstrip("/")


def unsafe_member(m):
    """Why tar member m must never be extracted, or None. Belt and braces with the `data`
    filter: an absolute path, a `..` component, a link leaving the archive, a device or fifo."""
    n = m.name
    if n.startswith("/") or os.path.isabs(n):
        return "absolute path"
    if ".." in n.replace("\\", "/").split("/"):
        return "'..' in path"
    if m.issym() or m.islnk():
        t = m.linkname
        if not t or os.path.isabs(t):
            return "link to an absolute path"
        base = os.path.dirname(n) if m.issym() else ""     # a hard link names an archive path
        if os.path.normpath(os.path.join(base, t)).split(os.sep)[0] == "..":
            return "link pointing outside the archive"
    elif not (m.isfile() or m.isdir()):
        return "device or fifo"
    return None


def _secret_member(rel, allowed):
    """A member that may hold a live credential: never written to disk by the drill."""
    return bool(SECRETISH.search(rel)) or any(fnmatch.fnmatch(rel, a) or rel == a
                                              for a in allowed)


def _live_unchanged(src, rel, m, a_mtime):
    """The live file, if it has provably not changed since tar read it: a regular file whose
    mtime predates the archive AND equals the mtime tar recorded. (Predating the archive alone
    would flag a file written while tar was still running.)"""
    lp = os.path.join(src, rel)
    try:
        st = os.lstat(lp)
    except OSError:
        return None
    if (stat.S_ISREG(st.st_mode) and st.st_mtime < a_mtime
            and int(st.st_mtime) == int(m.mtime)):
        return lp
    return None


def _sqlite_problem(path):
    """None when PRAGMA integrity_check says ok (read-only URI), else the reason."""
    try:
        with open(path, "rb") as f:
            magic = f.read(16)
    except OSError as e:
        return f"unreadable: {e}"[:120]
    if magic != b"SQLite format 3\x00":
        return "not a SQLite file"
    try:
        con = sqlite3.connect("file:" + urllib.parse.quote(path) + "?mode=ro", uri=True,
                              timeout=5)
        try:
            r = con.execute("PRAGMA integrity_check").fetchall()
        finally:
            con.close()
    except sqlite3.Error as e:
        return f"{type(e).__name__}: {e}"[:120]
    return None if r == [("ok",)] else "; ".join(str(x[0]) for x in r[:3])[:160]


def _rmtree(path):
    """Remove the drill dir even if the archive carried read-only directories."""
    for dp, dns, _fns in os.walk(path):
        for d in dns:
            p = os.path.join(dp, d)
            if not os.path.islink(p):
                try:
                    os.chmod(p, 0o700)
                except OSError:
                    pass
    shutil.rmtree(path, ignore_errors=True)


def _now_z():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def drill_one(name, spec, dry=False, seen=None):
    """Restore the newest archive of one source and check it. Returns the jsonl row.

    Checks, in order: the recorded sha256 (verify's code), a size drop of more than 30%
    against the median of the previous up to 7 archives, every member safe to extract, every
    declared path present (a live path older than the archive must be in it), then, extracted:
    a sha256 sample (<= 200 files, 0 < size <= 200 MB) against live files that provably have
    not changed since tar read them, json.load of every .json, PRAGMA integrity_check of every
    .sqlite/.db. A credential-shaped or `secrets_ok` member is checked in memory and never
    written to disk. The temp dir is removed in a finally. `seen` (selftest only) collects the
    relative paths written to disk."""
    row = {"ts": _now_z(), "source": name, "archive": None, "ok": False,
           "checks": {}, "failures": []}
    ch, fails = row["checks"], row["failures"]
    a = newest(name)
    if not a:
        fails.append("no archive on disk")
        return row
    row["archive"] = os.path.relpath(a, root())
    a_mtime, a_size = os.path.getmtime(a), os.path.getsize(a)
    ch["bytes"] = a_size

    prev = sorted((p for p in archives(dest_dir(name)) if p != a),
                  key=os.path.getmtime, reverse=True)[:DRILL_PREV]
    if prev:
        med = statistics.median(os.path.getsize(p) for p in prev)
        drop = (med - a_size) / med if med else 0.0
        ch.update(prev_n=len(prev), median_prev_bytes=int(med), size_drop_pct=round(100 * drop, 1))
        if drop > DRILL_DROP:
            fails.append(f"size dropped {drop:.0%} against the median of the previous "
                         f"{len(prev)} archive(s) ({a_size} < {int(med)} bytes)")
    else:
        ch["prev_n"] = 0

    state, _sha, bad, _gone = record_check(a, _ledger(), hash_it=not dry)
    ch["verify"] = state
    if state == "mismatch":
        fails.append(f"sha256 differs from the {' + '.join(bad)} record")

    src, live_paths, live_missing = expand(name, spec)
    try:
        tf = tarfile.open(a, "r:gz")
    except (tarfile.TarError, OSError, EOFError) as e:
        fails.append(f"archive unreadable: {type(e).__name__}: {str(e)[:100]}")
        return row
    with tf:
        try:
            members = tf.getmembers()
        except (tarfile.TarError, OSError, EOFError) as e:
            fails.append(f"archive unreadable: {type(e).__name__}: {str(e)[:100]}")
            return row
        ch["members"] = len(members)
        ch["uncompressed_bytes"] = sum(m.size for m in members if m.isfile())

        refused = [(m.name, why) for m in members for why in [unsafe_member(m)] if why]
        ch["refused_members"] = len(refused)
        for n, why in refused[:5]:
            fails.append(f"refused member {n[:80]!r}: {why}")
        bad_names = {n for n, _w in refused}

        have = set()                               # every member and every ancestor dir
        for m in members:
            parts = _member_name(m).split("/")
            for i in range(1, len(parts) + 1):
                have.add("/".join(parts[:i]))
        declared = spec.get("paths", [])
        notes, absent = [], []
        for p in declared:
            depth = p.count("/")
            if any(h.count("/") == depth and fnmatch.fnmatch(h, p) for h in have):
                continue
            if p in live_missing:
                notes.append(f"{p}: declared, matches nothing live or archived")
        for rel in live_paths:
            if rel in have:
                continue
            try:
                older = os.lstat(os.path.join(src, rel)).st_mtime < a_mtime
            except OSError:
                older = False
            if older:
                absent.append(rel)
            else:
                notes.append(f"{rel}: live, newer than the archive")
        ch["paths_declared"] = len(declared)
        ch["paths_missing"] = absent
        if notes:
            ch["notes"] = notes
        for rel in absent:
            fails.append(f"declared path {rel!r} is live (older than the archive) but not in it")

        allowed = spec.get("secrets_ok") or []
        safe = [m for m in members if m.name not in bad_names]
        secret = [m for m in safe if m.isfile() and _secret_member(_member_name(m), allowed)]
        secret_ids = {id(m) for m in secret}
        to_disk = [m for m in safe if id(m) not in secret_ids]
        files = [m for m in to_disk if m.isfile()]
        cands = [m for m in files if 0 < m.size <= DRILL_MAX_FILE
                 and _live_unchanged(src, _member_name(m), m, a_mtime)]
        ch["secret_in_stream"] = len(secret)
        ch["sha_candidates"] = len(cands)
        if dry:
            ch["json_files"] = sum(_member_name(m).endswith(".json") for m in files)
            ch["sqlite_files"] = sum(_member_name(m).endswith((".sqlite", ".db")) for m in files)
            ch["sha_would_compare"] = min(DRILL_SAMPLE, len(cands))
            row["dry"] = True
            row["ok"] = not fails
            return row

        free = shutil.disk_usage(DRILL_TMPDIR).free
        if free < ch["uncompressed_bytes"] + (512 << 20):
            fails.append(f"not enough space in {DRILL_TMPDIR}: {free >> 20} MB free, "
                         f"{ch['uncompressed_bytes'] >> 20} MB to extract")
            return row

        # Secrets first, in memory only: hash, json-parse if .json, compare with live.
        sec_bad = []
        for m in secret:
            rel = _member_name(m)
            fh = tf.extractfile(m)
            data = fh.read() if fh else b""
            h = hashlib.sha256(data).hexdigest()
            if rel.endswith(".json"):
                try:
                    json.loads(data)
                except ValueError:
                    sec_bad.append(f"{rel}: not valid JSON")
            data = None
            lp = _live_unchanged(src, rel, m, a_mtime)
            if lp and sha256_file(lp) != h:
                sec_bad.append(f"{rel}: differs from the unchanged live file")
        for msg in sec_bad:
            fails.append(f"secret member (checked in memory) {msg}")

        tmp = tempfile.mkdtemp(prefix="backup-drill-", dir=DRILL_TMPDIR)
        try:
            os.chmod(tmp, 0o700)
            try:
                tf.extractall(tmp, members=to_disk, filter="data")
            except (tarfile.TarError, OSError) as e:
                fails.append(f"extraction failed: {type(e).__name__}: {str(e)[:120]}")
            if seen is not None:
                seen.extend(_member_name(m) for m in to_disk)

            jbad, sbad, nj, ns = [], [], 0, 0
            for m in files:
                rel = _member_name(m)
                p = os.path.join(tmp, rel)
                if rel.endswith(".json"):
                    nj += 1
                    try:
                        with open(p, encoding="utf-8") as f:
                            json.load(f)
                    except (OSError, ValueError) as e:
                        jbad.append(f"{rel}: {type(e).__name__}")
                elif rel.endswith((".sqlite", ".db")):
                    if m.size == 0 and rel.endswith(".db"):
                        continue
                    ns += 1
                    why = _sqlite_problem(p)
                    if why:
                        sbad.append(f"{rel}: {why}")
            ch.update(json_files=nj, json_bad=jbad, sqlite_files=ns, sqlite_bad=sbad)
            fails.extend(f"json.load failed: {x}" for x in jbad[:10])
            fails.extend(f"sqlite integrity: {x}" for x in sbad[:10])

            sample = random.Random(row["archive"]).sample(cands, min(DRILL_SAMPLE, len(cands)))
            mism = []
            for m in sample:
                rel = _member_name(m)
                lp = _live_unchanged(src, rel, m, a_mtime)   # re-check: live may move during the drill
                if not lp:
                    continue
                try:
                    if sha256_file(os.path.join(tmp, rel)) != sha256_file(lp):
                        mism.append(rel)
                except OSError as e:
                    mism.append(f"{rel} ({type(e).__name__})")
            ch.update(sha_compared=len(sample), sha_mismatch=mism)
            fails.extend(f"restored file differs from the unchanged live file: {x}"
                         for x in mism[:10])
        finally:
            _rmtree(tmp)
            ch["tmp_removed"] = not os.path.exists(tmp)
            if not ch["tmp_removed"]:
                fails.append(f"temp dir {tmp} could not be removed")
    row["ok"] = not fails
    return row


def _drill_report(month):
    """reports/restore-drill-<YYYY-MM>.md from this month's rows, the last one per source, so
    a single-source drill updates its own line without dropping the others."""
    last = {}
    try:
        with open(DRILLS) as f:
            for ln in f:
                try:
                    r = json.loads(ln)
                except ValueError:
                    continue
                if str(r.get("ts", "")).startswith(month):
                    last[r.get("source")] = r
    except FileNotFoundError:
        pass
    out = [f"# Restore drill, {month}", "",
           "Written by `bin/backup.py drill` (memo wa-restore-and-rebuild ask 1). The newest "
           "archive of each source is extracted into a 0700 temp dir under /tmp, checked, and "
           "the dir removed. One line per source: its latest drill this month. Raw rows: "
           "`state/restore_drills.jsonl`.", "",
           "| source | drilled (UTC) | archive | result | checks |", "|---|---|---|---|---|"]
    for name in sorted(last, key=str.lower):
        r = last[name]
        c = r.get("checks", {})
        if r.get("skipped"):
            out.append(f"| {name} | {r['ts']} | — | skipped | {r['skipped']} |")
            continue
        bits = [f"{c.get('members', '?')} members",
                f"sum {c.get('verify', '?')}",
                f"paths {c.get('paths_declared', 0) - len(c.get('paths_missing', []))}"
                f"/{c.get('paths_declared', 0)}",
                f"sha {c.get('sha_compared', 0)} compared of {c.get('sha_candidates', 0)} "
                f"unchanged, {len(c.get('sha_mismatch', []))} differ",
                f"json {c.get('json_files', 0)} ({len(c.get('json_bad', []))} bad)",
                f"sqlite {c.get('sqlite_files', 0)} ({len(c.get('sqlite_bad', []))} bad)"]
        if c.get("secret_in_stream"):
            bits.append(f"{c['secret_in_stream']} secret member(s) checked in memory only")
        if "size_drop_pct" in c:
            bits.append(f"size {(-c['size_drop_pct'] or 0.0):+.1f}% vs the median of the previous "
                        f"{c['prev_n']}")
        bits.append("temp dir removed" if c.get("tmp_removed") else "TEMP DIR LEFT")
        res = "**pass**" if r.get("ok") else "**FAIL**"
        out.append(f"| {name} | {r['ts']} | `{r.get('archive')}` | {res} | {'; '.join(bits)} |")
    fails = [(n, f) for n in sorted(last, key=str.lower) for f in last[n].get("failures", [])]
    out += ["", "## Failures", ""] + ([f"- **{n}**: {f}" for n, f in fails] or ["None."])
    notes = [(n, x) for n in sorted(last, key=str.lower)
             for x in last[n].get("checks", {}).get("notes", [])]
    if notes:
        out += ["", "## Notes (not failures)", ""] + [f"- {n}: {x}" for n, x in notes]
    os.makedirs(DRILL_REPORTS, exist_ok=True)
    path = os.path.join(DRILL_REPORTS, f"restore-drill-{month}.md")
    _write_atomic(path, "\n".join(out) + "\n")
    return path


def drill(only=None, dry=False):
    """Run the drill over every non-delegated source (or one). Exit 1 when any fails."""
    rows = []
    for name, spec in cfg()["sources"].items():
        if only and name != only:
            continue
        if spec.get("delegated_to"):
            print(f"  skip {name}: {DELEGATED_NOTE}")
            rows.append({"ts": _now_z(), "source": name, "archive": None, "ok": None,
                         "skipped": DELEGATED_NOTE, "checks": {}, "failures": []})
            continue
        r = drill_one(name, spec, dry=dry)
        rows.append(r)
        c = r["checks"]
        if dry:
            print(f"  {name}: would restore {r['archive'] or '(no archive)'}"
                  + (f", {c.get('bytes', 0) / 1e6:.1f} MB, {c.get('members', '?')} members, "
                     f"{c.get('uncompressed_bytes', 0) / 1e6:.1f} MB extracted, sum "
                     f"{c.get('verify')}" if r["archive"] else ""))
            if r["archive"]:
                print(f"       paths: {', '.join(spec.get('paths', []))}"
                      + (f"  (MISSING from archive: {', '.join(c['paths_missing'])})"
                         if c.get("paths_missing") else ""))
                print(f"       would compare {c.get('sha_would_compare', 0)} of "
                      f"{c.get('sha_candidates', 0)} unchanged live files; json "
                      f"{c.get('json_files', 0)}, sqlite {c.get('sqlite_files', 0)}, "
                      f"{c.get('secret_in_stream', 0)} secret member(s) in memory only; "
                      f"previous {c.get('prev_n', 0)}"
                      + (f", size {(-c['size_drop_pct'] or 0.0):+.1f}% vs their median"
                         if "size_drop_pct" in c else ""))
            for n in c.get("notes", []):
                print(f"       note: {n}")
            for f in r["failures"]:
                print(f"       WOULD FAIL: {f}")
            continue
        head = "  ok   " if r["ok"] else "  FAIL "
        print(head + f"{name}: {os.path.basename(r['archive'] or '(no archive)')}, "
              f"{c.get('members', '?')} members, sum {c.get('verify', '?')}, "
              f"sha {c.get('sha_compared', 0)}/{c.get('sha_candidates', 0)}, "
              f"json {c.get('json_files', 0)}, sqlite {c.get('sqlite_files', 0)}, "
              f"tmp {'removed' if c.get('tmp_removed') else 'n/a' if 'tmp_removed' not in c else 'LEFT'}")
        for f in r["failures"]:
            print(f"       {f}")
    if dry:
        print(f"drill --dry: {len(rows)} source(s) listed, nothing extracted or written")
        return 0
    os.makedirs(os.path.dirname(DRILLS), exist_ok=True)
    with open(DRILLS, "a") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    rep = _drill_report(datetime.now(timezone.utc).strftime("%Y-%m"))
    n_bad = sum(r["ok"] is False for r in rows)
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} drill: "
          f"{sum(r['ok'] is True for r in rows)} pass, {n_bad} failed, "
          f"{sum(r['ok'] is None for r in rows)} skipped -> {rep}")
    return 1 if n_bad else 0


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
    global root, SUMS, DRILLS, DRILL_REPORTS, DRILL_TMPDIR
    real = (root, SUMS, DRILLS, DRILL_REPORTS, DRILL_TMPDIR)
    root = (lambda: os.path.join(tmp, "dest"))
    SUMS = os.path.join(tmp, "state", "backup_sums.jsonl")
    DRILLS = os.path.join(tmp, "state", "restore_drills.jsonl")
    DRILL_REPORTS = os.path.join(tmp, "reports")
    DRILL_TMPDIR = os.path.join(tmp, "drilltmp")
    os.makedirs(DRILL_TMPDIR)
    try:
        bad = _selftest_body(tmp, src_dir)
        bad += _selftest_drill(tmp)
        print("ALL PASS" if not bad else f"SELFTEST FAILED ({bad})")
        return 1 if bad else 0
    finally:
        root, SUMS, DRILLS, DRILL_REPORTS, DRILL_TMPDIR = real
        shutil.rmtree(tmp, ignore_errors=True)


def _selftest_body(tmp, src_dir):
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
    # `paths` and write_one() share expand(): one matching and one missing glob
    _s, got, missing = expand("fakeproj", {"src": src_dir, "paths": ["state", "nope/*"]})
    good = got == ["state"] and missing == ["nope/*"]
    bad += not good
    print(f"  {'ok  ' if good else 'FAIL'} {'expand: one match, one missing glob named':<48} "
          f"-> {got} / missing {missing}")

    # ---- checksums at write time, verify, pruning (memo wa-backups-integrity) ----
    def chk(cond, label, detail=""):
        nonlocal bad
        bad += not cond
        print(f"  {'ok  ' if cond else 'FAIL'} {label:<48}" + (f" -> {detail}" if detail else ""))

    d = dest_dir("fakeproj")
    for f in os.listdir(d):                       # start the sums fixtures from an empty folder
        os.unlink(os.path.join(d, f))
    open(SUMS, "w").close()
    old = time.time() - 10 * 86400
    olds = []
    for i, day in enumerate(("2020-01-01", "2020-01-02")):
        a = os.path.join(d, f"fakeproj_{day}.tar.gz")
        subprocess.run(["tar", "czf", a, "-C", src_dir, "state"], check=True)
        olds.append(a)
        if i == 0:                                # the oldest one is recorded (sidecars) ...
            for sfx in SIDECARS:
                open(a + sfx, "w").write("x\n")
        for f in [a] + [a + sfx for sfx in SIDECARS if os.path.exists(a + sfx)]:
            os.utime(f, (old + i, old + i))
    stray = olds[0] + ".note"                     # ... and a lookalike that is NOT a sidecar
    open(stray, "w").write("keep me")
    spec = {"src": src_dir, "paths": ["state"], "keep_days": 2}
    ok, msg = write_one("fakeproj", spec)
    out = os.path.join(d, f"fakeproj_{datetime.now(timezone.utc):%Y-%m-%d}.tar.gz")
    chk(ok and os.path.exists(out), "a fresh archive is written", msg[-40:])
    side = _sidecar_sum(out)
    real = sha256_file(out)
    chk(side == real, ".sha256 sidecar holds the archive's sha256")
    try:
        man = json.load(open(out + ".manifest.json"))
    except (OSError, ValueError):
        man = {}
    want = [{"path": "state/data.json", "size": 2,
             "sha256": hashlib.sha256(b"{}").hexdigest()}]
    chk(man.get("members") == want, ".manifest.json lists path/size/sha256")
    rows = [json.loads(ln) for ln in open(SUMS)]
    chk(len(rows) == 1 and rows[0]["sha256"] == real and rows[0]["members"] == 1
        and rows[0]["archive"] == os.path.relpath(out, root())
        and rows[0]["bytes"] == os.path.getsize(out),
        "backup_sums.jsonl gets one matching row", f"{len(rows)} row(s)")
    chk(not any(os.path.exists(olds[0] + s) for s in [""] + list(SIDECARS)),
        "pruning removes the archive and its sidecars")
    chk(os.path.exists(olds[1]) and os.path.exists(stray),
        "keep counts archives only; a lookalike survives")
    for sfx in SIDECARS:                          # sidecars newer than the archive
        os.utime(out + sfx, (time.time() + 60, time.time() + 60))
    chk(newest("fakeproj") == out, "newest() ignores the sidecars",
        os.path.basename(newest("fakeproj") or "None"))
    srcs = {"fakeproj": spec}
    import io
    import contextlib

    def quiet_verify():
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = verify(None, srcs)
        return rc, buf.getvalue()

    rc, txt = quiet_verify()
    chk(rc == 0 and "1 ok, 0 mismatched" in txt and "1 unrecorded" in txt,
        "verify passes; an unrecorded archive does not fail", txt.strip().splitlines()[-1])
    with open(out, "r+b") as f:                   # flip one byte, keep the size
        f.seek(os.path.getsize(out) // 2)
        b = f.read(1)
        f.seek(-1, 1)
        f.write(bytes([b[0] ^ 0xFF]))
    rc, txt = quiet_verify()
    chk(rc == 1 and "MISMATCH" in txt, "a flipped byte fails verify")
    _write_atomic(out + ".sha256", f"{sha256_file(out)}  {os.path.basename(out)}\n")
    rc, txt = quiet_verify()
    chk(rc == 1 and "from the ledger record" in txt, "…even with the sidecar rewritten (ledger wins)")
    return bad


def _selftest_drill(tmp):
    """The restore drill's fixtures: clean passes, a missing declared path fails, a changed
    byte in a file whose live mtime predates the archive fails, a `..` member is refused, a
    size drop fails, a secret never reaches disk, --dry writes nothing, no temp dir is left."""
    import io
    import contextlib
    bad = 0

    def chk(cond, label, detail=""):
        nonlocal bad
        bad += not cond
        print(f"  {'ok  ' if cond else 'FAIL'} {label:<48}" + (f" -> {detail}" if detail else ""))

    old = time.time() - 3 * 86400
    src = os.path.join(tmp, "drillsrc")
    os.makedirs(os.path.join(src, "data"))
    files = {"data/a.json": '{"x": 1}', "data/b.txt": "hello drill\n" * 50,
             ".env": "TOKEN=fixture-not-a-secret\n"}
    for rel, body in files.items():
        with open(os.path.join(src, rel), "w") as f:
            f.write(body)
    con = sqlite3.connect(os.path.join(src, "data", "d.sqlite"))
    con.execute("create table t (x)")
    con.execute("insert into t values (1)")
    con.commit()
    con.close()
    with open(os.path.join(src, "extra.txt"), "w") as f:
        f.write("declared, never archived")
    for dp, dns, fns in os.walk(src):
        for n in dns + fns:
            os.utime(os.path.join(dp, n), (old, old))

    def make(name, paths, extra=None):
        d = dest_dir(name)
        os.makedirs(d, exist_ok=True)
        a = os.path.join(d, f"{name}_2026-01-01.tar.gz")
        subprocess.run(["tar", "czf", a, "-C", src] + paths, check=True)
        if extra:                                   # rewrite with hand-made members appended
            with tarfile.open(a, "r:gz") as tf:
                ms = [(m, tf.extractfile(m).read() if m.isfile() else None)
                      for m in tf.getmembers()]
            with tarfile.open(a, "w:gz") as tf:
                for m, body in ms + extra:
                    tf.addfile(m, io.BytesIO(body) if body is not None else None)
        return a

    def run(name, spec, dry=False, seen=None):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            r = drill_one(name, {**spec, "src": src}, dry=dry, seen=seen)
        return r

    spec = {"paths": ["data", ".env"], "secrets_ok": [".env"]}
    a = make("drillok", ["data", ".env"])
    record_sums("drillok", a)
    seen = []
    r = run("drillok", spec, seen=seen)
    c = r["checks"]
    chk(r["ok"] and c["verify"] == "ok" and c["json_files"] == 1 and c["sqlite_files"] == 1
        and c["sha_compared"] == 3 and not c["sha_mismatch"],
        "drill: a clean archive passes", f"{r['failures'][:1] or c.get('sha_compared')}")
    chk(c["secret_in_stream"] == 1 and ".env" not in seen and "data/b.txt" in seen,
        "drill: a secrets_ok member never reaches disk", f"{len(seen)} on disk")
    chk(not os.listdir(DRILL_TMPDIR) and c["tmp_removed"], "drill: the temp dir is removed")

    b = os.path.join(src, "data", "b.txt")         # same length, same mtime, one byte changed
    body = open(b).read()
    with open(b, "w") as f:
        f.write("j" + body[1:])
    os.utime(b, (old, old))
    r = run("drillok", spec)
    chk(not r["ok"] and r["checks"]["sha_mismatch"] == ["data/b.txt"],
        "drill: a changed byte (live mtime older) fails", (r["failures"] or ["-"])[0][:50])
    with open(b, "w") as f:
        f.write(body)
    os.utime(b, (old, old))
    with open(os.path.join(src, ".env"), "w") as f:
        f.write("TOKEN=fixture-not-a-secreX\n")
    os.utime(os.path.join(src, ".env"), (old, old))
    r = run("drillok", spec)
    chk(not r["ok"] and any("secret member" in f for f in r["failures"]),
        "drill: …and so does a changed secret, in memory")
    with open(os.path.join(src, ".env"), "w") as f:
        f.write(files[".env"])
    os.utime(os.path.join(src, ".env"), (old, old))

    r = run("drillok", {**spec, "paths": ["data", ".env", "extra.txt"]})
    chk(not r["ok"] and r["checks"]["paths_missing"] == ["extra.txt"],
        "drill: a missing declared path fails", (r["failures"] or ["-"])[0][:50])

    ti = tarfile.TarInfo("../evil-drill.txt")
    ti.size, ti.mtime = 4, int(old)
    make("drilldot", ["data"], extra=[(ti, b"evil")])
    r = run("drilldot", {"paths": ["data"]})
    chk(not r["ok"] and r["checks"]["refused_members"] == 1
        and not os.path.exists(os.path.join(DRILL_TMPDIR, "evil-drill.txt"))
        and not os.path.exists(os.path.join(tmp, "evil-drill.txt")),
        "drill: a '..' member is refused, never written", (r["failures"] or ["-"])[0][:50])
    lk = tarfile.TarInfo("data/link")
    lk.type, lk.linkname = tarfile.SYMTYPE, "../../etc/passwd"
    ab = tarfile.TarInfo("/etc/evil")
    ok_lk = tarfile.TarInfo("data/ok")
    ok_lk.type, ok_lk.linkname = tarfile.SYMTYPE, "b.txt"
    chk(unsafe_member(lk) and unsafe_member(ab) and unsafe_member(ok_lk) is None,
        "drill: escaping link / absolute refused, inner ok")

    d = dest_dir("drillok")                         # three bigger, older archives: a size drop
    for i in range(3):
        p = os.path.join(d, f"drillok_2025-12-0{i + 1}.tar.gz")
        with open(p, "wb") as f:
            f.write(os.urandom(4 * os.path.getsize(a)))
        os.utime(p, (old - 86400 * (i + 1),) * 2)
    r = run("drillok", spec)
    chk(not r["ok"] and any("size dropped" in f for f in r["failures"]),
        "drill: a >30% size drop fails", f"{r['checks'].get('size_drop_pct')}%")

    rows_before = os.path.exists(DRILLS)
    r = run("drilldot", {"paths": ["data"]}, dry=True)
    chk(r.get("dry") and not rows_before and not os.path.exists(DRILLS)
        and not os.listdir(DRILL_TMPDIR) and r["checks"]["members"] >= 1,
        "drill --dry: lists, extracts and writes nothing")
    real_cfg = cfg
    globals()["cfg"] = lambda: {"sources": {"drilldot": {"paths": ["data"], "src": src},
                                            "Frozen": {"delegated_to": "x"}}}
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = drill()
    finally:
        globals()["cfg"] = real_cfg
    rows = [json.loads(ln) for ln in open(DRILLS)]
    rep = os.path.join(DRILL_REPORTS, f"restore-drill-{datetime.now(timezone.utc):%Y-%m}.md")
    chk(rc == 1 and len(rows) == 2 and rows[1]["ok"] is None
        and rows[1]["skipped"] == DELEGATED_NOTE and os.path.exists(rep)
        and "FAIL" in open(rep).read(),
        "drill: one row per source, report, exit 1 on a fail", f"rc {rc}, {len(rows)} rows")
    chk(not os.listdir(DRILL_TMPDIR), "drill: no temp dir left behind after all cases")
    return bad


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    USAGE = ("usage: backup.py run [source] | paths [source] | verify [source] "
             "| drill [source] [--dry|--dry-run] | check | list | selftest")
    args = sys.argv[1:]
    cmd = args[0] if args else "run"
    rest = args[1:]
    # An unknown argument exits 2 and never runs the live job (MC CLAUDE.md, 2026-10-03):
    # `run --dry` used to be read as a source name, ran nothing, then the stale check could push.
    if cmd in ("run", "paths", "verify"):
        if len(rest) > 1 or (rest and rest[0] not in cfg()["sources"]):
            why = (f"unexpected argument {rest[1]!r}" if len(rest) > 1
                   else f"unknown source {rest[0]!r}")
            print(USAGE + "\n" + why, file=sys.stderr)
            sys.exit(2)
    elif cmd == "drill":
        flags = [a for a in rest if a.startswith("-")]
        pos = [a for a in rest if not a.startswith("-")]
        wrong = [f for f in flags if f not in ("--dry", "--dry-run")]
        if wrong or len(pos) > 1 or (pos and pos[0] not in cfg()["sources"]):
            why = (f"unknown flag {wrong[0]!r}" if wrong
                   else f"unexpected argument {pos[1]!r}" if len(pos) > 1
                   else f"unknown source {pos[0]!r}")
            print(USAGE + "\n" + why, file=sys.stderr)
            sys.exit(2)
        sys.exit(drill(pos[0] if pos else None, dry=bool(flags)))
    elif rest:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
    if cmd == "selftest":
        sys.exit(selftest())
    if cmd == "run":
        sys.exit(run(rest[0] if rest else None))
    elif cmd == "paths":
        sys.exit(paths_report(rest[0] if rest else None))
    elif cmd == "verify":
        sys.exit(verify(rest[0] if rest else None))
    elif cmd == "check":
        sys.exit(1 if check() else 0)
    elif cmd == "list":
        _list()
    else:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
