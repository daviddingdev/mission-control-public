#!/usr/bin/env python3
"""Local-model resolver — Mission Control's back office for ollama.

Jobs ask for a ROLE ("dense", "bulk", "embed"); this picks the best model that is
actually installed right now. One registry (~/maintenance/config/models.json) is the
only place a model tag is written down, so upgrading the box's local models is a
one-line edit there instead of a grep across five repos.

    from models import require
    MODEL = require("dense")     # pre-check: exits 75 with a clear reason if the
                                 # box can't serve this role, BEFORE the job burns time

    from models import resolve, ModelUnavailable
    m = resolve("bulk")          # same, but raises instead of exiting

Why a pre-check at all: every local job used to hardcode a tag. If ollama was down or
the tag had been renamed/removed, the job ran anyway and failed deep inside its work
loop — half a night's queue burned, an empty report, no obvious cause in the log.

CLI:
    models.py list                  what's installed, and which roles claim it
    models.py resolve dense         print the resolved tag (for shell jobs)
    models.py check                 drift report. exit 0 ok · 1 a role fell back to a
                                    weaker model · 2 a role cannot run at all (healthcheck
                                    alerts on 2, since that is jobs failing tonight)
"""
import json
import os
import sys
import time
import urllib.request

HOME = os.path.expanduser("~")
REGISTRY = os.path.join(HOME, "maintenance/config/models.json")
_TAG_TTL = 30.0                  # seconds; a sweep resolving 12 roles shouldn't poll 12 times
_cache = {"at": 0.0, "tags": None}


class ModelUnavailable(RuntimeError):
    """No model in the role's preference list is installed, or ollama is unreachable."""


def registry():
    with open(REGISTRY) as f:
        return json.load(f)


def _base():
    return registry().get("ollama", "http://127.0.0.1:11434").rstrip("/")


def chat_url():
    """The /api/chat endpoint, from the registry — so the host lives in one place too."""
    return _base() + "/api/chat"


def installed(timeout=5, force=False):
    """Tags ollama can serve right now. Raises ModelUnavailable if it can't be asked."""
    if not force and _cache["tags"] is not None and time.time() - _cache["at"] < _TAG_TTL:
        return _cache["tags"]
    url = _base() + "/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        raise ModelUnavailable(f"ollama unreachable at {url} ({e.__class__.__name__}: {e})")
    tags = [m["name"] for m in data.get("models", [])]
    _cache.update(at=time.time(), tags=tags)
    return tags


def _match(want, have):
    """Registry entries may omit the tag; `qwen3.8` then matches `qwen3.8:latest`."""
    if want in have:
        return want
    if ":" not in want and f"{want}:latest" in have:
        return f"{want}:latest"
    return None


def resolve(role, timeout=5):
    """Best INSTALLED model for `role`. Raises ModelUnavailable if none of them are."""
    roles = registry()["roles"]
    if role not in roles:
        raise ModelUnavailable(f"unknown role {role!r} — registry has {sorted(roles)}")
    prefer = roles[role].get("prefer", [])
    have = installed(timeout=timeout)
    for want in prefer:
        hit = _match(want, have)
        if hit:
            return hit
    raise ModelUnavailable(
        f"role {role!r}: none of {prefer} is installed. Pull the preferred one: "
        f"`ollama pull {prefer[0]}`" if prefer else f"role {role!r} declares no models")


def options(role):
    """Sampling defaults the registry recommends for this role (may be empty)."""
    return dict(registry()["roles"].get(role, {}).get("options", {}))


def require(role, job=None):
    """resolve(), but a failure ends the job cleanly instead of half-running it.

    Exit 75 = EX_TEMPFAIL: 'the box could not serve this, try again later' — distinct
    from a genuine job bug, so cron log triage and the daily narrator can tell them apart."""
    try:
        model = resolve(role)
    except ModelUnavailable as e:
        who = job or os.path.basename(sys.argv[0]) or "local job"
        print(f"PRECHECK FAIL [{who}] local-model role {role!r} unavailable: {e}",
              file=sys.stderr, flush=True)
        sys.exit(75)
    declared = registry()["roles"][role]["prefer"][0]
    if model != declared:
        who = job or os.path.basename(sys.argv[0]) or "local job"
        print(f"PRECHECK WARN [{who}] role {role!r} degraded: using {model}; "
              f"preferred {declared} is not installed", file=sys.stderr, flush=True)
    return model


def report():
    """Registry + live state, for the dashboard and `check`."""
    reg = registry()
    out = {"updated": reg.get("updated"), "up": True, "error": "",
           "roles": [], "models": [], "jobs": reg.get("jobs", {})}
    try:
        have = installed()
    except ModelUnavailable as e:
        out.update(up=False, error=str(e))
        have = []

    claims = {}
    for name, spec in reg["roles"].items():
        prefer = spec.get("prefer", [])
        got = next((h for h in (_match(w, have) for w in prefer) if h), None)
        status = ("down" if not out["up"] else
                  "missing" if got is None else
                  "ok" if got == _match(prefer[0], have) or got == prefer[0] else "degraded")
        out["roles"].append({"role": name, "desc": spec.get("desc", ""), "prefer": prefer,
                             "resolved": got, "status": status, "note": spec.get("note", "")})
        if got:
            claims.setdefault(got, []).append(name)

    jobs_by_role = {}
    for jname, j in reg.get("jobs", {}).items():
        jobs_by_role.setdefault(j.get("role"), []).append(jname)
    for r in out["roles"]:
        r["jobs"] = jobs_by_role.get(r["role"], [])

    for tag in have:
        out["models"].append({"name": tag, "roles": claims.get(tag, [])})
    out["models"].sort(key=lambda m: (not m["roles"], m["name"]))
    return out


def _cli(argv):
    cmd = argv[0] if argv else "check"
    if cmd == "resolve":
        if len(argv) < 2:
            print("usage: models.py resolve <role>", file=sys.stderr)
            return 2
        print(require(argv[1], job="models.py"))
        return 0
    if cmd == "list":
        rep = report()
        if not rep["up"]:
            print(f"ollama DOWN — {rep['error']}")
            return 1
        print("installed:")
        for m in rep["models"]:
            print(f"  {m['name']:<50} {', '.join(m['roles']) or '-'}")
        return 0
    if cmd == "check":
        rep = report()
        if not rep["up"]:
            print(f"FAIL  ollama unreachable — {rep['error']}")
            return 2
        drift = missing = 0
        for r in rep["roles"]:
            mark = {"ok": "ok   ", "degraded": "DRIFT", "missing": "FAIL "}[r["status"]]
            drift += r["status"] == "degraded"
            missing += r["status"] == "missing"
            print(f"{mark} {r['role']:<8} -> {r['resolved'] or '(none)':<22} "
                  f"prefer {r['prefer'][0]:<22} {len(r['jobs'])} job(s)")
        print(f"registry updated {rep['updated']}; {drift} degraded, {missing} unservable")
        # 2 = a role cannot run at all (page it); 1 = running on a fallback (report it).
        return 2 if missing else (1 if drift else 0)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
