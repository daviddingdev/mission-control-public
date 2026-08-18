#!/usr/bin/env python3
"""Admission control for the box's one GPU — priority, queueing, and anti-starvation.

ollama already serialises (OLLAMA_NUM_PARALLEL=1) behind a 512-deep queue, but that queue
is FIFO: whoever's request lands first wins. On this box that is the wrong order. The Bench
issues ~800 calls between 22:00 and 03:00; the hourly sentinel and the 23:00 digest land
inside that window; Stocks' scout runs every 30 minutes through market hours. FIFO means a
market-hours job can sit behind a housekeeping sweep, and nothing can ever say "this one
matters more".

So the ordering decision is made HERE, before the request is issued, and only one request
is in flight at a time. ollama's own queue stays empty by construction, which is what makes
our priority the real one.

    import gpu
    with gpu.slot(job="the Bench", model=model):   # blocks until it's our turn
        ...one inference call...

Priority is David's (2026-08-18): Stocks first, then any other personal project, then
maintenance. A call from a terminal (no cron parent) counts as interactive and jumps the
queue — if he's sitting there, he's the most important thing on the box.

Design notes for whoever extends this:

  * COOPERATIVE, NOT ENFORCED. Every local-model caller on this box goes through
    ~/maintenance/bin/localllm.py or asks for a slot directly, so a cooperative scheme is
    enough — and it costs no daemon, nothing to keep alive, nothing to fall over. The
    back-office audit has a rule (`gpu-unmanaged`) that catches a new caller that skips it.
  * FAIL OPEN, ALWAYS. Every failure path here lets the call through and logs a `bypass`.
    A scheduler that can wedge the box's AI is worse than no scheduler.
  * ONE CALL IS THE UNIT. A slot is held for a single inference, not a whole job. That is
    what makes preemption free: a 5-hour batch yields to a higher tier at its next call
    boundary (~15s), with no mid-generation kill and no lost work.
  * AGEING BEATS STARVATION. Strict priority alone would let the Bench starve the sentinel
    all night. A waiter's score improves the longer it waits, capped, so the worst case for
    a maintenance job behind continuous Stocks work is bounded (~40 min at the defaults)
    instead of unbounded.
  * MODEL AFFINITY, BOUNDED. Preferring a waiter that needs the already-loaded model avoids
    an 18GB reload, but the bonus is deliberately smaller than one tier gap, so affinity can
    reorder within a tier and never across one.
  * A DEAD HOLDER CANNOT WEDGE IT. The lease carries a pid and a TTL; the next waiter
    reclaims it.

CLI:  gpu.py status | queue | events [n] | selftest
"""
import contextlib
import fcntl
import json
import os
import random
import sys
import time
import urllib.request
import uuid

HOME = os.path.expanduser("~")
MC = os.path.join(HOME, "maintenance")
CFG = os.path.join(MC, "config/gpu.json")
DIR = os.path.join(MC, "state/gpu")
WAITERS = os.path.join(DIR, "waiters")
HOLDER = os.path.join(DIR, "holder.json")
LOCK = os.path.join(DIR, "lock")
EVENTS = os.path.join(DIR, "events.jsonl")
EVENT_CAP = 4000

DEFAULTS = {
    "slots": 1, "lease_ttl_s": 1800, "wait_timeout_s": 2700, "age_step_s": 120,
    "max_age_bonus": 25, "affinity_bonus": 5, "poll_s": 0.4, "keep_alive": "30m",
    "tiers": {"interactive": 0, "Stocks": 10, "maintenance": 30}, "default_tier": 20,
}
_cfg_cache = {"mt": 0, "v": None}


def cfg():
    try:
        mt = os.path.getmtime(CFG)
    except OSError:
        return DEFAULTS
    if _cfg_cache["mt"] != mt:
        v = dict(DEFAULTS)
        try:
            with open(CFG) as f:
                v.update({k: x for k, x in json.load(f).items() if not k.startswith("_")})
        except Exception:
            pass
        _cfg_cache.update(mt=mt, v=v)
    return _cfg_cache["v"]


# ---------------------------------------------------------------- identity

def _caller_path():
    for p in (sys.argv[0] or "", os.getcwd()):
        if p:
            yield os.path.abspath(p)


def project(explicit=None):
    """Which project is asking. Derived from the running script's path so a new job is
    classified correctly without anyone remembering to declare it."""
    if explicit:
        return explicit
    env = os.environ.get("SPARK_GPU_PROJECT")
    if env:
        return env
    for path in _caller_path():
        rel = os.path.relpath(path, HOME)
        if rel.startswith(".."):
            continue
        top = rel.split(os.sep)[0]
        if top and not top.startswith("."):
            return "maintenance" if top == "maintenance" else top
    return ""


def _interactive():
    """A human at a terminal outranks every batch job on the box."""
    if os.environ.get("SPARK_GPU_INTERACTIVE") == "1":
        return True
    try:
        return os.isatty(0) or os.isatty(2)
    except Exception:
        return False


def tier(proj=None, job=None, interactive=None):
    """Tier for this caller. A "<project>/<job>" key wins over the bare project, so one
    job can be ranked apart from its siblings — the Bench is Stocks work, but it is
    5 hours of opportunistic background reading and must not outrank a market-hours job
    from the same project."""
    c = cfg()
    if interactive if interactive is not None else _interactive():
        return c["tiers"].get("interactive", 0)
    proj = proj or project()
    if job and f"{proj}/{job}" in c["tiers"]:
        return c["tiers"][f"{proj}/{job}"]
    return c["tiers"].get(proj, c["default_tier"])


# ---------------------------------------------------------------- plumbing

def _ensure():
    os.makedirs(WAITERS, exist_ok=True)


def _read(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def _write_atomic(path, obj):
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except (OSError, TypeError):
        return False


@contextlib.contextmanager
def _locked():
    _ensure()
    f = open(LOCK, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(f, fcntl.LOCK_UN)
        finally:
            f.close()


def event(ev, **kw):
    try:
        _ensure()
        with open(EVENTS, "a") as f:
            f.write(json.dumps({"at": int(time.time()), "ev": ev, **kw}) + "\n")
        if random.random() < 0.02:          # amortised trim; the file is a rolling record
            lines = open(EVENTS).readlines()
            if len(lines) > EVENT_CAP:
                open(EVENTS, "w").writelines(lines[-EVENT_CAP:])
    except Exception:
        pass


_ps = {"at": 0.0, "models": []}


def loaded_models(ttl=10.0):
    """What ollama currently has resident — the input to model affinity."""
    if time.time() - _ps["at"] < ttl:
        return _ps["models"]
    try:
        with urllib.request.urlopen(f"{_base()}/api/ps", timeout=3) as r:
            _ps["models"] = [m.get("name") for m in json.loads(r.read()).get("models", [])]
    except Exception:
        _ps["models"] = []
    _ps["at"] = time.time()
    return _ps["models"]


def _base():
    try:
        sys.path.insert(0, os.path.join(MC, "bin"))
        import models
        return models.chat_url().rsplit("/api/", 1)[0]
    except Exception:
        return "http://127.0.0.1:11434"


# ---------------------------------------------------------------- scheduling

def _waiters():
    out = []
    try:
        for name in os.listdir(WAITERS):
            w = _read(os.path.join(WAITERS, name))
            if not w:
                continue
            if not _alive(w.get("pid", -1)):     # crashed before it got its turn
                with contextlib.suppress(OSError):
                    os.unlink(os.path.join(WAITERS, name))
                continue
            out.append(w)
    except FileNotFoundError:
        pass
    return out


def score(w, now, resident):
    """Lower wins. tier − ageing − affinity. The affinity bonus is capped below one tier
    gap on purpose: it may reorder equals, never overtake a more important project."""
    c = cfg()
    age = max(0, now - w.get("since", now))
    aged = min(c["max_age_bonus"], int(age // max(1, c["age_step_s"])))
    aff = c["affinity_bonus"] if w.get("model") and w["model"] in resident else 0
    return w.get("tier", c["default_tier"]) - aged - aff


def _holder_valid(h, now):
    return bool(h) and _alive(h.get("pid", -1)) and h.get("expires", 0) > now


def _try_admit(me):
    now = time.time()
    h = _read(HOLDER)
    if _holder_valid(h, now):
        return h.get("id") == me["id"]
    if h:
        event("reclaim", project=h.get("project"), job=h.get("job"),
              reason="dead" if not _alive(h.get("pid", -1)) else "expired")
    resident = loaded_models()
    field = _waiters()
    if not field:
        field = [me]
    best = min(field, key=lambda w: (score(w, now, resident), w.get("since", 0)))
    if best["id"] != me["id"]:
        return False
    _write_atomic(HOLDER, {**me, "acquired": now, "expires": now + cfg()["lease_ttl_s"]})
    return True


def _waiter_path(wid):
    return os.path.join(WAITERS, f"{wid}.json")


@contextlib.contextmanager
def slot(job=None, model=None, proj=None, timeout=None):
    """Hold the GPU for one inference. Blocks until this caller is the best waiter.

    Fails open: any internal error, or a wait past the timeout, lets the call proceed and
    records a `bypass` event rather than blocking a job forever.
    """
    c = cfg()
    me, held, t0 = None, False, time.time()
    try:
        _ensure()
        me = {"id": uuid.uuid4().hex[:12], "pid": os.getpid(), "since": t0,
              "project": proj or project() or "?", "job": job or os.path.basename(sys.argv[0]),
              "model": model}
        me["tier"] = tier(me["project"], me["job"])
        _write_atomic(_waiter_path(me["id"]), me)
        deadline = t0 + (timeout or c["wait_timeout_s"])
        while time.time() < deadline:
            with _locked():
                if _try_admit(me):
                    held = True
                    break
            time.sleep(c["poll_s"] * (1 + random.random() * 0.5))
        wait_s = round(time.time() - t0, 1)
        if held:
            if wait_s > 1:
                event("grant", project=me["project"], job=me["job"], wait_s=wait_s,
                      tier=me["tier"], model=model)
        else:
            event("bypass", project=me["project"], job=me["job"], wait_s=wait_s,
                  reason="wait timeout")
    except Exception as e:                       # never let the scheduler break the work
        event("bypass", job=job or "?", reason=f"{type(e).__name__}: {e}"[:120])
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            if me:
                with contextlib.suppress(OSError):
                    os.unlink(_waiter_path(me["id"]))
                if held:
                    with _locked():
                        h = _read(HOLDER)
                        if h and h.get("id") == me["id"]:
                            with contextlib.suppress(OSError):
                                os.unlink(HOLDER)
                    event("release", project=me["project"], job=me["job"],
                          held_s=round(time.time() - t0, 1), model=model)


def should_yield(proj=None, job=None):
    """True when someone more important is waiting. A batch job calls this between items
    and sleeps a beat — cheap politeness that keeps a long run from monopolising the box
    even inside its own tier."""
    try:
        now, mine = time.time(), tier(proj, job)
        resident = loaded_models()
        return any(score(w, now, resident) < mine for w in _waiters())
    except Exception:
        return False


def wait_turn(proj=None, max_s=120):
    """Block while higher-priority work is queued (bounded). For use between batch items."""
    t0 = time.time()
    while should_yield(proj) and time.time() - t0 < max_s:
        time.sleep(1.0)
    return round(time.time() - t0, 1)


# ---------------------------------------------------------------- reporting

def status():
    now = time.time()
    h = _read(HOLDER)
    if not _holder_valid(h, now):
        h = None
    field = sorted(_waiters(), key=lambda w: score(w, now, loaded_models()))
    ev = []
    try:
        with open(EVENTS) as f:
            ev = [json.loads(l) for l in f.readlines()[-600:] if l.strip()]
    except Exception:
        pass
    day = now - 86400
    recent = [e for e in ev if e.get("at", 0) > day]
    by = {}
    # every call logs a `release`; only a call that actually queued logs a `grant`. Count
    # calls from releases and average the wait over the contended ones, or an idle-box run
    # reads as "1 call, 3 minutes of GPU".
    for e in recent:
        if e["ev"] in ("grant", "release"):
            b = by.setdefault(e.get("project", "?"),
                              {"calls": 0, "contended": 0, "wait_s": 0.0, "held_s": 0.0})
            if e["ev"] == "grant":
                b["contended"] += 1
                b["wait_s"] += e.get("wait_s", 0)
            else:
                b["calls"] += 1
                b["held_s"] += e.get("held_s", 0)
    return {
        "holder": h and {"project": h["project"], "job": h["job"], "model": h.get("model"),
                         "held_s": round(now - h["acquired"], 1)},
        "queue": [{"project": w["project"], "job": w["job"], "tier": w["tier"],
                   "waiting_s": round(now - w["since"], 1),
                   "score": score(w, now, loaded_models())} for w in field],
        "resident": loaded_models(),
        "day": {"by_project": by,
                "bypasses": len([e for e in recent if e["ev"] == "bypass"]),
                "reclaims": len([e for e in recent if e["ev"] == "reclaim"])},
        "tiers": cfg()["tiers"], "slots": cfg()["slots"],
    }


def _cli(argv):
    cmd = argv[1] if len(argv) > 1 else "status"
    if cmd == "status":
        s = status()
        h = s["holder"]
        print(f"holder : {h['project']}/{h['job']} · {h['held_s']}s" if h else "holder : idle")
        print(f"queue  : {len(s['queue'])} waiting")
        for w in s["queue"]:
            print(f"   {w['score']:>3}  {w['project']:<14} {w['job']:<28} {w['waiting_s']:>6.1f}s")
        print(f"resident: {', '.join(s['resident']) or 'none'}")
        print("last 24h:")
        for p, b in sorted(s["day"]["by_project"].items(), key=lambda x: -x[1]["calls"]):
            avg = b["wait_s"] / b["contended"] if b["contended"] else 0
            print(f"   {p:<14} {b['calls']:>4} calls · gpu {b['held_s'] / 60:>6.1f} min · "
                  f"{b['contended']} queued"
                  + (f" (avg wait {avg:.1f}s)" if b["contended"] else ""))
        if s["day"]["bypasses"] or s["day"]["reclaims"]:
            print(f"   bypasses {s['day']['bypasses']} · reclaims {s['day']['reclaims']}")
    elif cmd == "queue":
        print(json.dumps(status()["queue"], indent=1))
    elif cmd == "events":
        n = int(argv[2]) if len(argv) > 2 else 20
        try:
            for l in open(EVENTS).readlines()[-n:]:
                e = json.loads(l)
                ts = time.strftime("%m-%d %H:%M:%S", time.localtime(e["at"]))
                print(f"{ts} {e['ev']:<8} {e.get('project', ''):<14} {e.get('job', '')[:30]:<30} "
                      + " ".join(f"{k}={v}" for k, v in e.items()
                                 if k not in ("at", "ev", "project", "job")))
        except FileNotFoundError:
            print("no events yet")
    elif cmd == "selftest":
        _selftest()
    else:
        print(__doc__.strip().splitlines()[-1])


def _selftest():
    """Ordering is the whole product, so it gets a test: build a synthetic field and check
    the winner is the one the doctrine says it should be."""
    c = cfg()
    now = time.time()
    def w(proj, age=0, model=None, t=None):
        return {"id": proj + str(age), "project": proj, "job": "t", "since": now - age,
                "model": model, "tier": t if t is not None else c["tiers"].get(proj, c["default_tier"])}
    def winner(field, resident=()):
        return min(field, key=lambda x: (score(x, now, resident), x["since"]))["project"]
    cases = [
        ("Stocks beats maintenance", [w("Stocks"), w("maintenance")], (), "Stocks"),
        ("Stocks beats another project", [w("poker"), w("Stocks")], (), "Stocks"),
        ("other project beats maintenance", [w("poker"), w("maintenance")], (), "poker"),
        ("interactive beats everything", [w("Stocks"), w("x", t=0)], (), "x"),
        ("ageing eventually beats a higher tier",
         [w("Stocks"), w("maintenance", age=60 * 60)], (), "maintenance"),
        ("ageing does NOT flip it early",
         [w("Stocks"), w("maintenance", age=300)], (), "Stocks"),
        ("affinity reorders within a tier",
         [w("projA", model="a", t=20), w("projB", model="b", t=20)], ("b",), "projB"),
        ("affinity does NOT cross a tier",
         [w("Stocks", model="a"), w("maintenance", model="b")], ("b",), "Stocks"),
        ("oldest wins a true tie", [w("projA", age=5, t=20), w("projB", age=50, t=20)], (),
         "projB"),
        ("a Stocks job outranks the Bench",
         [w("Stocks"), w("bench", t=c["tiers"].get("Stocks/the Bench", 15))], (), "Stocks"),
        ("the Bench still outranks other projects",
         [w("poker"), w("bench", t=c["tiers"].get("Stocks/the Bench", 15))], (), "bench"),
    ]
    bad = 0
    for name, field, resident, want in cases:
        got = winner(field, resident)
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    print(f"{len(cases) - bad}/{len(cases)} passed")
    return bad


if __name__ == "__main__":
    sys.exit(_cli(sys.argv) or 0)
