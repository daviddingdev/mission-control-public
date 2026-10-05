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
  * BACKFILL IS BELOW EVERYTHING AND NEVER AGES (2026-10-04, the COO build; David: "the gpu
    allocation is properly being divided based on priorities (and fully utilized)"). A process
    started with SPARK_GPU_BACKFILL=1 (bin/gpu_backfill.py launches idle-time work that way)
    takes the `backfill` tier (40, below maintenance's 30) unless a human is at the terminal,
    and a backfill waiter gets no ageing bonus, so it can never tie real work however long it
    waits: it only ever takes a slot nobody else wants. Nothing changes for any other caller.
  * A JOB CAN DECLARE ITSELF BACKFILL (2026-10-05, the COO's GPU classes; David 2026-10-04: "other
    leads should be encouraged to define the priority of their gpu jobs as backfil jobs or time
    sensitive. the COO can manage all that scheduling through coded rules"). Each owner declares in
    ~/<project>/.claude/gpu_jobs.json; bin/coo.py build compiles them into state/coo/gpu_classes.json
    (catalog id maintenance/gpu_classes), and tier() reads that file, mtime-cached and fail-open: a
    "<project>/<job>" (or "prefix*") of class `backfill` takes the backfill tier with no ageing, exactly
    as SPARK_GPU_BACKFILL=1 does. time-sensitive and unclassified jobs keep today's tier, and a Stocks
    entry is never applied while Stocks is closed (its tiers stay config/gpu.json's). A missing, corrupt
    or half-written file changes nothing.
  * THE EVENT LOG IS THE QUEUE'S RECORD (state/gpu/events.jsonl, catalog id
    maintenance/gpu_events). `release` carries the tier; a `grant` is written when the wait was
    over 1 s OR other waiters were in the field, and carries `others` and `others_min_tier`, so
    a lower tier winning over a higher one is countable (an inversion is granted tier >
    others_min_tier that `aged` does not explain). The file keeps the last EVENT_CAP lines,
    trimmed once it passes EVENT_TRIM_BYTES: at ~13k calls/day that is three to four days, the
    48 h the COO's GPU ledger needs with margin (it was 4000 lines, ~11 h, before 10-04).

CLI:  gpu.py status | queue | events [n] | selftest
"""
import contextlib
import fcntl
import json
import os
import random
import re
import subprocess
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
ANNOUNCE = os.path.join(DIR, "announce")
# Kept: the last EVENT_CAP lines. A trim is considered on ~2% of writes but only reads the file
# once it is past EVENT_TRIM_BYTES (a stat, not a read, on every other try), so a trim happens
# about once a day and the record always holds 60k-80k lines: >= 48 h at 25k events/day.
EVENT_CAP = 60000
EVENT_TRIM_BYTES = 10_000_000
STATUS_TAIL_BYTES = 2_000_000      # status() reads this much of the tail: ~16k events, about a day
BACKFILL_TIER = 40                 # when config/gpu.json predates the backfill tier
CLASSES = os.path.join(MC, "state/coo/gpu_classes.json")   # coo.py build compiles it daily
CLASS_PREFIX_MIN = 3               # a "prefix*" shorter than this is ignored (coo.py drops it too)

DEFAULTS = {
    "slots": 1, "lease_ttl_s": 1800, "wait_timeout_s": 2700, "age_step_s": 120,
    "max_age_bonus": 25, "affinity_bonus": 5, "poll_s": 0.4, "keep_alive": "30m",
    "tiers": {"interactive": 0, "Stocks": 10, "maintenance": 30, "backfill": BACKFILL_TIER},
    "default_tier": 20, "announce_pushes": True, "announce_min_gap_s": 600,
    "box_target_hours_per_day": 20,
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


def backfill_tier():
    return cfg().get("tiers", {}).get("backfill", BACKFILL_TIER)


def _backfill_env():
    return os.environ.get("SPARK_GPU_BACKFILL") == "1"


_cls_cache = {"key": None, "exact": frozenset(), "prefix": ()}


def _load_classes():
    """(exact {"<project>/<job>"}, prefix ((project, prefix), ...)) of the declared-backfill jobs, re-read
    only when the file's mtime or size changes. Fail-open: no file, bad JSON or a wrong shape -> empty,
    so every caller keeps its config/gpu.json tier. Stocks entries are skipped (Stocks is closed)."""
    try:
        st = os.stat(CLASSES)
    except OSError:
        _cls_cache.update(key=None, exact=frozenset(), prefix=())
        return _cls_cache["exact"], _cls_cache["prefix"]
    key = (CLASSES, st.st_mtime_ns, st.st_size)
    if _cls_cache["key"] != key:
        exact, prefix = set(), []
        try:
            with open(CLASSES) as f:
                jobs = json.load(f).get("jobs") or {}
            for k, e in jobs.items():
                if not isinstance(e, dict) or e.get("class") != "backfill" or e.get("applied") is False:
                    continue
                gp = str(e.get("gpu_project") or str(k).split("/", 1)[0])
                label = str(e.get("label") or str(k).split("/", 1)[-1])
                if gp.lower() == "stocks" or str(e.get("project") or "").lower() == "stocks" or not label:
                    continue
                if label.endswith("*"):
                    if len(label) - 1 >= CLASS_PREFIX_MIN:
                        prefix.append((gp, label[:-1]))
                else:
                    exact.add(f"{gp}/{label}")
        except Exception:
            exact, prefix = set(), []
        _cls_cache.update(key=key, exact=frozenset(exact), prefix=tuple(prefix))
    return _cls_cache["exact"], _cls_cache["prefix"]


def declared_backfill(proj, job):
    """True when the owner declared this job `backfill` in its .claude/gpu_jobs.json (compiled by coo.py).
    Never for Stocks; never raises."""
    try:
        if not proj or not job or str(proj).lower() == "stocks":
            return False
        exact, prefix = _load_classes()
        return f"{proj}/{job}" in exact or any(proj == p and job.startswith(x) for p, x in prefix)
    except Exception:
        return False


def tier(proj=None, job=None, interactive=None, backfill=None):
    """Tier for this caller. A "<project>/<job>" key wins over the bare project, so one
    job can be ranked apart from its siblings — the Bench is Stocks work, but it is
    5 hours of opportunistic background reading and must not outrank a market-hours job
    from the same project. A process launched as idle-time backfill (SPARK_GPU_BACKFILL=1)
    is the backfill tier whatever its project — unless a human is at the terminal. So is a job its
    owner declared `backfill` (state/coo/gpu_classes.json; never a Stocks job)."""
    c = cfg()
    if interactive if interactive is not None else _interactive():
        return c["tiers"].get("interactive", 0)
    if backfill if backfill is not None else _backfill_env():
        return backfill_tier()
    proj = proj or project()
    if declared_backfill(proj, job):
        return backfill_tier()
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
    except (OSError, TypeError):
        return False
    try:  # os.kill(pid, 0) succeeds for <defunct> zombies; check /proc to confirm
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(") ", 1)[1].split(" ", 1)[0] != "Z"
    except OSError:
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
            _trim_events()
    except Exception:
        pass


def _trim_events(path=None, cap=None, trigger_bytes=None):
    """Keep the last `cap` lines once the file is past `trigger_bytes`. Written to a temp file
    and swapped in (an appender racing the swap loses at most its one line; the old in-place
    rewrite could interleave a concurrent append into the middle of the file)."""
    path, cap = path or EVENTS, cap or EVENT_CAP
    if os.path.getsize(path) <= (trigger_bytes or EVENT_TRIM_BYTES):
        return False
    with open(path, errors="replace") as f:
        lines = f.readlines()
    if len(lines) <= cap:
        return False
    tmp = f"{path}.{os.getpid()}.trim"
    with open(tmp, "w") as f:
        f.writelines(lines[-cap:])
    os.replace(tmp, path)
    return True


def tail_events(nbytes=STATUS_TAIL_BYTES, path=None):
    """The newest events, reading at most `nbytes` from the end (the first partial line is
    dropped). Read-only; [] when there is no log."""
    try:
        with open(path or EVENTS, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - nbytes))
            buf = f.read()
    except OSError:
        return []
    lines = buf.decode(errors="replace").splitlines()
    if size > nbytes and lines:
        lines = lines[1:]
    out = []
    for l in lines:
        try:
            out.append(json.loads(l))
        except Exception:
            continue
    return out


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

def _waiters(clean=True):
    """The live waiters. clean=False is the read-only form (a sampler or a status page): a
    dead waiter's file is skipped, not deleted."""
    out = []
    try:
        for name in os.listdir(WAITERS):
            w = _read(os.path.join(WAITERS, name))
            if not w:
                continue
            if not _alive(w.get("pid", -1)):     # crashed before it got its turn
                if clean:
                    with contextlib.suppress(OSError):
                        os.unlink(os.path.join(WAITERS, name))
                continue
            out.append(w)
    except FileNotFoundError:
        pass
    return out


def is_backfill(w):
    """A backfill waiter: flagged by slot(), or any tier number at or past backfill's (40+)."""
    return bool(w.get("backfill")) or w.get("tier", cfg()["default_tier"]) >= backfill_tier()


def aged(w, now):
    """Ageing points this waiter has earned. Backfill earns none: it must never tie real work."""
    c = cfg()
    if is_backfill(w):
        return 0
    age = max(0, now - w.get("since", now))
    return min(c["max_age_bonus"], int(age // max(1, c["age_step_s"])))


def score(w, now, resident):
    """Lower wins. tier − ageing − affinity (backfill: tier − affinity). The affinity bonus is
    capped below one tier gap on purpose: it may reorder equals, never overtake a more
    important project."""
    c = cfg()
    aff = c["affinity_bonus"] if w.get("model") and w["model"] in resident else 0
    return w.get("tier", c["default_tier"]) - aged(w, now) - aff


def _holder_valid(h, now):
    return bool(h) and _alive(h.get("pid", -1)) and h.get("expires", 0) > now


def _try_admit(me, seen=None):
    """True when `me` now holds the slot. `seen`, when given, is filled with the tiers of the
    OTHER live waiters in the field this decision was made against (the grant event's
    others / others_min_tier)."""
    now = time.time()
    if seen is not None:
        seen["others"] = []
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
    if seen is not None:
        seen["others"] = [w.get("tier", cfg()["default_tier"]) for w in field if w.get("id") != me["id"]]
        seen["aged"] = aged(me, now)
    _write_atomic(HOLDER, {**me, "acquired": now, "expires": now + cfg()["lease_ttl_s"]})
    return True


def _waiter_path(wid):
    return os.path.join(WAITERS, f"{wid}.json")


def _should_announce(prev, pid, now, min_gap):
    """One push per job RUN. A run is a new pid outside the min gap: the same pid never
    re-announces (a 5-hour Bench process is one run, however many slots it takes), and
    the gap keeps a job that spawns a process per item from paging every item."""
    if prev and prev.get("pid") == pid:
        return False
    if prev and now - prev.get("time", 0) < min_gap:
        return False
    return True


def _announce(me):
    """Phone push when a batch job starts using the GPU (David 2026-08-31: "i need to see
    whenever a cron job is running ... a local model. those shouldn't be filtered out").
    Interactive callers stay silent — a human at a terminal announces nothing, the same
    rule the headless-session hook applies to Claude. A local call made from INSIDE a
    Claude session (CLAUDECODE in env) is also silent — the session already announced
    itself, and it reports its own failures (David 2026-08-31: "local calls don't need
    to notify during a session, only if something is wrong"). Fire-and-forget Popen so
    a slow ntfy can never delay the inference; notify_policy.json decides phone vs
    ledger — as of 2026-08-31 these all tier digest (rollup only)."""
    with contextlib.suppress(Exception):
        if _interactive() or os.environ.get("CLAUDECODE"):
            return
        c = cfg()
        if not c.get("announce_pushes", True):
            return
        os.makedirs(ANNOUNCE, exist_ok=True)
        key = re.sub(r"[^A-Za-z0-9._-]", "_", f"{me['project']}__{me['job']}")[:120]
        path = os.path.join(ANNOUNCE, key + ".json")
        now = time.time()
        if not _should_announce(_read(path), me["pid"], now, c.get("announce_min_gap_s", 600)):
            return
        _write_atomic(path, {"pid": me["pid"], "time": now})
        ch = {"Stocks": "stocks", "clientco-db": "clientco"}.get(me["project"], "maintenance")
        msg = me["job"] + (f" · {me['model']}" if me.get("model") else "")
        with open(os.devnull, "wb") as null:
            subprocess.Popen([os.path.join(MC, "bin/notify.sh"), ch,
                              f"Local model — {me['project']}", msg],
                             stdout=null, stderr=null)


class Grant:
    """What `with gpu.slot(...) as g:` hands back — the slot's own clock, so a caller can
    meter the inference apart from the queue (2026-10-04: localllm's `secs` used to start
    before the slot and so counted the wait as GPU time).

    t_request  when slot() was entered          t_grant  when the work may start (granted,
    wait_s     t_grant - t_request                        or the fail-open bypass moment)
    held       True on a real grant, False on a bypass
    hold_s()   seconds since t_grant (final once the slot is released: t_release is set)
    Existing callers that write `with gpu.slot(...):` without `as` are unaffected."""
    __slots__ = ("t_request", "t_grant", "t_release", "held")

    def __init__(self, t_request):
        self.t_request = self.t_grant = t_request
        self.t_release = None
        self.held = False

    @property
    def wait_s(self):
        return max(0.0, self.t_grant - self.t_request)

    def hold_s(self):
        return max(0.0, (self.t_release or time.time()) - self.t_grant)


@contextlib.contextmanager
def slot(job=None, model=None, proj=None, timeout=None):
    """Hold the GPU for one inference. Blocks until this caller is the best waiter.
    Yields a Grant (t_grant, wait_s, hold_s()) for callers that meter the inference.

    Fails open: any internal error, or a wait past the timeout, lets the call proceed and
    records a `bypass` event rather than blocking a job forever.
    """
    c = cfg()
    me, held, t0 = None, False, time.time()
    t_grant = t0   # updated when the slot is actually granted; separates wait from hold
    g = Grant(t0)
    seen = {}
    try:
        _ensure()
        me = {"id": uuid.uuid4().hex[:12], "pid": os.getpid(), "since": t0,
              "project": proj or project() or "?", "job": job or os.path.basename(sys.argv[0]),
              "model": model}
        me["tier"] = tier(me["project"], me["job"])
        if me["tier"] == backfill_tier() and (_backfill_env() or declared_backfill(me["project"], me["job"])):
            me["backfill"] = True                # no ageing (score); the holder shows it too
        _write_atomic(_waiter_path(me["id"]), me)
        deadline = t0 + (timeout or c["wait_timeout_s"])
        while time.time() < deadline:
            with _locked():
                if _try_admit(me, seen):
                    held = True
                    t_grant = time.time()
                    break
            time.sleep(c["poll_s"] * (1 + random.random() * 0.5))
        wait_s = round(t_grant - t0, 1)
        if held:
            others = seen.get("others") or []
            if wait_s > 1 or others:
                event("grant", project=me["project"], job=me["job"], wait_s=wait_s,
                      tier=me["tier"], model=model, others=len(others),
                      others_min_tier=min(others) if others else None, aged=seen.get("aged", 0))
        else:
            event("bypass", project=me["project"], job=me["job"], tier=me["tier"],
                  wait_s=round(time.time() - t0, 1), reason="wait timeout")
    except Exception as e:                       # never let the scheduler break the work
        event("bypass", job=job or "?", reason=f"{type(e).__name__}: {e}"[:120])
    if me:                                       # inference proceeds on both paths
        _announce(me)
    # the work starts now on every path: a real grant (t_grant), or a bypass after the wait
    g.held = held
    g.t_grant = t_grant if held else time.time()
    try:
        yield g
    finally:
        g.t_release = time.time()
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
                          held_s=round(time.time() - t_grant, 1), model=model, tier=me["tier"])


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


USAGE = os.path.join(DIR, "..", "local_usage.jsonl")


REFUSAL = re.compile(r"I can'?t (help|assist|provide|comply)|I'?m (unable|not able) to|"
                     r"I must decline|as an AI|I cannot (help|assist|provide)|"
                     r"against my guidelines|not appropriate for me", re.I)


def record_usage(job=None, model=None, prompt_tokens=0, output_tokens=0, seconds=None,
                 proj=None, text=None, wait_s=None, hold_s=None):
    """Log one local inference's token counts.

    Recorded here rather than in each caller because this module already knows who is
    asking — the project and job labels are the same ones the queue orders by, so usage
    and priority are reported in the same terms.

    `seconds` is the inference (time HELD). A caller that knows the slot's clock also passes
    `hold_s` and `wait_s` (gpu.Grant), written as their own fields so a queue wait is never
    read as GPU time; both are optional and appended last, so every older caller still fits.

    Purely observational: any failure is swallowed. A ledger that can break a job is worse
    than no ledger.
    """
    try:
        with open(os.path.abspath(USAGE), "a") as f:
            f.write(json.dumps({
                "at": int(time.time()), "project": proj or project() or "?",
                "job": job or os.path.basename(sys.argv[0]), "model": model,
                "in": int(prompt_tokens or 0), "out": int(output_tokens or 0),
                "secs": round(seconds, 1) if seconds else None,
                **({"hold_s": round(hold_s, 1)} if hold_s is not None else {}),
                **({"wait_s": round(wait_s, 1)} if wait_s is not None else {}),
                # A refusal is a successful HTTP call returning nothing usable — the exact
                # silent-failure shape this box treats as the cardinal sin. Counted so the
                # "should we run an uncensored model?" question stays a measurement.
                **({"refused": True} if text and REFUSAL.search(text[:600]) else {})})
                    + "\n")
    except Exception:
        pass


# ---------------------------------------------------------------- reporting

def status():
    now = time.time()
    h = _read(HOLDER)
    if not _holder_valid(h, now):
        h = None
    field = sorted(_waiters(clean=False), key=lambda w: score(w, now, loaded_models()))  # read-only
    ev = tail_events()
    day = now - 86400
    recent = [e for e in ev if e.get("at", 0) > day]
    # the window these numbers really cover: 24 h, or less when the tail read (or the log
    # itself) starts later. It used to say "24h" over the last 600 lines, ~6 h (2026-10-04).
    window_s = int(now - max(day, min((e.get("at", now) for e in ev), default=now)))
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
    # What ran most recently, so the dashboard can answer "what is the GPU doing?" even
    # when it polls between two calls. A holder is only set for the seconds a request is
    # actually in flight; without this the panel reads "idle" all through a five-hour batch.
    last = None
    for e in reversed(ev):
        if e.get("ev") == "release":
            last = {"project": e.get("project"), "job": e.get("job"),
                    "held_s": e.get("held_s"), "ago_s": int(now - e.get("at", now))}
            break
    return {
        "holder": h and {"project": h["project"], "job": h["job"], "model": h.get("model"),
                         "held_s": round(now - h["acquired"], 1)},
        "last": last,
        "queue": [{"project": w["project"], "job": w["job"], "tier": w["tier"],
                   "waiting_s": round(now - w["since"], 1),
                   "score": score(w, now, loaded_models())} for w in field],
        "resident": loaded_models(),
        "day": {"by_project": by, "window_s": window_s, "window": _window_label(window_s),
                "bypasses": len([e for e in recent if e["ev"] == "bypass"]),
                "reclaims": len([e for e in recent if e["ev"] == "reclaim"])},
        "tiers": cfg()["tiers"], "slots": cfg()["slots"],
    }


def _window_label(window_s):
    return "24h" if window_s >= 86400 - 60 else f"{window_s / 3600:.1f}h"


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
        print(f"last {s['day']['window']}:")
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
        return 1 if _selftest() else 0
    else:                                        # an unknown argument never runs a mode
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2


def _selftest():
    """Ordering is the whole product, so it gets a test: build a synthetic field and check
    the winner is the one the doctrine says it should be."""
    c = cfg()
    now = time.time()
    def w(proj, age=0, model=None, t=None, bf=False):
        return {"id": proj + str(age), "project": proj, "job": "t", "since": now - age,
                "model": model, "tier": t if t is not None else c["tiers"].get(proj, c["default_tier"]),
                **({"backfill": True} if bf else {})}
    BF = backfill_tier()
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
        ("a Stocks job outranks desk:search",
         [w("search", t=c["tiers"].get("data-desk/desk:search", 20)), w("Stocks")], (), "Stocks"),
        ("desk:search outranks the Bench",
         [w("bench", t=c["tiers"].get("Stocks/the Bench", 15)),
          w("search", t=c["tiers"].get("data-desk/desk:search", 20))], (), "search"),
        # backfill (2026-10-04): below everything, and it never ages
        ("a waiting tier-10 beats an aged backfill",
         [w("Stocks"), w("bf", age=10 * 3600, t=BF, bf=True)], (), "Stocks"),
        ("an aged backfill never ties maintenance",
         [w("bf", age=10 * 3600, t=BF, bf=True), w("maintenance")], (), "maintenance"),
        ("nor with affinity on its side",
         [w("bf", age=10 * 3600, model="a", t=BF, bf=True), w("maintenance", model="b")], ("a",),
         "maintenance"),
        ("an unflagged tier-40 waiter does not age either",
         [w("bf", age=10 * 3600, t=BF), w("maintenance")], (), "maintenance"),
        ("backfill takes an empty field", [w("bf", t=BF, bf=True)], (), "bf"),
    ]
    bad = 0
    for name, field, resident, want in cases:
        got = winner(field, resident)
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    announce_cases = [
        ("first run of a job announces", _should_announce(None, 100, now, 600), True),
        ("same pid never re-announces",
         _should_announce({"pid": 100, "time": now - 9999}, 100, now, 600), False),
        ("new pid inside the gap is held",
         _should_announce({"pid": 100, "time": now - 60}, 200, now, 600), False),
        ("new pid past the gap announces",
         _should_announce({"pid": 100, "time": now - 700}, 200, now, 600), True),
    ]
    for name, got, want in announce_cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    # zombie liveness: a <defunct> child must NOT pass _alive() (the 2026-09-09 starvation bug)
    cpid = os.fork()
    if cpid == 0:
        os._exit(0)
    time.sleep(0.05)  # let child enter zombie state before parent calls waitpid
    zombie_alive = _alive(cpid)
    os.waitpid(cpid, 0)
    after_reap = _alive(cpid)
    self_alive = _alive(os.getpid())
    zombie_cases = [
        ("zombie pid is not alive", zombie_alive, False),
        ("reaped pid is not alive", after_reap, False),
        ("self is alive", self_alive, True),
    ]
    for name, got, want in zombie_cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    # the meter (2026-10-04): a Grant splits queue wait from hold, and record_usage writes
    # both apart — no real slot is taken and the live ledger is never touched
    import tempfile
    g = Grant(now)
    g.t_grant, g.t_release, g.held = now + 2.0, now + 2.0, True
    global USAGE
    real_usage, fd = USAGE, tempfile.NamedTemporaryFile("w+", suffix=".jsonl", delete=False)
    fd.close()
    try:
        USAGE = fd.name
        record_usage(job="selftest", model="fake", prompt_tokens=1, output_tokens=1,
                     seconds=g.hold_s(), proj="selftest", wait_s=g.wait_s, hold_s=g.hold_s())
        record_usage("selftest", "fake", 1, 1, 0.5, "selftest", None)   # an old positional caller
        rows = [json.loads(l) for l in open(fd.name) if l.strip()]
    finally:
        USAGE = real_usage
        os.unlink(fd.name)
    meter_cases = [
        ("a 2 s queue wait books wait_s=2", abs(g.wait_s - 2.0) < 1e-6, True),
        ("and holds ~0 s", g.hold_s() < 1e-6, True),
        ("usage row carries wait_s and hold_s apart",
         (rows[0].get("wait_s"), rows[0].get("hold_s"), rows[0].get("secs")), (2.0, 0.0, None)),
        ("an older caller's row is unchanged",
         ("wait_s" in rows[1] or "hold_s" in rows[1], rows[1].get("secs")), (False, 0.5)),
    ]
    for name, got, want in meter_cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    backfill_cases = _selftest_backfill(c, now, cpid)
    for name, got, want in backfill_cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    class_cases = _selftest_classes(c, now)
    for name, got, want in class_cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<42} -> {got}")
    total = (len(cases) + len(announce_cases) + len(zombie_cases) + len(meter_cases) + len(backfill_cases)
             + len(class_cases))
    print(f"{total - bad}/{total} passed")
    return bad


def _selftest_backfill(c, now, dead_pid):
    """The 2026-10-04 additions: the backfill tier, no ageing for it, the grant's
    others_min_tier, release tier, the read-only waiter scan, the trim and the status window.
    One real slot is taken against a scratch state dir (never state/gpu), with the announce
    push and the ollama /api/ps lookup stubbed, so nothing leaves the process."""
    import shutil
    import tempfile
    global DIR, WAITERS, HOLDER, LOCK, EVENTS, ANNOUNCE, _announce
    BF = backfill_tier()
    out = [
        ("SPARK_GPU_BACKFILL -> the backfill tier",
         tier("data-desk", "desk:shift", interactive=False, backfill=True), BF),
        ("backfill tier is below maintenance", BF > c["tiers"].get("maintenance", 30), True),
        ("a human at the terminal still outranks it",
         tier("data-desk", "desk:shift", interactive=True, backfill=True), c["tiers"].get("interactive", 0)),
        ("no env, no change for any caller",
         tier("Stocks", "x", interactive=False, backfill=False), c["tiers"].get("Stocks", 10)),
        ("a backfill waiter earns no ageing",
         aged({"tier": BF, "backfill": True, "since": now - 36000}, now), 0),
        ("a tier-30 waiter still ages",
         aged({"tier": 30, "since": now - 36000}, now), c["max_age_bonus"]),
        ("the status label says the window it read", (_window_label(86400), _window_label(23040)),
         ("24h", "6.4h")),
    ]
    saved = (DIR, WAITERS, HOLDER, LOCK, EVENTS, ANNOUNCE, _announce, dict(_ps))
    d = tempfile.mkdtemp(prefix="gpu-selftest-")
    try:
        DIR, WAITERS = d, os.path.join(d, "waiters")
        HOLDER, LOCK = os.path.join(d, "holder.json"), os.path.join(d, "lock")
        EVENTS, ANNOUNCE = os.path.join(d, "events.jsonl"), os.path.join(d, "announce")
        _announce = lambda me: None
        _ps.update(at=time.time() + 3600, models=[])
        _ensure()
        _write_atomic(_waiter_path("other30"), {"id": "other30", "pid": os.getpid(), "since": now,
                                                "project": "maintenance", "job": "o", "tier": 30})
        with slot(job="selftest", proj="Stocks", timeout=5) as g1:
            pass
        os.unlink(_waiter_path("other30"))
        with slot(job="selftest-alone", proj="Stocks", timeout=5):
            pass
        ev = [json.loads(l) for l in open(EVENTS) if l.strip()]
        grants = [e for e in ev if e["ev"] == "grant"]
        rels = [e for e in ev if e["ev"] == "release"]
        out += [
            ("a grant with others waiting is logged", (g1.held, len(grants), grants[0].get("job") if grants else None),
             (True, 1, "selftest")),
            ("and carries others_min_tier",
             (grants[0].get("others"), grants[0].get("others_min_tier")) if grants else None, (1, 30)),
            ("an uncontended fast grant stays unlogged", any(e.get("job") == "selftest-alone" for e in grants), False),
            ("release carries the tier", [("tier" in e) for e in rels], [True, True]),
        ]
        _write_atomic(_waiter_path("dead"), {"id": "dead", "pid": dead_pid, "since": now, "tier": 20})
        kept = os.path.exists(_waiter_path("dead")) and not _waiters(clean=False) \
            and os.path.exists(_waiter_path("dead"))
        _waiters()
        out += [("read-only scan skips a dead waiter, keeps its file", kept, True),
                ("the admitting scan still clears it", os.path.exists(_waiter_path("dead")), False)]
        with open(EVENTS, "w") as f:
            f.writelines(json.dumps({"at": i, "ev": "release"}) + "\n" for i in range(50))
        small = _trim_events(EVENTS, cap=10, trigger_bytes=10 ** 6)
        big = _trim_events(EVENTS, cap=10, trigger_bytes=100)
        rows = [json.loads(l)["at"] for l in open(EVENTS)]
        tail = [e["at"] for e in tail_events(64, EVENTS)]
        out += [("trim waits for the byte trigger", small, False),
                ("then keeps the newest cap lines", (big, rows[0], rows[-1], len(rows)), (True, 40, 49, 10)),
                ("tail read drops its partial first line", bool(tail) and tail[-1] == 49 and tail[0] > 40, True)]
    finally:
        DIR, WAITERS, HOLDER, LOCK, EVENTS, ANNOUNCE, _announce = saved[:7]
        _ps.clear()
        _ps.update(saved[7])
        shutil.rmtree(d, ignore_errors=True)
    return out


def _selftest_classes(c, now):
    """The declared classes (2026-10-05) against a temp gpu_classes.json, never the live one: a declared
    backfill job takes the backfill tier and earns no ageing, a prefix matches, time-sensitive and
    unclassified keep their tier, a Stocks entry is ignored, and a missing or corrupt file changes nothing."""
    import shutil
    import tempfile
    global CLASSES
    BF = backfill_tier()
    m30 = c["tiers"].get("maintenance", 30)
    bench = c["tiers"].get("Stocks/the Bench", c["tiers"].get("Stocks", 10))
    saved, cache = CLASSES, dict(_cls_cache)
    d = tempfile.mkdtemp(prefix="gpu-classes-selftest-")
    T = lambda p, j: tier(p, j, interactive=False, backfill=False)  # noqa: E731
    try:
        CLASSES = os.path.join(d, "gpu_classes.json")
        out = [("no classes file -> tiers unchanged", (T("maintenance", "model watch"), T("Stocks", "the Bench")),
                (m30, bench))]
        doc = {"jobs": {
            "maintenance/model watch": {"project": "maintenance", "gpu_project": "maintenance",
                                        "label": "model watch", "class": "backfill", "applied": True},
            "maintenance/sentinel.py": {"project": "maintenance", "gpu_project": "maintenance",
                                        "label": "sentinel.py", "class": "time-sensitive", "applied": True},
            "hbs/hbs-extract-*": {"project": "hbs", "gpu_project": "hbs", "label": "hbs-extract-*",
                                  "class": "backfill", "applied": True},
            "hbs/x*": {"project": "hbs", "gpu_project": "hbs", "label": "x*", "class": "backfill"},
            "Stocks/the Bench": {"project": "stocks", "gpu_project": "Stocks", "label": "the Bench",
                                 "class": "backfill", "applied": True},
            "Stocks/grunt:qoq": {"project": "stocks", "gpu_project": "Stocks", "label": "grunt:qoq",
                                 "class": "backfill"}}}
        with open(CLASSES, "w") as f:
            json.dump(doc, f)
        t_bf = T("maintenance", "model watch")
        out += [
            ("declared backfill -> the backfill tier", t_bf, BF),
            ("and earns no ageing however long it waits", aged({"tier": t_bf, "since": now - 36000}, now), 0),
            ("so maintenance work still beats it",
             min([{"id": "a", "project": "bf", "tier": t_bf, "since": now - 36000},
                  {"id": "b", "project": "maintenance", "tier": m30, "since": now}],
                 key=lambda x: (score(x, now, ()), x["since"]))["project"], "maintenance"),
            ("a prefix* entry matches a per-run label", T("hbs", "hbs-extract-netflix"), BF),
            ("a too-short prefix is ignored", T("hbs", "xyz"), c["tiers"].get("hbs", c["default_tier"])),
            ("time-sensitive keeps its tier", T("maintenance", "sentinel.py"), m30),
            ("unclassified keeps its tier", T("maintenance", "daily log"), m30),
            ("a Stocks backfill entry is ignored", (T("Stocks", "the Bench"), T("Stocks", "grunt:qoq")),
             (bench, c["tiers"].get("Stocks", 10))),
            ("a human at the terminal still outranks it",
             tier("maintenance", "model watch", interactive=True, backfill=False), c["tiers"].get("interactive", 0)),
            ("same label, another project: no match", T("poker", "model watch"), c["tiers"].get("poker", c["default_tier"])),
        ]
        with open(CLASSES, "w") as f:
            f.write('{"jobs": {"maintenance/model watch": ')      # half-written / corrupt
        os.utime(CLASSES, ns=(time.time_ns(), time.time_ns() + 10 ** 9))
        out.append(("corrupt file -> tiers unchanged (fail-open)", T("maintenance", "model watch"), m30))
        with open(CLASSES, "w") as f:
            json.dump({"jobs": ["not", "a", "dict"]}, f)
        out.append(("wrong shape -> tiers unchanged", T("maintenance", "model watch"), m30))
        os.unlink(CLASSES)
        out.append(("file removed -> tiers unchanged", T("maintenance", "model watch"), m30))
    finally:
        CLASSES = saved
        _cls_cache.clear()
        _cls_cache.update(cache)
        _cls_cache["key"] = None                 # re-read the live file on the next call
        shutil.rmtree(d, ignore_errors=True)
    return out


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    sys.exit(_cli(sys.argv) or 0)
