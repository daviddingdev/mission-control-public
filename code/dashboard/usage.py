#!/usr/bin/env python3
"""Real token usage from Claude Code transcripts (~/.claude/projects/*/*.jsonl).

Covers everything that runs ON THIS BOX: headless cron sessions and interactive
sessions (terminal + claude.ai Remote Control both execute here). claude.ai chat
and cloud-hosted Code sessions never touch this disk and are invisible — say so
in the UI, don't pretend.

Incremental: per-file aggregates cached by (mtime,size) in state/usage_cache.json,
so only new/updated transcripts are re-parsed on each call — and since 2026-09-24 only the
bytes APPENDED to them: an entry remembers how far into its file it has read (`off`), so a
live 50 MB session transcript costs a seek and its newest lines, not a full re-read on every
refresh. The cache is also kept in memory between calls (reloaded only if another process
rewrote the file), instead of re-parsing its 330 KB each time.

Metric: "processed" = input + output + cache_creation (real work; excludes
cache_read, which is bulk-but-cheap replay and reported separately)."""
import json, os, time, glob, threading

HOME = os.path.expanduser("~")
CACHE = f"{HOME}/maintenance/state/usage_cache.json"

# first-user-message prefix -> job label (order matters, first match wins)
JOB_PREFIXES = [
    ("You are the monthly DB-delta ingest", "clientco-db · wiki ingest"),
    ("# Monthly Spark Maintenance Sweep", "Mission Control · monthly sweep"),
    ("You are a headless maintenance session", "Mission Control · monthly sweep"),
    ("# Weekly Frontier Scan", "Mission Control · frontier scan"),
    ("You are a headless research session", "Mission Control · frontier scan"),
    ("You are writing a DESIGN MEMO", "Mission Control · design memo"),
    ("You are executing the green-lit experiment", "Mission Control · experiment"),
    ("Refresh the weekly candidate board", "Stocks · candidate board"),
    ("You are David's investment strategist", "Stocks · weekly digest"),
    ("You are the autonomous trading agent", "Stocks · trading session"),
    ("READ-ONLY sync of the BrokerB", "Stocks · agent sync"),
]


# job label -> work-type group (what David actually wants totals for)
GROUPS = {
    "Stocks · trading session": "Trading agent",
    "Stocks · agent sync": "Trading agent",
    "Stocks · weekly digest": "Investing research (scheduled)",
    "Stocks · candidate board": "Investing research (scheduled)",
    "clientco-db · wiki ingest": "Factory intelligence",
    "Mission Control · monthly sweep": "Platform upkeep",
    "Mission Control · frontier scan": "Platform upkeep",
    "Mission Control · design memo": "Platform upkeep",
    "Mission Control · experiment": "Platform upkeep",
}


def _classify(first_user, proj):
    fu = (first_user or "").lstrip()
    for pre, label in JOB_PREFIXES:
        if fu.startswith(pre):
            return "scheduled", label, GROUPS.get(label, "Other scheduled")
    proj = proj.split("--")[0] or "general"
    nice = {"home": "general", "Stocks": "Stocks", "clientco-db": "clientco-db",
            "maintenance": "Mission Control", "poker": "poker"}.get(proj, proj)
    return "interactive", f"interactive · {proj}", f"Sessions — {nice}"


def _parse_file(path, proj, prev=None):
    """-> {'kind','label','group','days':{date:{'in','out','cc','cr','msgs'}},'off','fu'}

    With `prev` (this file's entry, parsed up to prev['off']), only the bytes after it are
    read. `fu` says whether the first user message has been seen: the classification is fixed
    by it, so once it is, the label never changes. Only complete lines are read; a line still
    being written is picked up on the next call."""
    days, fu, cls, off = {}, False, None, 0
    if prev and "off" in prev and isinstance(prev.get("days"), dict):
        off = prev["off"]
        days = {d: dict(u) for d, u in prev["days"].items()}
        fu = bool(prev.get("fu"))
        if fu:
            cls = (prev.get("kind"), prev.get("label"), prev.get("group"))
    first_user = None
    try:
        with open(path, "rb") as fh:
            fh.seek(off)
            data = fh.read()
    except Exception:
        data = b""
    nl = data.rfind(b"\n")
    body = data[:nl + 1] if nl >= 0 else b""
    for raw in body.split(b"\n"):
        if not raw.strip():
            continue
        try:
            j = json.loads(raw.decode("utf-8", "replace"))
        except Exception:
            continue
        try:
            t = j.get("type")
            if t == "user" and not fu:
                c = j.get("message", {}).get("content")
                if isinstance(c, list):
                    c = " ".join(b.get("text", "") for b in c if isinstance(b, dict))
                first_user = (c or "")[:120]
                fu = True
            elif t == "assistant":
                u = j.get("message", {}).get("usage") or {}
                if not u:
                    continue
                ts = j.get("timestamp")
                try:
                    day = ts[:10] if isinstance(ts, str) else time.strftime("%Y-%m-%d")
                except Exception:
                    day = time.strftime("%Y-%m-%d")
                d = days.setdefault(day, {"in": 0, "out": 0, "cc": 0, "cr": 0, "msgs": 0})
                d["in"] += u.get("input_tokens", 0) or 0
                d["out"] += u.get("output_tokens", 0) or 0
                d["cc"] += u.get("cache_creation_input_tokens", 0) or 0
                d["cr"] += u.get("cache_read_input_tokens", 0) or 0
                d["msgs"] += 1
        except Exception:
            continue
    kind, label, group = cls or _classify(first_user, proj)
    return {"kind": kind, "label": label, "group": group, "days": days,
            "off": off + len(body), "fu": fu}


_MEM = {"sig": None, "cache": None}
_LOCK = threading.Lock()


def _file_sig(path):
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def usage(days_back=30):
    with _LOCK:
        return _usage(days_back)


def _usage(days_back):
    if _MEM["cache"] is not None and _MEM["sig"] == _file_sig(CACHE):
        cache = _MEM["cache"]
    else:
        try:
            cache = json.load(open(CACHE))
        except Exception:
            cache = {}
    changed = False
    for d in glob.glob(f"{HOME}/.claude/projects/*/"):
        proj = os.path.basename(d.rstrip("/")).replace("-home-user", "").strip("-") or "home"
        proj = proj.split("--claude-worktrees")[0].strip("-") or "home"
        for f in glob.glob(d + "*.jsonl"):
            try:
                st = os.stat(f)
            except Exception:
                continue
            key = f
            sig = f"{int(st.st_mtime)}:{st.st_size}"
            prev = cache.get(key) or {}
            if prev.get("sig") != sig:
                # grown: carry on from where the last read stopped; shrunk or unknown: re-read
                grew = isinstance(prev.get("off"), int) and st.st_size >= prev["off"]
                cache[key] = {"sig": sig, **_parse_file(f, proj, prev if grew else None)}
                changed = True
    if changed:
        os.makedirs(os.path.dirname(CACHE), exist_ok=True)
        tmp = f"{CACHE}.{os.getpid()}.tmp"
        with open(tmp, "w") as fh:
            json.dump(cache, fh)
        os.replace(tmp, CACHE)              # a reader never sees half a file
    _MEM.update(sig=_file_sig(CACHE), cache=cache)
    # aggregate
    cutoff = time.strftime("%Y-%m-%d", time.localtime(time.time() - days_back * 86400))
    daily, jobs = {}, {}
    for e in cache.values():
        if not isinstance(e, dict) or "days" not in e:
            continue
        touched = False
        for day, u in e["days"].items():
            if day < cutoff:
                continue
            touched = True
            proc = u["in"] + u["out"] + u["cc"]
            dd = daily.setdefault(day, {"scheduled": 0, "interactive": 0, "cache_read": 0})
            dd[e["kind"]] += proc
            dd["cache_read"] += u["cr"]
            jj = jobs.setdefault(e["label"], {"kind": e["kind"], "sessions": 0, "proc": 0,
                                              "out": 0, "cr": 0})
            jj["proc"] += proc
            jj["out"] += u["out"]
            jj["cr"] += u["cr"]
        if touched:
            jobs[e["label"]]["sessions"] += 1   # one transcript file = one session
    # aggregate labels into work-type groups (cache entries may predate 'group' field)
    groups = {}
    for e in cache.values():
        if not isinstance(e, dict) or "days" not in e:
            continue
        recent = {d: u for d, u in e["days"].items() if d >= cutoff}
        if not recent:
            continue
        g = e.get("group") or _classify_label_fallback(e)
        gg = groups.setdefault(g, {"kind": e["kind"], "sessions": 0, "proc": 0, "out": 0, "cr": 0})
        gg["sessions"] += 1
        for u in recent.values():
            gg["proc"] += u["in"] + u["out"] + u["cc"]
            gg["out"] += u["out"]
            gg["cr"] += u["cr"]
    group_rows = [{"group": g, **v, "avg": v["proc"] // max(v["sessions"], 1)}
                  for g, v in groups.items()]
    group_rows.sort(key=lambda r: -r["proc"])
    return {"daily": [{"date": k, **v} for k, v in sorted(daily.items())],
            "groups": group_rows, "generated_at": int(time.time())}


def _classify_label_fallback(e):
    label = e.get("label", "")
    if e.get("kind") == "scheduled":
        return GROUPS.get(label, "Other scheduled")
    proj = label.replace("interactive · ", "").split("--")[0] or "general"
    nice = {"home": "general", "maintenance": "Mission Control"}.get(proj, proj)
    return f"Sessions — {nice}"
