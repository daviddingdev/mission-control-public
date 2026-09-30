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
cache_read, which is bulk-but-cheap replay and reported separately), plus what a server-side
ADVISOR processed for that message (usage.iterations[type=advisor_message], Fable 5.1 since
2026-09-25), also reported apart as `advisor`.

ONE API MESSAGE, COUNTED ONCE (2026-09-26). Claude Code writes an assistant message as one
transcript line per content block (thinking, text, each tool_use), and every one of those lines
repeats the message's full `usage`. Counting per line overstated every figure here by 3-7x
(a Stocks job: 90 lines for 30 messages, output 407k counted for 55k real). Lines of one message
are consecutive, so a line whose message id equals the last one counted is skipped; the last id
is kept with the file's entry so an incremental read that splits a message still counts it once.
Entries from before this rule (no `v`) are re-read from the start once."""
_V = 4            # 2: one count per message; 3: session ids; 4: Eastern days, page names (09-26)
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


# the project names the page uses everywhere (index.html PROJ): "HBS", "Home (~)", never "hbs",
# "general" (2026-09-26 polish: Usage was the one page that spelled them its own way)
_NICE = {"home": "Home (~)", "general": "Home (~)", "hbs": "HBS", "maintenance": "Mission Control",
         "stocks": "Stocks", "clientco-db": "clientco-db", "poker": "poker", "thesis": "thesis",
         "poker-appstore": "poker-appstore", "data-desk": "Data Desk"}


def _nice(proj):
    return _NICE.get(str(proj).lower(), proj)


def _et_day(ts):
    """The Eastern calendar day of an ISO timestamp — the dashboard reads ET (CLAUDE.md, TIME)."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return ts[:10]


def _et_today(back_days=0):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    return (datetime.now(ZoneInfo("America/New_York")) - timedelta(days=back_days)).strftime("%Y-%m-%d")


def _classify(first_user, proj):
    fu = (first_user or "").lstrip()
    for pre, label in JOB_PREFIXES:
        if fu.startswith(pre):
            return "scheduled", label, GROUPS.get(label, "Other scheduled")
    proj = proj.split("--")[0] or "general"
    return "interactive", f"interactive · {proj}", f"Sessions — {_nice(proj)}"


def _parse_file(path, proj, prev=None):
    """-> {'kind','label','group','days':{date:{'in','out','cc','cr','adv','msgs'}},'off','fu','lid','v'}

    With `prev` (this file's entry, parsed up to prev['off']), only the bytes after it are
    read. `fu` says whether the first user message has been seen: the classification is fixed
    by it, so once it is, the label never changes. `lid` is the last message id counted (one
    message = one count, see the module doc). Only complete lines are read; a line still being
    written is picked up on the next call."""
    days, fu, cls, off, lid = {}, False, None, 0, None
    if prev and "off" in prev and isinstance(prev.get("days"), dict):
        off = prev["off"]
        days = {d: dict(u) for d, u in prev["days"].items()}
        fu = bool(prev.get("fu"))
        lid = prev.get("lid")
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
                m = j.get("message", {}) or {}
                u = m.get("usage") or {}
                if not u:
                    continue
                mid = m.get("id")
                if mid and mid == lid:
                    continue                      # another content block of a message already counted
                lid = mid or lid
                ts = j.get("timestamp")
                try:
                    day = _et_day(ts) if isinstance(ts, str) else _et_today()
                except Exception:
                    day = time.strftime("%Y-%m-%d")
                d = days.setdefault(day, {"in": 0, "out": 0, "cc": 0, "cr": 0, "adv": 0, "msgs": 0})
                d["in"] += u.get("input_tokens", 0) or 0
                d["out"] += u.get("output_tokens", 0) or 0
                d["cc"] += u.get("cache_creation_input_tokens", 0) or 0
                d["cr"] += u.get("cache_read_input_tokens", 0) or 0
                for it in u.get("iterations") or ():
                    if isinstance(it, dict) and it.get("type") == "advisor_message":
                        d["adv"] = d.get("adv", 0) + sum(it.get(k, 0) or 0 for k in (
                            "input_tokens", "output_tokens", "cache_creation_input_tokens"))
                d["msgs"] += 1
        except Exception:
            continue
    kind, label, group = cls or _classify(first_user, proj)
    return {"kind": kind, "label": label, "group": group, "days": days,
            "off": off + len(body), "fu": fu, "lid": lid, "v": _V}


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


_LEDGER = {"sig": None, "kinds": {}}


def _ledger_kinds():
    """{session id: 'scheduled' | 'interactive'} from the session ledger (claude-session-notify.sh
    writes `headless` for every session since 2026-08-12). Who ran a session is a fact there; the
    prompt-prefix guess in _classify missed every job not in JOB_PREFIXES — 302 headless sessions,
    most of Stocks, were counted as David's own (2026-09-26)."""
    p = f"{HOME}/maintenance/state/claude_sessions.jsonl"
    sig = _file_sig(p)
    if sig and sig == _LEDGER["sig"]:
        return _LEDGER["kinds"]
    kinds = {}
    try:
        with open(p, errors="replace") as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                sid, h = r.get("session"), r.get("headless")
                if sid and isinstance(h, bool):
                    kinds[sid] = "scheduled" if h else "interactive"
    except OSError:
        pass
    _LEDGER.update(sig=sig, kinds=kinds)
    return kinds


def _usage(days_back):
    if _MEM["cache"] is not None and _MEM["sig"] == _file_sig(CACHE):
        cache = _MEM["cache"]
    else:
        try:
            cache = json.load(open(CACHE))
        except Exception:
            cache = {}
    changed = False
    # ONE FILE, ONE ENTRY, keyed by its real path (2026-09-26). The 09-24 build ran test servers
    # with HOME set to scratch folders that symlink to the real ~/.claude and ~/maintenance/state,
    # so each wrote the SAME transcripts into the live cache under its own path — 16 copies,
    # 97% of what this page counted. Keys are realpaths now, a file reached twice is read once,
    # and an entry from outside the real projects root is dropped.
    root = os.path.realpath(f"{HOME}/.claude/projects") + os.sep
    for k in [k for k in cache if not k.startswith(root)]:
        del cache[k]
        changed = True
    seen = set()
    for d in glob.glob(f"{HOME}/.claude/projects/*/"):
        proj = os.path.basename(d.rstrip("/")).replace("-home-user", "").strip("-") or "home"
        proj = proj.split("--claude-worktrees")[0].strip("-") or "home"
        # the session's own transcript, and (2026-09-26) every subagent and workflow agent it ran:
        # <sid>/subagents/**.jsonl is real Claude work on the same window — 764 MB of transcripts
        # in 30 days against 670 MB of top-level ones, none of it counted before
        for f in glob.glob(d + "*.jsonl") + glob.glob(d + "*/subagents/**/*.jsonl", recursive=True):
            key = os.path.realpath(f)
            if key in seen or not key.startswith(root):
                continue
            seen.add(key)
            try:
                st = os.stat(key)
            except Exception:
                continue
            sig = f"{int(st.st_mtime)}:{st.st_size}"
            prev = cache.get(key) or {}
            if prev.get("sig") != sig or prev.get("v") != _V:
                # grown: carry on from where the last read stopped; shrunk, unknown or counted
                # under the old per-line rule: re-read from the start
                grew = (prev.get("v") == _V and isinstance(prev.get("off"), int)
                        and st.st_size >= prev["off"])
                ent = _parse_file(key, proj, prev if grew else None)
                rel = key[len(root):].split(os.sep)
                ent["sid"] = rel[1] if len(rel) > 2 else rel[-1][:-6]   # a subagent: its session
                ent["sub"] = len(rel) > 2
                cache[key] = {"sig": sig, **ent}
                changed = True
    if changed:
        os.makedirs(os.path.dirname(CACHE), exist_ok=True)
        tmp = f"{CACHE}.{os.getpid()}.tmp"
        with open(tmp, "w") as fh:
            json.dump(cache, fh)
        os.replace(tmp, CACHE)              # a reader never sees half a file
    _MEM.update(sig=_file_sig(CACHE), cache=cache)
    # who ran it: the ledger first (by session id; a subagent inherits its session's), the prompt
    # prefix only for sessions from before the ledger. A subagent is filed under its session's
    # label, so a job's cost includes the agents it ran; it is not a session of its own.
    lk = _ledger_kinds()
    top = {e.get("sid"): e for e in cache.values() if isinstance(e, dict) and not e.get("sub")}

    def who(e):
        base = top.get(e.get("sid"), e) if e.get("sub") else e
        kind, label, group = base.get("kind"), base.get("label"), base.get("group")
        k = lk.get(e.get("sid"))
        if k == "scheduled" and kind != "scheduled":
            kind = "scheduled"
            label = str(label or "").replace("interactive · ", "headless · ", 1) or "headless"
            group = str(group or "").replace("Sessions — ", "Scheduled — ", 1) or "Other scheduled"
        elif k == "interactive" and kind != "interactive":
            kind = "interactive"
        return kind or "interactive", label or "interactive", group
    # aggregate
    # `days_back` Eastern days INCLUDING today — "30 days" means 30 bars, not 31 (2026-09-26 polish)
    cutoff = _et_today(days_back - 1)
    daily, jobs = {}, {}
    for e in cache.values():
        if not isinstance(e, dict) or "days" not in e:
            continue
        kind, label, _g = who(e)
        touched = False
        for day, u in e["days"].items():
            if day < cutoff:
                continue
            touched = True
            adv = u.get("adv", 0) or 0
            proc = u["in"] + u["out"] + u["cc"] + adv
            dd = daily.setdefault(day, {"scheduled": 0, "interactive": 0, "cache_read": 0, "advisor": 0})
            dd[kind] += proc
            dd["cache_read"] += u["cr"]
            dd["advisor"] += adv
            jj = jobs.setdefault(label, {"kind": kind, "sessions": 0, "proc": 0,
                                         "out": 0, "cr": 0})
            jj["proc"] += proc
            jj["out"] += u["out"]
            jj["cr"] += u["cr"]
        if touched and not e.get("sub"):
            jobs[label]["sessions"] += 1        # one top-level transcript = one session
    # aggregate labels into work-type groups (cache entries may predate 'group' field)
    groups = {}
    for e in cache.values():
        if not isinstance(e, dict) or "days" not in e:
            continue
        recent = {d: u for d, u in e["days"].items() if d >= cutoff}
        if not recent:
            continue
        kind, _l, g = who(e)
        g = g or _classify_label_fallback(e)
        gg = groups.setdefault(g, {"kind": kind, "sessions": 0, "proc": 0, "out": 0, "cr": 0})
        gg["sessions"] += 0 if e.get("sub") else 1
        for u in recent.values():
            gg["proc"] += u["in"] + u["out"] + u["cc"] + (u.get("adv", 0) or 0)
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
    return f"Sessions — {_nice(proj)}"


def selftest():
    """One message written as three lines counts once — also when an incremental read splits it —
    and its advisor iterations are counted apart. Throwaway files only."""
    import tempfile
    ok = True

    def check(name, cond, info=""):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name + (f"  — {info}" if info and not cond else ""))
        ok = ok and bool(cond)

    u1 = {"input_tokens": 5, "output_tokens": 100, "cache_creation_input_tokens": 1000,
          "cache_read_input_tokens": 50000,
          "iterations": [{"type": "message"}, {"type": "advisor_message", "input_tokens": 30000,
                                               "output_tokens": 300}]}
    u2 = {"input_tokens": 1, "output_tokens": 40, "cache_creation_input_tokens": 200,
          "cache_read_input_tokens": 51000}
    ln = lambda mid, u, blk: json.dumps({"type": "assistant", "timestamp": "2026-09-26T04:31:00Z",
                                         "message": {"id": mid, "model": "claude-opus-5-5", "usage": u,
                                                     "content": [{"type": blk}]}}) + "\n"
    first = json.dumps({"type": "user", "message": {"content": "You are writing COMPANY ONE-PAGERS"}}) + "\n"
    part1 = first + ln("m1", u1, "thinking") + ln("m1", u1, "text")
    part2 = ln("m1", u1, "tool_use") + ln("m2", u2, "thinking") + ln("m2", u2, "tool_use")
    d = tempfile.mkdtemp(prefix="usage-selftest.")
    f = os.path.join(d, "t.jsonl")
    with open(f, "w") as fh:
        fh.write(part1 + part2)
    whole = _parse_file(f, "Stocks")["days"]["2026-09-26"]
    check("one message on three lines counts once", whole["msgs"] == 2 and whole["out"] == 140
          and whole["cc"] == 1200 and whole["cr"] == 101000, whole)
    check("the advisor's tokens are counted apart", whole.get("adv") == 30300, whole)
    with open(f, "w") as fh:
        fh.write(part1)
    e = _parse_file(f, "Stocks")
    with open(f, "a") as fh:
        fh.write(part2)
    inc = _parse_file(f, "Stocks", e)["days"]["2026-09-26"]
    check("an incremental read that splits a message still counts it once", inc == whole, (inc, whole))

    # a real home, a scratch "home" that symlinks to it (the 09-24 test servers), a subagent, a ledger
    global HOME, CACHE
    saved = (HOME, CACHE, dict(_MEM), dict(_LEDGER))
    try:
        real = os.path.join(d, "real")
        pdir = os.path.join(real, ".claude", "projects", "-home-user-Stocks")
        os.makedirs(os.path.join(pdir, "s1", "subagents", "workflows", "wf1"))
        os.makedirs(os.path.join(real, "maintenance", "state"))
        with open(os.path.join(pdir, "s1.jsonl"), "w") as fh:
            fh.write(part1 + part2)
        with open(os.path.join(pdir, "s2.jsonl"), "w") as fh:
            fh.write(json.dumps({"type": "user", "message": {"content": "hello"}}) + "\n" + ln("m9", u2, "text"))
        with open(os.path.join(pdir, "s1", "subagents", "workflows", "wf1", "agent-a.jsonl"), "w") as fh:
            fh.write(json.dumps({"type": "user", "message": {"content": "review this"}}) + "\n" + ln("m7", u2, "text"))
        with open(os.path.join(real, "maintenance", "state", "claude_sessions.jsonl"), "w") as fh:
            fh.write(json.dumps({"session": "s1", "headless": True}) + "\n"
                     + json.dumps({"session": "s2", "headless": False}) + "\n")
        fake = os.path.join(d, "fake")
        os.makedirs(fake)
        for n in (".claude", "maintenance"):
            os.symlink(os.path.join(real, n), os.path.join(fake, n))
        CACHE = os.path.join(real, "maintenance", "state", "usage_cache.json")
        _MEM.update(sig=None, cache=None)
        _LEDGER.update(sig=None, kinds={})
        HOME = fake                                   # the test server's view first ...
        _usage(3650)
        HOME = real                                   # ... then the live one, same cache file
        r = _usage(3650)
        keys = list(json.load(open(CACHE)))
        check("a scratch home that symlinks to the real one adds no second copy",
              len(keys) == 3 and all(k.startswith(os.path.realpath(real)) for k in keys), keys)
        day = r["daily"][0]
        check("the ledger decides who ran it: the headless session and its subagent are scheduled, "
              "the other is yours", day["scheduled"] == 1105 + 30300 + 241 + 241 and day["interactive"] == 241,
              day)
        check("a subagent is not a session of its own", sum(g["sessions"] for g in r["groups"]) == 2, r["groups"])
        with open(CACHE) as fh:
            c = json.load(fh)
        c["/elsewhere/.claude/projects/x/old.jsonl"] = {"days": {"2026-09-26": {"in": 9, "out": 9, "cc": 9, "cr": 0,
                                                                              "msgs": 1}}, "kind": "interactive"}
        with open(CACHE, "w") as fh:
            json.dump(c, fh)
        _MEM.update(sig=None, cache=None)
        r2 = _usage(3650)
        check("an entry from outside the real projects root is dropped", r2["daily"] == r["daily"]
              and "/elsewhere/.claude/projects/x/old.jsonl" not in json.load(open(CACHE)))
    finally:
        HOME, CACHE = saved[0], saved[1]
        _MEM.clear(); _MEM.update(saved[2])
        _LEDGER.clear(); _LEDGER.update(saved[3])
    return ok


if __name__ == "__main__":
    import sys
    if sys.argv[1:] == ["selftest"]:
        sys.exit(0 if selftest() else 1)
    print(json.dumps(usage(), indent=1)[:2000])
