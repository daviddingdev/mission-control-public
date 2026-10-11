#!/usr/bin/env python3
"""
Open-model release watcher — code + local model, zero Claude tokens.

Weekly cron. Watches notable orgs on the Hugging Face API for new text-gen
model releases, dedupes against state, has qwen (ollama) write a two-line
"is this worth pulling for the Spark?" assessment per release, then pushes a
digest via notify.sh (maintenance channel) and appends reports/model-watch.md.

Rationale (David, 2026-08-11): the Spark's local-model layer should keep
itself current — better open models directly upgrade the summarize/search
assist work — and the WATCHING itself must not cost Claude tokens.

CLI: model-watch.py run [--days N] [--dry|--dry-run]   (first run: use --days 30)
     model-watch.py ab <model-tag> [--golden reports/bench-golden-YYYY-MM-DD.json]
                       [--dry|--dry-run] [--strict] [--tier-ok]
     model-watch.py ab --next [same flags]   (the first pending challenger; the backfill tick's form)
     model-watch.py selftest
`--dry` reads Hugging Face and prints what it found — no local model, no state, no report,
no push. Any other argument exits 2 without running.

`ab` (2026-10-11, memo wa-gpu-meter-and-serving slice 4) scores a challenger tag on the frozen
golden set of the 10-10 serving bench (reports/bench-parallel-2026-10-10.md): each item is
re-resolved by fact id through catalog.read("data-desk/facts_recent"), its expected answer is
checked against expected_sha256 (gone or changed -> skipped and counted), its prompt is rebuilt
with the bench's recipe and checked against prompt_sha256 (facts_recent is a rolling 30-day
window, so the 80 distractors drift: a drifted item still runs, flagged and counted, unless
--strict skips it). Each answer is right / wrong / fabricated; ANY fabricated figure makes the
verdict `reject` (the Nemotron rule). `--dry` prints the plan and touches nothing but a logged
catalog read. The real run must be backfill tier (launched with SPARK_GPU_BACKFILL=1 and no tty;
`--tier-ok` overrides), pulls the tag only if `ollama list` lacks it and removes only what it
pulled, holds gpu.slot per call (no slot -> stop, never bypass), writes reports/ab-<tag>-<date>.md
+ .json (ids and verdicts only: the source is private), and exits 1 if `ollama list` afterwards
differs from before.

`ab --next` (2026-10-11) takes its tag from config/models.json `challengers` (the only place a tag
is written): the first, in order, with no reports/ab-<slug>-YYYY-MM-DD.md yet. With none pending it
prints one line and exits 0. It is the run of the GPU backfill consumer `model-ab`
(config/gpu_backfill.json), whose probe (backfill_probe.py model-ab) applies the same rule in its
own copy, since importing this file resolves a model; the selftest pins the two copies together.

state/model_watch.json is written on EVERY run, "nothing new" included (2026-10-04): a
weekly job that only wrote when it found something read as stale for weeks, and a fetch
error was swallowed, so "nothing new" and "Hugging Face unreachable" looked the same. The
file carries checked_at, the orgs checked, and errors (count + list), and each failed fetch logs one
stderr line (2026-10-10); every org failing
exits 1.
"""
import contextlib
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOME = Path.home()
STATE = HOME / "maintenance/state/model_watch.json"
REPORT = HOME / "maintenance/reports/model-watch.md"
ORGS = ["Qwen", "meta-llama", "mistralai", "deepseek-ai", "openai", "google",
        "microsoft", "allenai", "nvidia", "moonshotai", "zai-org"]
sys.path.insert(0, str(HOME / "maintenance/bin"))
import models  # noqa: E402  — the local-model registry (config/models.json)
import gpu     # noqa: E402  — the GPU queue (config/gpu.json)

OLLAMA = models.chat_url()
LOCAL_MODEL = models.require("dense", job="model-watch")

# GPU reality on the DGX Spark (~99-120GB usable for weights): flag things we could run.
# The current driver is interpolated, not typed: this job's whole purpose is to find the
# model that replaces it, and a hardcoded name here would go stale the day it succeeds.
ASSESS = f"""You advise on open-weight LLMs for a single NVIDIA DGX Spark (~100GB VRAM-equivalent,
runs quantized models via ollama; current daily driver: {LOCAL_MODEL}). For the model release
below, output EXACTLY two lines:
VERDICT: PULL-CANDIDATE | WATCH | SKIP
WHY: <one concrete sentence — size/fit, claimed strengths, what Spark job it would improve (news
summarize/rank, filing navigation notes, log triage), or why it's irrelevant (too big, vision-only,
base-not-instruct, dedup of existing)>"""


def hf(url):
    req = urllib.request.Request(url, headers={"User-Agent": "spark-model-watch/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def ask_local(text):
    body = json.dumps({"model": LOCAL_MODEL, "think": False, "stream": False,
                       "messages": [{"role": "system", "content": ASSESS},
                                    {"role": "user", "content": text}],
                       "options": {"num_predict": 120, "temperature": 0.1}}).encode()
    req = urllib.request.Request(OLLAMA, data=body, headers={"Content-Type": "application/json"})
    t0 = time.time()
    with gpu.slot(job="model watch", model=LOCAL_MODEL) as g:
        t_body = time.time()                 # the grant, if the slot yields no Grant
        with urllib.request.urlopen(req, timeout=180) as r:
            d = json.loads(r.read())
    t_grant = getattr(g, "t_grant", None) or t_body
    hold = time.time() - t_grant             # the inference, never the queue wait
    gpu.record_usage(job="model watch", model=LOCAL_MODEL, prompt_tokens=d.get("prompt_eval_count"),
                     output_tokens=d.get("eval_count"), seconds=hold,
                     wait_s=max(0.0, t_grant - t0), hold_s=hold)
    return d["message"]["content"].strip()


def _state(seen, checked, errors, fresh_n, days, prev=None):
    """The state row written on every run — nothing-new and all-failed included."""
    now = datetime.now(timezone.utc).isoformat()
    return {"seen": sorted(seen),
            "updated": now if fresh_n else (prev or {}).get("updated", now),  # last time seen grew
            "checked_at": now, "days": days, "orgs_checked": checked,
            "new": fresh_n, "errors": len(errors), "error_list": errors[:20]}


def _write_state(row):
    tmp = STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(row, indent=1))
    os.replace(tmp, STATE)


def scan(seen, days, fetch=None):
    """-> (fresh, orgs_checked, errors). A fetch that fails is COUNTED, never skipped silently."""
    fetch = fetch or hf
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    fresh, checked, errors = [], [], []
    for org in ORGS:
        try:
            models = fetch(f"https://huggingface.co/api/models?author={org}&sort=createdAt&direction=-1&limit=12")
        except Exception as e:
            errors.append({"org": org, "error": f"{type(e).__name__}: {e}"[:160]})
            print(f"model-watch: fetch error {org}: {errors[-1]['error']}", file=sys.stderr, flush=True)
            continue                         # one log line per failed org, then the next org
        checked.append(org)
        for m in models:
            mid = m.get("id", "")
            created = m.get("createdAt", "")
            tags = m.get("tags", [])
            if not mid or mid in seen:
                continue
            try:
                if datetime.fromisoformat(created.replace("Z", "+00:00")) < cutoff:
                    continue
            except Exception:
                continue                     # no/odd createdAt: not a dated release, not an error
            if not any(t in tags for t in ("text-generation", "text2text-generation", "conversational")):
                continue
            fresh.append({"id": mid, "created": created[:10], "likes": m.get("likes", 0),
                          "downloads": m.get("downloads", 0)})
        time.sleep(0.3)
    return fresh, checked, errors


def _report_clean():
    """True when reports/model-watch.md has no uncommitted change, so the lines this run appends are
    the only change and may be committed by path (the 10-05 run's lines sat uncommitted for 2 days)."""
    try:
        r = subprocess.run(["git", "-C", str(REPORT.parent.parent), "status", "--porcelain", "--",
                            str(REPORT.relative_to(REPORT.parent.parent))],
                           capture_output=True, text=True, timeout=10)
        return r.returncode == 0 and not r.stdout.strip()
    except Exception:
        return False


def run(days=8, dry=False):
    prev = json.loads(STATE.read_text()) if STATE.exists() else {}
    seen = set(prev.get("seen", []))
    fresh, checked, errors = scan(seen, days)
    tail = f" · {len(errors)} fetch error(s): " + ", ".join(e["org"] for e in errors) if errors else ""
    if dry:
        print(f"model-watch --dry: {len(checked)}/{len(ORGS)} orgs read, {len(fresh)} new{tail}")
        for f in fresh:
            print(f"  would assess {f['id']} ({f['created']}, likes {f['likes']})")
        print("  (no local model, no state, no report, no push)")
        return 1 if not checked else 0
    STATE.parent.mkdir(exist_ok=True)
    REPORT.parent.mkdir(exist_ok=True)

    if not fresh:
        _write_state(_state(seen, checked, errors, 0, days, prev))
        print(f"model-watch: nothing new ({len(checked)}/{len(ORGS)} orgs read){tail}")
        return 1 if not checked else 0       # every org failed: "nothing new" would be a lie
    lines, notable = [], 0
    for f in sorted(fresh, key=lambda x: -x["likes"]):
        seen.add(f["id"])
        try:
            v = ask_local(json.dumps(f))
        except Exception as e:
            v = f"VERDICT: WATCH\nWHY: (local assess failed: {str(e)[:40]})"
            errors.append({"model": f["id"], "error": f"assess: {type(e).__name__}: {e}"[:160]})
        f["assessment"] = v
        if "PULL-CANDIDATE" in v:
            notable += 1
        lines.append(f"- **{f['id']}** ({f['created']}, ♥{f['likes']}) — {v.replace(chr(10), ' · ')}")
    _write_state(_state(seen, checked, errors, len(fresh), days, prev))
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    was_clean = _report_clean()
    with open(REPORT, "a") as fh:
        fh.write(f"\n## {stamp} — {len(fresh)} new, {notable} pull-candidate(s)\n" + "\n".join(lines) + "\n")
    if was_clean:     # commit our own lines by path; never sweep up someone else's edit (2026-10-07)
        subprocess.run(["git", "-C", str(REPORT.parent.parent), "commit", "-q", "-m",
                        f"model-watch: {stamp}, {len(fresh)} new, {notable} pull-candidate(s)", "--",
                        str(REPORT.relative_to(REPORT.parent.parent))], capture_output=True, timeout=30)
    msg = f"{len(fresh)} new open-model release(s), {notable} pull-candidate(s). reports/model-watch.md"
    subprocess.run([str(HOME / "maintenance/bin/notify.sh"), "maintenance", "Open models", msg],
                   capture_output=True, timeout=30)
    print(f"model-watch: {len(fresh)} new, {notable} pull-candidates -> report + ntfy{tail}")
    return 0


# ---------------------------------------------------------------- ab: a challenger on the golden set

REPORTS = HOME / "maintenance/reports"
AB_SOURCE = "data-desk/facts_recent"
AB_JOB = "model watch ab"
AB_DISTRACT = 80                 # the bench's recipe (bench-parallel-2026-10-10.md, "Golden set")
AB_CTX = 16384                   # the bench's best width: same answers as 262144, ~9% faster
AB_PREDICT = 48                  # answer-only: a value, a unit, or NOT FOUND
OLLAMA_BIN = "ollama"
NUM_RE = re.compile(r"\d[\d,]*\.?\d*")
# The incumbent on the same items, 2026-10-10 (report: 11/11 answerable right, 5/5 NOT FOUND, 0
# fabricated; 5.68 s/call at NUM_PARALLEL 1, num_ctx 16384). A challenger is judged against this.
INCUMBENT = {"model": "qwen3.8:27b", "right": 16, "of": 16, "fabricated": 0, "s_per_call": 5.68,
             "source": "reports/bench-parallel-2026-10-10.md"}


def _nums(s):
    return {n.replace(",", "").rstrip(".") for n in NUM_RE.findall(s or "")}


def _sha(s):
    return hashlib.sha256(str(s).encode()).hexdigest()


def _eligible(rows):
    """The bench's filter: verified exact quote, numbers_ok, stance fact, a numeric value, an entity
    and metric that occur once, sorted in sha256(id) order (the distractor pool is drawn from these)."""
    ok = [r for r in rows if (r.get("verify") or {}).get("quote_match") == "exact"
          and (r.get("verify") or {}).get("numbers_ok") and r.get("stance") == "fact"
          and NUM_RE.search(str(r.get("value") or "")) and r.get("entity") and r.get("metric")
          and r.get("quote")]
    key = lambda r: (r["entity"].lower(), r["metric"].lower())  # noqa: E731
    cnt = {}
    for r in ok:
        cnt[key(r)] = cnt.get(key(r), 0) + 1
    ok = [r for r in ok if cnt[key(r)] == 1]
    ok.sort(key=lambda r: _sha(r["id"]))
    return ok


def _ab_prompt(r, pool, n_distract=AB_DISTRACT):
    """-> (prompt, passage). Byte for byte the bench's answer-only prompt: the fact's verbatim quote
    shuffled among n_distract quotes from other documents, seeded by the fact id."""
    rng = random.Random(r["id"])
    dis = rng.sample(pool, n_distract)
    quotes = [d["quote"] for d in dis] + [r["quote"]]
    rng.shuffle(quotes)
    passage = "\n".join(f"[{j+1}] {q}" for j, q in enumerate(quotes))
    prompt = (f"Passages (verbatim quotes from documents):\n{passage}\n\n"
              f"Question: according to the passages, what is the value of \"{r['metric']}\" for "
              f"\"{r['entity']}\"? Reply with the value only (number and unit), nothing else. "
              f"If it is not in the passages, reply NOT FOUND.")
    return prompt, passage


def ab_resolve(golden, rows, strict=False, n_distract=AB_DISTRACT):
    """-> (items, skipped). Each golden item is re-found by fact id and its expected answer checked
    against expected_sha256; a gone or changed fact is skipped (counted by reason). The prompt is
    rebuilt and compared with prompt_sha256: a drift (the rolling window moved the distractor pool)
    is flagged `prompt_match: False`, and skipped too under strict."""
    byid = {r.get("id"): r for r in rows}
    found = [(g, byid.get(g.get("fact_id"))) for g in golden.get("items", [])]
    docs = {r.get("source_id") for _, r in found if r}
    pool = [r for r in _eligible(rows) if r.get("source_id") not in docs]
    items, skipped = [], {}
    for g, r in found:
        why = None
        if r is None:
            why = "fact gone"
        elif _sha(str(r.get("value"))) != g.get("expected_sha256"):
            why = "fact changed"
        elif len(pool) < n_distract:
            why = "pool too small"
        if why is None:
            prompt, passage = _ab_prompt(r, pool, n_distract)
            match = _sha(prompt) == g.get("prompt_sha256")
            if strict and not match:
                why = "prompt drifted"
        if why:
            skipped.setdefault(why, []).append(g.get("n"))
            continue
        q = prompt.rsplit("\n\nQuestion:", 1)[-1]
        items.append({"n": g.get("n"), "fact_id": r["id"], "doc_kind": r.get("doc_kind"),
                      "expected": str(r["value"]), "prompt": prompt, "prompt_match": match,
                      # the bench's caveat: an entity absent from its own quote is unanswerable
                      # from the passage, and NOT FOUND is then the right answer
                      "answerable": str(r["entity"]).lower() in str(r["quote"]).lower(),
                      "allowed_nums": sorted(_nums(passage) | _nums(q))})
    return items, skipped


def ab_score(item, ans):
    """-> (verdict, fabricated numbers). fabricated: a number in the answer that is in neither the
    passage nor the question (the bench counted the passage; a figure echoed from the question is
    not invented). right: the expected value is in the answer (exact, or all its numbers), or NOT
    FOUND on an unanswerable item. Anything else is wrong."""
    exp = item["expected"].strip().lower()
    a = (ans or "").strip().lower()
    en, an = _nums(exp), _nums(a)
    fab = sorted(an - set(item["allowed_nums"]))
    if fab:
        return "fabricated", fab
    if a and (exp in a or (bool(en) and en <= an)):
        return "right", []
    if not item["answerable"] and "not found" in a:
        return "right", []
    return "wrong", []


def ab_verdict(scored):
    """reject on ANY fabricated figure (the Nemotron rule); else below or at the incumbent's accuracy."""
    n = len(scored)
    fab = sum(s["verdict"] == "fabricated" for s in scored)
    right = sum(s["verdict"] == "right" for s in scored)
    if fab:
        return "reject"
    if not n:
        return "no items"
    inc = INCUMBENT["right"] / INCUMBENT["of"]
    return "matches incumbent" if right / n >= inc else "below incumbent"


def _tag_slug(tag):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", tag).strip("-") or "model"


def ab_write(tag, plan, scored, skipped, timing, lists, out_dir=None, day=None, partial=False):
    """reports/ab-<tag>-<date>.md + .json. The json carries ids, verdicts and seconds only: the
    golden source is private, so no quote, prompt, expected value or answer text leaves /tmp."""
    out_dir = Path(out_dir or REPORTS)
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    stem = f"ab-{_tag_slug(tag)}-{day}"     # not with_suffix: a tag like qwen3.8 has dots of its own
    if partial:     # a run that stopped early is kept, but never counts as the challenger's A/B (`ab --next` retries)
        stem += "-partial"
    md_path, js_path = out_dir / f"{stem}.md", out_dir / f"{stem}.json"
    verdict = ab_verdict(scored)
    n = len(scored)
    cnt = {k: sum(s["verdict"] == k for s in scored) for k in ("right", "wrong", "fabricated")}
    secs = [s["s"] for s in scored if s.get("s") is not None]
    mean = round(sum(secs) / len(secs), 2) if secs else None
    drift = sum(not s["prompt_match"] for s in scored)
    row = {"model": tag, "date": day, "golden": plan["golden"], "source": AB_SOURCE, "private_source": True,
           "verdict": verdict, "counts": cnt, "items_run": n,
           "skipped": {k: sorted(v) for k, v in skipped.items()}, "prompt_drifted": drift,
           "s_per_call_mean": mean, "s_per_call_max": max(secs) if secs else None,
           "load_s": timing.get("load_s"), "num_ctx": AB_CTX, "num_predict": AB_PREDICT,
           "incumbent": INCUMBENT, "pulled": plan.get("pulled", False),
           "ollama_list_unchanged": lists.get("unchanged"),
           "items": [{"n": s["n"], "fact_id": s["fact_id"], "doc_kind": s.get("doc_kind"),
                      "verdict": s["verdict"], "fabricated_count": len(s.get("fab") or []),
                      "prompt_match": s["prompt_match"], "s": s.get("s")} for s in scored]}
    out_dir.mkdir(parents=True, exist_ok=True)
    js_path.write_text(json.dumps(row, indent=1) + "\n")
    skip_txt = ", ".join(f"{k}: {len(v)} (items {', '.join(map(str, sorted(v)))})"
                         for k, v in skipped.items()) or "none"
    lines = [f"# ab — {tag} on the golden set ({day})", "",
             f"**Verdict: {verdict}.** Right {cnt['right']}/{n}, wrong {cnt['wrong']}, "
             f"fabricated {cnt['fabricated']} (any fabricated figure = reject).", "",
             "## Setup", "",
             f"- Golden set: `{plan['golden']}` ({plan['golden_items']} items), re-read by fact id from "
             f"`{AB_SOURCE}` (private: this report and its json carry ids and verdicts only).",
             f"- Skipped: {skip_txt}.",
             f"- Prompt drift: {drift} of {n} prompts no longer hash to the frozen prompt_sha256 (the "
             f"rolling window moved the distractor pool; the target quote and question are unchanged)"
             + (", so those items compare to the incumbent only approximately." if drift else "."),
             f"- Asked as the 10-10 bench did: /api/chat, temperature 0, think off, num_predict "
             f"{AB_PREDICT}, num_ctx {AB_CTX}, one call at a time, each under gpu.slot "
             f"(job `{AB_JOB}`, tier {plan.get('tier')}). One warm-up call (the load) first, not scored.",
             f"- Pulled for this run: {'yes, and removed after' if plan.get('pulled') else 'no (already installed)'}.",
             "", "## Results", "",
             "| model | right | wrong | fabricated | s/call mean (max) | load s |",
             "|---|---|---|---|---|---|",
             f"| {tag} | {cnt['right']}/{n} | {cnt['wrong']} | {cnt['fabricated']} | "
             f"{mean} ({max(secs) if secs else None}) | {timing.get('load_s')} |",
             f"| {INCUMBENT['model']} (10-10, incumbent) | {INCUMBENT['right']}/{INCUMBENT['of']} | 0 | "
             f"{INCUMBENT['fabricated']} | {INCUMBENT['s_per_call']} | — |", "",
             "Per item (n: verdict, seconds): " + ", ".join(
                 f"{s['n']}: {s['verdict']}{'' if s['prompt_match'] else '*'} {s.get('s')}" for s in scored)
             + ("  (* prompt drifted)" if drift else ""), "",
             "## ollama list, before and after", "",
             "Unchanged (name + id)." if lists.get("unchanged") else
             f"**CHANGED.** before {len(lists.get('before') or {})} models, after "
             f"{len(lists.get('after') or {})}: {lists.get('diff')}", ""]
    md_path.write_text("\n".join(lines))
    return md_path, js_path


def ollama_list(run=subprocess.run):
    """{name: id} from `ollama list` (the CLI, as David reads it)."""
    r = run([OLLAMA_BIN, "list"], capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(f"ollama list failed: {(r.stderr or '').strip()[:160]}")
    out = {}
    for ln in r.stdout.splitlines()[1:]:
        parts = ln.split()
        if len(parts) >= 2:
            out[parts[0]] = parts[1]
    return out


def _installed(tag, have):
    return tag in have or (":" not in tag and f"{tag}:latest" in have)


AB_BLOCKED = HOME / "maintenance/state/model_watch_ab_blocked.json"
NEWER_OLLAMA = re.compile(r"requires a newer version of ollama", re.I)


def ollama_version():
    """`ollama --version`'s last line ("ollama version is 0.32.13"), or "" when it cannot be read."""
    try:
        r = subprocess.run([OLLAMA_BIN, "--version"], capture_output=True, text=True, timeout=15)
        lines = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
        return lines[-1] if lines else ""
    except Exception:
        return ""


def ab_note_blocked(tag, text, path=None, version=None):
    """A pull refused because the tag needs a newer ollama is not worth retrying daily: record the tag
    and the ollama version that refused it (state/model_watch_ab_blocked.json); backfill_probe.py
    model-ab answers no work for that tag until `ollama --version` changes. True when written."""
    if not NEWER_OLLAMA.search(text or ""):
        return False
    path = path or AB_BLOCKED
    try:
        with open(path, "w") as f:
            json.dump({"tag": tag, "ollama": version if version is not None else ollama_version(),
                       "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "why": "pull refused: the model requires a newer version of Ollama"}, f)
        return True
    except OSError:
        return False


def ab_tier_gate(tier_ok, env=None, tier=None, backfill=None):
    """-> None when the real run may start, else the reason it may not. The run must hold the slot at
    the backfill tier (40, below everything, never ages): launched with SPARK_GPU_BACKFILL=1 from a
    process with no terminal (gpu.tier() gives a tty the interactive tier 0 even then)."""
    if tier_ok:
        return None
    env = os.environ if env is None else env
    if env.get("SPARK_GPU_BACKFILL") != "1":
        return "not launched with SPARK_GPU_BACKFILL=1 (pass --tier-ok to run at your own tier)"
    if tier != backfill:
        return (f"gpu.tier() is {tier}, not the backfill tier {backfill} (a terminal is attached?"
                f" run it detached, < /dev/null >> log 2>&1, or pass --tier-ok)")
    return None


def _ab_report_re(tag):
    """The name ab_write gives the report: ab-<slug>-YYYY-MM-DD.md, the date matched exactly so that
    qwen3.8's report never stands in for qwen3.8-flash-next's (or the reverse)."""
    return re.compile(r"ab-" + re.escape(_tag_slug(tag)) + r"-\d{4}-\d{2}-\d{2}\.md$")


def ab_pending(challengers, reports=None):
    """Challenger tags, in models.json order, with no A/B report in reports/ yet."""
    reports = Path(reports or REPORTS)
    names = [f.name for f in reports.glob("ab-*.md")] if reports.is_dir() else []
    tags = [c.get("tag") for c in (challengers or [])
            if isinstance(c, dict) and isinstance(c.get("tag"), str) and c["tag"].strip()]
    return [t for t in tags if not any(_ab_report_re(t).match(n) for n in names)]


def ab_next(reg=None, reports=None):
    """-> (the first pending challenger tag or None, the pending list)."""
    reg = models.registry() if reg is None else reg
    pend = ab_pending(reg.get("challengers"), reports)
    return (pend[0] if pend else None), pend


def _latest_golden():
    found = sorted(REPORTS.glob("bench-golden-*.json"))
    return found[-1] if found else None


def _ab_chat(tag, prompt):
    body = {"model": tag, "stream": False, "think": False, "keep_alive": gpu.cfg().get("keep_alive", "10m"),
            "messages": [{"role": "user", "content": prompt}],
            "options": {"temperature": 0, "num_predict": AB_PREDICT, "num_ctx": AB_CTX}}
    def post(b):
        req = urllib.request.Request(OLLAMA, data=json.dumps(b).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=900) as r:
            return json.loads(r.read())
    try:
        return post(body)
    except urllib.error.HTTPError as e:      # a model with no think switch can refuse the field
        if e.code == 400 and "think" in (e.read() or b"").decode(errors="replace").lower():
            body.pop("think")
            return post(body)
        raise


def _ab_call(tag, prompt):
    """One call under the slot. -> (answer, seconds of inference). No slot -> RuntimeError: a
    backfill run that bypasses the queue would take the card from the work it exists to yield to."""
    t0 = time.time()
    with gpu.slot(job=AB_JOB, model=tag, proj="maintenance") as g:
        if not getattr(g, "held", True):
            raise RuntimeError("no GPU slot (the wait timed out): stopping, never bypassing")
        t_grant = getattr(g, "t_grant", None) or time.time()
        d = _ab_chat(tag, prompt)
    hold = time.time() - t_grant
    gpu.record_usage(job=AB_JOB, model=tag, prompt_tokens=d.get("prompt_eval_count"),
                     output_tokens=d.get("eval_count"), seconds=hold,
                     wait_s=max(0.0, t_grant - t0), hold_s=hold)
    ans = re.sub(r"<think>.*?</think>", "", (d.get("message") or {}).get("content", ""), flags=re.S)
    return ans.strip(), round(hold, 2)


def ab(tag, golden_path=None, dry=False, strict=False, tier_ok=False):
    import catalog                           # the only door to another project's data (box rule 8)
    gp = Path(golden_path) if golden_path else _latest_golden()
    if gp and not gp.is_absolute() and not gp.exists():
        gp = HOME / "maintenance" / gp
    if not gp or not gp.exists():
        print(f"model-watch ab: no golden set ({golden_path or 'reports/bench-golden-*.json'})", file=sys.stderr)
        return 2
    golden = json.loads(gp.read_text())
    if golden.get("source") not in (None, AB_SOURCE):
        print(f"model-watch ab: golden source {golden.get('source')!r} is not {AB_SOURCE}", file=sys.stderr)
        return 2
    rows = catalog.read(AB_SOURCE)
    items, skipped = ab_resolve(golden, rows, strict=strict)
    before = ollama_list()
    have = _installed(tag, before)
    tier_now, bf = gpu.tier("maintenance", AB_JOB), gpu.backfill_tier()
    gate = ab_tier_gate(tier_ok, tier=tier_now, backfill=bf)
    est = round((90 if not have else 60) / 60 + len(items) * INCUMBENT["s_per_call"] * 1.5 / 60, 1)
    rel = str(gp.relative_to(HOME / "maintenance")) if str(gp).startswith(str(HOME / "maintenance")) else str(gp)
    plan = {"golden": rel, "golden_items": len(golden.get("items", [])), "tier": tier_now}
    skip_txt = ", ".join(f"{k} {len(v)} ({','.join(map(str, v))})" for k, v in skipped.items()) or "none"
    drift = sum(not it["prompt_match"] for it in items)
    print(f"model-watch ab{' --dry' if dry else ''}: {tag}")
    print(f"  golden     {rel}: {plan['golden_items']} items -> {len(items)} to run, skipped {skip_txt}")
    print(f"  prompts    {len(items) - drift} match prompt_sha256, "
          f"{drift + len(skipped.get('prompt drifted', []))} drifted"
          f"{' (skipped: --strict)' if strict else ' (run, flagged)'}")
    print(f"  installed  {'yes' if have else 'no -> the run would `ollama pull` it, then `ollama rm` it after'}"
          f" (ollama list: {len(before)} models)")
    print(f"  estimate   ~{est} min of GPU ({len(items)} calls + a warm-up; num_ctx {AB_CTX})"
          f"{'' if have else ', plus the pull'}")
    print(f"  tier       {tier_now} (backfill {bf}) -> " + ("ok" if gate is None else f"REFUSED: {gate}"))
    if dry:
        print("  (dry: no pull, no GPU, no report; only the catalog read is logged)")
        return 0
    if gate:
        print(f"model-watch ab: refused, {gate}", file=sys.stderr)
        return 2
    if not items:
        print("model-watch ab: no golden item resolves; nothing to score", file=sys.stderr)
        return 1
    pulled, scored, timing, rc = False, [], {}, 0
    try:
        if not have:
            r = subprocess.run([OLLAMA_BIN, "pull", tag], capture_output=True, text=True, timeout=4 * 3600)
            if r.returncode != 0:
                print(f"model-watch ab: pull failed: {(r.stderr or '').strip()[-200:]}", file=sys.stderr)
                ab_note_blocked(tag, (r.stderr or "") + (r.stdout or ""))
                return 1
            pulled = True
        t = time.time()
        _ab_call(tag, items[0]["prompt"])            # the load, not scored
        timing["load_s"] = round(time.time() - t, 1)
        for it in items:
            ans, s = _ab_call(tag, it["prompt"])
            v, fab = ab_score(it, ans)
            scored.append({"n": it["n"], "fact_id": it["fact_id"], "doc_kind": it["doc_kind"],
                           "verdict": v, "fab": fab, "prompt_match": it["prompt_match"], "s": s})
            print(f"  {it['n']:>2} {v:<10} {s:>6.2f}s", flush=True)
    except Exception as e:
        print(f"model-watch ab: run stopped: {type(e).__name__}: {e}", file=sys.stderr)
        rc = 1
    finally:
        with contextlib.suppress(Exception):
            subprocess.run([OLLAMA_BIN, "stop", tag], capture_output=True, timeout=60)
        if pulled:
            subprocess.run([OLLAMA_BIN, "rm", tag], capture_output=True, timeout=300)
    after = ollama_list()
    diff = {"added": sorted(set(after) - set(before)), "removed": sorted(set(before) - set(after)),
            "id_changed": sorted(k for k in set(before) & set(after) if before[k] != after[k])}
    lists = {"before": before, "after": after, "unchanged": after == before, "diff": diff}
    plan["pulled"] = pulled
    md, js = ab_write(tag, plan, scored, skipped, timing, lists, partial=rc != 0)
    print(f"model-watch ab: {ab_verdict(scored)} -> {md.relative_to(HOME)} (+ .json)")
    if after != before:
        print(f"model-watch ab: ollama list CHANGED: {diff}", file=sys.stderr)
        return 1
    return rc


def _ab_args(argv):
    """-> {tag, golden, dry, strict, tier_ok} for `ab <tag> [flags]` or `ab --next [flags]` (tag None),
    or None for anything else."""
    if argv[:1] != ["ab"] or len(argv) < 2 or not argv[1].strip():
        return None
    if argv[1] == "--next":
        tag = None
    elif argv[1].startswith("-"):
        return None
    else:
        tag = argv[1]
    out, rest = {"tag": tag, "golden": None, "dry": False, "strict": False, "tier_ok": False}, argv[2:]
    while rest:
        a = rest.pop(0)
        if a in ("--dry", "--dry-run"):
            out["dry"] = True
        elif a == "--strict":
            out["strict"] = True
        elif a == "--tier-ok":
            out["tier_ok"] = True
        elif a == "--golden" and rest and not rest[0].startswith("-"):
            out["golden"] = rest.pop(0)
        else:
            return None
    return out


def _ab_selftest():
    """Fakes only: fixture rows, fixture answers, a temp report dir. No catalog, network or GPU."""
    import tempfile
    rows = []
    for i in range(30):                      # 30 one-fact documents
        rows.append({"id": f"f{i:02d}", "source_id": f"doc{i}", "doc_kind": "mna_8k", "stance": "fact",
                     "verify": {"quote_match": "exact", "numbers_ok": True},
                     "entity": f"Acme {chr(65 + i % 26)}{i}", "metric": f"metric {i}", "value": f"{i + 10}.5%",
                     "quote": f"Acme {chr(65 + i % 26)}{i} reported metric {i} of {i + 10}.5% in the period."})
    rows[1]["quote"] = "The company reported metric 1 of 11.5% in the period."   # entity absent
    n_dis = 5
    docs = {"doc0", "doc1", "doc2", "doc3"}
    pool = [r for r in _eligible(rows) if r["source_id"] not in docs]
    golden = {"source": AB_SOURCE, "items": []}
    for n, r in enumerate(rows[:4], 1):
        golden["items"].append({"n": n, "fact_id": r["id"], "expected_sha256": _sha(r["value"]),
                                "prompt_sha256": _sha(_ab_prompt(r, pool, n_dis)[0])})
    golden["items"].append({"n": 5, "fact_id": "gone", "expected_sha256": "x", "prompt_sha256": "y"})
    live = [dict(r) for r in rows]
    live[2]["value"] = "99.9%"               # the fact behind item 3 changed since the freeze
    items, skipped = ab_resolve(golden, live, n_distract=n_dis)
    cases = [("sha: a changed fact is skipped", skipped.get("fact changed"), [3]),
             ("sha: a gone fact is skipped", skipped.get("fact gone"), [5]),
             ("sha: the others resolve", [it["n"] for it in items], [1, 2, 4]),
             ("prompt rebuilt byte-identical", [it["prompt_match"] for it in items], [True, True, True]),
             ("an entity absent from its quote is unanswerable", [it["answerable"] for it in items],
              [True, False, True])]
    drifted = dict(golden, items=[dict(g, prompt_sha256="0" * 64) if g["n"] == 4 else g for g in golden["items"]])
    _, sk = ab_resolve(drifted, live, strict=True, n_distract=n_dis)
    it_d, _ = ab_resolve(drifted, live, strict=False, n_distract=n_dis)
    cases += [("strict skips a drifted prompt", sk.get("prompt drifted"), [4]),
              ("default runs it, flagged", [i["prompt_match"] for i in it_d if i["n"] == 4], [False])]
    a, b, d = items
    cases += [("score: the value -> right", ab_score(a, "10.5%")[0], "right"),
              ("score: value with extra words -> right", ab_score(a, "10.5% per annum")[0], "right"),
              ("score: NOT FOUND on unanswerable -> right", ab_score(b, "NOT FOUND")[0], "right"),
              ("score: NOT FOUND on answerable -> wrong", ab_score(a, "NOT FOUND")[0], "wrong"),
              ("score: a passage number, wrong one -> wrong",
               ab_score(a, next(x for x in a["allowed_nums"] if "." in x and x != "10.5") + "%")[0], "wrong"),
              ("score: a figure in no passage -> fabricated", ab_score(a, "73.2%"), ("fabricated", ["73.2"])),
              ("score: a number from the question is not invented", ab_score(a, "metric 0: NOT FOUND")[0], "wrong")]
    clean = [{"verdict": "right"}] * 3
    cases += [("verdict: all right -> matches incumbent", ab_verdict(clean), "matches incumbent"),
              ("verdict: one wrong -> below incumbent", ab_verdict(clean[:2] + [{"verdict": "wrong"}]),
               "below incumbent"),
              ("verdict: one fabricated -> reject", ab_verdict(clean + [{"verdict": "fabricated"}]), "reject")]
    cases += [("tier gate: no env -> refused", ab_tier_gate(False, env={}, tier=40, backfill=40) is not None, True),
              ("tier gate: env + tier 40 -> ok", ab_tier_gate(False, env={"SPARK_GPU_BACKFILL": "1"},
                                                              tier=40, backfill=40), None),
              ("tier gate: env but a tty (tier 0) -> refused",
               ab_tier_gate(False, env={"SPARK_GPU_BACKFILL": "1"}, tier=0, backfill=40) is not None, True),
              ("tier gate: --tier-ok -> ok", ab_tier_gate(True, env={}, tier=30, backfill=40), None)]
    fake = lambda *a, **k: subprocess.CompletedProcess(a, 0, "NAME  ID  SIZE  MODIFIED\nqwen3.8:27b  2213  17 GB  8 weeks ago\n"
                                                         "llama3.2:3b  a80c  2.0 GB  1 week ago\n", "")  # noqa: E731
    have = ollama_list(run=fake)
    cases += [("ollama list parses name -> id", have, {"qwen3.8:27b": "2213", "llama3.2:3b": "a80c"}),
              ("installed: exact tag", _installed("qwen3.8:27b", have), True),
              ("installed: a missing tag", _installed("qwen3.8-flash-next", have), False)]
    d_out = tempfile.mkdtemp()
    try:
        scored = [{"n": it["n"], "fact_id": it["fact_id"], "doc_kind": it["doc_kind"], "prompt_match": True,
                   "verdict": v, "fab": f, "s": 4.2} for it, (v, f) in
                  zip(items, [ab_score(a, "10.5%"), ab_score(b, "NOT FOUND"), ab_score(d, "73.2%")])]
        md, js = ab_write("hf.co/org/Model:Q6_K", {"golden": "reports/bench-golden-x.json", "golden_items": 5,
                                                   "tier": 40}, scored, skipped, {"load_s": 50.1},
                          {"unchanged": True}, out_dir=d_out, day="2026-10-11")
        row = json.loads(js.read_text())
        blob = md.read_text() + js.read_text()
        private = [r["quote"] for r in live[:4]] + [r["value"] for r in live[:4]] + [r["entity"] for r in live[:4]]
        md2, _ = ab_write("qwen3.8-flash-next", {"golden": "g", "golden_items": 0}, [], {}, {}, {"unchanged": True},
                          out_dir=d_out, day="2026-10-11")
        cases += [("report: tag sanitised in the name", md.name, "ab-hf.co-org-Model-Q6_K-2026-10-11.md"),
                  ("report: a dotted tag keeps its whole name", md2.name, "ab-qwen3.8-flash-next-2026-10-11.md"),
                  ("report: a run that stopped early is named -partial (never counts as done)",
                   ab_write("qwen3.8-flash-next", {"golden": "g", "golden_items": 0}, [], {}, {}, {"unchanged": True},
                            out_dir=d_out, day="2026-10-12", partial=True)[0].name,
                   "ab-qwen3.8-flash-next-2026-10-12-partial.md"),
                  ("report: the fabricated item makes it reject", row["verdict"], "reject"),
                  ("report: json keeps ids and verdicts", [(i["fact_id"], i["verdict"]) for i in row["items"]],
                   [("f00", "right"), ("f01", "right"), ("f03", "fabricated")]),
                  ("report: no private text in md or json", [p for p in private if p in blob], []),
                  ("report: skips are recorded", row["skipped"], {"fact changed": [3], "fact gone": [5]})]
    finally:
        for f in Path(d_out).iterdir():
            f.unlink()
        os.rmdir(d_out)
    cases += [("argv: ab <tag>", _ab_args(["ab", "m:1"]),
               {"tag": "m:1", "golden": None, "dry": False, "strict": False, "tier_ok": False}),
              ("argv: ab --golden --dry-run --tier-ok",
               _ab_args(["ab", "m", "--golden", "g.json", "--dry-run", "--tier-ok"]),
               {"tag": "m", "golden": "g.json", "dry": True, "strict": False, "tier_ok": True}),
              ("argv: ab --dry == --dry-run", _ab_args(["ab", "m", "--dry"])["dry"], True),
              ("argv: ab with no tag is refused", _ab_args(["ab", "--dry"]), None),
              ("argv: ab unknown flag is refused", _ab_args(["ab", "m", "--pull"]), None),
              ("argv: ab a second tag is refused", _ab_args(["ab", "m", "n"]), None),
              ("argv: ab --golden with no path is refused", _ab_args(["ab", "m", "--golden"]), None),
              ("argv: run parser refuses ab", _args(["ab", "m"]), None),
              ("argv: ab --next --dry", _ab_args(["ab", "--next", "--dry"]),
               {"tag": None, "golden": None, "dry": True, "strict": False, "tier_ok": False}),
              ("argv: ab --next with a tag is refused", _ab_args(["ab", "--next", "m"]), None),
              ("argv: ab m --next is refused", _ab_args(["ab", "m", "--next"]), None)]
    # ab --next: models.json `challengers` -> the first with no report; pinned to backfill_probe's copy
    import backfill_probe                     # side-effect free: no registry, no GPU (its selftest pins it)
    d_n = tempfile.mkdtemp()
    try:
        reg = {"challengers": [{"tag": "org/Big:Q4_K_M"}, {"tag": "qwen3.8"}, {"tag": "qwen3.8-flash-next:1b"},
                               {"no": "tag"}, {"tag": "  "}]}
        mj = Path(d_n) / "models.json"
        mj.write_text(json.dumps(reg))
        rep_dir = Path(d_n) / "reports"
        rep_dir.mkdir()
        agree = lambda: ab_pending(reg["challengers"], rep_dir) == backfill_probe.ab_pending(str(mj), str(rep_dir))  # noqa: E731
        cases += [("next: no reports -> the first challenger", ab_next(reg, rep_dir)[0], "org/Big:Q4_K_M"),
                  ("next: agrees with the probe (none done)", agree(), True),
                  ("next: no challengers key -> None", ab_next({}, rep_dir), (None, [])),
                  ("next: a missing reports dir -> all pending", len(ab_pending(reg["challengers"], rep_dir / "x")), 3)]
        md_b, _ = ab_write("org/Big:Q4_K_M", {"golden": "g", "golden_items": 0}, [], {}, {}, {"unchanged": True},
                           out_dir=rep_dir, day="2026-10-11")      # the real writer names the report
        cases += [("next: ab_write's report marks it done", ab_next(reg, rep_dir)[0], "qwen3.8"),
                  ("next: agrees with the probe (one done)", agree(), True)]
        ab_write("qwen3.8-flash-next:1b", {"golden": "g", "golden_items": 0}, [], {}, {}, {"unchanged": True},
                 out_dir=rep_dir, day="2026-10-12")
        cases += [("next: a longer tag's report leaves its prefix pending", ab_next(reg, rep_dir)[1], ["qwen3.8"]),
                  ("next: agrees with the probe (prefix case)", agree(), True)]
        ab_write("qwen3.8", {"golden": "g", "golden_items": 0}, [], {}, {}, {"unchanged": True},
                 out_dir=rep_dir, day="2026-10-13")
        cases += [("next: all done -> None", ab_next(reg, rep_dir), (None, [])),
                  ("next: agrees with the probe (all done)", agree(), True),
                  ("slug: the probe's copy matches", [backfill_probe.tag_slug(t) for t in
                   ("hf.co/org/M:Q6_K", "qwen3.8-flash-next:125b-a6b-q4_K_M", "a b//c", "///")],
                   [_tag_slug(t) for t in ("hf.co/org/M:Q6_K", "qwen3.8-flash-next:125b-a6b-q4_K_M", "a b//c", "///")])]
        import io
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = ab_main(None, reg=reg, reports=rep_dir)
        cases += [("next: none pending prints a line, exit 0",
                   (rc, out.getvalue().startswith("model-watch ab --next: no challenger")), (0, True))]
    finally:
        import shutil
        shutil.rmtree(d_n, ignore_errors=True)
    return cases


def ab_main(tag, golden=None, dry=False, strict=False, tier_ok=False, reg=None, reports=None):
    """`ab <tag>`, or with tag None (`ab --next`) the first pending challenger in models.json."""
    if tag is None:
        tag, pend = ab_next(reg, reports)
        if tag is None:
            print("model-watch ab --next: no challenger without an A/B report (config/models.json challengers)")
            return 0
        print(f"model-watch ab --next: {tag} ({len(pend)} pending: {', '.join(pend)})")
    return ab(tag, golden, dry=dry, strict=strict, tier_ok=tier_ok)


def _selftest():
    """Fakes only: no Hugging Face, no local model, no push, and the live state file is
    never touched (STATE is pointed at a temp file)."""
    import tempfile
    global STATE
    bad, cases = 0, []
    def fetch(url):
        if "author=openai" in url or "author=google" in url:
            raise OSError("HTTP 503")
        return [{"id": "old/one", "createdAt": "2020-01-01T00:00:00Z", "tags": ["text-generation"]}]
    import contextlib, io
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        fresh, checked, errors = scan(set(), 8, fetch=fetch)
    logged = [ln for ln in err.getvalue().splitlines() if ln.startswith("model-watch: fetch error ")]
    cases += [("fetch errors are counted, not swallowed", len(errors), 2),
              ("each fetch error logs one line", sorted(ln.split()[3].rstrip(":") for ln in logged),
               ["google", "openai"]),
              ("the other orgs still count as checked", len(checked), len(ORGS) - 2),
              ("nothing new inside the window", len(fresh), 0)]
    real, d = STATE, tempfile.mkdtemp()
    try:
        STATE = Path(d) / "model_watch.json"
        _write_state(_state({"a/b"}, checked, errors, 0, 8, {"updated": "2026-09-21"}))
        row = json.loads(STATE.read_text())
        cases += [("nothing-new still writes checked_at", bool(row.get("checked_at")), True),
                  ("it records the orgs checked", row.get("orgs_checked"), checked),
                  ("and the error count", row.get("errors"), 2),
                  ("'updated' stays the last time seen grew", row.get("updated"), "2026-09-21"),
                  ("seen is kept", row.get("seen"), ["a/b"])]
    finally:
        STATE = real
        for f in Path(d).iterdir():
            f.unlink()
        os.rmdir(d)
    cases += _ab_selftest()
    cases += [("argv: run", _args(["run"]), (8, False)),
              ("argv: run --days 30 --dry-run", _args(["run", "--days", "30", "--dry-run"]), (30, True)),
              ("argv: unknown flag is refused", _args(["run", "--selftest"]), None),
              ("argv: no command is refused", _args([]), None)]
    for name, got, want in cases:
        ok = got == want
        bad += not ok
        print(f"  {'ok ' if ok else 'FAIL'} {name:<44} -> {got}")
    print(f"{len(cases) - bad}/{len(cases)} passed")
    return bad


def _args(argv):
    """-> (days, dry) for `run [--days N] [--dry|--dry-run]`, or None for anything else."""
    if argv[:1] != ["run"]:
        return None
    days, dry, rest = 8, False, argv[1:]
    while rest:
        a = rest.pop(0)
        if a in ("--dry", "--dry-run"):
            dry = True
        elif a == "--days" and rest and rest[0].isdigit() and int(rest[0]) > 0:
            days = int(rest.pop(0))
        else:
            return None
    return days, dry


if __name__ == "__main__":
    if any(a in ("-h", "--help") for a in sys.argv[1:]):   # `--help` never runs the job (2026-09-26)
        print((__doc__ or "").strip() or "usage: see the header of " + __file__)
        sys.exit(0)
    if sys.argv[1:] == ["selftest"]:
        sys.exit(1 if _selftest() else 0)
    if sys.argv[1:2] == ["ab"]:
        a = _ab_args(sys.argv[1:])
        if a is not None:
            sys.exit(ab_main(a["tag"], a["golden"], dry=a["dry"], strict=a["strict"], tier_ok=a["tier_ok"]) or 0)
        parsed = None
    else:
        parsed = _args(sys.argv[1:])
    if parsed is None:                       # an unknown argument never runs the live job
        print("usage: model-watch.py run [--days N] [--dry|--dry-run] | ab <tag>|--next [--golden FILE] "
              "[--dry|--dry-run] [--strict] [--tier-ok] | selftest", file=sys.stderr)
        sys.exit(2)
    sys.exit(run(*parsed) or 0)
