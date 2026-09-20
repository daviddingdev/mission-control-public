#!/usr/bin/env python3
"""Shared helper for local-model calls (ollama). One place for the think:false gotcha.
Usage: from localllm import ask;  ask("prompt") -> str

No model tag is written down here. Which model a call gets is decided by the registry
(~/maintenance/config/models.json) via models.resolve("dense") — see models.py. That means
upgrading the box's local models is one edit in the registry, not a grep across five repos.

Nor does any caller decide WHEN it runs. Every call takes a slot from gpu.py, which orders
the box's one GPU by project priority and queues the rest (config/gpu.json). Callers need
no code for this beyond passing a `job` label if they want a readable queue.
"""
import json, os, re, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gpu
import models

ROLE = "dense"          # careful reading/extraction — what every localllm caller wants
_resolved = None        # pre-check runs once per process, not once per prompt


def _model():
    global _resolved
    if _resolved is None:
        _resolved = models.require(ROLE, job="localllm")
    return _resolved


def __getattr__(name):
    """`from localllm import DEFAULT_MODEL` still works — it now resolves live (PEP 562),
    so callers that print the model name print the one that actually ran."""
    if name in ("DEFAULT_MODEL", "FALLBACK_MODEL"):
        return _model()
    if name == "OLLAMA":
        return models.chat_url()
    raise AttributeError(name)


# When a caller turns thinking on, the model spends tokens reasoning BEFORE its answer, and
# ollama counts those against num_predict. A think call that keeps a 400-token cap returns an
# empty answer mid-thought. So a think call gets this much headroom on top of what it asked.
THINK_HEADROOM = 2500


def ask(prompt, model=None, num_predict=400, temperature=None, timeout=900,
        force_json=False, job=None, think=None, num_ctx=None, system=None):
    """One local-model call. Sampling defaults come from the registry's per-role `options`
    (models.json: temperature, think, num_ctx) so the box can change them in one place;
    an explicit argument wins. `think` on a reasoning model (qwen3.*) is a real quality
    lever for adjudication/extraction and a real cost (seconds) for bulk reads — callers
    choose per prompt. `num_ctx` is sent EXPLICITLY: the server happens to be sized at
    256K today, but an unrequested window is a default that can change under us."""
    model = model or _model()
    opts = models.options(ROLE)
    if think is None:
        think = bool(opts.get("think", False))
    if temperature is None:
        temperature = opts.get("temperature", 0.2)
    if num_ctx is None:
        num_ctx = opts.get("num_ctx")
    if think:
        num_predict = num_predict + THINK_HEADROOM
    messages = ([{"role": "system", "content": system}] if system else []) + \
               [{"role": "user", "content": prompt}]
    body = {"model": model, "messages": messages,
            "think": bool(think), "stream": False, "keep_alive": gpu.cfg()["keep_alive"],
            "options": {"num_predict": num_predict, "temperature": temperature}}
    if num_ctx:
        body["options"]["num_ctx"] = int(num_ctx)
    if force_json:
        body["format"] = "json"
    req = urllib.request.Request(models.chat_url(), json.dumps(body).encode(),
                                 {"Content-Type": "application/json"})
    t0 = time.time()
    with gpu.slot(job=job, model=model):
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.loads(r.read().decode())
    txt = d.get("message", {}).get("content", "")
    gpu.record_usage(job=job, model=model, prompt_tokens=d.get("prompt_eval_count"),
                     output_tokens=d.get("eval_count"), seconds=time.time() - t0, text=txt)
    return re.sub(r"<think>.*?</think>", "", txt, flags=re.S).strip()


PARSE_FAIL_LOG = os.path.expanduser("~/maintenance/state/local_parse_failures.jsonl")


def ask_json(prompt, **kw):
    """ask() but parses a JSON object out of the reply; returns {} on failure — and RECORDS
    the failure (job, model, first 300 chars) in state/local_parse_failures.jsonl, because
    an empty dict at the call site is indistinguishable from an empty answer and nobody could
    say how many local calls failed to parse in a month (audit 2026-09-07)."""
    txt = ask(prompt, force_json=True, **kw)
    try:
        return json.loads(txt)
    except Exception:
        m = re.search(r"\{.*\}", txt, re.S)
        try:
            return json.loads(m.group(0)) if m else _parse_fail(txt, kw)
        except Exception:
            return _parse_fail(txt, kw)


def _parse_fail(txt, kw):
    with __import__("contextlib").suppress(Exception):
        with open(PARSE_FAIL_LOG, "a") as f:
            f.write(json.dumps({"at": time.time(), "job": kw.get("job"), "think": kw.get("think"),
                                "num_predict": kw.get("num_predict"), "head": (txt or "")[:300]}) + "\n")
    print(f"localllm: unparseable JSON from job={kw.get('job')} ({len(txt or '')} chars)", file=sys.stderr)
    return {}
