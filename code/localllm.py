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
import json, os, re, sys, urllib.request

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


def ask(prompt, model=None, num_predict=400, temperature=0.2, timeout=900,
        force_json=False, job=None):
    model = model or _model()
    body = {"model": model, "messages": [{"role": "user", "content": prompt}],
            "think": False, "stream": False, "keep_alive": gpu.cfg()["keep_alive"],
            "options": {"num_predict": num_predict, "temperature": temperature}}
    if force_json:
        body["format"] = "json"
    req = urllib.request.Request(models.chat_url(), json.dumps(body).encode(),
                                 {"Content-Type": "application/json"})
    with gpu.slot(job=job, model=model):
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.loads(r.read().decode())
    txt = d.get("message", {}).get("content", "")
    return re.sub(r"<think>.*?</think>", "", txt, flags=re.S).strip()


def ask_json(prompt, **kw):
    """ask() but parses a JSON object out of the reply; returns {} on failure."""
    txt = ask(prompt, force_json=True, **kw)
    try:
        return json.loads(txt)
    except Exception:
        m = re.search(r"\{.*\}", txt, re.S)
        try:
            return json.loads(m.group(0)) if m else {}
        except Exception:
            return {}
