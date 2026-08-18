# Code

Nine components of the platform, copied verbatim from the private repo — no edits, no
reconstruction. Python standard library only; the box runs no third-party dependency in
this layer on purpose, so nothing here can rot out from under a cron job.

| File | Lines | What it demonstrates |
|---|---:|---|
| [`gpu.py`](gpu.py) | 470 | **GPU admission control.** A cooperative priority queue in front of a single local LLM: per-project tiers, capped ageing so nothing starves, model affinity bounded below one tier gap, per-job ranks, lease reclamation by pid+TTL, and a fail-open path so the scheduler can never wedge the machine. Includes its own ordering self-test. |
| [`models.py`](models.py) | 194 | **Role registry.** Jobs ask for a capability (`dense`, `bulk`, `embed`), never a model name. Resolves to the best model actually installed, pre-checks it before the job starts work, and exits with a distinct code when the box cannot serve the role — so a dead model server kills a job at second zero instead of halfway through a night's queue. The preference list doubles as the fallback chain. |
| [`backoffice.py`](backoffice.py) | 820 | **The janitor.** Census of the machine, ~14 coded audit rules against everything written down, auto-repair of the platform's own files with a git commit, memo filing for anything in another project, and a local-model brief per project. Findings are fingerprinted with state so "new" is a real event. Note the guard that refuses to commit a file another agent session already had open. |
| [`backup.py`](backup.py) | 145 | **Backups as a declaration.** One config lists what is *gitignored and unrecoverable*; the writer, the watchdog's freshness alarm and the audit's coverage rule all read it. Every archive is verified by reading it back before old ones are pruned — a backup you have never restored is a hypothesis. |
| [`localllm.py`](localllm.py) | 67 | The shared client every local-model caller uses. Small on purpose: it is where the model resolution, the GPU slot and one provider-specific gotcha live, so no caller has to know about any of them. |
| [`daily-log.py`](daily-log.py) | 105 | A narrator that **refuses to re-report**. A weekly job's log tail sits unchanged for six days, so a naive summarizer flags the same stale error every night; this one diffs against the previous snapshot and tells the model which lines are actually new. |
| [`model-watch.py`](model-watch.py) | 122 | Weekly scan of open-model releases with a local model writing the pull/skip verdict — the model layer keeping itself current at zero API cost. The prompt interpolates the current driver rather than naming it, because this job's whole purpose is to find the thing that replaces it. |
| [`memo-triage.py`](memo-triage.py) | 45 | Staleness nudge for the cross-project memo bus. |
| [`build-journal.py`](build-journal.py) | 42 | Weekly "what got built" journal across every repo on the box — derived from what is on disk, not from a maintained list. |

_Read `gpu.py` first if you only read one._
