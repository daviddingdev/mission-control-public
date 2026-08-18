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

| [`dashboard/server.py`](dashboard/server.py) | 1065 | **The dashboard itself** — stdlib HTTP server, no framework. Aggregates crontab, logs, git state, ports, notification history, token usage and model health into one JSON payload, with per-probe TTL caching so a phone refresh never waits on a slow check. |
| [`dashboard/usage.py`](dashboard/usage.py) | 169 | Parses agent-session transcripts into per-job token accounting — the basis of the load figures shown next to each scheduled job. |
| [`sentinel.py`](sentinel.py) | 95 | Hourly log-anomaly pass. Its rule is the interesting part: **state files outrank log tails**, because a log tail is the last thing that happened and a state file is what is true now. Alerts on new issues only, with a cooldown. |
| [`evening-digest.py`](evening-digest.py) | 42 | The day's rollup to a phone, written by the local model from structured job state. |
| [`sweep-brief.py`](sweep-brief.py) | 66 | Compresses a month of activity into a brief the expensive monthly review reads instead of re-deriving the box. |
| [`publish.py`](publish.py) | 419 | **The pipeline that produced this repo.** Authored prose plus an allowlist of code, a pseudonym pass, a leak scanner built from the machine's live secrets, quarantine for a published file that starts leaking, and pruning for one that leaves the allowlist. Its own scanner rules are visible here — masked, which is itself the demonstration. |
| [`healthcheck.sh`](healthcheck.sh) | 50 | The 15-minute watchdog: ports, disk, service state, model roles, backup freshness — alerting only on state change. |
| [`notify.sh`](notify.sh) | 16 | Sixteen lines, but every push on the machine goes through it, which is why the permanent history exists at all. |

_Read `gpu.py` first if you only read one._
