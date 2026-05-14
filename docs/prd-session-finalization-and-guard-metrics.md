# PRD: Session Finalization and Guard Metrics

Status: Draft

Date: 2026-05-14

## Problem Statement

cc-blackbox successfully captures Claude Code proxy traffic, tool activity, token usage, cache behavior, context status, and hard guard blocks. The litmus run against KubeAttention proved that the capture path works, but it also exposed a product gap:

When `cc-blackbox run` launches a one-shot Claude process and that process exits, the corresponding cc-blackbox session can remain marked as active and in progress. The same happens after a hard token-budget block. The raw rows exist, partial postmortems can be generated, and watch events are visible, but final diagnosis rows, final session metrics, `SessionEnd` events, and immediate `/api/diagnosis/<session_id>` responses are missing.

From the user's perspective, this means a completed or blocked run still looks alive. Post-run diagnosis is delayed or unavailable, Prometheus final-session metrics stay empty, and the user cannot trust the app's state immediately after a run.

The litmus also showed that hard guard blocks are visible in logs, watch events, SQLite findings, and guard status, but they are not exposed as dedicated Prometheus counters. That makes alerting and dashboarding on actual blocks unnecessarily hard.

## Solution

Make session finalization explicit and deterministic.

When `cc-blackbox run` owns a Claude child process, the CLI should track the live cc-blackbox sessions started by that child. When the child exits, the CLI should ask the core service to finalize those sessions. Finalization must synchronously complete local deterministic artifacts: set the session end time, persist diagnosis, update recall/final metadata, broadcast `SessionEnd`, update final metrics, and make diagnosis and final postmortem APIs immediately usable.

When the core hard-blocks a request for a per-session token budget or trusted-dollar budget, the core should finalize that affected session immediately. Hard budget block means the session is terminal from cc-blackbox's perspective. API cooldown remains separate because it can be global and temporary.

Add low-cardinality Prometheus counters for guard findings, hard guard blocks, and session finalization outcomes. These metrics must not use `session_id`, prompts, paths, or raw messages as labels.

## User Stories

1. As a `cc-blackbox run` user, I want a completed Claude process to produce a completed cc-blackbox session, so that the app state matches what happened in my terminal.

2. As a user running `claude -p`, I want diagnosis to be available immediately after the command exits, so that I can inspect the result without waiting for an inactivity timeout.

3. As a user whose request was blocked by budget policy, I want that session to be marked terminal, so that I know the run has stopped by policy.

4. As a user, I want `/api/sessions` to show ended sessions as ended, so that recent history is not confused with active work.

5. As a user, I want `/api/diagnosis/<session_id>` to return a diagnosis after a run ends, so that post-run debugging has a stable API.

6. As a user, I want `cc-blackbox postmortem <session_id>` to render a final report after a run exits, so that I do not get a partial report for completed work.

7. As a user, I want `cc-blackbox postmortem latest` to find the just-finished run, so that the documented workflow works reliably.

8. As a user running multiple Claude sessions in the same project, I want only the sessions created by my `cc-blackbox run` process to be finalized, so that unrelated sessions are not ended accidentally.

9. As a user running multiple sessions from one child process, I want all sessions started by that child to be finalized on exit, so that no owned session is left active.

10. As a user joining watch mid-session, I want existing watch replay behavior to keep working, so that session headers and events still appear.

11. As a watch user, I want a `SessionEnd` event when a run exits, so that watch output reflects completion.

12. As a tmux watch user, I want ended panes to stop looking active, so that the pane layout reflects reality.

13. As a guard watch user, I want a hard budget block to produce a terminal state for the affected session, so that I can stop thinking it might continue.

14. As a dashboard user, I want final-session histograms to update when a run exits, so that session duration, turn count, and cost charts are meaningful.

15. As a dashboard user, I want counters for guard findings, so that I can see which rules are firing over time.

16. As a dashboard user, I want counters for hard guard blocks, so that I can alert when budget blocks occur.

17. As a maintainer, I want finalization counters, so that regressions in finalization are visible.

18. As a maintainer, I want finalization to be idempotent, so that duplicate CLI calls or retry behavior cannot corrupt session state.

19. As a maintainer, I want core-owned timestamps, so that clients cannot forge arbitrary session end times.

20. As a maintainer, I want finalization to reuse the existing session-end path, so that diagnosis, recall, metrics, and watch broadcasts do not fork into competing implementations.

21. As a maintainer, I want response-side ext_proc behavior unchanged, so that finalization work does not move onto the Envoy hot path.

22. As a maintainer, I want the request-side budget block path to finalize after the block decision is made, so that policy enforcement remains clear and deterministic.

23. As a maintainer, I want block metrics without unbounded labels, so that Prometheus remains healthy.

24. As a maintainer, I want no `session_id` metric labels, so that per-session drilldown stays in SQLite and JSON APIs.

25. As a maintainer, I want tests for CLI-owned finalization, so that one-shot run behavior does not regress again.

26. As a maintainer, I want tests for budget-block finalization, so that hard policy stops stay terminal.

27. As a maintainer, I want tests for finalization idempotency, so that retries are safe.

28. As a maintainer, I want tests for Prometheus counters, so that guard block observability remains available.

29. As a maintainer, I want passive proxy sessions to keep using inactivity timeout, so that sessions not launched by `cc-blackbox run` still end eventually.

30. As a user, I want a clear failure if finalization cannot complete, so that I know to inspect core health or logs.

31. As a user, I want finalization to avoid optional Claude-assisted analysis, so that final local evidence is fast and reliable.

32. As a privacy-conscious user, I want finalization to store only the existing derived metadata, so that raw prompts, assistant text, file contents, and tool outputs are not persisted.

## Implementation Decisions

- Add a local core API for explicit session finalization.

- The finalization API accepts a list of session IDs, a finalization reason, and optional low-cardinality metadata such as child exit code or guard rule ID.

- The finalization API is idempotent. It reports per-session outcomes such as finalized, already finalized, not found, or failed.

- The finalization API uses core server time for `ended_at`. Clients do not provide end timestamps.

- The finalization API synchronously completes deterministic local artifacts before returning success.

- Successful finalization sets session end time, persists a diagnosis row, persists recall/final summary data where available, emits `SessionEnd`, removes active in-memory state, updates final session metrics, and makes diagnosis and final postmortem APIs final rather than partial.

- The finalization API reuses the existing core session-end machinery instead of adding a second diagnosis or cleanup path.

- `cc-blackbox run` tracks live `SessionStart` events while its child process is running.

- `cc-blackbox run` ignores replayed session-start history when binding sessions to the child process.

- `cc-blackbox run` finalizes every session started by that child process after the child exits.

- `cc-blackbox run` does not finalize sessions that existed before the child process started.

- If no session was observed during the child run, `cc-blackbox run` should not treat that as fatal; it should preserve the existing "no proxied Claude API request" behavior.

- Hard budget blocks finalize the affected session immediately.

- Per-session token budget blocks are terminal.

- Per-session trusted-dollar budget blocks are terminal.

- API cooldown does not automatically finalize a session because it can be global and temporary.

- Warning-only guard findings do not finalize a session.

- Add a guard findings counter with labels for rule ID, severity, action, and source.

- Add a guard blocks counter with a rule ID label.

- Add a session finalization counter with reason and outcome labels.

- Keep all new metrics low-cardinality. Do not include session IDs, prompts, paths, messages, tool payloads, or arbitrary error strings in metric labels.

- Preserve existing proxy invariants: response-side ext_proc phases return `CONTINUE` immediately, response parsing and broadcasting stay off the hot path, response body mode remains streamed, and `failure_mode_allow` remains true.

- Existing passive sessions still use inactivity timeout as a fallback finalization mechanism.

## Testing Decisions

- Tests should verify externally visible behavior rather than private helper details.

- Core API tests should verify that explicit finalization sets session end state, creates a diagnosis row, emits final session behavior, and is idempotent.

- Core API tests should verify per-session results for finalized, already finalized, and not found sessions.

- CLI tests should verify that `cc-blackbox run` tracks live session starts during child execution and calls finalization after child exit.

- CLI tests should verify that replayed session starts are ignored for run ownership.

- CLI tests should verify that multiple sessions started by one child are all finalized.

- CLI tests should verify that sessions started before the run are not finalized.

- Guard tests should verify that per-session token budget blocks finalize the affected session.

- Guard tests should verify that trusted-dollar budget blocks finalize the affected session when trusted spend enforcement applies.

- Guard tests should verify that API cooldown does not finalize an arbitrary session.

- Metrics tests should verify guard findings counters, guard blocks counters, and finalization counters.

- Metrics tests should verify that new metrics do not include session IDs as labels.

- Regression tests should cover a one-shot `claude -p` style flow and assert that diagnosis is available immediately after the child exits.

- Regression tests should cover a hard budget-block flow and assert that the blocked session is no longer partial.

- Existing parallel-session regression should continue to pass because session identity and watch replay are load-bearing.

## Out of Scope

- Adding a read-only command mode for validation runs.

- Blocking or rewriting unsafe Bash commands.

- Changing Claude Code permission behavior.

- Adding user authentication to local core APIs.

- Adding high-cardinality Prometheus labels or per-session metric labels.

- Reworking session identity.

- Changing response body streaming mode.

- Moving diagnosis, broadcasting, or postmortem work onto the ext_proc hot path.

- Implementing optional Claude-assisted postmortem analysis as part of finalization.

- Changing JSONL enrichment or correlation behavior except where finalization makes existing postmortem APIs immediately usable.

## Further Notes

This PRD is based on the 2026-05-14 KubeAttention litmus validation. The validation ran five independent Claude sessions through cc-blackbox and found no P0 capture failure. The repeated P1 was finalization: completed or blocked sessions had persisted request/tool/turn data but stayed active and partial. The budget block itself worked, but dedicated block metrics were missing.

The most important design decision is ownership. The proxy observes traffic, but `cc-blackbox run` owns the child process lifecycle. When the child exits, the wrapper should explicitly finalize the sessions it created. The core remains responsible for actual finalization semantics, persistence, diagnosis, events, and metrics.
