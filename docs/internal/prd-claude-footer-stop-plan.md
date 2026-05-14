# PRD: Claude Footer Decisions and Stop Plan

Status: Draft

Date: 2026-05-14

## Problem Statement

cc-blackbox already captures the signals that explain why a Claude Code session is getting expensive, slow, or unproductive: cache rebuilds, idle gaps, context pressure, tool failure streaks, model route mismatches, quota burn, guard findings, and postmortem evidence.

The current product problem is not lack of evidence. The problem is timing and placement.

Users run Claude through `cc-blackbox run claude` and make the next prompt decision inside the Claude terminal UI. If the important signal only appears later in `postmortem`, `guard status`, `watch`, Grafana, or a separate status command, it arrives after the user has already burned the next prompt. The product should intervene at the moment the user is about to continue, compact, or restart.

From the user's perspective, cc-blackbox should answer:

> Is this Claude session still healthy enough to continue, or should I compact or restart before spending another large turn?

The answer needs to fit in the Claude footer. If the footer says to stop, the footer should also say the main reason. The expanded explanation belongs in a short `stop-plan`, not in the footer.

## Solution

Make the Claude Code footer the primary cc-blackbox product surface when users run Claude through cc-blackbox.

`cc-blackbox run claude` should provide a live footer line inside the Claude UI using Claude Code's supported status line mechanism. The footer should render one compact decision:

- current state,
- most important reason,
- best next action,
- optional drill-down command when space allows.

Example healthy footer:

```text
cc-blackbox: Healthy | cache good | context 42% | 7 turns to compact
```

Example risky footer:

```text
cc-blackbox: Risky | idle 91m | cache rebuild likely | compact or restart
```

Example stop footer:

```text
cc-blackbox: Stop | 4 Bash failures | fix precondition before retry
```

Example wide stop footer:

```text
cc-blackbox: Stop | 4 Bash failures | context 87% | restart recommended | run stop-plan
```

Add `cc-blackbox stop-plan` as the expanded action command for cases where the footer says `Risky` or `Stop`. `stop-plan` should not be another full postmortem. It should be a short intervention plan:

1. why to stop,
2. what failed,
3. what to fix before restart,
4. clean restart prompt,
5. commands or tests to run first.

Keep `postmortem` as the full forensic report. Keep `guard` as the live policy/watch surface. Hide or deprecate `reconcile` from top-level help because billed-cost reconciliation is not central to the continue/compact/restart product loop.

## Current Code Baseline

The latest implementation already has several pieces this PRD should build on rather than replace:

- `cc-blackbox run` starts or validates the local stack before launching the child command.
- Interactive `cc-blackbox run` currently tries to create a live guard split with tmux unless `--watch`, `--no-live`, non-interactive mode, or missing tmux changes the mode.
- `run` starts a run-owned session monitor that subscribes to `/watch?replay=false`, binds only live `SessionStart` events, and ignores replayed history for child ownership.
- After the child exits, `run` calls `/api/sessions/finalize` with reason `child_exit` for every session started by that child.
- Core has explicit session finalization, idempotent finalization outcomes, budget-block finalization, and low-cardinality finalization metrics.
- The run-owned guard panel already maps watch events into user-facing states and next actions, but that logic is embedded in CLI rendering rather than extracted as a reusable decision model.
- `guard watch` and the run-owned panel already render many of the same facts the footer needs: guard findings, context pressure, cache rebuilds, cache expiry warnings, quota burn, model route mismatch, request errors, compaction loops, and postmortem readiness.
- `postmortem` already returns structured JSON with diagnosis, impact, cache/context/model/tool signals, findings, evidence, recommendations, caveats, and JSONL enrichment status.

The PRD still holds, but the implementation should be framed as extraction and product placement:

1. extract the shared decision model from existing guard/watch/run-panel behavior,
2. use the footer as the primary in-Claude placement for the same decision,
3. use `stop-plan` as the short expanded action workflow,
4. keep the current guard split/watch/postmortem surfaces as supporting and advanced surfaces.

## Grilled Questions And Resolved Decisions

1. Should this be a separate `cc-blackbox statusline` command?

   Recommended answer: no, not as the user-facing product. The footer should appear through `cc-blackbox run claude`. A hidden helper command or managed script can exist internally, but users should not have to discover or run it.

2. Should cc-blackbox draw its own terminal overlay over Claude's UI?

   Recommended answer: no. Terminal overlays will fight Claude redraws, autocomplete, permission prompts, fullscreen rendering, and terminal differences. Use Claude Code's supported status line mechanism.

3. Should the footer just say `run stop-plan`?

   Recommended answer: no. The footer must include the main reason because most users will not stop to run another command unless the reason is visible. `run stop-plan` is optional drill-down text when there is room.

4. Should `stop-plan` be a top-level command or a `postmortem` flag?

   Recommended answer: top-level if it remains short and operational. If it grows into another report, collapse it into `postmortem --stop-plan`. The intended boundary is that `stop-plan` is an intervention workflow, while `postmortem` is the evidence report.

5. Should `stop-plan` replace `postmortem`?

   Recommended answer: no. The commands answer different questions. `stop-plan` answers "what should I do before the next prompt?" `postmortem` answers "what happened and what evidence supports it?"

6. Should the default footer ever block the next prompt?

   Recommended answer: no. The default should warn and recommend. Blocking belongs in explicit strict mode through policy or hook decision control.

7. Should strict mode be part of the MVP?

   Recommended answer: no. Design the decision engine so strict mode can reuse it later, but ship the footer and stop-plan first.

8. Should official Claude Code OpenTelemetry replace cc-blackbox's proxy signals?

   Recommended answer: no. Treat OTel as an optional facts source later. cc-blackbox's product value is interpretation and decisioning, not replacing official telemetry.

9. Should `reconcile` be deleted?

   Recommended answer: remove it from visible top-level help first, but keep a hidden/deprecated compatibility path for one release. It can later move under a finance or advanced command group.

10. How should cc-blackbox map Claude's status line session to a proxy session?

    Recommended answer: use direct Claude status line fields where available, then correlate through current working directory, project directory, transcript path, hook telemetry, active sessions, and recent prompt/session metadata. If the match is ambiguous, show a conservative aggregate footer rather than pretending to know the exact session.

11. Should the footer run expensive JSONL or postmortem analysis?

    Recommended answer: no. Footer rendering must be fast and local. Use in-memory active state and recent persisted guard findings. `stop-plan` and `postmortem` can do richer synthesis.

12. Should `cc-blackbox run claude` automatically modify Claude settings?

    Recommended answer: only when it can do so safely. It may install or update a cc-blackbox-owned status line configuration when no custom status line exists, or when the existing value is already managed by cc-blackbox. If a user has a custom status line, do not clobber it. Show a clear warning and documented setup path.

13. Should the existing default tmux live guard split remain the primary `run claude` experience?

    Recommended answer: no, not once the footer is reliable. The footer should become the primary default because it appears inside the user's actual Claude session. The existing tmux split and `guard watch` should remain as richer/advanced views and as fallbacks while the footer is unavailable or explicitly disabled.

14. Should the new decision engine replace the current run-owned session monitor and finalization flow?

    Recommended answer: no. The monitor and finalization flow are now useful infrastructure. The decision engine should consume the same active-session and watch-event state, not create a competing lifecycle tracker.

## User Stories

1. As a Claude Code user, I want `cc-blackbox run claude` to show cc-blackbox status inside the Claude footer, so that I do not need a separate dashboard while working.

2. As a Claude Code user, I want the footer to tell me when the session is healthy, so that I can continue without checking another command.

3. As a Claude Code user, I want the footer to tell me when the session is risky, so that I can avoid an expensive or unproductive next prompt.

4. As a Claude Code user, I want the footer to tell me when stopping is recommended, so that I do not keep retrying a degraded session.

5. As a Claude Code user, I want the footer to include the main reason for a stop recommendation, so that I understand the warning immediately.

6. As a Claude Code user, I want the footer to include the best next action when there is room, so that I know whether to continue small, compact, fix a precondition, or restart.

7. As a Claude Code user, I want the footer to degrade gracefully in narrow terminals, so that it remains readable instead of wrapping into noise.

8. As a Claude Code user, I want idle risk to show in the footer, so that I know when a cache rebuild is likely before sending another large prompt.

9. As a Claude Code user, I want cache rebuild risk to show in the footer, so that I can decide whether to continue while cache is warm or restart cleanly.

10. As a Claude Code user, I want context pressure to show in the footer, so that I know when `/compact` or restart is safer than another broad prompt.

11. As a Claude Code user, I want repeated Bash failures to show in the footer, so that I stop retrying a broken command without fixing its precondition.

12. As a Claude Code user, I want repeated tool failures to show in the footer, so that I can redirect Claude before it loops.

13. As a Claude Code user, I want model route mismatch warnings to show when relevant, so that I know when the response model differed from what was requested.

14. As a Claude Code user, I want quota or budget pressure to show in the footer when available, so that I can avoid surprise burn.

15. As a Claude Code user, I want `cc-blackbox stop-plan latest`, so that I can expand a footer warning into a concrete restart plan.

16. As a Claude Code user, I want `cc-blackbox stop-plan SESSION_ID`, so that I can inspect a specific session from history or another terminal.

17. As a Claude Code user, I want `stop-plan` to explain why stopping is recommended, so that I can trust the decision.

18. As a Claude Code user, I want `stop-plan` to list what failed, so that I know what not to retry blindly.

19. As a Claude Code user, I want `stop-plan` to list what to fix before restart, so that the next session starts from a cleaner state.

20. As a Claude Code user, I want `stop-plan` to produce a restart prompt, so that I can recover work without dragging the entire degraded context forward.

21. As a Claude Code user, I want `stop-plan` to include commands or tests to run first, so that I can validate the repaired preconditions before another large prompt.

22. As a Claude Code user, I want `stop-plan` to be short, so that it is faster than reading a full postmortem.

23. As a Claude Code user, I want `postmortem` to remain available, so that I can inspect full evidence when I need a forensic report.

24. As a Claude Code user, I want the footer and `stop-plan` to agree, so that I do not get contradictory advice from cc-blackbox.

25. As a Claude Code user, I want cc-blackbox to avoid overwriting my custom Claude status line, so that adopting cc-blackbox does not destroy my setup.

26. As a Claude Code user with a custom status line, I want a clear message when cc-blackbox cannot safely install its footer, so that I know what tradeoff I need to make.

27. As a Claude Code user, I want the footer to keep updating during idle periods, so that idle and cache TTL warnings do not freeze.

28. As a Claude Code user, I want the footer to keep working if the core is briefly unavailable, so that Claude's UI is not broken by cc-blackbox.

29. As a Claude Code user, I want the footer to say when cc-blackbox has no matching session yet, so that early-session state is honest.

30. As a user running multiple Claude sessions, I want the footer to avoid showing another session's warning as if it were mine, so that I do not act on the wrong signal.

31. As a user running multiple Claude sessions in one repository, I want correlation to prefer the active Claude session rather than only the working directory, so that parallel sessions do not collide.

32. As a user in a tmux workflow, I want the footer to complement the guard/watch panes, so that I can keep the main Claude pane focused.

33. As a user who does not use tmux, I want the footer to be enough for day-to-day work, so that I do not need a second terminal.

34. As a privacy-conscious user, I want the footer and stop-plan to avoid raw prompts, raw tool payloads, file contents, and secrets, so that the product remains safe to screenshot.

35. As a privacy-conscious user, I want any restart prompt to be derived from redacted or summarized evidence by default, so that it does not leak raw transcript content unnecessarily.

36. As a maintainer, I want one shared decision engine, so that footer, guard, stop-plan, and postmortem do not drift.

37. As a maintainer, I want the decision engine to return structured reasons, so that renderers can choose the highest-signal reason for their space constraints.

38. As a maintainer, I want the decision engine to label evidence levels, so that heuristic cache and context guidance is not presented as provider-confirmed fact.

39. As a maintainer, I want footer rendering to be deterministic, so that snapshot tests can cover narrow and wide terminals.

40. As a maintainer, I want `stop-plan` rendering to be deterministic, so that regression tests can assert the sections and action priority.

41. As a maintainer, I want the footer path to avoid the ext_proc hot path, so that proxy latency and fail-open behavior remain intact.

42. As a maintainer, I want request-side budget and cooldown blocks to keep their existing guard semantics, so that warnings do not accidentally become enforcement.

43. As a maintainer, I want strict mode to reuse the decision engine later, so that hook blocking does not introduce a competing policy brain.

44. As a maintainer, I want OpenTelemetry to be optional, so that solo users do not need an OTel collector before the footer is useful.

45. As a maintainer, I want visible top-level CLI help to focus on the core workflow, so that finance-oriented commands do not distract from continue/compact/restart decisions.

46. As an advanced team user, I want a compatibility path for cost reconciliation, so that existing workflows do not break abruptly when `reconcile` disappears from top-level help.

47. As a maintainer, I want docs to explain the footer/postmortem/stop-plan boundary, so that future features do not collapse them into one overloaded report.

48. As a maintainer, I want e2e coverage for `cc-blackbox run claude` footer setup, so that the main user path stays reliable.

## Implementation Decisions

- Introduce a shared session decision model that is independent of any one renderer.

- Extract decision behavior from the current guard watch and run-owned guard panel logic instead of adding a third independent interpretation layer.

- The decision model should return a state, ordered reasons, best next action, alternative actions, evidence labels, caveats, and optional drill-down command.

- The decision states should be optimized for user action, not internal implementation detail. Recommended visible states are healthy, watching, risky, stop recommended, blocked, cooldown, and ended.

- The top decision reason should be the highest-signal reason that can fit in the footer. Examples include idle gap, cache rebuild likely, context percentage, tool failure streak, budget pressure, model mismatch, and cooldown.

- Footer rendering should use the same decision model as `stop-plan`.

- `stop-plan` should use the same decision model as the footer, plus richer postmortem and JSONL enrichment when available.

- `postmortem` should continue to own full evidence reporting and timeline rendering.

- `guard` should continue to own policy status, watch output, and future strict-mode enforcement.

- The existing run-owned session monitor and `/api/sessions/finalize` flow should remain the lifecycle source for `cc-blackbox run` sessions.

- Footer work should not duplicate session finalization or child-session ownership logic.

- Once stable, the footer should become the default live surface for `cc-blackbox run claude`; the current tmux live guard split should become an explicit richer view or fallback, not the only first-class live UX.

- Add a compact footer API that accepts Claude status line JSON and returns a rendered footer string plus structured decision metadata.

- The footer API should be fast and resilient. It should use active in-memory state and recent persisted findings. It should not perform expensive JSONL scans or Claude-assisted analysis.

- The footer API should return a useful fallback when no cc-blackbox session can be confidently matched.

- The footer API should prefer direct session correlation when available, then fall back to working directory and project directory matching, then to latest active session only when the match is unambiguous.

- Ambiguous correlation should produce a conservative footer, not a wrong per-session warning.

- Footer rendering should support width-aware degradation. The renderer should choose from full, medium, short, and minimal formats.

- Footer content priority is state, main reason, best action, then optional drill-down command.

- The footer should use ASCII-safe separators by default to avoid terminal compatibility issues.

- `cc-blackbox run claude` should attempt to enable a cc-blackbox-managed Claude status line only when it can do so without clobbering user configuration.

- A status line configuration is safe to manage when it is absent or already marked as cc-blackbox-managed.

- If a custom non-managed status line exists, `cc-blackbox run claude` should not overwrite it. It should continue the run and print a clear setup warning outside the Claude UI.

- The managed status line should call a local cc-blackbox helper or endpoint that reads Claude status line JSON from stdin and prints one footer line.

- The managed status line should set a refresh interval so idle age and cache TTL warnings update while the user is not actively sending prompts.

- The status line helper should time out quickly and print a minimal fallback instead of blocking the Claude UI.

- Add `cc-blackbox stop-plan latest`.

- Add `cc-blackbox stop-plan last` as an alias for latest if that matches existing command conventions.

- Add `cc-blackbox stop-plan SESSION_ID`.

- Add `cc-blackbox stop-plan --json` for scripts and future UI consumers.

- `stop-plan` should default to local deterministic synthesis.

- `stop-plan latest` should benefit from the existing child-exit finalization path: after `cc-blackbox run claude` exits, it should usually read a final postmortem/decision rather than a stale active session.

- `stop-plan` should still support active or idle sessions because `postmortem latest` can render active snapshots and idle postmortem readiness exists.

- `stop-plan` should clearly label heuristic cache and context reasoning.

- `stop-plan` should output a clean restart prompt only when restart is recommended or viable.

- If continuing is safe, `stop-plan` should say so and provide the next safe prompt shape rather than forcing restart.

- Hide or deprecate `reconcile` from visible top-level help. Keep a compatibility path for at least one release unless the maintainer explicitly chooses a breaking CLI cleanup.

- Preserve proxy invariants: response-side ext_proc phases return continue immediately, response body mode remains streamed, broadcasting stays off the response hot path, and the proxy remains fail-open.

- Do not use session IDs as Prometheus labels.

- Do not store raw prompts, raw assistant text, raw tool payloads, or file contents as part of the decision model.

## Testing Decisions

- Tests should verify externally visible behavior rather than private helper details.

- Decision engine tests should cover state ranking across healthy, risky, stop recommended, blocked, cooldown, and ended sessions.

- Decision engine tests should cover reason priority when multiple signals are present.

- Decision engine tests should verify that direct evidence outranks heuristic evidence when deciding the main reason.

- Decision engine tests should verify that heuristic evidence is labeled as heuristic in structured output.

- Footer renderer tests should cover wide, medium, narrow, and minimal terminal widths.

- Footer renderer tests should verify that the main reason remains visible before the optional `run stop-plan` drill-down.

- Footer renderer tests should verify that output remains one compact line by default.

- Footer API tests should cover direct session match, working-directory match, ambiguous match, no match, and core fallback.

- Footer API tests should verify that expensive postmortem or JSONL enrichment is not required for footer rendering.

- Status line integration tests should verify safe install when no status line exists.

- Status line integration tests should verify update when the existing status line is cc-blackbox-managed.

- Status line integration tests should verify no clobber when the user has a custom status line.

- Status line helper tests should verify timeout/fallback behavior when core is unavailable.

- `run claude` tests should verify that managed footer setup happens before launching Claude when safe.

- `run claude` tests should verify that a custom status line warning does not prevent the Claude command from running.

- `run claude` tests should verify the intended live-surface precedence once the footer ships: footer when safely configured, explicit rich guard split when requested, fallback text when neither footer nor tmux can be used.

- Existing run-owned finalization tests should continue to pass, proving footer work did not break child-session ownership or `/api/sessions/finalize`.

- Existing run guard panel tests should either continue to pass or be migrated to the shared decision model, proving extracted decisions preserve current warning semantics.

- `stop-plan` tests should verify the required sections: decision, why stop, failed signals, fix before restart, restart prompt, commands/tests to run first, and alternatives.

- `stop-plan` tests should verify that healthy sessions do not produce false stop recommendations.

- `stop-plan` tests should verify that repeated tool failures produce a fix-precondition recommendation.

- `stop-plan` tests should verify that context pressure produces compact or restart guidance depending on severity.

- `stop-plan` tests should verify that cache rebuild and idle signals are caveated as local or heuristic when appropriate.

- CLI parser tests should verify `stop-plan latest`, `stop-plan last`, explicit session IDs, and `--json`.

- CLI help tests should verify that `reconcile` is no longer visible in the primary help output if it remains as a compatibility command.

- Existing postmortem tests should continue to pass, proving `stop-plan` did not break full report generation.

- Existing guard tests should continue to pass, proving warning decisions did not accidentally become blocks.

- Existing parallel session tests should continue to pass because session identity remains load-bearing.

## Out of Scope

- Building a raw terminal overlay over Claude's UI.

- Replacing Claude Code's supported status line mechanism.

- Making strict-mode prompt blocking the default behavior.

- Requiring OpenTelemetry collector setup for the MVP.

- Deleting billed-cost reconciliation storage or historical data.

- Changing session identity.

- Changing response body streaming mode.

- Moving decision synthesis onto the ext_proc response hot path.

- Persisting raw full transcripts, file contents, or tool payloads.

- Building a full dashboard redesign.

- Making `stop-plan` a long forensic report.

- Auto-running `/compact`, killing Claude, or starting a new Claude session without explicit user action.

## Further Notes

The product boundary should stay simple:

- Footer: tiny live decision inside Claude.
- Stop plan: short intervention workflow when the footer says risky or stop.
- Postmortem: full forensic report and evidence.
- Guard: live policy state and future strict enforcement.

The highest-risk implementation area is not the decision engine. It is safely enabling Claude's status line without overwriting user configuration. The MVP should bias toward non-destructive setup and explicit warnings over clever terminal manipulation.

Claude Code's status line documentation says the status line runs a local command, receives JSON session data on stdin, supports refresh intervals, and does not consume API tokens. Claude Code hook documentation also supports future strict-mode blocking through `UserPromptSubmit`, but that should be a later policy feature rather than the MVP default.
