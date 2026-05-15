# Claude Footer + Postmortem Drill-Down

## Decision

The Claude Code footer is the primary live decision surface. It should tell the user the current state, the main reason, and the next action directly in the footer.

There is no separate short intervention command. `cc-blackbox postmortem latest` is the only drill-down command for fuller evidence.

## Footer Shape

Healthy:

```text
cc-blackbox: Healthy | cache warm 22m | context 31% | continue
```

Risky:

```text
cc-blackbox: Risky | cache rebuild likely | narrow next | cc-blackbox postmortem latest
```

Stop:

```text
cc-blackbox: Stop | 4 Bash failures | fix before retry | cc-blackbox postmortem latest
```

Blocked:

```text
cc-blackbox: Blocked | per-session budget exceeded | restart narrower | cc-blackbox postmortem latest
```

## Rendering Rules

- The footer must remain one line.
- The footer must preserve state and the main reason first.
- For Risky, Stop, Blocked, and Cooldown states, include the full command `cc-blackbox postmortem latest` when it fits.
- At narrow widths, drop optional context/action/command before dropping the state or main reason.
- JSON footer output must remain uncolored.
- Text footer output may color the whole footer by decision state.

## Command Boundary

`postmortem` answers the expanded question: what happened, what evidence supports it, and what should the user do next.

The footer answers the live question: can I keep going, and what is the main reason?

Keeping one expanded command avoids duplicate report shapes and drift between multiple partial explanations.

`cc-blackbox run` is intentionally Claude-only. If the child command basename is not `claude`, it exits before stack startup, footer setup, tmux work, or child launch.

## Tests

- Top-level CLI help should expose `postmortem`, not a separate drill-down command.
- Footer tests should verify that Risky/Stop states include `cc-blackbox postmortem latest` at normal widths.
- Narrow footer tests should verify that state and main reason are preserved.
- Existing postmortem tests should cover deterministic evidence, heuristic caveats, and optional Claude analysis.
