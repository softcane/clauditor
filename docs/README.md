# docs/

## Public assets

The public README now leans much harder on product proof, so these assets are
worth treating as part of the launch surface:

- `docs/demo.gif` — short demo asset for the README hero section. Current file
  is a best-effort illustrative composite, not a literal live capture.
- `docs/tmux-dashboard.png` — current best-effort illustrative mock of the live
  multi-session tmux view, based on real UI strings and layout.
- `docs/grafana-overview.png` — curated from real trend dashboard crops taken
  from the local Grafana stack.
- `docs/before-after.png` — best-effort social/share image showing messy
  terminals vs. Clauditor's dashboard.

These are good enough for a public pass, but the long-term goal should still be
to replace the illustrative tmux assets with real captures from a live session.


## demo.gif — what to capture

If you promote `docs/demo.gif` into the root README hero section, it should
show the pain-to-payoff story in ~10–15 seconds:

1. **Show the pain.** Open two or three `claude` terminals so the screen already
   feels busy and hard to track.
2. **Start Clauditor.** In a clean shell, run `clauditor watch --tmux`. The
   dashboard bootstraps its own tmux session and shows `Waiting for sessions...`.
3. **Create overlap.** In each `claude` terminal, send a short read-only prompt
   (for example: *"grep for `broadcast` and read the top three matches"*).
4. **Cut to the payoff.** Panes appear one by one with session headers, tool
   events, and activity state.
5. **Pause on one strong signal.** Make sure the clip clearly shows at least one
   of: cache TTL countdown, context-pressure line, or model-fallback warning.

The visual story: overlapping Claude sessions become calm and legible.

## Screenshot plan

### `docs/tmux-dashboard.png`

Capture a 2-4 pane dashboard with:

- clear session headers
- visible tool events
- one cache or context signal
- the orchestrator strip visible

Suggested caption: `One tmux pane per session, with live tool activity and
session-health signals.`

### `docs/grafana-overview.png`

Capture the dashboard that best communicates longer-horizon value:

- estimated cost or token trend
- cache waste / hit ratio
- model fallback or degraded-session signal

Suggested caption: `Trend view for the sessions you were too busy to watch live.`

### `docs/before-after.png`

If you make a comparison image, keep it simple:

- left: several noisy terminal windows
- right: Clauditor's organized tmux dashboard

This is mainly for social posts, GitHub previews, and launch threads.

## Recording tips

- `asciinema rec` -> `agg` is a lightweight path to a small GIF.
- A raw screen recording converted with `ffmpeg` or `gifski` is also fine.
- Aim for ~800 px width, ~10 fps, and under 2 MB for fast GitHub loading.
- Crop tightly to the terminal; avoid desktop chrome.
- Favor one obvious story over a tour of every feature.
