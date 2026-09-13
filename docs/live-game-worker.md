# Central MHL game-only worker

`scripts/live_game_monitor.py` is the durable scorebar collector migrated from Clarence's `my-web-scrapers` repository. It has no runtime dependency on that repository. Central metadata acquisition owns the schedule; this worker never discovers games from HockeyTech and has no idle or daily HockeyTech request path.

## Configuration and authorization

The worker reads `config/hockeytech.json` beside the central repository. The default plan is `https://raw.githubusercontent.com/ThomasMcCrossin/amherst-display/main/monitor_plan.json`; `--plan-file /absolute/path/monitor_plan.json` accepts a locally delivered plan instead. `--plan-url` selects another trusted plan endpoint. Plans must use `amherst.monitor-plan.v1`, be complete, match the central identity, seasons and monitoring configuration, and carry a nonexpired timezone-aware acquisition timestamp. Invalid or failed refreshes revoke cached authorization until a successful refresh. An HTTP 304 never extends the plan's acquisition lifetime.

Refreshes use conditional ETag/Last-Modified requests, bounded response bytes and a whole-request timeout. Refresh results and their next refresh time survive restart in the private storage root. Plan reads are GitHub/data requests, not HockeyTech acquisition. With a cached valid plan, startup and restart wait until a central game window or the next plan refresh, whichever comes first. Expiry also wakes the process and revokes authorization.

Only a monitorable, nonfinal, configured-team game authorizes a scorebar attempt in `[starts_at - before_minutes, starts_at + after_hours)`. `--game-id` filters these windows; it never forces an off-game request. The full returned scorebar is retained. Explicit scorebar `GameStatus=4` stops that game's polling, with durable Final checkpoints and journal-tail recovery across restart. Zero clock, scores and intermission do not infer Final or an event.

## Invocation

Provide `HOCKEYTECH_API_KEY` only through the process environment or the host's existing protected environment binding. There is no key in JSON configuration, command arguments or status output. Do not create a second worker on a different archive root.

```sh
python3 scripts/live_game_monitor.py \
  --enabled \
  --storage-root /home/clarencehub/.local/state/my-web-scrapers/amherst-live-observations \
  --instance-key clarencehub-amherst-mhl \
  --max-polls 1000000
```

The default storage path remains `~/.local/state/my-web-scrapers/hockeytech-live`; pass the existing private archive path if the host uses a different one. Reuse its existing instance identity. The command is disabled without `--enabled`. `--max-polls` bounds authorized attempts, not waiting time; the default is one attempt. SIGINT/SIGTERM interrupt waiting cleanly. No service installation or activation is supplied here.

`status.json` while running/waiting includes active game IDs, `next_window_at`, `next_plan_refresh_at`, `next_attempt_at`, `next_wake_at`, plan error and the last attempt/success/error. A predicted next attempt remains conditional on continued plan authorization. No next window is advertised beyond the current plan's expiry.

## Archive compatibility and proof

The existing `journal.sqlite3`, writer lease, source identity, AUTOINCREMENT sequences, UUID poll IDs and immutable `polls/00000000000000000001.json` envelopes are preserved. The schema remains `hockeytech.scorebar.poll.v1`; request metadata remains the five-field safe whitelist. Raw bodies, hashes, unchanged observations, failures and interrupted attempts retain the original format. Startup may recover genuinely interrupted/unpublished journal records using the original durability rules; this source migration does not rewrite or move historical archives.

Focused verification (stdlib only):

```sh
python3 -m unittest scripts.test_live_game_monitor -v
```

The suite uses real local HTTP servers and a controllable clock to cover cached restart, exact pregame wake, no requests before/between/after games, filtering, Final persistence/replay, intermission, failed/invalid/expired plans, conditional refresh without freshness extension, raw-byte retention, single-writer exclusion, killed-attempt recovery and immutable publication replay. It never calls live HockeyTech.
