# 2026-10-07: clip-review model bake-off (report)

Dated evidence, not policy. Current behaviour lives in `README.md` (Vision clip review). Issue #21.

## Question

Which cheap vision reviewer should choose highlight in/out points and be allowed to overrule
the engine: the provider-neutral API backend, or an agent harness (pi, Claude Code) driving a
cheap model through `skills/hockey-clip-review/`? And does an adversary help?

## Contestants

| name | backend | model | harness command (config, not code) |
|---|---|---|---|
| `api-deepseek-flash` | api (two passes, thinking off) | DeepSeek `deepseek-flash` | none: OpenAI-compatible endpoint |
| `pi-deepseek-v4.1-flash` | agent | `ollama-cloud/deepseek-v4.1-flash` | `pi -p --mode json --no-session --no-context-files --no-skills --no-extensions --tools read,bash --model <m> {prompt}` |
| `pi-gemma4-31b` | agent | `ollama-cloud/gemma4:31b` (no thinking) | same |
| `pi-glm-5.3-flash` | agent | `ollama-cloud/glm-5.3-flash` | same |
| `cu-deepseek-v4.1-flash` | agent | `deepseek-v4.1-flash` via the local gateway | `claude-unified -p --model deepseek-v4.1-flash --output-format json --tools Read,Bash --permission-mode bypassPermissions --no-session-persistence {prompt}` |
| `pi-deepseek-v4.1-flash-lean` (added 2026-10-08) | agent | `ollama-cloud/deepseek-v4.1-flash`, `--thinking off` | the pi command plus `-e {skill}/harness/pi-tool-budget.ts`, `HCR_MAX_TOOL_CALLS=20` |
| `escalate` (added 2026-10-08) | derived | api, then the lean agent | not run: the api verdict, or the lean verdict where `should_escalate()` hands off (fight/major, scorebug-alert game, unsure/failed, confidence < 0.6) |

The engine's own window (`engine`) is scored alongside them.

## Corpus

- 2026-27 recorded games (current engine output in `~/amherst-display/Games/`): 09-10, 09-12,
  09-16, 09-24, 09-26, 10-03; recordings in `~/.local/state/watch-rams/archive/<run>/`.
- 2026 spring playoff series vs Summerside, games 1-6 (HockeyTech 4943-4948), re-run with the
  current engine by `scripts/run_engine_offline.py` (no Drive/email side effects) into
  `/mnt/rams-archive/watch-rams/games-2025-26-rerun/Games/`. Games 1, 3, 4, 5 carried
  `SCOREBOARD_ALERT.txt` under the old engine (stuck/glitching bug) and count as hard cases.

## Method

- Packets once per incident (`clip_review_bakeoff.py packets`); every contestant gets the
  same `incident.json` and coarse sheets. Verdicts cached by (contestant, incident, prompt hash).
- Stratified sample (`sample`, seed 7): goals in normal games, goals in scorebug-alert games,
  minors, majors/fights. Review set = sample + every alert-game goal + every major/fight.
- Reviewer-only run per contestant (`run`), a second run on a subset for stability
  (`run --run-tag run2 --subset 12`), adversary pairings (`adversary --pairs r:a,...`).
- Human judging page (`render` -> `site/`): each contestant's clip and the engine's, blinded
  and deduplicated, marks in localStorage, export JSON; `score --marks` turns it into the final
  leaderboard. Blinded judge-agent packets (`judge-packets` -> `~/.local/state/watch-rams/clip-judge/`)
  with `scripts/judge_leaderboard.py`.
- Provisional leaderboard (`provisional`): objective checks plus cross-model agreement (consensus
  event = median of the largest group of >= 3 reviewers within 5 s). It measures agreement, not
  truth; the human or judge-agent marks replace it.

## Decisions from Tom during the run (2026-10-08)

- **The ranking is highlight quality, not "goal in clip".** The event being in the clip is the
  floor. The headline is the blinded judges' 0-10 highlight score (`judge@2`: overall plus
  build_up, moment, ending, cleanliness) and Tom's marks, with spend per incident beside it.
  The consensus board below is a diagnostic.
- **Goal clips show the build-up.** In-point at the start of the scoring play, never less than
  15 s before the goal (`CLIP_REVIEW_MIN_LEAD_S`), up to 45 s. Before the rule the reviewers'
  median build-up was api 8 s, pi-deepseek 10 s, glm 13 s, cu 14 s, gemma 20 s, engine 32 s.
  The floor is re-applied by code to every saved verdict for the boards and judge packets; no
  model re-run.
- **Token budget.** About 20 turns and 12 frame pulls per incident in SKILL.md and the prompts,
  with hard guards (`claude --max-turns`; pi via `harness/pi-tool-budget.ts`).

## Results

### Judged highlight quality (the ranking)

Blinded Sonnet judges, 2 per packet plus a tie-break judge on the 10 most disputed (88 verdicts,
all schema-valid), on the 39-incident review set as it stood on 2026-10-08 02:44. Full report,
leaderboard and category split: [`2026-10-08-clip-judge/`](2026-10-08-clip-judge/report.md).

| contestant | highlight score (0-10) | event-seen set (32) | tokens / incident | median wall s |
|---|---|---|---|---|
| `pi-glm-5.3-flash` | **7.09** | 7.48 | 672k | 278 |
| `pi-deepseek-v4.1-flash-lean` | 6.39 | 6.95 | 360k | 109 |
| `cu-deepseek-v4.1-flash` | 6.04 | 6.56 | 814k | 505 |
| `pi-deepseek-v4.1-flash` | 5.81 | 6.65 | 1.32M | 383 |
| `pi-gemma4-31b` | 5.60 | 6.24 | 199k | 123 |
| `escalate` (hand-off 59%) | 5.39 | 5.82 | 229k | 93 |
| engine (no review) | 4.56 | 5.37 | - | - |
| `api-deepseek-flash` | 3.88 | 4.62 | 11k | 23 |

- glm leads mostly on penalties; on goals glm and lean are level (glm - lean +0.53, 95% CI
  -0.03 to +1.21). Every agent beats the engine; the api backend alone does not.
- The engine's minors score 2.2 (windows land after the call); its goals cut the celebration on
  42% of clips.
- `escalate` as built is no better than the api on normal-game goals (4.97): the confidence gate
  never handed those to the agent and the api misses the build-up 43% of the time.
- Judges agree on scores (mean |diff| 0.84, Spearman 0.76) but pick the same best clip only about
  half the time; use mean scores, not win rates.
- Five 2026-03-21 packets are stream-outage voids (no goal in the footage); the event-seen column
  leaves them out.
- Ollama-cloud (pi) is flat-rate, so its dollar cost is 0; claude-unified reports $4.67/incident at
  the list price of a mapped model, not a real bill; the api backend records tokens only.

### Token budget, before/after (same 4 incidents, 2026-10-08)

| contestant | tokens before -> after (total of 4) | worst incident before -> after |
|---|---|---|
| `cu-deepseek-v4.1-flash` (`--max-turns`) | 4.05M -> 2.00M | major 13_p3: 1.44M, 68 turns -> 584k, 32 turns |
| `pi-deepseek-v4.1-flash` (tool-budget extension) | 6.16M -> 1.88M | major 13_p3: 4.06M, 73 turns -> 479k, 18 turns |

All 8 budgeted runs still returned valid overrides with >= 15 s build-up on the goals. The lean
variant (thinking off, 20 tool calls) averages 360k tokens and 109 s per incident.

### Rule-based tail trim (2026-10-08)

The judges flagged 34% of glm and 38% of lean clips `too_long` (dead air after the celebration or
call). `enforce()` now ends an override at most 16 s after a goal, 12 s after a minor's foul and
35 s after a major's event (fights keep their `fight_post` bound). On the 257 judged agent clips:

| contestant | clips trimmed | median length before -> after | judged too_long clips the trim catches | trimmed clips already flagged ends_early |
|---|---|---|---|---|
| `pi-glm-5.3-flash` | 26 / 35 | 36.3 -> 31.0 s | 14 / 14 | 1 |
| `pi-deepseek-v4.1-flash-lean` | 15 / 36 | 38.5 -> 35.2 s | 9 / 16 (the rest are long lead-ins) | 2 |
| all agents | 100 / 257 | 32.0 -> 31.0 s (mean 31.9 -> 29.3) | 46 / 64 | 11 |

By class: goals 33.5 -> 31.0 s median (71 trimmed), minors 14.5 -> 14.0 s (21), majors unchanged at
the median (8 trimmed), fights untouched. Risk: 54 trimmed clips were not judged too long, and 11
were already flagged `ends_early`; the trim makes those worse. The trim and the 15 s floor are
applied when results are read, so the boards below include them, but the frozen judge packets
show the untrimmed windows the judges saw.

ADVERSARY_AND_SPRING_PLACEHOLDER

### Failure modes seen

- **Replay picked as the event** (api, gemma): on 2026-03-14 #14 they placed the goal at the
  replay (+27..+32 s) while three other reviewers placed it at -1 s.
- **Missing build-up**: api placed a near-fixed -8 s in-point; before the 15 s floor the reviewers'
  median build-up was 8-20 s.
- **Engine cuts the celebration**: its -32..+3 goal window usually contains the goal but ends as
  the celebration starts; its minor windows usually land after the call.
- **claude-unified is slow and heavy**: about 9-10 min, ~70 turns and 100k+ output tokens per
  incident before the budget; occasional 900 s timeouts (5 retried incidents).
- **Scorebug autodetect** (engine, not fixed here): on spring games 3 and 5 (Summerside home
  broadcasts) the vision pick overrode the OCR hit for `mhl_summerside_home_banner`
  (`mhl_flo_strip` / `mhl_flo_stacked_topleft` at 0.95) and 0 events matched; both re-ran with
  `--profile mhl_summerside_recording` (8/10 and 20/20 matched).
- **Stream outages**: five 2026-03-21 incidents have only a "technical issues" slate; the
  correct answer is a drop, which glm and cu gave.
