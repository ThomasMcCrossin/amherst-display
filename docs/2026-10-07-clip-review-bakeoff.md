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

## Results

Filled in below as runs complete.
