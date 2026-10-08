# Clip bake-off: blinded judge report (amherst-display#21)

Run 2026-10-08 by the judging orchestrator (Opus 5.5) per `JUDGE_ORCHESTRATOR.md`. 39 packets, 2 independent
Sonnet judges each (78 verdicts), plus a third Sonnet tie-break judge on the 10 most-disputed packets (88 verdicts
in total). All 88 are schema-valid (`validate_verdicts.py`). The judges were blind: no judge saw `key.json` or
another judge's verdict. I opened `key.json` only after every first-pass verdict existed.

**Headline.** `pi-glm-5.3-flash` makes the best highlights: 7.09/10 against 4.56 for the engine's own clips. Its
lead over the runner-up, `pi-deepseek-v4.1-flash-lean` (6.39), comes almost entirely from penalties. On goals the two
are level, and the gap between them is not statistically clear. Every agent except the API-only
`api-deepseek-flash` beats the engine. The engine's worst category is penalties (minors 2.19).

## 1. Leaderboard (final, with tie-breaks)

Output of `scripts/judge_leaderboard.py` (`leaderboard.md`/`leaderboard.json` here; see §8 for the script fix):

| contestant | incidents | **highlight score** | tokens / incident | cost $ / incident | wall s / incident | build-up | moment | ending | cleanliness | win rate (equiv.) | starts late | wrong event | missing event | ends early | too long | dropped (event seen) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| pi-glm-5.3-flash | 35 | **7.09** | 672161 | 0.0 | 277.8 | 7.31 | 7.96 | 7.34 | 7.38 | 0.295 | 0.038 | 0.0 | 0.038 | 0.051 | 0.342 | 4 (0) |
| pi-deepseek-v4.1-flash-lean | 36 | **6.39** | 359524 | 0.0 | 109.1 | 6.77 | 7.22 | 6.36 | 6.79 | 0.231 | 0.11 | 0.061 | 0.098 | 0.146 | 0.378 | 3 (0) |
| cu-deepseek-v4.1-flash | 36 | **6.04** | 814072 | 4.6679 | 504.9 | 5.75 | 7.46 | 6.76 | 7.14 | 0.171 | 0.222 | 0.049 | 0.074 | 0.111 | 0.16 | 3 (0) |
| pi-deepseek-v4.1-flash | 38 | **5.81** | 1317360 | 0.0 | 382.8 | 5.54 | 7.1 | 6.28 | 6.84 | 0.145 | 0.233 | 0.058 | 0.116 | 0.116 | 0.163 | 1 (0) |
| pi-gemma4-31b | 37 | **5.6** | 199462 | 0.0 | 123.2 | 6.0 | 6.42 | 5.6 | 6.48 | 0.234 | 0.095 | 0.167 | 0.214 | 0.143 | 0.155 | 2 (0) |
| escalate (hand-off 59%) | 36 | **5.39** | 229265 | 0.0 | 93.4 | 5.49 | 6.85 | 5.44 | 7.02 | 0.13 | 0.232 | 0.085 | 0.122 | 0.341 | 0.159 | 3 (0) |
| engine | 39 | **4.56** | – | – | – | 5.9 | 5.99 | 4.38 | 5.96 | 0.154 | 0.159 | 0.091 | 0.17 | 0.42 | 0.193 | 0 (0) |
| api-deepseek-flash | 39 | **3.88** | 10650 | – | 22.5 | 3.67 | 5.58 | 4.25 | 6.12 | 0.051 | 0.432 | 0.148 | 0.261 | 0.386 | 0.023 | 0 (0) |

The tie-breaks barely moved the board. Before them (two judges only, `leaderboard-2judge.md`) glm scored 7.10,
lean 6.39 and the engine 4.58, with the same order.

### Outage packets distort the headline, so compare on a common set too

Five packets from the 2026-03-21 game have no goal anywhere in `context.mp4`, only a "stream experiencing technical
issues" slate and a frozen scoreboard: `_08`, `_09`, `_10`, `_11` and `_16`. Every submitted clip there scored about
0. The script counts a drop as 0 only when some candidate scored 6 or more, so the contestants that dropped these
incidents are left out of them, while the engine and `api-deepseek-flash`, which always submit, eat the zeros.
Dropping was the right call there. Still, the fair comparison is the 32 incidents where the judges saw the event
(some candidate averaged 6 or more), with a drop counted as 0:

| contestant | event-seen incidents | mean score |
|---|---|---|
| pi-glm-5.3-flash | 32 | **7.48** |
| pi-deepseek-v4.1-flash-lean | 32 | 6.95 |
| pi-deepseek-v4.1-flash | 32 | 6.65 |
| cu-deepseek-v4.1-flash | 32 | 6.56 |
| pi-gemma4-31b | 32 | 6.24 |
| escalate | 32 | 5.82 |
| engine | 32 | 5.37 |
| api-deepseek-flash | 32 | 4.62 |

The order is the same as the headline, but the engine's deficit shrinks from 2.5 to 2.1 points.

Paired bootstrap 95% intervals on those 32 incidents (`analysis.py`):

| comparison | mean difference | 95% CI |
|---|---|---|
| glm − lean | +0.53 | −0.03 to +1.21 (not clear) |
| glm − cu-deepseek | +0.92 | +0.39 to +1.50 |
| glm − engine | +2.11 | +1.29 to +2.94 |
| lean − engine | +1.58 | +0.79 to +2.33 |
| escalate − api | +1.20 | +0.55 to +1.93 |
| escalate − engine | +0.45 | −0.56 to +1.49 (not clear) |

## 2. Results by category

The scores are mean judge scores. The first table per category is what `split_report.py` computes over all incidents in
the category. The "event-seen" column drops the void outage packets.

| contestant | goals, normal games (12) | goals, scorebug-alert games, all (14) | goals, scorebug-alert, event-seen (9) | minors (8; event-seen 7) | fights/majors (5; event-seen 4) |
|---|---|---|---|---|---|
| pi-glm-5.3-flash | 7.69 | 6.92 (10 clips) | 7.64 | 6.60 / 7.01 | 6.78 / 7.31 |
| pi-deepseek-v4.1-flash-lean | **7.88** | 6.26 (11) | 7.59 | 5.24 / 5.49 | 4.93 / 5.25 |
| pi-deepseek-v4.1-flash | 6.95 | 4.94 (13) | 7.08 | 5.73 / 6.15 | 5.43 / 5.62 |
| cu-deepseek-v4.1-flash | 6.58 | 5.69 (11) | 6.90 | 5.97 / 6.25 | 5.60 / 6.25 |
| pi-gemma4-31b | 6.94 | 5.47 (12) | 6.78 | 4.54 / 4.94 | 4.35 / 5.19 |
| escalate | 4.97 | 6.41 (11) | **7.78** | 4.89 / 5.08 | 4.93 / 5.25 |
| engine | 6.92 | 3.98 (14) | 6.13 | **2.14 / 2.19** | 4.38 / 4.56 |
| api-deepseek-flash | 4.97 | 2.82 (14) | 4.31 | 4.29 / 4.65 | 3.55 / 4.19 |

What the table shows:
- **Goals, normal games.** Lean, glm and the engine are close: lean 7.88, glm 7.69, engine 6.92. The engine's main
  failure is `ends_early`: it cuts the celebration on 42% of its clips. `escalate` equals `api-deepseek-flash` here
  (4.97) because its confidence gate never handed a normal-game goal to the lean agent. The API pass misses the
  build-up on 43% of its clips (`starts_late`).
- **Goals, scorebug-alert games.** Without the outage packets, escalate (which always hands off here), glm and lean
  are level at about 7.6–7.8. Escalate's 7.78 against lean's 7.59 should be equal in principle; I did not track down
  the 0.2 difference. Nine incidents, so this is a small sample.
- **Minors.** This is the clearest gap. The engine's minor windows mostly land after the call, on faceoffs or penalty-box
  dead air, and score 2.2. glm scores 7.0, and nothing else gets above 6.3. Two minors (`2026-09-10_02` and
  `2026-09-26_05`) had no candidate that showed the foul. On `2026-09-10_02` every cut starts after the foul at about
  76–84 s, so its best score is 4.
- **Fights/majors.** Only 4–5 incidents, so treat this as anecdote. glm leads at 7.3. The judges disagreed most here:
  mean |score diff| 1.23, and 3 of 7 best-pick pairs agree.

## 3. The engine's clips against the best agent

On the 32 event-seen incidents, glm beat the engine on 22, was within 0.25 on 4, and lost on 6. Its mean margin was
+2.11 and the median +2.25. Lean has the same 22/4/6 record with a +1.58 margin. An oracle that picks the best agent
clip per incident would average 8.10.

The engine wins on goals where its long fixed lead catches a full build-up. Examples: `2026-03-21_06` (judges 2 and 3
picked the engine), `2026-03-21_13` (judges 1 and 2), `2026-09-26_06` (judges 2 and 3) and `2026-09-26_11` (both judges
scored the engine 9). It loses badly on penalties and wherever it cuts the celebration.

The engine's sub-scores say the same thing. Its build-up (5.9) is close to the field's, but its ending is 4.38, the
second lowest after the API.

## 4. Judge agreement

| measure | value |
|---|---|
| best-pick agreement, all judge pairs (leaderboard script, window-equivalent) | 0.492 over 59 pairs |
| best-pick agreement, judge 1 against judge 2 only (window-equivalent) | 20 / 39 |
| mean \|score difference\| on the same clip | 0.84 |
| Spearman rank correlation, judge 1 against judge 2, per event-seen incident | mean 0.76, median 0.86 |

So the judges agree on how good each clip is, but often not on which clip is best. Many packets have two or three
clips within half a point, and the best pick in those is close to a coin flip. Score-based rankings, the headline, are
far more stable than win rates. Treat the win-rate column as noise.

Tie-breaks: in 9 of the 10 a majority formed. `2026-10-03_05` (a delay-of-game minor) split three ways, A, B and H,
because no judge could see the foul itself. The largest real disagreement was `2026-03-21_10`. Judge 2 scored a
neighbouring period-1 Summerside goal as the event (A 7.5). Judges 1 and 3 found that the actual period-2 goal is not
in the footage at all, so the packet is void.

## 5. Five representative incidents

**`2026-03-14_06` goal, normal game: agents beat the engine on the ending.** Both judges picked glm (D: 9 and 8.5).
Judge 1: "D has a long build-up and a clean end; C and H are shorter; F is slightly early at the end; E cuts off the
celebration; G is only the aftermath." The engine (E, 4/4) has a good build-up "but ends ~ctx 123 as the celebration
begins". `api-deepseek-flash` (G, 1/1.5) "starts ctx 129 after the goal; celebration and replay with logo wipe only".
Lean and escalate (A/B, byte-identical, 7.5) are "55 s and a long wait", flagged `too_long`.

**`2026-09-16_12` minor: only glm caught the foul.** Both judges: G (glm) 7, every other clip 0.5–3. Judge 1: "Only
G (context 25-50 s) covers the play, net-front scrum and raised arm; all others are post-call faceoff/box dead time."
The engine's clip (B, 0.5/1) is "five seconds of players lined up for a faceoff; dead air". This is the minors
pattern in miniature.

**`2026-09-26_11` goal, normal game: the engine at its best.** Both judges picked E (engine, 9/9). Lean (H) is the same
window and also scored 9. Judge 1: "E/H start at 88 and end at 123 after the celebration, so they are best; A is
slightly shorter on build-up; B/F start ~15s before but end quickly; C/D start late." glm (A) scored 8/7.5 with
"~19s build-up". `cu-deepseek` (D, 3.5/4.5) had a "late start plus dead time of scoreboard shot". When the engine's
fixed lead fits the play, the agents add nothing.

**`2026-09-12_07` major: the judges disagreed on where the event is.** Judges 1 and 3 placed the slash and crease scrum at
ctx 51–66 s and picked A/H (`pi-deepseek-v4.1-flash` and glm, byte-identical, 7 and 6). Judge 2 placed the event at
ctx ~187 s, where #77 is sent to the box, and picked the 120 s C/F/G window (lean, engine and escalate, 6). Judge 2
wrote: "Confidence moderate: an earlier whistle at ~62 s could be the event instead." Judge 3, the tie-break, wrote:
"Slash ~51-57s, net-front scrum 66-80s, referee call/misconduct discussion 80-105s", and called C/F/G "120s of
faceoffs". Penalty incidents with a frozen scorebug are ambiguous even for careful judges.

**`2026-03-21_10` goal, scorebug-alert game: a void packet.** Three judges looked. Two, plus the tie-break, found the
goal absent: "context.mp4 holds P1 end, a stream outage slate and the pre-P2 scoreboard at 15:00 (1-1); the P2 8:32
Corbett goal is not in it or in any candidate." Five contestants submitted the same byte-identical outage-slate
clip. glm and cu-deepseek dropped the incident, which was the correct behaviour, and the leaderboard does not count
those drops against them.

## 6. Recommendation

**Reviewer: `pi-glm-5.3-flash`.** It has the highest score overall (7.09; 7.48 event-seen), first or within 0.2 of first
in every category, the lowest `starts_late` (4%) and `wrong_event` (0%)
rates, and it is the only contestant that handles penalties well. It runs flat-rate on ollama-cloud ($0) at about
672k tokens and 278 s median per incident.
- **Main risk: its clips run long.** 34% are flagged `too_long`, mostly tails of scoreboard and faceoff dead air. Its
  lead over lean is also not statistically clear on this sample (+0.53, CI −0.03 to +1.21). On goals alone the two
  are tied, and lean costs about half the tokens and 40% of the wall time. Before trusting the choice on goals, re-run
  more incidents. Its four drops were all correct on void packets, but nothing here tests whether it ever drops a
  real event.

**Adversary pairing: glm reviewer with `pi-deepseek-v4.1-flash-lean` as adversary.** Lean is the second-strongest
contestant. Its per-incident scores correlate only weakly with glm's (r = 0.28 on event-seen incidents). Of all the
pairs with glm, taking the better of glm and lean per incident gives the highest oracle score (7.83, against 7.48 for
glm alone). Lean also covers glm's one sub-6 event-seen incident. An equivalent framing is routing by event type:
lean for goals, glm for minors, fights and majors.
- **Main risk: this pairing was never run.** These numbers are an oracle upper bound computed from reviewer-only
  clips. The only adversary runs (`clip-bakeoff/adversary_pairs.json`) cover other pairs, and they were scored by
  cross-model consensus, not by the judges. The one pairing with glm, `api-deepseek-flash:pi-glm-5.3-flash`, disputed
  13 of 21 incidents, so a strong adversary may generate a lot of disputes and human holds. Both models also share
  the `too_long` habit (glm 34%, lean 38%), so an adversary won't catch long tails. Trim those by rule, not by
  review. Run `adversary --pairs pi-glm-5.3-flash:pi-deepseek-v4.1-flash-lean` and judge it before adopting.

**Do not ship the current `escalate` design as is.** It scores 4.97 on normal-game goals, identical to the API alone,
because the API's confidence gate never hands those to the agent, and the API misses the build-up 43% of the time. If
the cheap-first design is kept, hand off on every goal whose lead is under the 15 s floor, or drop the API stage.

## 7. Uncertainty and method notes

- **Small samples.** 39 incidents in total, 32 with a visible event. By category that is 12 normal-game goals, 9
  event-seen alert-game goals, 7–8 minors and 4–5 fights/majors. Only the overall glm-over-engine and lean-over-engine
  gaps are clearly outside the noise.
- **Low best-pick agreement.** The judges agree on the best clip only about half the time (§4). Rely on mean scores,
  not win rates.
- **Data problems in packets.**
  - Five 2026-03-21 packets are stream-outage voids (§1).
  - `2026-03-21_01` has a 41 s `context.mp4` and no `context.window`, though `incident.json` says 180 s.
  - `2026-09-24_06` has a 93 s `context.mp4` that ends before the incident.
  - `2026-03-21_06` has A/B durations that differ from `incident.json` (32.9/25.6 s against 46.0/39.9 s), and neither A nor B has a `.window` file.
  - `2026-09-24_06` also has no `context.window`.
  - In `2026-03-21_13`, the tie-break judge saw the red team celebrate while `incident.json` credits Summerside, and scored the goal anyway.
- **Judges departed from the brief.**
  - About a third of the judges sampled frames with ffmpeg contact sheets instead of the `watch` skill's `--detail
    efficient --timestamps` passes. They reported that the efficient keyframe pass returned too few frames (9–12 on
    `context.mp4`) to place the event.
  - Several judges scored some clips from the window arithmetic instead of frames:
    - `2026-03-21_15` judge 1, clips C, E, F, G and H.
    - `2026-09-16_04` judge 1. Its frame directory was deleted mid-run, likely a shared `/tmp/m` collision between
      concurrent judges. Later judges were told to use their own `mktemp -d`.
    - `2026-09-26_11` judge 1, `2026-10-03_04` judge 1 (E), `2026-10-03_10` judges 1 and 2 (B, C, F),
      `2026-10-03_11` judge 2 (endings), `2026-09-26_03` judge 3 and `2026-09-26_06` judge 3.
  - One judge breached the starts_late cap: `2026-09-26_06` judge 3 flagged H `starts_late` but scored it 7, against
    the brief's maximum of 5.
  - One tie-break judge (`2026-09-16_04`) stopped after a denied Bash call. It was resumed and finished normally.
- **What "dropped" means.** A drop counts as 0 only when some candidate averaged 6 or more, so the event was visible
  (the script's rule). No contestant dropped a seen event.

## 8. Leaderboard script bug (not fixed in the worktree)

`~/worktrees/amherst-display-clip-review/scripts/judge_leaderboard.py` crashes on any judge@2 verdict:
`AttributeError: 'str' object has no attribute 'get'` at the `k.get("dropped_by")` line. The sub-score loop
`for k in SUBS:` reuses the name `k`, which also holds the incident's key entry, so by the time the drop check runs `k`
is the string `"cleanliness"`. The worktree is read-only for this task, so I ran a copy with only that loop variable
renamed (`judge_leaderboard_fixed.py` here, `--root .`). Apply the same three-line change in the repo:

```diff
-            for k in SUBS:
-                sv = [v["candidates"][letter]["subs"][k] for v in verdicts if k in v["candidates"].get(letter, {}).get("subs", {})]
+            for sub in SUBS:
+                sv = [v["candidates"][letter]["subs"][sub] for v in verdicts if sub in v["candidates"].get(letter, {}).get("subs", {})]
                 if sv:
-                    r["subs"][k].append(statistics.mean(sv))
+                    r["subs"][sub].append(statistics.mean(sv))
```

## Files here

| file | contents |
|---|---|
| `packets/<id>/judge-1.json`, `judge-2.json` | first-pass verdicts |
| `packets/<id>/judge-3.json` | tie-break verdicts, 10 packets |
| `leaderboard.md`, `leaderboard.json` | final leaderboard, 3 judges where present |
| `leaderboard-2judge.md`, `leaderboard-2judge.json` | leaderboard before tie-breaks |
| `split.json` | category split, from `split_report.py` |
| `validate_verdicts.py` | schema check |
| `disagreement.py` | blind tie-break selection |
| `analysis.py` | event-seen view, engine comparison, per-incident matrix |
| `judge_leaderboard_fixed.py` | the leaderboard script with the bug fixed |
