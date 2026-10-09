# 2026-10-09: clip windows learned from judged data (report)

Dated evidence, not policy. Current behaviour lives in `README.md` and `config.py`. Issue #25.
Method code: `scripts/eval_clip_windows.py`; derived labels: `docs/clip-window-labels.json`;
tests: `tests/test_eval_clip_windows.py`.

## Question

The mechanical engine scored 4.56 / 10 from the blinded judges against 7.09 for the best vision
reviewer (2026-10-07 bake-off). Where does it lose, and how much of that gap can code alone
close, with no model or API in the engine path?

## Labels: how they are derived, and how noisy they are

39 judged incidents (`~/.local/state/watch-rams/clip-judge`, 2 Sonnet judges each, 3 on the ten
most disputed). Times are seconds relative to the engine's anchor (clock stop for goals, the
called time for penalties). For each incident:

| label | derived from |
|---|---|
| play start | median in-point of the windows scoring within 1.0 of the best, among those with build_up >= 8 |
| end | median out-point of those same top windows with ending >= 8 |
| moment | median `event_t` of the vision reviewers who saw the event (>= 3 reviewers, else the anchor) |

Incidents whose best window scores < 5 are `unlabelled` (6: five 2026-03-21 stream-outage voids
plus one major no one found); one incident whose reviewers' moment is outside the judged span is
`conflict` (2026-09-16 minor, anchored about 70 s off). That leaves 31 labelled incidents (21
goals, 6 minors, 4 majors/fights); `firm` means best >= 7 (28 of them).

Noise, plainly: judges agree on scores to within 0.84 on average but pick the same best clip only
about half the time; several contestants share a window (escalate copies api/lean), so the spread
of the top windows (median IQR 0.1 s in, 1.0 s out) understates the real uncertainty. Targets
are also limited by what candidates were tried: the reviewers' goal in-points are capped at
32-45 s, so "play start" for a long build-up is a lower bound. Labels are therefore used with
tolerances (start +3 s, end -2 s, moment +-1 s), the n for minors (6) and majors (4) is small, and
no constant below should be read to better than a few seconds.

Held-out games (chosen before the first run, never used to pick a constant): 2026-09-16,
2026-03-14, 2026-10-03. Honest caveat: the response curve I read the goal tail from (judge score
and flags by out-point, all games) was drawn before the split was applied, so the held-out column
confirms rather than blindly validates.

## Where the engine loses

Recorded engine windows against the labels (31 labelled incidents):

| failure | evidence |
|---|---|
| Goal tail cuts the celebration | goal ending covered on 19% of clips (3 s tail; judged ends sit 3-19 s after the clock stop, median 14 s); mean 9.7 s short |
| Minor window sits on the stoppage | minors: build-up covered 17%, whistle inside the window 33%, ending 17%; window is 5 s long (-2/+3) against judged -9/+14 |
| Goal lead is fine | build-up covered on 100% of goals at 32 s; no goal needed more |
| Majors/fights are 120 s review clips | 70 s of dead air on average against judged -25/+15..36 |
| Replay wipe inside the window | reviewers saw the replay start at a median 14 s after the clock stop (11 of 19 goals before +16 s): a fixed tail cannot avoid it |
| Anchor wrong by a minute or more | 1 of 7 minors (2026-09-16 14:48, about 70 s off), and the five stream-outage voids had no goal in the footage at all |
| Delayed penalty calls | the foul can be 20-35 s before the whistle (2026-09-26 07:20, 2026-09-10 10:28); no clock signal separates these from a normal call |

## Changes kept (all code-only)

| constant | before | after | evidence |
|---|---|---|---|
| `GOAL_CLOCK_STOP_AFTER_SECONDS` | 3 s (and capped by `min(.., after)`) | 16 s | judge score and ending sub-score rise with out-point to +12..+20 (7.5 / 8.4) and fall off after; matches the review layer's 16 s trim |
| `PENALTY_ALL_*` / `PENALTY_PP_*` | -2 / +3 s | -9 / +14 s | judged minors start -5..-21 (median -9) and end +4..+23 (median +12) |
| `SCRUM_BEFORE/AFTER_SECONDS` (new) | n/a (-30 / +90 review clip for majors) | -30 / +30 s | 3 of the 4 labelled "majors" are multi-penalty stoppages; judged starts -20..-30, ends +12..+36 |
| scrum incident (new, `penalty_incidents.py`) | one clip per penalty | one clip per stoppage | rule from Tom: 2+ penalties within 5 s on the clock, or any major/misconduct/match penalty |

## Result

Same 39-incident judged set, recorded engine vs this branch replayed through the engine's own
window code (`--policy replay`). Percent columns are shares of labelled incidents.

| subset | n | build_up_ok | moment_ok | ending_ok | all_ok | wrong incident | excess s | before: all_ok | before: ending_ok |
|---|---|---|---|---|---|---|---|---|---|
| goals | 21 | 100 | 100 | **85.7** | **85.7** | 0 | 2.8 | 19.0 | 19.0 |
| minors | 6 | **83.3** | 100 | **83.3** | **66.7** | **0** | 5.1 | 0 | 16.7 |
| majors / fights / scrums | 4 | 100 | 100 | 75.0 | 75.0 | 0 | 12.5 | 100 | 100 |
| all labelled | 31 | 96.8 | 100 | 83.9 | **80.6** | 0 | 4.5 | 25.8 | 29.0 |
| held-out games only | 15 | 100 | 100 | 86.7 | 86.7 | 0 | 3.3 | 0.0 | 0.0 |
| tuning games only | 16 | 93.8 | 100 | 81.2 | 75.0 | 0 | 5.6 | 50.0 | 56.2 |

Before, for reference (recorded): all labelled build_up_ok 83.9, moment_ok 87.1, wrong incident
6.5%, excess 9.7 s (mostly the 120 s major clips, which "pass" ending only because they are two
minutes long). Upper bound (the labels themselves): 100 on every column.

Proxy score (mean judged score of the nearest already-judged window, within 2.5 s on both edges):
engine 5.53 over all 31; this branch 7.76 but only over the 12 whose new window has a judged
neighbour, so it is a sanity check, not a headline. The real score needs a judge pass on the new
clips; that has not been run.

Not changed by this: majors' manual-review clip (`MAJOR_PENALTY_*`, -30/+90, a human reviews it),
goals the clock could not time, and the replay wipe.

## Tried and rejected

- **Start at the preceding faceoff (clock resume) within 45 s, else a lead floor.** Prototyped on
  the saved OCR timeline (5 s sampling): a restart is visible within 45 s on only 6 of 21 goals,
  and where it fired (38, 29.5, 3.8, 25, 10 s back) it disagreed with the judged starts
  (-32, -32, -32, -32, -24) in the direction that would cut build-up. The judges' lead is
  already met by 32 s on every goal. Not kept.
- **Scene-cut / replay-wipe detection.** Probed frame differences and colour uniformity every
  0.5 s around the reviewers' replay marks on 6 goals: broadcast camera cuts and the wipe are
  both 30-100 on the same scale, and the reviewers' own replay marks disagree by seconds. Needs
  a per-broadcast logo template matcher; not attempted beyond the probe, nothing kept.
- **Audio energy for the celebration end.** Not attempted: the labels (end +-2 s) are too coarse
  to validate it and the fixed 16 s tail already covers 86% of goals.
- **Anchoring penalties on an earlier clock stop.** The anchor is already the first clock
  reading of the called time (reviewers put the whistle at a median 1.5 s before it); the missing
  piece is the foul lead-in, which no clock signal gives for a delayed call. A longer fixed lead
  (9 s) covers 5 of 6 labelled minors.

## Scrums and consequential stoppages

Box scores reachable on this box (12 games: six 2026-27 games in `Games/`, six 2026 spring games
in the rerun archive; the HockeyTech feed needs an API key not available here): 172 penalties
form 116 stoppages; 25 are scrums (2+ penalties within 5 s on the clock), 1 is a fight, 0 are a
lone major, and 90 are lone minors. The 26 consequential stoppages hold 78 of the 172 penalties
(about 2.2 stoppages per game). Largest clusters: 16 rows at 2026-09-12 P3 17:38 (Roughing and a misconduct repeated eight times in the sheet), 5 at 2026-09-12 P3 10:23, 5 at 2026-09-24 P2 19:07.
