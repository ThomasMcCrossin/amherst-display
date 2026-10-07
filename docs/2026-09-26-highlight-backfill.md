# 2026-09-26: highlight backfill and goal validation (report)

Dated evidence, not policy. Current behaviour lives in `README.md`.

Every archived 2026-27 recording on canteenhub was run through the highlight pipeline and every
HockeyTech goal was checked against the source with `scripts/validate_goal_clips.py`
(deepseek-flash, thinking disabled). Each failure was traced to a cause before anything was
changed, and the fix was replayed against the saved OCR readings before the game was rerun.

## Failure modes and fixes

| # | Seen on | What went wrong | Fix |
|---|---|---|---|
| 1 | 09-10 Pictou | Normalizer always started at period 1; a recording that joined in the 3rd relabelled every period. | Start from the period the bug shows first (majority of the first readings). |
| 2 | 09-10 Pictou | Interpolation clamped events outside the recording to its edge: 5 goals clipped at one second. | Only interpolate between bracketing readings; otherwise no match. |
| 3 | 09-16 Valley | Minimum-video-time guard measured game time from the 1st-period puck drop, so a recording that joined in the 1st intermission could not match 2nd-period events. | Measure from the game time on the bug when the readings begin. |
| 4 | 09-12 Grand Falls | Flo bug froze (clock and score) at 15:24 of the 2nd for ~15 min of video; a goal inside it was clipped two minutes late. | `goal_locator.py`: bracket the goal from the readings around the freeze, label frames across it with the vision model, place the goal at the group celebration. |
| 5 | 09-12 Grand Falls | After the freeze the operator jumped the clock to 5:17; the fast-forward guard then discarded the rest of the period. | Accept a jump the following readings keep counting down from. |
| 6 | 09-24 West Kent | Bug held 20:00 for ten minutes of play, then jumped ahead; the time guard rejected two exact P1 readings. | Strict guard first, start-only guard when it leaves no candidate. |
| 7 | 09-12 Grand Falls | Scorebug vote 4-3 between overlapping crops (Summerside's crop sits on the Flo strip's clock block) counted as a win. | A win needs a clear margin; close votes go to the vision check. |
| 8 | 09-16 Valley | Validator matched the scorer by team name; the bug labelled Valley "Red Wings". | With the opponent recognised, the scorer is the other side. |
| 9 | 09-16 Valley | The bug score changed 1-2 min after the goal, past the validator's last frame. | Read up to +240 s, never past the next goal. |
| 10 | 09-12 Grand Falls | Validator only looked at full frames to +3 s; a celebration-placed goal peaks later. | Full frames through +15 s. |
| 11 | all | Production reel never built on canteenhub: Playwright's Chromium was missing. | `npx playwright install chromium-headless-shell`; README setup step. |

Failure modes 1, 3, 5 and 6 are the same wrong assumption in different places: that the
recording starts at puck drop and the bug clock runs at real speed. Flo's bug is
operator-driven, and recordings join late or restart after a crash.

## Results

Goal verdicts from `validate_goal_clips.py`, first run vs. after the fixes.

| Game | Recording | HockeyTech goals | First run | After fixes |
|---|---|---|---|---|
| 09-10 Pictou | joined with 13:40 left in the 3rd | 5 | 5 clips, all wrong (suspect) | 1 confirmed, 4 not recorded |
| 09-12 Grand Falls | full game; bug frozen ~15 min in the 2nd | 6 | 5 confirmed, 1 clipped 2 min late | 5 confirmed, 1 visual (placed from the celebration, checked by eye) |
| 09-16 Valley | joined in the 1st intermission | 8 | 2 confirmed, 1 missed, 1 unclear, 4 not recorded | 4 confirmed, 4 not recorded |
| 09-24 West Kent | full game; bug held 20:00 for ten minutes | 2 + shootout | 2 confirmed | 2 confirmed, shootout clip (12:10) |

Every goal the recordings contain now has a correct clip. 09-24 was last run before fix 6; its
two unmatched P1 penalties match on replay (16/16) and are not in the goals-only reel.
Per-run logs: `~/.local/state/watch-rams/highlight-validate/backfill/` on canteenhub.

## Cost

Goal validation is about 2.5k prompt tokens per goal. The goal locator is about 30k prompt
tokens per frozen stretch, only for goals the clock cannot time. Scorebug detection is about
1.7k tokens, only when the OCR vote is close. The whole backfill, including the reruns and
the experiments behind the locator, used about 210k prompt and 30k output tokens: a few cents.

## Open

- Recordings that miss part of a game (09-10 joined mid-3rd, 09-16 joined in the 1st
  intermission) are ingest-side gaps; the pipeline now reports them as `not_recorded`.
- The shootout clip takes the whole shootout (up to 12 min), which makes the production reel
  take ~25 min to render on canteenhub.
- `build-jsons.yml` runs on any branch push that touches `scripts/**` and commits data back to
  that branch, so feature branches collect nightly-data commits.
