# Postgame step 5 (`match_events`): what it costs and how it got faster

2026-11-08, amherst-display#48. Measured on the 2026-11-07 Amherst Ramblers vs Truro Bearcats
recording (`cubuntucanteen`). Step 5 was 7 minutes of that run; this note records the split, the
replay harness that measures it, and what changed.

## What step 5 spends its time on

`HighlightPipeline._step5_match_events` matches the 14 box-score events to OCR readings (0.2 s),
then does two things that read video frames and talk to a model:

| phase | what it does |
|---|---|
| `clock_stop` | For every goal, sample the scorebug clock at 2.0 s steps around the matched time, then at 0.25 s inside the candidate crossing ranges, to find the first stable clock reading equal to the goal's time. One tesseract read per sample. |
| `local_ocr` | The same scan for questionable non-goal matches, 60 s each side at 0.5 s steps. Empty in `goals_only` mode. |
| `vision` | `goal_locator.locate_goals`: goals the clock could not confirm are bracketed from the OCR readings, frames are captured every 6 s across the bracket, and a vision model labels 24 frames a request. |

Split of the 2026-11-07 run (`Games/2026-11-07_…/logs/pipeline.log`, 23:29:26 → 23:36:23):
**417 s total: 180 s clock stop, 237 s vision locator, <1 s matching.**

## Replay harness

The joined recording is deleted after the game is archived, so step 5 is replayed from the
production run's own OCR readings and matched events against an archive segment of the same
game that still exists (`archive/20261107T231505Z_e258346/recording.mp4`, joined seconds
2438.472-9903.947 from `postgame/20261107T223121Z_e258346/joined.json`). Matching is re-run from
those readings, so the input is exactly the production pre-refinement state, shifted by the
segment offset.

```bash
cd ~/worktrees/amherst-display/postgame-speed
GAMES="/home/canteenhub/amherst-display/Games/2026-11-07_Amherst Ramblers_vs_Truro Bearcats"
VIDEO=/home/canteenhub/.local/state/watch-rams/archive/20261107T231505Z_e258346/recording.mp4
OUT=~/.local/state/watch-rams/issue-48/match

.venv/bin/python scripts/bench_match_events.py --game-dir "$GAMES" --video "$VIDEO" \
    --video-offset 2438.472 --out "$OUT/before" --label before
.venv/bin/python scripts/bench_match_events.py --game-dir "$GAMES" --video "$VIDEO" \
    --video-offset 2438.472 --out "$OUT/after" --label after
.venv/bin/python scripts/bench_match_events.py --compare-with "$OUT/before" --out "$OUT/after"
```

The game folder is read only; everything the pipeline writes (events, normalization and matching
logs, `goal_locator.json`, the live progress file) goes to `--out`. `bench.json` per run holds
wall/CPU seconds per phase, the load average, the vision usage and the placed events.
Vision calls use the postgame runner's endpoint (ollama-cloud DeepSeek Flash, low think), read
from `~/.config/curlys/ollama-cloud/active.env`.

## Before and after

Same input, same box, one after another. The box was loaded throughout (the reel build for the
same game ran alongside), so these are wall times under contention:

| run | wall | CPU | match+save | clock stop | vision | load |
|---|---|---|---|---|---|---|
| before | 380.0 s | 521 s | 0.2 s | 258.2 s | 121.6 s | 17 |
| after | 235.5 s | 1036 s | 0.3 s | 165.7 s | 69.5 s | 28 |
| after, repeat | 261.7 s | 1064 s | 0.2 s | 186.3 s | 75.2 s | 23 |
| after, `MATCH_OCR_CAPTURE=moviepy` | 383.2 s | 598 s | 0.3 s | 274.5 s | 108.4 s | 30 |

Step 5 total: **380 s → 234-262 s, 1.5-1.6x**. Clock stop 1.6x, vision locator 1.7x. CPU went up
2x because the work that used to wait on a seek now waits on neither; it is the same work.

## Anchors are unchanged

`--compare-with` on the six goals and six penalties the segment contains:

```
12 events: 0 anchor difference(s), 2 vision-placed difference(s)
clock-stop and matched anchors identical
```

All four clock-stop refinements and every matched timestamp are bit-identical to the before run
(`603.778`, `1400.528`, `5799.028`, `6664.528` on the replay timeline). They are also the
production values shifted by 2438.472 s (`1978.25`, `3042.25`, `8237.5`, `9103`), so the replay
reproduces the production anchors.

The two P2 goals the clock could not confirm are placed by the vision model, and model labels are
not reproducible: the same code and the same frames returned one celebration in one run and two
in another, which places both goals or neither. That is why `--compare-with` counts vision-placed
placements apart from the anchors instead of failing on them.

## What changed

- `MATCH_OCR_WORKERS` (default 8) clock samples read at once: a frame is captured and handed to a
  thread that runs tesseract, instead of seek → read → seek → read on one thread. Frames come from
  the same ffmpeg seek the OCR capture pass uses (`ocr_engine._parallel_frame_grabber`), or from
  the single MoviePy reader with `MATCH_OCR_CAPTURE=moviepy`; on this archive segment the parallel
  seek is the faster one (274 s vs 163 s for the phase), and the MoviePy path is kept for a
  recording where a fresh seek is the slower way in.
- The pinned scorebug box is detected once per run and passed to every read. `_ensure_ocr_engine()`
  re-applies the broadcast type, which dropped the engine's cached ROI, so the refinement loop
  re-detected a fixed layout for every frame (and logged two lines a frame).
- `GOAL_LOCATOR_WORKERS` (default 6) vision requests in flight, and every bracket's requests are
  submitted together instead of one request per capture loop, so a second bracket no longer waits
  for the first bracket's whole scan. A failed request labels that batch only.
- Live progress inside the step, on the same record as the other steps (one line for the whole
  step, `done`/`total` growing as each phase's work is planned):

  ```
  match_events 42/121 (35%), 2s in, ETA 4s - clock-stop OCR: P3 1:43 (4/6 goals)
  match_events done in 3m54s 603/603: 12/12 events placed, 4 by clock stop, 1 vision bracket(s)
  ```

  Details move per goal, per clock window, and per vision batch (`vision locator: 3/5 frame
  batches read`). `_step_timings` now carries `match_events.clock_stop`, `.local_ocr` and
  `.vision`, so the pipeline's own summary shows the split.

## Caveats

- The replay is one archive segment: six of the game's eight goals, and its P2 vision bracket. The
  P3 17:47 goal and its 15 s bracket (10112-10127 s) are outside it.
- The engine reads the postgame join (`-movflags +faststart`, re-encoded when the runs' streams
  differ), not a raw archive capture. The capture path is the limiting factor here and it is the
  one thing that differs: on the join, the parallel ffmpeg seek already measured ~2x the MoviePy
  reader in `extract_timestamps` (869 s → 493 s for the 2026-11-07 run), so these numbers are a
  lower bound for a real run.
- Wall times move with box load (load 17-30 across these runs). CPU seconds are the stable column.
- The clock-stop pass still reads ~600 frames and tesseract spawns a process per read. Cutting
  either means fewer samples or a different OCR backend, which would move the anchors; that is a
  separate decision, not a speed tweak.
- Vision throughput is bounded by the provider, not the client: five requests at once still cost
  ~70 s, against 122 s when they ran one after another.

## Re-checking later

`scripts/bench_match_events.py --help`. Any change that should not move anchors must keep
`--compare-with` at `0 anchor difference(s)`; a changed number there is a changed clip start.
