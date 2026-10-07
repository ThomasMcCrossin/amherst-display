---
name: hockey-clip-review
version: "1.0.0"
description: Review one hockey highlight incident (a goal, penalty, major or fight from the official game sheet) against the broadcast recording and choose the clip's in/out points from the play on screen, or refute another reviewer's clip as the adversary. Reads frame contact sheets, can pull more frames with a bundled script, writes a JSON verdict and checks it.
allowed-tools: Bash, Read
---

# hockey-clip-review

Code found this incident cheaply: the event comes from the official game sheet (HockeyTech),
the moment in the video comes from reading the broadcast scorebug clock. Both can be wrong in
the video: the scorebug is run by a person and lags, freezes or jumps. Your job is to look at
the pictures and decide where the clip should start and end. You may **overrule** the code.

## Resolve `SKILL_DIR` first

`SKILL_DIR` is the absolute path of the directory containing this SKILL.md (your harness told
you the path when you read it). Scripts are siblings: `$SKILL_DIR/scripts/frames.py`,
`$SKILL_DIR/scripts/check_verdict.py`. Use the python in `incident.json` → `tools.python` if
given, else `python3` (needs Pillow; `ffmpeg` on PATH).

## The packet

You are given a packet directory. Everything is relative to it.

- `incident.json`: the facts. `kind` (`goal`|`penalty`), `class` (`goal`, `minor`, `major`,
  `fight`), `sheet_rows` (the game-sheet lines: scorer and assists, or player, infraction,
  minutes), period and clock, `teams`, `anchor` (video seconds where the code placed it),
  `engine_window` (the code's current clip, relative), `neighbours` (other game-sheet events
  nearby: they are **not** this incident), `scorebug_alert` (true = this game's bug was
  stuck or glitching), `bounds` (your limits), `coarse_sheets` (contact sheets already made).
- **All times are seconds relative to the anchor**: the label printed on each frame. `0` is
  the anchor (highlighted yellow). Negative = before it. Your verdict uses the same numbers.
- Contact sheets: 5x4 frames, time order left to right then down.

## Procedure (reviewer)

1. Read `incident.json`, then Read every coarse sheet it lists.
2. Find the event. A goal: puck crosses the line, then the goal light / celebration. A
   penalty: the foul, then the whistle with the referee's arm up. A fight: gloves drop and
   punches. If it is not in the coarse range, scan wider before giving up:
   `python3 $SKILL_DIR/scripts/frames.py sheet --packet . --from -240 --to -60 --step 4`
   (and the other side). Never beyond `bounds.relocate_max`.
3. Pin each boundary on dense frames (0.5 s), around the in-point, the event and the out-point:
   `python3 $SKILL_DIR/scripts/frames.py sheet --packet . --from -38 --to -26 --step 0.5 --name start`
   Use `frames.py frame --packet . --at <t> --width 960` for a single big frame (a scorebug,
   a jersey number). Keep it to about 12 image reads; sheets beat single frames.
4. Write the verdict JSON to the path you were given (schema below), then run
   `python3 $SKILL_DIR/scripts/check_verdict.py <verdict path> --packet .`
   Fix every ERROR and run it again until it prints OK. NOTE lines say how the code will clamp
   your window; that is allowed.
5. Reply with one line: `VERDICT <path>`.

## What things look like on a Flo Hockey (MHL) broadcast

- **Goal**: shot, puck in the net (often hard to see), goal light, the scorer's arms go up,
  teammates pile in, then the line skates past the bench for fist bumps. The game clock stops
  at the goal and the score digit changes **10 s to 2 min later**. Then usually a **replay**
  (another angle, slow motion, often introduced by a wipe/swoosh or FloHockey logo graphic)
  and a goal graphic with the scorer's name, then the faceoff at centre ice.
- **Minor penalty**: the foul (trip, hook, slash, hold, interference, high stick, roughing)
  lasts under a second; the referee's arm goes up (delayed call) and play continues until the
  offending team touches the puck, then the whistle. Then the referee signals, a penalty
  graphic may appear, the player skates to the box.
- **Major / misconduct / scrum**: a big hit, a crowd of players shoving after a whistle,
  linesmen wading in, players sent to the box or off the ice.
- **Fight**: gloves (and often helmets) on the ice, two players squared up or grappling,
  punches, linesmen step in when one falls or they tire, then the players are escorted away.
  Fight replays are common afterwards.
- **Not live**: replays (a second look at something you already saw, slow motion, different
  angle, a wipe before and after), graphics and lower thirds over a still shot, crowd shots,
  intermission panels, commercials, the big FLO HOCKEY ident. Never pick a replay as the event.

## Where to cut

- **Goal in-point**: the start of the play that produced the goal: the faceoff win in that
  zone, the turnover, the zone entry or the rush. After a long offensive-zone cycle, about
  8 s before the shot. Never mid-shot; never a long stretch of unrelated play. Usually
  10-30 s before the goal (`bounds.lead_min`..`lead_max`).
- **Goal out-point**: when the live celebration ends (fist bumps done, players skating off) or
  the first replay/graphic wipe, whichever comes first, and at least `bounds.tail_min` (8 s)
  after the puck crosses the line. Set `replay_t` when you see the replay start; if it starts
  sooner than 8 s, end at the replay, but never less than `bounds.tail_min_replay` (5 s) after
  the goal (a "replay" 2 s after the goal is usually still the live celebration).
- **Minor**: in 2-3 s before the foul (8 s before the whistle if you cannot see the foul);
  out at the referee's signal or the player heading to the box.
- **Major / scrum**: in a few seconds before the hit or the shoving starts; out when it is
  broken up and players are sent off. **Fight**: in no earlier than `bounds.fight_pre` (10 s)
  before the gloves drop, out no later than `bounds.fight_post` (10 s) after the linesmen
  have them apart; total at most `bounds.len_max` (75 s). Do not cut the fight off.
- Never end on or include a replay, a graphic wipe, a commercial or the station ident.

## Scorebug caveats

The anchor comes from the scorebug clock, so it inherits its faults. The bug can **freeze**
(same clock/score for minutes), **lag** (clock stopped 30-100 s after the real stoppage on
some penalties), **jump** (operator fixes the clock), or be hidden by replays/graphics. With
`scorebug_alert: true` trust the picture over the bug, and expect the event to be well away
from 0. A score change confirms that a goal happened **before** it, not when.

## Authority and limits

- You decide `keep` (the engine window is right), `adjust` (new in/out around an event near
  the anchor), `relocate` (the event is more than `bounds.near` seconds from the anchor),
  `drop` (the event is not in the recording at all: a gap, an intermission, the broadcast was
  elsewhere) or `unsure` (the engine window stays and a human looks).
- You **never add an event**: only this incident from the game sheet. If what you see is a
  neighbour's event, that is not this incident.
- Relocation needs **evidence**: at least two `evidence` entries within 15 s of `event_t`
  saying what is on screen, and must stay within `bounds.relocate_max` (240 s; 360 s for
  majors/fights).
- Dropping needs evidence and `confidence >= bounds.drop_min_confidence`; a goal you cannot
  find is `unsure` unless you have positive evidence it is not there (e.g. the recording
  shows an intermission panel across the whole range).
- The code re-checks all of this and clamps the window to the bounds; a verdict that fails
  the check after clamping is thrown away and the engine window kept.

## Reviewer verdict (`hockey-clip-review/verdict@1`)

```json
{
  "schema": "hockey-clip-review/verdict@1",
  "role": "reviewer",
  "incident_id": "<from incident.json>",
  "decision": "keep | adjust | relocate | drop | unsure",
  "event_visible": true,
  "event_t": -4.5,
  "in_t": -22.0,
  "out_t": 9.5,
  "in_kind": "faceoff | zone_entry | possession_change | rush | dump_in | cycle | pre_foul | pre_whistle | pre_confrontation | other",
  "out_kind": "celebration_over | cut_to_replay | graphic | whistle | referee_signal | to_penalty_box | players_separated | sent_off | other",
  "replay_t": 10.0,
  "whistle_t": null,
  "foul_visible": null,
  "foul_what": null,
  "fight": null,
  "scorebug": "ok | frozen | lagging | jumping | missing | unknown",
  "evidence": [{"t": -4.5, "what": "puck crosses the line, goalie down"}, {"t": -2.0, "what": "white #17 arms up"}],
  "confidence": 0.85,
  "reason": "one or two sentences"
}
```

`event_t`: goal = puck crosses the line; penalty = the foul if visible, else the whistle;
fight = gloves drop / first punch. Penalties need `foul_visible` true/false (and `foul_what`,
`whistle_t` when seen). Fights need `fight: {"gloves_drop_t": n, "separated_t": n}`.
For `keep`, `in_t`/`out_t` may be null (the engine window is used). For `unsure`/`drop`,
times may be null.

## Adversary role

You get another reviewer's verdict and contact sheets of the **proposed final clip** (listed in
`adversary_input.json` in the packet, together with the verdict path). Try to refute it. Read
`incident.json`, the verdict, the final-clip sheets, and any coarse sheet you need; use
`frames.py` for more. Look for:

- `event_not_in_clip` (the goal/foul/fight is not between in and out),
  `cut_before_event` (ends before the puck crosses the line / before the foul),
  `starts_mid_play` (starts in the middle of the scoring play), `starts_too_early` (long
  unrelated play first), `ends_early` (celebration cut, fight cut off), `fight_cut_off`,
  `replay_included`, `wrong_incident` (a neighbour's event, or not this team's goal),
  `foul_not_in_clip`, `too_long`, `drop_unjustified` (a dropped or `unsure` event is visible
  in the frames), `other`.
- Object only to **material** faults, judged by the cut rules above, not by taste:
  - `starts_too_early` only with more than ~20 s of play unrelated to the scoring play (or
    to the incident) before it. A 5-30 s lead-in that shows the play developing is wanted.
  - `ends_early` only when the clip stops less than `bounds.tail_min` after the event, or
    while a fight is still on, or mid-celebration with the scorer still celebrating on screen.
  - `replay_included` only when at least ~1.5 s of replay is inside the clip; a replay that
    starts at the out-point is correct.
  - `too_long` only past `bounds.len_max` or with long dead time (more than ~15 s) after the
    celebration/whistle.
  - A boundary within about 3 s of where you would put it is fine.
  Agree when none apply. Disputes cost a second review, so a weak objection is a wrong one.

Write `hockey-clip-review/adversary@1` JSON to the path you were given and run
`check_verdict.py` on it:

```json
{
  "schema": "hockey-clip-review/adversary@1",
  "role": "adversary",
  "incident_id": "<from incident.json>",
  "agree": false,
  "objections": [{"kind": "cut_before_event", "t": 6.0, "detail": "puck crosses at +6, clip ends +4"}],
  "suggested_in_t": null,
  "suggested_out_t": 16.0,
  "confidence": 0.8,
  "reason": "one sentence"
}
```

## Second review

If the packet has `objection.json`, an adversary disputed a first review. Review the incident
fresh as the reviewer, read the objection, check it against the frames yourself (the
adversary can be wrong too), and answer it in `objection_answer`.
