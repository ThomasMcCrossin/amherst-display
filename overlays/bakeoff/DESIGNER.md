# Overlay designer brief

You are designing one broadcast overlay theme for a league-neutral sports graphics system.
The first real use is the Amherst Ramblers (MHL junior hockey): highlight reels and a canteen
TV across the room. The same theme must work for any league and sport through the spec.

Repo root is the current directory. Read `overlays/README.md` (the contract) and
`overlays/themes/baseline/theme.mjs` (a plain working reference) first.

Your theme: `overlays/themes/{name}/`. Write only inside that directory. Do not edit any other
file, and do not install packages.

Creative direction: **{direction}**

Make it look designed, not like the baseline: a clear visual identity, hierarchy (the player
name is the hero on lower thirds; the score is the hero on full-screen cards), team colour used
with intent, logos treated well (sizing, backing plate for logos that vanish on dark or light
panels), and good typography. It must cover every kind in the spec. Fights show both players.
Full-screen cards (`break`, `final`, `intro`) can be opaque.

Loop until it is good:
1. `node overlays/render.mjs --theme {name}`. Fix every problem it prints (clipped text,
   off-frame, scorebug zone). Zero problems is required.
2. `python3 overlays/composite.py --theme {name}`, then look at
   `overlays/.bakeoff/out/{name}/sheet.jpg` and at single frames
   (`overlays/.bakeoff/out/{name}/<sample>.jpg`) with the read tool. Check that every name,
   number, score and clock matches `overlays/samples/<sample>.json`, that long names fit, that
   the yellow (CAM, RDG) and white (SCA) teams stay readable, and that the soccer samples look
   as good as the hockey ones.
3. Improve, and repeat. Look at single frames at full size before you finish.

Finish by writing `overlays/themes/{name}/NOTES.md`: the design intent in 3-6 lines, the
fonts used and their licences, and any known weakness. You have a limited tool budget; spend it
on rendering and looking, not on reading unrelated files.
