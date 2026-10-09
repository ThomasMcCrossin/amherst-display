# Porting to the public repo

[HockeyHighlightExtractor](https://github.com/ThomasMcCrossin/HockeyHighlightExtractor) is the
public, league-neutral highlight generator. This repo stays the Ramblers fork: team data, the
canteen display, Drive layouts, our model choices and private judging data live here only.
Changes flow one way, from here to there, when we choose. The public repo never feeds back
into this repo automatically.

The public repo has its own edits (league packs, box-score providers, a neutral `config.py`, a
followed-team setting), so a port is a 3-way merge of what changed here, never a copy:

```
scripts/port_to_public.sh ../HockeyHighlightExtractor        # port up to HEAD
```

- `.amherst-display-port` in the public repo records the last ported commit.
- The script diffs generic code since then: `highlight_extractor/` (renamed `hockey_extractor/`
  there), the scorebug catalog and detector, `goal_locator.py`, `penalty_incidents.py`,
  `clip_review/`, the clip-review skill, overlays and tests. It applies the diff with
  `git apply -3`.
- `config.py`, `drive_config.py` and `README.md` are listed for porting by hand.
- Files built on private data (the clip-window eval) are never ported.
- Afterwards, in the public checkout: resolve any conflicts, run `python -m pytest -q` and
  `scripts/leak_scan.sh`, then commit and open a PR.

First port: 2026-10-09 (`bfba9be..968749d`, learned clip windows and scrums;
HockeyHighlightExtractor#11).
