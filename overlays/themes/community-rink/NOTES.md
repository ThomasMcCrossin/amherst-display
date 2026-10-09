# community-rink

**Intent.** A well-made community rink board: warm cream and charcoal, brass hairlines,
and one bold team colour doing the heavy lifting. The lower third is built like a painted
sign—crest on a cream plate, team-colour number token, and the player's name as the hero at
up to 96 px. Full-screen cards let the score be the hero, with the winning side ringed in
brass (colour only, no invented words). A three-part team-colour stripe grounds every card.

**Identity.** Diagonal board texture at 4% keeps large dark areas from going flat. Team
colours are used structurally: the left rail, the scoreboard lights, the number token, the
tag chips, and the bottom stripe. Light teams (CAM yellow, SCA white, RDG yellow) keep
`team.text` for contrast; dark team colours are auto-lightened only when used as small
headline text. Every logo sits on a cream plate with a dark inset, so nothing vanishes on
dark panels or light ones.

**Fonts.** Bebas Neue (Ryoichi Tsunekawa, Dharma Type) for names, numbers and headlines —
SIL Open Font License. Barlow Semi Condensed (Jeremy Tribby / Google Fonts) for clocks,
stats and supporting lines — SIL Open Font License. Both are the harness's bundled fonts;
no extra font files are shipped with this theme.

**Known weaknesses.** Longest names are auto-shrunk to fit one line, so a 30+ character
name sits around 52 px — legible, but it cedes some of the "hero" size to safety. The
brass winner ring is a colour cue only; on a tie (soccer final) neither side is marked, as
intended. Two dark-purple teams (e.g. TRU vs AMH) produce score pills that are close in
hue and are told apart mainly by their crests and short codes.
