"""
Group box-score penalties into incidents (pure; no video, OCR or model calls).

A scrum or a bad hit produces several penalties at one stoppage (coincidental minors, a major
plus a roughing pair, a fight and its extras).  One clip tells that story; one clip per
penalty repeats it.  The rule, from Tom (issue #25):

  * two or more penalties at the same stoppage (same period, game clock within
    SCRUM_CLOCK_TOLERANCE_S), or
  * any major, misconduct, game misconduct or match penalty

is ONE consequential incident.  Its ``kind`` is "fight" when any penalty is a fight, "scrum"
when two or more penalties share the stoppage, "major" for a single major / misconduct,
and "minor" for a lone ordinary penalty (not consequential).

Used by the engine (pipeline all-penalty clips) and by clip_review (incident classes), so both
agree.  Remove: delete this file and its call sites.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Sequence, TypeVar

T = TypeVar("T")

# Penalties this close on the game clock (same period) belong to one stoppage.  The box score
# stamps a scrum's penalties with one time, but a delayed call or a late coincidental can be a
# few seconds apart.
SCRUM_CLOCK_TOLERANCE_S = 5
SCRUM_MIN_PENALTIES = 2

CONSEQUENTIAL_RE = re.compile(r"fight|major|misconduct|match penalty|gross", re.I)
FIGHT_RE = re.compile(r"fight", re.I)
MAJOR_MINUTES = 5

KINDS = ("minor", "major", "scrum", "fight")


def is_fight(infraction: Any) -> bool:
    return bool(FIGHT_RE.search(str(infraction or "")))


def is_consequential_penalty(infraction: Any, minutes: Any) -> bool:
    """A major (5+ minutes) or a misconduct / game misconduct / match penalty / fight."""
    try:
        mins = int(minutes or 0)
    except (TypeError, ValueError):
        mins = 0
    return mins >= MAJOR_MINUTES or bool(CONSEQUENTIAL_RE.search(str(infraction or "")))


def cluster_by_stoppage(
    items: Sequence[T],
    key: Callable[[T], tuple],
    *,
    tolerance_s: float = SCRUM_CLOCK_TOLERANCE_S,
) -> List[List[T]]:
    """Cluster items by (period, seconds): same period, consecutive gap <= tolerance_s.

    Chaining is on the previous item's time, so a run of penalties each a few seconds apart is
    one stoppage.  Items keep input order inside a cluster; clusters come out in time order.
    """
    keyed = sorted(((key(it), idx, it) for idx, it in enumerate(items)), key=lambda x: (x[0][0], x[0][1], x[1]))
    clusters: List[List[T]] = []
    last: tuple = ()
    for (period, secs), _idx, it in keyed:
        if clusters and last[0] == period and secs - last[1] <= tolerance_s:
            clusters[-1].append(it)
        else:
            clusters.append([it])
        last = (period, secs)
    return clusters


def incident_kind(infractions_minutes: Sequence[tuple]) -> str:
    """Kind of one cluster from its (infraction, minutes) pairs: fight, scrum, major or minor."""
    if any(is_fight(inf) for inf, _ in infractions_minutes):
        return "fight"
    if len(infractions_minutes) >= SCRUM_MIN_PENALTIES:
        return "scrum"
    if any(is_consequential_penalty(inf, mins) for inf, mins in infractions_minutes):
        return "major"
    return "minor"


def scrum_summary(penalties: Sequence[Dict[str, Any]]) -> str:
    """Overlay text: '3 penalties: Roughing (A), Roughing (B), Slashing (C)'."""
    n = len(penalties)
    parts = [f"{p.get('infraction', 'Penalty')} ({p.get('player', '?')})" for p in penalties]
    head = f"{n} penalt{'y' if n == 1 else 'ies'}"
    return f"{head}: " + ", ".join(parts)
