"""
Penalty Analyzer - Links penalties to powerplay goals and identifies major penalties

This module analyzes penalty and goal data to:
1. Find penalties that contributed to powerplay goals
2. Identify 5-minute major penalties for special handling
3. Group related penalties (e.g., coincidental minors, fighting majors)
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from .time_utils import time_string_to_seconds, PERIOD_LENGTH_SECONDS, OT_LENGTH_SECONDS


def _parse_penalty_minutes(minutes_raw) -> List[int]:
    """
    Parse penalty minutes from a feed value.

    Feeds sometimes provide compound strings like "2+10" (minor + misconduct) or "5+10"
    (major + misconduct). We extract the numeric components so we can:
    - detect 5-minute majors reliably
    - ignore 10-minute misconducts for powerplay linkage
    """
    if minutes_raw is None:
        return []
    if isinstance(minutes_raw, (int, float)):
        try:
            return [int(minutes_raw)]
        except Exception:
            return []
    text = str(minutes_raw).strip()
    if not text:
        return []
    import re

    return [int(m) for m in re.findall(r"\d+", text)]


def _team_slug(team_value: str, our_team: str = 'ramblers') -> str:
    """
    Normalize various team representations to 'ramblers' or 'opponent'.

    Inputs may be slugs ('ramblers', 'opponent'), abbreviations ('AMH'),
    or full names ('Amherst Ramblers').
    """
    v = (team_value or '').strip().lower()
    our = (our_team or '').strip().lower()

    if v in {'ramblers', 'amherst-ramblers', 'amherst ramblers', 'amherst', 'amh'}:
        return 'ramblers'
    if 'rambler' in v or 'amherst' in v:
        return 'ramblers'
    if our and (our in v or v in our):
        return 'ramblers'

    if v in {'opponent', 'opp'}:
        return 'opponent'

    return 'opponent'


def _period_length_seconds(period: int) -> int:
    return OT_LENGTH_SECONDS if int(period or 0) >= 4 else PERIOD_LENGTH_SECONDS


def _to_remaining_seconds(time_str: str, *, period: int, time_is_elapsed: bool) -> int:
    """
    Normalize a box-score time string to *remaining seconds* in the period.

    HockeyTech/box-scores typically report time ELAPSED; broadcast clocks show time REMAINING.
    Most downstream logic (penalty-active checks) assumes remaining seconds.
    """
    value = time_string_to_seconds(time_str)
    if not time_is_elapsed:
        return value
    period_len = _period_length_seconds(period)
    return max(0, min(period_len, period_len - value))


def _creates_power_play(penalty: "PenaltyInfo") -> bool:
    """
    Return True if a penalty should be considered when linking PP goals.

    10-minute misconducts (and similar) do not create a manpower advantage and should not
    be linked to powerplay goals.
    """
    try:
        minutes = int(getattr(penalty, "minutes", 0) or 0)
    except Exception:
        minutes = 0
    if minutes >= 10:
        return False
    inf = str(getattr(penalty, "infraction", "") or "").lower()
    if "misconduct" in inf:
        return False
    return minutes > 0


@dataclass
class PenaltyInfo:
    """Structured penalty information"""
    period: int
    time: str
    time_seconds: int  # Time remaining in period
    team: str  # 'ramblers' or 'opponent'
    player_name: str
    player_number: Optional[int]
    infraction: str
    minutes: int
    is_major: bool
    video_time: Optional[float] = None  # Filled during event matching

    @classmethod
    def from_dict(cls, data: Dict, our_team: str = 'ramblers', *, time_is_elapsed: bool = True) -> 'PenaltyInfo':
        """Create PenaltyInfo from JSON dict"""
        time_str = data.get('time', '0:00')
        period = int(data.get('period', 1) or 1)
        minutes_raw = data.get('minutes', 2)
        components = _parse_penalty_minutes(minutes_raw)
        minutes_val = min(components) if components else 2
        if minutes_val <= 0:
            minutes_val = 2
        infraction = data.get('infraction') or data.get('description') or 'Unknown'
        is_major = 5 in components if components else (minutes_val == 5)
        return cls(
            period=period,
            time=time_str,
            time_seconds=_to_remaining_seconds(time_str, period=period, time_is_elapsed=time_is_elapsed),
            team=_team_slug(str(data.get('team', 'opponent')), our_team=our_team),
            player_name=data.get('player', {}).get('name', 'Unknown'),
            player_number=data.get('player', {}).get('number'),
            infraction=str(infraction),
            minutes=minutes_val,
            is_major=is_major
        )

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'period': self.period,
            'time': self.time,
            'time_seconds': self.time_seconds,
            'team': self.team,
            'player_name': self.player_name,
            'player_number': self.player_number,
            'infraction': self.infraction,
            'minutes': self.minutes,
            'is_major': self.is_major,
            'video_time': self.video_time
        }


def parse_penalties(penalties_data: List[Dict], our_team: str = 'ramblers', *, time_is_elapsed: bool = True) -> List[PenaltyInfo]:
    """Parse raw penalty data into structured PenaltyInfo objects"""
    return [PenaltyInfo.from_dict(p, our_team=our_team, time_is_elapsed=time_is_elapsed) for p in penalties_data]


def calculate_penalty_expiry(penalty: PenaltyInfo) -> Tuple[int, int]:
    """
    Calculate when a penalty expires (period, time_seconds).

    Hockey clock counts DOWN, so a penalty at 15:00 with 2 minutes
    expires at 13:00 in the same period (or carries over to next period).

    Returns:
        Tuple of (expiry_period, expiry_time_seconds)
    """
    penalty_seconds = penalty.minutes * 60
    remaining_in_period = penalty.time_seconds

    if remaining_in_period >= penalty_seconds:
        # Penalty expires in same period
        expiry_time = remaining_in_period - penalty_seconds
        return (penalty.period, expiry_time)
    else:
        # Penalty carries over to next period(s)
        seconds_remaining = penalty_seconds - remaining_in_period
        expiry_period = penalty.period + 1

        while seconds_remaining > PERIOD_LENGTH_SECONDS:
            seconds_remaining -= PERIOD_LENGTH_SECONDS
            expiry_period += 1

        expiry_time = PERIOD_LENGTH_SECONDS - seconds_remaining
        return (expiry_period, max(0, expiry_time))


def is_penalty_active_at_goal(penalty: PenaltyInfo, goal_period: int, goal_time_seconds: int) -> bool:
    """
    Check if a penalty was active (player still in box) when a goal was scored.

    Args:
        penalty: The penalty to check
        goal_period: Period when goal was scored
        goal_time_seconds: Time remaining when goal was scored

    Returns:
        True if penalty was active at time of goal
    """
    # Penalty must start before or at the goal time
    if penalty.period > goal_period:
        return False
    if penalty.period == goal_period and penalty.time_seconds < goal_time_seconds:
        return False

    # Check if penalty has expired by goal time
    expiry_period, expiry_time = calculate_penalty_expiry(penalty)

    if expiry_period < goal_period:
        return False
    if expiry_period == goal_period and expiry_time >= goal_time_seconds:
        return False

    return True


def find_contributing_penalty(
    goal: Dict,
    penalties: List[PenaltyInfo],
    our_team: str = 'ramblers',
    *,
    time_is_elapsed: bool = True,
) -> Optional[PenaltyInfo]:
    """
    Find the penalty that created the powerplay opportunity for a PP goal.

    For a powerplay goal:
    - The scoring team is on the powerplay
    - The opponent had a penalty that was active at goal time

    Args:
        goal: Goal data with period, time, team, power_play fields
        penalties: List of parsed penalties
        our_team: Team identifier for "our" team (default 'ramblers')

    Returns:
        The contributing penalty, or None if not found
    """
    if not goal.get('power_play'):
        return None

    goal_period = goal.get('period', 1)
    goal_time = goal.get('time', '0:00')
    goal_time_seconds = _to_remaining_seconds(goal_time, period=int(goal_period or 1), time_is_elapsed=time_is_elapsed)
    goal_team = goal.get('team', '')
    goal_team_slug = _team_slug(str(goal_team), our_team=our_team)

    # The penalized team is the opponent of the scoring team
    if goal_team_slug == 'ramblers':
        penalized_team = 'opponent'
    else:
        penalized_team = 'ramblers'

    # Find active penalties from the penalized team
    active_penalties = []
    for penalty in penalties:
        if penalty.team != penalized_team:
            continue
        if not _creates_power_play(penalty):
            continue
        if is_penalty_active_at_goal(penalty, goal_period, goal_time_seconds):
            active_penalties.append(penalty)

    if not active_penalties:
        return None

    # Return the most recent penalty (closest to the goal)
    # Sort by period (desc) then time (asc, since lower time = closer to goal)
    active_penalties.sort(
        key=lambda p: (-p.period, p.time_seconds)
    )

    return active_penalties[0]


def find_major_penalties(penalties: List[PenaltyInfo]) -> List[PenaltyInfo]:
    """
    Find all 5-minute major penalties that need special handling.

    Only 5-minute majors are included in the review workflow.
    10-minute misconducts and other non-5-minute penalties are excluded.

    Args:
        penalties: List of parsed penalties

    Returns:
        List of major penalties
    """
    majors = [penalty for penalty in penalties if penalty.is_major]
    return majors


def group_coincidental_penalties(penalties: List[PenaltyInfo]) -> List[List[PenaltyInfo]]:
    """
    Group penalties that occurred at the same time (coincidental minors, fights, etc.)

    Args:
        penalties: List of parsed penalties

    Returns:
        List of penalty groups (each group is a list of penalties at same time)
    """
    groups = []
    used = set()

    for i, penalty in enumerate(penalties):
        if i in used:
            continue

        group = [penalty]
        used.add(i)

        for j, other in enumerate(penalties):
            if j in used or j == i:
                continue
            # Same period and time = coincidental
            if other.period == penalty.period and other.time == penalty.time:
                group.append(other)
                used.add(j)

        groups.append(group)

    return groups


def analyze_game_penalties(
    goals: List[Dict],
    penalties_data: List[Dict],
    our_team: str = 'ramblers',
    *,
    time_is_elapsed: bool = True,
) -> Dict:
    """
    Analyze all penalties for a game and link them to powerplay goals.

    Args:
        goals: List of goal data dicts
        penalties_data: Raw penalty data from box score
        our_team: Team identifier for "our" team

    Returns:
        Dict with:
        - pp_penalty_map: Maps goal index to contributing penalty
        - major_penalties: List of 5-minute majors
        - penalty_groups: Coincidental penalty groups
    """
    penalties = parse_penalties(penalties_data, our_team=our_team, time_is_elapsed=time_is_elapsed)

    # Find contributing penalties for PP goals
    pp_penalty_map = {}
    for i, goal in enumerate(goals):
        if goal.get('power_play'):
            contributing = find_contributing_penalty(goal, penalties, our_team, time_is_elapsed=time_is_elapsed)
            if contributing:
                pp_penalty_map[i] = contributing

    # Find major penalties
    major_penalties = find_major_penalties(penalties)

    # Group coincidental penalties
    penalty_groups = group_coincidental_penalties(penalties)

    return {
        'penalties': penalties,
        'pp_penalty_map': pp_penalty_map,
        'major_penalties': major_penalties,
        'penalty_groups': penalty_groups
    }
