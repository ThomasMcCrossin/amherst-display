"""
YouTube Description Generator - Creates formatted game summaries for video descriptions

Generates markdown/plain text descriptions with:
- Game info (date, teams, final score)
- Venue and attendance
- Clickable timestamps for each goal
- Box score stats (shots, PP, PK)
- Three stars
- Goaltender stats
"""

from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import json


def format_timestamp(seconds: float) -> str:
    """Convert seconds to YouTube timestamp format (M:SS or H:MM:SS)"""
    if seconds is None:
        return ""

    total_seconds = int(seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"


def get_scorer_name(goal: Dict) -> str:
    """Extract scorer name from goal data"""
    scorer = goal.get('scorer', {})
    if isinstance(scorer, dict):
        return scorer.get('name', 'Unknown')
    return str(scorer) if scorer else 'Unknown'


def get_assists(goal: Dict) -> List[str]:
    """Extract assist names from goal data"""
    assists = []

    # Try 'assists' array first
    assists_data = goal.get('assists', [])
    if assists_data:
        for a in assists_data:
            if isinstance(a, dict):
                name = a.get('name', '')
                if name:
                    assists.append(name)
            elif isinstance(a, str):
                assists.append(a)
        return assists

    # Try individual assist fields
    for key in ['assist1', 'assist2', 'primary_assist', 'secondary_assist']:
        assist = goal.get(key)
        if assist:
            if isinstance(assist, dict):
                name = assist.get('name', '')
                if name:
                    assists.append(name)
            elif isinstance(assist, str):
                assists.append(assist)

    return assists


def generate_youtube_description(
    game_data: Dict,
    matched_goals: List[Dict],
    our_team: str = 'ramblers'
) -> str:
    """
    Generate a comprehensive YouTube description from game data.

    Args:
        game_data: Full game data from JSON
        matched_goals: Goals with video timestamps
        our_team: Our team identifier

    Returns:
        Formatted description string
    """
    lines = []

    # Parse game info
    opponent = game_data.get('opponent', {})
    opponent_name = opponent.get('team_name', 'Opponent') if isinstance(opponent, dict) else str(opponent)

    date_str = game_data.get('date', '')
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        date_formatted = date_obj.strftime('%B %d, %Y')
    except (ValueError, TypeError):
        date_formatted = date_str

    result = game_data.get('result', {})
    final_score = result.get('final_score', '')
    is_home = game_data.get('home_game', True)

    # Header
    home_away = "vs" if is_home else "@"
    lines.append(f"Amherst Ramblers {home_away} {opponent_name}")
    lines.append(f"{date_formatted}")
    if final_score:
        lines.append(f"Final: {final_score}")
    lines.append("")

    # Venue & Attendance
    venue = game_data.get('venue', '')
    attendance = game_data.get('attendance')
    game_info = game_data.get('game_info', {})
    if game_info:
        venue = game_info.get('arena') or venue
        attendance = game_info.get('attendance') or attendance

    if venue or attendance:
        venue_line = venue or ''
        if attendance:
            venue_line += f" | Attendance: {attendance:,}" if venue_line else f"Attendance: {attendance:,}"
        lines.append(venue_line)
        lines.append("")

    # Scoring summary with timestamps
    lines.append("SCORING")
    lines.append("-" * 30)

    for goal in matched_goals:
        period = goal.get('period', 1)
        time = goal.get('time', '0:00')
        scorer = get_scorer_name(goal)
        goal_team = goal.get('team', '')

        # Determine team abbreviation
        if goal_team == our_team or goal_team == 'amherst-ramblers':
            team_abbr = 'AMH'
        else:
            # Use first 3 letters of opponent name
            team_abbr = opponent_name[:3].upper() if opponent_name else 'OPP'

        # Build assists string
        assists = get_assists(goal)
        if assists:
            assist_str = f" ({', '.join(assists)})"
        else:
            assist_str = " (Unassisted)"

        # Special teams marker
        special = ''
        if goal.get('power_play'):
            special = ' PP'
        elif goal.get('short_handed'):
            special = ' SH'
        elif goal.get('empty_net'):
            special = ' EN'

        # Video timestamp for clickable link
        video_time = goal.get('video_time')
        if video_time is not None:
            timestamp = format_timestamp(video_time)
            lines.append(f"{timestamp} - {team_abbr} P{period} {time}: {scorer}{special}{assist_str}")
        else:
            lines.append(f"P{period} {time}: {team_abbr} - {scorer}{special}{assist_str}")

    lines.append("")

    # Box score stats
    box_score = game_data.get('box_score', {})

    # Shots by period
    shots = box_score.get('shots_by_period')
    if shots:
        lines.append("SHOTS")
        lines.append("-" * 30)
        if shots.get('ramblers'):
            r = shots['ramblers']
            shots_line = f"Ramblers: {r.get('period1', 0)}-{r.get('period2', 0)}-{r.get('period3', 0)}"
            if r.get('overtime'):
                shots_line += f"-{r.get('overtime', 0)}"
            shots_line += f" = {r.get('total', 0)}"
            lines.append(shots_line)
        if shots.get('opponent'):
            o = shots['opponent']
            shots_line = f"{opponent_name}: {o.get('period1', 0)}-{o.get('period2', 0)}-{o.get('period3', 0)}"
            if o.get('overtime'):
                shots_line += f"-{o.get('overtime', 0)}"
            shots_line += f" = {o.get('total', 0)}"
            lines.append(shots_line)
        lines.append("")

    # Power play summary
    pp = box_score.get('power_play_summary')
    if pp:
        lines.append("POWER PLAY")
        lines.append("-" * 30)
        if pp.get('ramblers'):
            r = pp['ramblers']
            lines.append(f"Ramblers: {r.get('power_play_goals', 0)}/{r.get('power_play_opportunities', 0)}")
        if pp.get('opponent'):
            o = pp['opponent']
            lines.append(f"{opponent_name}: {o.get('power_play_goals', 0)}/{o.get('power_play_opportunities', 0)}")
        lines.append("")

    # Three stars
    stars = box_score.get('three_stars')
    if stars:
        lines.append("THREE STARS")
        lines.append("-" * 30)
        for star in stars:
            position = star.get('position', '')
            player = star.get('player', '')
            lines.append(f"{position}. {player}")
        lines.append("")

    # Goaltenders
    goalies = box_score.get('goaltenders')
    if goalies:
        lines.append("GOALTENDERS")
        lines.append("-" * 30)
        for team_key in ['ramblers', 'opponent']:
            team_goalies = goalies.get(team_key, [])
            team_name = 'Ramblers' if team_key == 'ramblers' else opponent_name
            for g in team_goalies:
                name = g.get('name', 'Unknown')
                decision = f" ({g['decision']})" if g.get('decision') else ''
                saves = g.get('saves', 0)
                shots_against = g.get('shots_against', 0)
                lines.append(f"{team_name}: {name}{decision} - {saves}/{shots_against} saves")
        lines.append("")

    # Hashtags
    lines.append("#AmherstRamblers #MHL #JuniorHockey #NovaScotia #MaritimeHockey")

    return "\n".join(lines)


def save_description(
    description: str,
    output_dir: Path,
    filename: str = "youtube_description.txt"
) -> Path:
    """
    Save the description to a file.

    Args:
        description: The description text
        output_dir: Directory to save to
        filename: Output filename

    Returns:
        Path to saved file
    """
    output_path = Path(output_dir) / filename
    output_path.write_text(description)
    return output_path


def generate_and_save_description(
    game_data: Dict,
    matched_goals: List[Dict],
    output_dir: Path,
    our_team: str = 'ramblers'
) -> Path:
    """
    Generate and save YouTube description in one step.

    Args:
        game_data: Full game data from JSON
        matched_goals: Goals with video timestamps
        output_dir: Directory to save to
        our_team: Our team identifier

    Returns:
        Path to saved file
    """
    description = generate_youtube_description(game_data, matched_goals, our_team)
    return save_description(description, output_dir)


def generate_description_from_game_dir(
    game_dir: Path,
    output_dir: Optional[Path] = None,
    *,
    our_team: str = 'ramblers',
) -> Optional[Path]:
    """
    Generate a YouTube description from saved game metadata + matched events.

    This is useful when a pipeline paused for review and needs to create the
    description later without re-running the full extraction.
    """
    metadata_path = Path(game_dir) / "data" / "game_metadata.json"
    events_path = Path(game_dir) / "data" / "matched_events.json"
    if not metadata_path.exists() or not events_path.exists():
        return None

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    try:
        events = json.loads(events_path.read_text(encoding="utf-8"))
    except Exception:
        events = []

    game_info = metadata.get("game_info") or {}
    box_score = metadata.get("box_score") or {}

    home_away = str(game_info.get("home_away") or "").lower()
    home_game = True if not home_away else (home_away == "home")
    opponent_name = str(game_info.get("away_team") if home_game else game_info.get("home_team") or "")

    game_data = {
        "date": str(game_info.get("date") or ""),
        "home_game": home_game,
        "opponent": {"team_name": opponent_name or "Opponent"},
        "venue": str(game_info.get("venue") or ""),
        "result": {},
        "box_score": box_score if isinstance(box_score, dict) else {},
    }

    if isinstance(box_score, dict):
        game_data["attendance"] = box_score.get("attendance")
        game_data["result"] = box_score.get("result", {})
        if isinstance(box_score.get("game_info"), dict):
            gi = box_score["game_info"]
            game_data["venue"] = gi.get("arena") or game_data["venue"]
            game_data["attendance"] = gi.get("attendance", game_data.get("attendance"))

    matched_goals = [
        e for e in (events if isinstance(events, list) else [])
        if isinstance(e, dict) and e.get("type") == "goal" and e.get("video_time") is not None
    ]

    target_dir = Path(output_dir) if output_dir is not None else (Path(game_dir) / "output")
    target_dir.mkdir(parents=True, exist_ok=True)
    return generate_and_save_description(game_data, matched_goals, target_dir, our_team=our_team)
