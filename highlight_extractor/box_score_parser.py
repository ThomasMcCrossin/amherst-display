"""
Box Score Parser - Parses goal and event data from HockeyTech API responses

This module separates box score parsing from API fetching, providing:
- Type-safe Goal extraction
- Robust handling of various API response formats
- Clear separation of concerns
"""

import logging
from typing import Dict, List, Optional, Any

from .goal import Goal, GoalType, GoalSummary
from .time_utils import time_string_to_seconds

logger = logging.getLogger(__name__)


class BoxScoreParser:
    """
    Parses box score data from HockeyTech API responses.

    This class is responsible for extracting structured Goal objects
    from raw API JSON responses.
    """

    def __init__(self):
        """Initialize the parser"""
        pass

    def parse_goals(self, box_score: Dict[str, Any]) -> List[Goal]:
        """
        Parse goals from a box score API response.

        Args:
            box_score: Raw box score dictionary from HockeyTech API

        Returns:
            List of Goal objects, sorted by period and time
        """
        goals = []

        try:
            # Navigate to the game summary data
            game_data = self._get_game_summary(box_score)
            if not game_data:
                logger.warning("Could not find game summary in box score")
                return []

            # Extract goals from various possible locations
            raw_goals = self._extract_raw_goals(game_data)

            for raw_goal in raw_goals:
                try:
                    goal = self._parse_single_goal(raw_goal)
                    if goal:
                        goals.append(goal)
                except Exception as e:
                    logger.warning(f"Failed to parse goal: {e}")
                    logger.debug(f"Raw goal data: {raw_goal}")

            # Sort by period and time (time is countdown, so higher time = earlier in period)
            goals.sort(key=lambda g: (g.period, -g.time_seconds))

            logger.info(f"Parsed {len(goals)} goals from box score")

        except Exception as e:
            logger.error(f"Failed to parse goals from box score: {e}")

        return goals

    def parse_goal_summary(
        self,
        box_score: Dict[str, Any],
        home_team: str,
        away_team: str
    ) -> GoalSummary:
        """
        Parse goals into a GoalSummary with team information.

        Args:
            box_score: Raw box score dictionary
            home_team: Home team name
            away_team: Away team name

        Returns:
            GoalSummary with all parsed goals
        """
        goals = self.parse_goals(box_score)
        return GoalSummary(
            home_team=home_team,
            away_team=away_team,
            goals=goals,
        )

    def _get_game_summary(self, box_score: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Navigate to the game summary section of the box score.

        Handles various HockeyTech API response structures.

        Args:
            box_score: Raw box score response

        Returns:
            Game summary dictionary or None if not found
        """
        if not isinstance(box_score, dict):
            return None

        # Try common paths
        # Path 1: SiteKit -> Gamesummary
        site_kit = box_score.get('SiteKit', {})
        if isinstance(site_kit, dict):
            game_summary = site_kit.get('Gamesummary')
            if game_summary:
                return game_summary

        # Path 2: Direct Gamesummary
        if 'Gamesummary' in box_score:
            return box_score['Gamesummary']

        # Path 3: gameData or game_data
        game_data = box_score.get('gameData') or box_score.get('game_data')
        if game_data:
            return game_data

        # Path 4: Just return the box_score if it has goals directly
        if 'goals' in box_score or 'scoring_plays' in box_score:
            return box_score

        return None

    def _extract_raw_goals(self, game_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract raw goal data from game summary.

        Handles various field names used in HockeyTech APIs.

        Args:
            game_data: Game summary dictionary

        Returns:
            List of raw goal dictionaries
        """
        # Try various field names
        goals = game_data.get('goals')
        if goals:
            return goals if isinstance(goals, list) else []

        scoring_plays = game_data.get('scoring_plays')
        if scoring_plays:
            return scoring_plays if isinstance(scoring_plays, list) else []

        scoring = game_data.get('scoring')
        if scoring:
            # Some APIs nest goals under periods
            if isinstance(scoring, list):
                return scoring
            elif isinstance(scoring, dict):
                all_goals = []
                for period_data in scoring.values():
                    if isinstance(period_data, list):
                        all_goals.extend(period_data)
                return all_goals

        return []

    def _parse_single_goal(self, raw: Dict[str, Any]) -> Optional[Goal]:
        """
        Parse a single goal from raw API data.

        Args:
            raw: Raw goal dictionary from API

        Returns:
            Goal object or None if parsing fails
        """
        # Extract period
        period = self._extract_period(raw)
        if period is None:
            logger.debug(f"Could not extract period from: {raw}")
            return None

        # Extract time
        time_str = self._extract_time(raw)
        if not time_str:
            logger.debug(f"Could not extract time from: {raw}")
            return None

        # Extract team
        team = self._extract_team(raw)
        if not team:
            logger.debug(f"Could not extract team from: {raw}")
            return None

        # Extract scorer
        scorer = self._extract_scorer(raw)
        if not scorer:
            logger.debug(f"Could not extract scorer from: {raw}")
            return None

        # Extract assists (optional)
        assist1 = self._extract_assist(raw, 1)
        assist2 = self._extract_assist(raw, 2)

        # Extract goal type (optional)
        goal_type = self._extract_goal_type(raw)

        return Goal(
            period=period,
            time=time_str,
            team=team,
            scorer=scorer,
            assist1=assist1,
            assist2=assist2,
            goal_type=goal_type,
        )

    def _extract_period(self, raw: Dict[str, Any]) -> Optional[int]:
        """Extract period number from raw goal data"""
        # Try various field names
        for field in ('period', 'period_id', 'periodNumber', 'period_number'):
            value = raw.get(field)
            if value is not None:
                try:
                    return int(value)
                except (ValueError, TypeError):
                    pass

        return None

    def _extract_time(self, raw: Dict[str, Any]) -> Optional[str]:
        """Extract time string from raw goal data"""
        for field in ('time', 'time_formatted', 'clock', 'game_time'):
            value = raw.get(field)
            if value and isinstance(value, str):
                # Validate format (MM:SS or M:SS)
                value = value.strip()
                if ':' in value:
                    return value

        return None

    def _extract_team(self, raw: Dict[str, Any]) -> Optional[str]:
        """Extract team name from raw goal data"""
        for field in ('team', 'team_name', 'teamName', 'scoring_team'):
            value = raw.get(field)
            if value and isinstance(value, str):
                return value.strip()

        # Some APIs use team as an object
        team_obj = raw.get('team') or raw.get('scoring_team_info')
        if isinstance(team_obj, dict):
            return team_obj.get('name') or team_obj.get('team_name')

        return None

    def _extract_scorer(self, raw: Dict[str, Any]) -> Optional[str]:
        """Extract scorer name from raw goal data"""
        # Direct scorer field
        for field in ('scorer', 'scorer_name', 'goal_scorer', 'player'):
            value = raw.get(field)
            if value and isinstance(value, str):
                return value.strip()

        # Scorer as 'goal' object (HockeyTech format)
        goal_obj = raw.get('goal')
        if isinstance(goal_obj, dict):
            name = goal_obj.get('name') or goal_obj.get('player_name')
            if name:
                return name.strip()

        # Player object
        player_obj = raw.get('scorer') or raw.get('goal_scorer')
        if isinstance(player_obj, dict):
            name = player_obj.get('name') or player_obj.get('player_name')
            if name:
                return name.strip()

        return None

    def _extract_assist(self, raw: Dict[str, Any], assist_num: int) -> Optional[str]:
        """Extract assist name from raw goal data"""
        field_names = {
            1: ['assist1', 'first_assist', 'primary_assist', 'assist_1'],
            2: ['assist2', 'second_assist', 'secondary_assist', 'assist_2'],
        }

        for field in field_names.get(assist_num, []):
            value = raw.get(field)

            # Direct string value
            if value and isinstance(value, str):
                return value.strip() or None

            # Object with name field
            if isinstance(value, dict):
                name = value.get('name') or value.get('player_name')
                if name:
                    return name.strip()

        # Try assists array
        assists = raw.get('assists', [])
        if isinstance(assists, list) and len(assists) >= assist_num:
            assist = assists[assist_num - 1]
            if isinstance(assist, str):
                return assist.strip() or None
            elif isinstance(assist, dict):
                return assist.get('name') or assist.get('player_name')

        return None

    def _extract_goal_type(self, raw: Dict[str, Any]) -> Optional[GoalType]:
        """Extract goal type (PP, SH, EN, etc.) from raw goal data"""
        for field in ('plus_minus', 'special', 'goal_type', 'type', 'situation'):
            value = raw.get(field)
            if value:
                goal_type = GoalType.from_string(str(value))
                if goal_type:
                    return goal_type

        # Check for boolean flags
        if raw.get('power_play') or raw.get('is_power_play'):
            return GoalType.POWER_PLAY
        if raw.get('short_handed') or raw.get('is_short_handed'):
            return GoalType.SHORT_HANDED
        if raw.get('empty_net') or raw.get('is_empty_net'):
            return GoalType.EMPTY_NET

        return None

    def goals_to_event_dicts(self, goals: List[Goal]) -> List[Dict[str, Any]]:
        """
        Convert Goals to event dictionaries for backward compatibility.

        This allows the new Goal model to work with existing pipeline code
        that expects event dictionaries.

        Args:
            goals: List of Goal objects

        Returns:
            List of event dictionaries compatible with EventMatcher
        """
        events = []
        for goal in goals:
            event = goal.to_dict()
            # Ensure backward compatibility fields
            event['type'] = 'goal'
            event['time'] = goal.time
            event['period'] = goal.period
            events.append(event)
        return events
