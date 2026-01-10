"""
Amherst Display Integration - Provides pre-fetched box score data from amherst-display

This module integrates with the amherst-display project to provide pre-fetched
box score data for Amherst Ramblers games, eliminating the need to fetch from
the HockeyTech API during highlight extraction.

Usage:
    from highlight_extractor.amherst_integration import AmherstBoxScoreProvider

    # Load games from amherst-display JSON
    provider = AmherstBoxScoreProvider('/path/to/amherst-display/games/amherst-ramblers.json')

    # Find a game by date and opponent
    game = provider.find_game('2026-01-09', 'Edmundston Blizzard')

    # Get box score in HockeyTech API format
    box_score = provider.get_box_score_for_game(game)

    # Or use as a drop-in replacement for BoxScoreFetcher
    fetcher = provider.create_fetcher()
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from .goal import Goal, GoalType
from .box_score import BoxScoreFetcher

logger = logging.getLogger(__name__)


class AmherstBoxScoreProvider:
    """
    Provides box score data from amherst-display JSON files.

    This class reads pre-fetched game data from the amherst-display project
    and converts it to the format expected by HockeyHighlightExtractor.
    """

    def __init__(self, games_json_path: str):
        """
        Initialize the provider with path to amherst-ramblers.json

        Args:
            games_json_path: Path to games/amherst-ramblers.json
        """
        self.games_json_path = Path(games_json_path)
        self.games_data = None
        self._load_games()

    def _load_games(self):
        """Load games from JSON file"""
        if not self.games_json_path.exists():
            raise FileNotFoundError(f"Games file not found: {self.games_json_path}")

        with open(self.games_json_path, 'r') as f:
            self.games_data = json.load(f)

        logger.info(f"Loaded {len(self.games_data.get('games', []))} games from {self.games_json_path}")

    def find_game(
        self,
        game_date: str,
        opponent: Optional[str] = None,
        game_id: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Find a game by date and/or opponent.

        Args:
            game_date: Game date in YYYY-MM-DD format
            opponent: Opponent team name (partial match)
            game_id: Specific game ID

        Returns:
            Game dictionary or None if not found
        """
        games = self.games_data.get('games', [])

        for game in games:
            # Match by game_id if provided
            if game_id and str(game.get('game_id')) == str(game_id):
                return game

            # Match by date
            if game.get('date') != game_date:
                continue

            # Match by opponent if provided
            if opponent:
                game_opponent = game.get('opponent', {}).get('team_name', '')
                if opponent.lower() not in game_opponent.lower():
                    continue

            return game

        logger.warning(f"No game found for date={game_date}, opponent={opponent}, id={game_id}")
        return None

    def find_game_by_teams(
        self,
        home_team: str,
        away_team: str,
        game_date: str
    ) -> Optional[Dict]:
        """
        Find a game matching home/away team configuration.

        Used for matching video filename metadata to game data.

        Args:
            home_team: Home team name
            away_team: Away team name
            game_date: Game date in YYYY-MM-DD format

        Returns:
            Game dictionary or None if not found
        """
        games = self.games_data.get('games', [])

        for game in games:
            if game.get('date') != game_date:
                continue

            is_home = game.get('home_game', False)
            opponent = game.get('opponent', {}).get('team_name', '')

            # Ramblers are home: home_team should match Ramblers, away_team matches opponent
            if is_home:
                if 'rambler' in home_team.lower() and opponent.lower() in away_team.lower():
                    return game
                if 'amherst' in home_team.lower() and opponent.lower() in away_team.lower():
                    return game

            # Ramblers are away: away_team should match Ramblers, home_team matches opponent
            else:
                if 'rambler' in away_team.lower() and opponent.lower() in home_team.lower():
                    return game
                if 'amherst' in away_team.lower() and opponent.lower() in home_team.lower():
                    return game

        return None

    def get_box_score_for_game(self, game: Dict) -> Dict:
        """
        Convert game data to HockeyTech API-compatible box score format.

        Args:
            game: Game dictionary from amherst-display

        Returns:
            Box score in HockeyTech SiteKit/Gamesummary format
        """
        scoring = game.get('scoring', [])
        penalties = game.get('penalties', [])
        opponent = game.get('opponent', {})
        is_home = game.get('home_game', False)

        # Determine team names
        ramblers_name = 'Amherst Ramblers'
        opponent_name = opponent.get('team_name', 'Opponent')

        # Convert scoring plays to goals format
        goals = []
        for play in scoring:
            goal = self._convert_scoring_play(play, ramblers_name, opponent_name)
            if goal:
                goals.append(goal)

        # Convert penalties
        penalty_list = []
        for pen in penalties:
            penalty = self._convert_penalty(pen, ramblers_name, opponent_name)
            if penalty:
                penalty_list.append(penalty)

        # Build HockeyTech-style response
        box_score = {
            'SiteKit': {
                'Gamesummary': {
                    'meta': {
                        'game_id': str(game.get('game_id', '')),
                        'date': game.get('date', ''),
                        'home_team': ramblers_name if is_home else opponent_name,
                        'away_team': opponent_name if is_home else ramblers_name,
                    },
                    'goals': goals,
                    'penalties': penalty_list,
                }
            },
            # Store original data for reference
            '_amherst_display': {
                'game_id': game.get('game_id'),
                'box_score': game.get('box_score'),
                'game_info': game.get('game_info'),
                'player_stats': game.get('player_stats'),
            }
        }

        return box_score

    def _convert_scoring_play(
        self,
        play: Dict,
        ramblers_name: str,
        opponent_name: str
    ) -> Optional[Dict]:
        """Convert amherst-display scoring play to HockeyTech goal format"""
        try:
            # Determine team name
            team_key = play.get('team', '')
            if team_key == 'ramblers':
                team = ramblers_name
            elif team_key == 'opponent':
                team = opponent_name
            else:
                team = team_key

            # Get scorer info
            scorer_obj = play.get('scorer', {})
            scorer_name = scorer_obj.get('name', 'Unknown')

            # Get assists
            assists = play.get('assists', [])
            assist1 = assists[0].get('name', '') if len(assists) > 0 else ''
            assist2 = assists[1].get('name', '') if len(assists) > 1 else ''

            # Determine goal type
            special = ''
            if play.get('power_play'):
                special = 'PP'
            elif play.get('short_handed'):
                special = 'SH'
            elif play.get('empty_net'):
                special = 'EN'

            goal = {
                'period': play.get('period', 1),
                'time': play.get('time', '0:00'),
                'team': team,
                'goal': {'name': scorer_name},
                'assist1': {'name': assist1} if assist1 else {},
                'assist2': {'name': assist2} if assist2 else {},
                'plus_minus': special,
            }

            return goal

        except Exception as e:
            logger.warning(f"Failed to convert scoring play: {e}")
            return None

    def _convert_penalty(
        self,
        pen: Dict,
        ramblers_name: str,
        opponent_name: str
    ) -> Optional[Dict]:
        """Convert amherst-display penalty to HockeyTech format"""
        try:
            team_key = pen.get('team', '')
            if team_key == 'ramblers':
                team = ramblers_name
            elif team_key == 'opponent':
                team = opponent_name
            else:
                team = team_key

            player_obj = pen.get('player', {})

            return {
                'period': pen.get('period', 1),
                'time': pen.get('time', '0:00'),
                'team': team,
                'player': {'name': player_obj.get('name', 'Unknown')},
                'description': pen.get('infraction', ''),
                'minutes': pen.get('minutes', 2),
            }

        except Exception as e:
            logger.warning(f"Failed to convert penalty: {e}")
            return None

    def get_goals_for_game(self, game: Dict) -> List[Goal]:
        """
        Get typed Goal objects for a game.

        Args:
            game: Game dictionary from amherst-display

        Returns:
            List of Goal objects
        """
        scoring = game.get('scoring', [])
        opponent = game.get('opponent', {})

        ramblers_name = 'Amherst Ramblers'
        opponent_name = opponent.get('team_name', 'Opponent')

        goals = []
        for play in scoring:
            try:
                team_key = play.get('team', '')
                team = ramblers_name if team_key == 'ramblers' else opponent_name

                scorer = play.get('scorer', {}).get('name', 'Unknown')
                assists = play.get('assists', [])
                assist1 = assists[0].get('name') if len(assists) > 0 else None
                assist2 = assists[1].get('name') if len(assists) > 1 else None

                # Determine goal type
                goal_type = None
                if play.get('power_play'):
                    goal_type = GoalType.POWER_PLAY
                elif play.get('short_handed'):
                    goal_type = GoalType.SHORT_HANDED
                elif play.get('empty_net'):
                    goal_type = GoalType.EMPTY_NET

                goal = Goal(
                    period=play.get('period', 1),
                    time=play.get('time', '0:00'),
                    team=team,
                    scorer=scorer,
                    assist1=assist1,
                    assist2=assist2,
                    goal_type=goal_type,
                )
                goals.append(goal)

            except Exception as e:
                logger.warning(f"Failed to create Goal object: {e}")

        return goals

    def list_games(self) -> List[Dict]:
        """
        List all available games with summary info.

        Returns:
            List of game summaries
        """
        games = self.games_data.get('games', [])
        summaries = []

        for game in games:
            result = game.get('result', {})
            opponent = game.get('opponent', {})

            summaries.append({
                'game_id': game.get('game_id'),
                'date': game.get('date'),
                'opponent': opponent.get('team_name'),
                'home_game': game.get('home_game'),
                'score': result.get('final_score'),
                'won': result.get('won'),
                'goal_count': len(game.get('scoring', [])),
            })

        return summaries

    def create_fetcher(self, game: Dict) -> 'PreloadedBoxScoreFetcher':
        """
        Create a BoxScoreFetcher that uses pre-loaded data for a specific game.

        Args:
            game: Game dictionary to pre-load

        Returns:
            PreloadedBoxScoreFetcher instance
        """
        box_score = self.get_box_score_for_game(game)
        return PreloadedBoxScoreFetcher(
            game_id=str(game.get('game_id', '')),
            box_score=box_score,
            goals=self.get_goals_for_game(game),
        )


class PreloadedBoxScoreFetcher(BoxScoreFetcher):
    """
    BoxScoreFetcher that uses pre-loaded data instead of API calls.

    This can be used as a drop-in replacement in the HighlightPipeline.
    """

    def __init__(
        self,
        game_id: str,
        box_score: Dict,
        goals: List[Goal],
        cache_dir: Optional[Path] = None
    ):
        """
        Initialize with pre-loaded data.

        Args:
            game_id: The game ID
            box_score: Pre-loaded box score dictionary
            goals: Pre-loaded Goal objects
            cache_dir: Optional cache directory (unused but kept for API compat)
        """
        super().__init__(cache_dir=cache_dir)
        self._game_id = game_id
        self._box_score = box_score
        self._goals = goals

    def find_game(
        self,
        league: str,
        home_team: str,
        away_team: str,
        game_date: str
    ) -> Optional[str]:
        """Return the pre-loaded game ID"""
        logger.info(f"Using pre-loaded game ID: {self._game_id}")
        return self._game_id

    def fetch_box_score(self, league: str, game_id: str) -> Optional[Dict]:
        """Return the pre-loaded box score"""
        logger.info("Using pre-loaded box score data")
        return self._box_score

    def get_goals(self, box_score: Dict) -> List[Goal]:
        """Return the pre-loaded Goal objects"""
        return self._goals


def find_amherst_display_path() -> Optional[Path]:
    """
    Try to find the amherst-display project directory.

    Searches common locations relative to this file.

    Returns:
        Path to amherst-display or None if not found
    """
    # Get the directory containing this file
    this_file = Path(__file__).resolve()
    highlight_extractor_dir = this_file.parent
    project_dir = highlight_extractor_dir.parent.parent  # Up to parent of HockeyHighlightExtractor

    # Try common locations
    candidates = [
        project_dir / 'amherst-display',
        project_dir.parent / 'amherst-display',
        Path.home() / 'amherst-display',
        Path('/home/clarencehub/amherst-display'),
    ]

    for path in candidates:
        games_file = path / 'games' / 'amherst-ramblers.json'
        if games_file.exists():
            logger.info(f"Found amherst-display at: {path}")
            return path

    return None
