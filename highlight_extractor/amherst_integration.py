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
import os
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
        self._remote_schedule_cache: Optional[List[Dict[str, Any]]] = None
        self._remote_game_cache: Dict[str, Dict[str, Any]] = {}
        self._live_fetcher = BoxScoreFetcher()
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

        remote_game = self._find_remote_game(game_date=game_date, opponent=opponent, game_id=game_id)
        if remote_game:
            return remote_game

        logger.warning(f"No game found for date={game_date}, opponent={opponent}, id={game_id}")
        return None

    def _find_remote_game(
        self,
        *,
        game_date: str,
        opponent: Optional[str] = None,
        game_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            schedule = self._fetch_remote_schedule()
        except Exception as e:
            logger.warning(f"Could not fetch remote Amherst schedule: {e}")
            return None

        opponent_norm = (opponent or "").strip().lower()
        for entry in schedule:
            if not isinstance(entry, dict):
                continue
            if not self._is_amherst_schedule_entry(entry):
                continue
            if game_id and str(entry.get("game_id") or entry.get("id") or "").strip() != str(game_id).strip():
                continue
            if game_date and str(entry.get("date_played") or "").strip() != str(game_date).strip():
                continue

            if opponent_norm:
                home_name = str(entry.get("home_team_name") or "").strip()
                away_name = str(entry.get("visiting_team_name") or "").strip()
                entry_opp = away_name if self._is_amherst_name(home_name) else home_name
                if opponent_norm not in entry_opp.lower():
                    continue

            game_key = str(entry.get("game_id") or entry.get("id") or game_date or "").strip()
            if game_key in self._remote_game_cache:
                return self._remote_game_cache[game_key]

            remote_game = self._build_remote_game(entry)
            if remote_game:
                self._cache_remote_game(remote_game)
                return remote_game
        return None

    def _cache_remote_game(self, game: Dict[str, Any]) -> None:
        game_id = str(game.get("game_id") or "").strip()
        if not game_id:
            return
        self._remote_game_cache[game_id] = game
        games = self.games_data.setdefault("games", []) if isinstance(self.games_data, dict) else None
        if not isinstance(games, list):
            return
        if any(str(existing.get("game_id") or "").strip() == game_id for existing in games if isinstance(existing, dict)):
            return
        games.append(game)

    def _fetch_remote_schedule(self) -> List[Dict[str, Any]]:
        if self._remote_schedule_cache is not None:
            return self._remote_schedule_cache

        self._live_fetcher._require_api_key()
        league_cfg = self._live_fetcher.LEAGUE_CONFIGS.get("MHL")
        if not league_cfg:
            raise RuntimeError("MHL league config is unavailable")

        params = {
            "feed": "modulekit",
            "view": "schedule",
            "key": self._live_fetcher.api_key,
            "fmt": "json",
            "client_code": league_cfg["client_code"],
            "league_id": league_cfg["league_id"],
        }
        if league_cfg.get("season_id"):
            params["season_id"] = league_cfg["season_id"]

        response = self._live_fetcher.session.get(
            f"{self._live_fetcher.API_BASE}index.php",
            params=params,
            timeout=(5, 15),
        )
        response.raise_for_status()
        payload = response.json()
        sitekit = payload.get("SiteKit", {}) if isinstance(payload, dict) else {}
        schedule = sitekit.get("Schedule", []) if isinstance(sitekit, dict) else []
        self._remote_schedule_cache = [entry for entry in schedule if isinstance(entry, dict)]
        return self._remote_schedule_cache

    @staticmethod
    def _is_amherst_name(value: str) -> bool:
        team = str(value or "").strip().lower()
        return "amherst" in team or "rambler" in team

    def _is_amherst_schedule_entry(self, entry: Dict[str, Any]) -> bool:
        return self._is_amherst_name(str(entry.get("home_team_name") or "")) or self._is_amherst_name(
            str(entry.get("visiting_team_name") or "")
        )

    @staticmethod
    def _period_name(period: int) -> str:
        if period == 1:
            return "1st"
        if period == 2:
            return "2nd"
        if period == 3:
            return "3rd"
        if period == 4:
            return "OT"
        if period > 4:
            return f"{period - 3}OT"
        return str(period)

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value in ("", None):
            return None
        try:
            return int(value)
        except Exception:
            return None

    def _build_remote_game(self, schedule_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        game_id = str(schedule_entry.get("game_id") or schedule_entry.get("id") or "").strip()
        if not game_id:
            return None

        home_name = str(schedule_entry.get("home_team_name") or "").strip()
        away_name = str(schedule_entry.get("visiting_team_name") or "").strip()
        is_home = self._is_amherst_name(home_name)
        opponent_name = away_name if is_home else home_name
        opponent_code = str(schedule_entry.get("visiting_team_code") if is_home else schedule_entry.get("home_team_code") or "").strip()
        opponent_team_id = self._safe_int(schedule_entry.get("visiting_team") if is_home else schedule_entry.get("home_team"))
        schedule_notes = str(schedule_entry.get("schedule_notes") or "").strip()

        raw_box = self._live_fetcher.fetch_box_score("MHL", game_id) or {}
        summary = (raw_box.get("SiteKit") or {}).get("Gamesummary", {}) if isinstance(raw_box, dict) else {}
        goals = summary.get("goals", []) if isinstance(summary, dict) else []
        penalties = summary.get("penalties", []) if isinstance(summary, dict) else []

        scoring: List[Dict[str, Any]] = []
        for goal in goals if isinstance(goals, list) else []:
            if not isinstance(goal, dict):
                continue
            goal_team = str(goal.get("team") or "").strip()
            plus_minus = str(goal.get("plus_minus") or "").strip().upper()
            assists: List[Dict[str, Any]] = []
            assist1 = goal.get("assist1") if isinstance(goal.get("assist1"), dict) else {}
            assist2 = goal.get("assist2") if isinstance(goal.get("assist2"), dict) else {}
            if str(assist1.get("name") or "").strip():
                assists.append({"name": str(assist1.get("name") or "").strip()})
            if str(assist2.get("name") or "").strip():
                assists.append({"name": str(assist2.get("name") or "").strip()})
            period = self._safe_int(goal.get("period")) or 0
            scoring.append(
                {
                    "period": period,
                    "period_name": self._period_name(period),
                    "time": str(goal.get("time") or "").strip(),
                    "team": "amherst-ramblers" if self._is_amherst_name(goal_team) else "opponent",
                    "scorer": {"name": str(((goal.get("goal") or {}).get("name") or "")).strip()},
                    "assists": assists,
                    "power_play": plus_minus == "PP",
                    "short_handed": plus_minus == "SH",
                    "game_winning": False,
                    "empty_net": plus_minus == "EN",
                }
            )

        local_penalties: List[Dict[str, Any]] = []
        for penalty in penalties if isinstance(penalties, list) else []:
            if not isinstance(penalty, dict):
                continue
            team_name = str(penalty.get("team") or "").strip()
            period = self._safe_int(penalty.get("period")) or 0
            player = penalty.get("player") if isinstance(penalty.get("player"), dict) else {}
            local_penalties.append(
                {
                    "period": period,
                    "period_name": self._period_name(period),
                    "time": str(penalty.get("time") or "").strip(),
                    "team": "amherst-ramblers" if self._is_amherst_name(team_name) else "opponent",
                    "player": {
                        "name": str(player.get("name") or "").strip(),
                        "number": self._safe_int(player.get("number")),
                    },
                    "infraction": str(penalty.get("description") or "").strip(),
                    "minutes": self._safe_int(penalty.get("minutes")) or 2,
                }
            )

        home_goals = self._safe_int(schedule_entry.get("home_goal_count"))
        away_goals = self._safe_int(schedule_entry.get("visiting_goal_count"))
        game_status = str(schedule_entry.get("game_status") or "").strip()
        is_final = str(schedule_entry.get("final") or "").strip() == "1" or str(schedule_entry.get("status") or "").strip() == "4" or game_status.lower().startswith("final")
        result: Dict[str, Any] = {}
        if is_final and home_goals is not None and away_goals is not None:
            ramblers_score = home_goals if is_home else away_goals
            opponent_score = away_goals if is_home else home_goals
            result = {
                "won": ramblers_score > opponent_score,
                "ramblers_score": ramblers_score,
                "opponent_score": opponent_score,
                "overtime": "ot" in game_status.lower(),
                "shootout": "so" in game_status.lower(),
                "final_score": f"{ramblers_score}-{opponent_score}" + (f" ({game_status.replace('Final', '').strip()})" if game_status.lower().startswith("final ") else ""),
            }

        attendance = self._safe_int(schedule_entry.get("attendance"))
        return {
            "game_id": game_id,
            "season_id": self._safe_int(schedule_entry.get("season_id")),
            "date": str(schedule_entry.get("date_played") or "").strip(),
            "date_time": str(schedule_entry.get("GameDateISO8601") or schedule_entry.get("date_time_played") or "").strip(),
            "opponent": {
                "team_id": opponent_team_id,
                "team_name": opponent_name,
                "team_code": opponent_code,
            },
            "home_game": is_home,
            "playoff": ("best of 7" in schedule_notes.lower()) or bool(self._safe_int(schedule_entry.get("game_number"))),
            "schedule_notes": schedule_notes,
            "venue": str(schedule_entry.get("venue_name") or schedule_entry.get("venue_location") or "").strip(),
            "result": result,
            "attendance": attendance,
            "scoring": scoring,
            "penalties": local_penalties,
            "box_score": raw_box,
            "game_info": {
                "venue_name": str(schedule_entry.get("venue_name") or "").strip(),
                "venue_location": str(schedule_entry.get("venue_location") or "").strip(),
                "scheduled_time": str(schedule_entry.get("scheduled_time") or schedule_entry.get("game_status") or "").strip(),
                "timezone": str(schedule_entry.get("timezone") or "").strip(),
            },
            "_remote_source": {
                "provider": "hockeytech",
                "cached_locally": False,
                "schedule_entry": schedule_entry,
            },
        }

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

        def _norm(value: str) -> str:
            v = (value or "").lower().strip()
            v = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in v)
            return " ".join(v.split())

        home_norm = _norm(home_team)
        away_norm = _norm(away_team)

        for game in games:
            if game.get('date') != game_date:
                continue

            is_home = game.get('home_game', False)
            opponent = game.get('opponent', {}).get('team_name', '')
            opp_norm = _norm(opponent)

            is_ramblers_home = ("rambler" in home_norm) or ("amherst" in home_norm)
            is_ramblers_away = ("rambler" in away_norm) or ("amherst" in away_norm)

            def _teams_match(team_norm: str, opponent_norm: str) -> bool:
                if not team_norm or not opponent_norm:
                    return False
                return (team_norm in opponent_norm) or (opponent_norm in team_norm)

            # Ramblers are home: home_team should match Ramblers, away_team matches opponent
            if is_home:
                if is_ramblers_home and _teams_match(away_norm, opp_norm):
                    return game
                continue

            # Ramblers are away: away_team should match Ramblers, home_team matches opponent
            else:
                if is_ramblers_away and _teams_match(home_norm, opp_norm):
                    return game
                continue

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
        if not penalties:
            penalties = (game.get('box_score') or {}).get('penalties', []) or []
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

        # Some amherst-display caches omit the per-penalty log (even when PP goals exist).
        # Fall back to HockeyTech for penalties so PP-penalty linking + major review workflows work end-to-end.
        if not penalty_list:
            game_id = str(game.get('game_id') or '').strip()
            if game_id:
                try:
                    fetcher = BoxScoreFetcher()
                    raw = fetcher.fetch_box_score('MHL', game_id)
                    api_penalties = (raw or {}).get('SiteKit', {}).get('Gamesummary', {}).get('penalties', []) or []
                    if api_penalties:
                        penalty_list = api_penalties
                        logger.info(f"Fetched {len(api_penalties)} penalties from HockeyTech for game {game_id}")
                except Exception as e:
                    logger.warning(f"Could not fetch penalties from HockeyTech for game {game_id}: {e}")

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
            def _team_name_from_value(value: str) -> str:
                v = str(value or "").strip().lower()
                if not v:
                    return opponent_name
                # Amherst-display uses a mix of slugs and labels.
                if v in {"ramblers", "amherst-ramblers", "amherst ramblers", "amherst", "amh"}:
                    return ramblers_name
                if v in {"opponent", "opp"}:
                    return opponent_name
                if "rambler" in v or "amherst" in v:
                    return ramblers_name
                return opponent_name

            # Determine team name
            team = _team_name_from_value(play.get("team", ""))

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
            def _team_name_from_value(value: str) -> str:
                v = str(value or "").strip().lower()
                if not v:
                    return opponent_name
                if v in {"ramblers", "amherst-ramblers", "amherst ramblers", "amherst", "amh"}:
                    return ramblers_name
                if v in {"opponent", "opp"}:
                    return opponent_name
                if "rambler" in v or "amherst" in v:
                    return ramblers_name
                return opponent_name

            team = _team_name_from_value(pen.get("team", ""))

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
                team_key = str(play.get("team", "") or "").strip().lower()
                if team_key in {"ramblers", "amherst-ramblers", "amherst ramblers", "amherst", "amh"} or (
                    "rambler" in team_key or "amherst" in team_key
                ):
                    team = ramblers_name
                else:
                    team = opponent_name

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
        # Some pipeline components key off `_last_game_id` (legacy BoxScoreFetcher behavior).
        # Mirror it here so major review workflows keep using the numeric game id.
        self._last_game_id = game_id
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
    env_override = os.environ.get("AMHERST_DISPLAY_DIR", "").strip()
    candidates = []
    if env_override:
        candidates.append(Path(env_override).expanduser())
    candidates.extend(
        [
            project_dir / 'amherst-display',
            project_dir.parent / 'amherst-display',
            Path.cwd() / 'amherst-display',
            Path.cwd().parent / 'amherst-display',
            Path.home() / 'amherst-display',
        ]
    )

    seen = set()
    for raw_path in candidates:
        path = raw_path.expanduser().resolve()
        if path in seen:
            continue
        seen.add(path)
        games_file = path / 'games' / 'amherst-ramblers.json'
        if games_file.exists():
            logger.info(f"Found amherst-display at: {path}")
            return path

    return None
