"""
Box Score Fetcher - Integrates with HockeyTech API to fetch game data

This module handles API communication with HockeyTech to retrieve box scores.
For parsing box score data, see box_score_parser.py.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry
from pathlib import Path
import json

from .box_score_parser import BoxScoreParser
from .goal import Goal, GoalSummary
from .time_utils import time_string_to_seconds

logger = logging.getLogger(__name__)


class BoxScoreFetcher:
    """Fetches box score data from HockeyTech API"""

    # HockeyTech API base URL
    API_BASE = "https://lscluster.hockeytech.com/feed/"

    # League configurations
    LEAGUE_CONFIGS = {
        'MHL': {
            'client_code': 'mhl',
            'league_id': '1',
            'season_id': None  # Will be determined dynamically
        },
        'BSHL': {
            'client_code': 'bshl',
            'league_id': '1',  # Update with actual BSHL league ID
            'season_id': None
        }
    }

    def __init__(self, cache_dir: Optional[Path] = None, *, api_key: Optional[str] = None):
        """
        Initialize BoxScoreFetcher

        Args:
            cache_dir: Directory for caching box score data
        """
        self.cache_dir = cache_dir
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)

        self.api_key = (api_key or os.environ.get("HOCKEYTECH_API_KEY") or "").strip()

        # Create session with retry logic
        self.session = self._create_session_with_retries()

        # Parser for extracting goals from box scores
        self.parser = BoxScoreParser()

    def _require_api_key(self) -> None:
        if not self.api_key:
            raise RuntimeError("Missing HockeyTech API key. Set HOCKEYTECH_API_KEY in your environment.")

    def _create_session_with_retries(self) -> requests.Session:
        """
        Create a requests session with retry logic

        Returns:
            Configured requests.Session object
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,                          # Total number of retries
            backoff_factor=1,                 # Wait 1s, 2s, 4s between retries
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
            allowed_methods=["HEAD", "GET", "OPTIONS"]   # Only retry safe methods
        )

        # Mount adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        logger.debug("Created HTTP session with retry logic (3 retries, exponential backoff)")

        return session

    def find_game(
        self,
        league: str,
        home_team: str,
        away_team: str,
        game_date: str
    ) -> Optional[str]:
        """
        Find game ID for specified matchup

        Args:
            league: League identifier (MHL, BSHL)
            home_team: Home team name
            away_team: Away team name
            game_date: Game date (YYYY-MM-DD)

        Returns:
            Game ID string or None if not found
        """
        try:
            self._require_api_key()

            # Get league configuration
            config = self.LEAGUE_CONFIGS.get(league.upper())
            if not config:
                logger.warning(f"Unknown league: {league}")
                return None

            # Build schedule API URL
            # Note: You may need to adjust these parameters based on actual HockeyTech API
            params = {
                'feed': 'modulekit',
                'view': 'schedule',
                'key': self.api_key,
                'fmt': 'json',
                'client_code': config['client_code'],
                'league_id': config['league_id'],
            }

            # Add season ID if available
            if config['season_id']:
                params['season_id'] = config['season_id']

            logger.info(f"Searching for game: {home_team} vs {away_team} on {game_date}")

            # Fetch schedule with retry logic
            response = self.session.get(
                f"{self.API_BASE}index.php",
                params=params,
                timeout=(5, 15)  # (connect timeout, read timeout)
            )
            response.raise_for_status()

            schedule_data = response.json()

            # Validate API response structure
            if not isinstance(schedule_data, dict):
                raise ValueError(f"Unexpected API response type: {type(schedule_data).__name__}")

            if 'SiteKit' not in schedule_data:
                raise ValueError(
                    f"Unexpected API response structure - missing 'SiteKit' key. "
                    f"Available keys: {list(schedule_data.keys())}"
                )

            site_kit = schedule_data.get('SiteKit', {})
            if not isinstance(site_kit, dict):
                raise ValueError(f"'SiteKit' is not a dictionary: {type(site_kit).__name__}")

            if 'Schedule' not in site_kit:
                raise ValueError(
                    f"Unexpected API response structure - missing 'Schedule' key in SiteKit. "
                    f"Available keys: {list(site_kit.keys())}"
                )

            # Parse date for comparison
            target_date = datetime.strptime(game_date, '%Y-%m-%d').date()

            # Search for matching game
            games = site_kit.get('Schedule', [])

            for game in games:
                game_date_str = game.get('date_played', '')
                game_home = game.get('home_team', '')
                game_away = game.get('visiting_team', '')

                # Parse game date
                try:
                    game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    continue

                # Check if this is the right game
                if (game_date_obj == target_date and
                    self._team_name_matches(game_home, home_team) and
                    self._team_name_matches(game_away, away_team)):

                    game_id = game.get('id') or game.get('game_id')
                    logger.info(f"Found game ID: {game_id}")
                    return str(game_id)

            logger.warning(f"No game found for {home_team} vs {away_team} on {game_date}")
            return None

        except Exception as e:
            logger.error(f"Failed to find game: {e}")
            return None

    def fetch_box_score(self, league: str, game_id: str) -> Optional[Dict]:
        """
        Fetch box score for specified game

        Args:
            league: League identifier (MHL, BSHL)
            game_id: Game ID

        Returns:
            Box score dictionary or None if failed
        """
        try:
            # Check cache first
            if self.cache_dir:
                cache_file = self.cache_dir / f"{league}_{game_id}_boxscore.json"
                if cache_file.exists():
                    logger.info(f"Loading box score from cache: {cache_file}")
                    with open(cache_file, 'r') as f:
                        return json.load(f)

            # Get league configuration
            config = self.LEAGUE_CONFIGS.get(league.upper())
            if not config:
                logger.warning(f"Unknown league: {league}")
                return None

            self._require_api_key()

            # HockeyTech stats payload: statviewfeed/gameSummary (modulekit tab is not exposed for this league).
            params = {
                'feed': 'statviewfeed',
                'view': 'gameSummary',
                'key': self.api_key,
                'fmt': 'json',
                'client_code': config['client_code'],
                'game_id': game_id,
            }

            logger.info(f"Fetching box score for game {game_id}")

            # Add delay to be polite to API
            time.sleep(0.2)

            # Fetch box score with retry logic
            response = self.session.get(
                f"{self.API_BASE}index.php",
                params=params,
                timeout=(5, 15)  # (connect timeout, read timeout)
            )
            response.raise_for_status()

            # HockeyTech may wrap statviewfeed responses in parentheses.
            body = (response.text or "").strip()
            if body.startswith("(") and body.endswith(")"):
                body = body[1:-1]
            raw = json.loads(body) if body else {}

            box_score = self._convert_statviewfeed_game_summary(raw, league=league, game_id=game_id)

            # Validate API response structure
            if not isinstance(box_score, dict):
                raise ValueError(f"Unexpected API response type: {type(box_score).__name__}")

            if 'SiteKit' not in box_score:
                raise ValueError(
                    f"Unexpected box score response structure - missing 'SiteKit' key. "
                    f"Available keys: {list(box_score.keys())}"
                )

            # Cache the result
            if self.cache_dir:
                with open(cache_file, 'w') as f:
                    json.dump(box_score, f, indent=2)
                logger.debug(f"Cached box score to {cache_file}")

            return box_score

        except Exception as e:
            logger.error(f"Failed to fetch box score: {e}")
            return None

    def _convert_statviewfeed_game_summary(self, raw: Dict[str, Any], *, league: str, game_id: str) -> Dict[str, Any]:
        """
        Convert statviewfeed/gameSummary schema into the minimal SiteKit/Gamesummary
        shape used throughout this codebase.
        """
        details = raw.get("details") if isinstance(raw, dict) else {}
        if not isinstance(details, dict):
            details = {}

        date_iso = str(details.get("GameDateISO8601") or "").strip()
        date_str = date_iso.split("T", 1)[0] if "T" in date_iso else str(details.get("date") or "").strip()

        home_info = {}
        away_info = {}
        home_team = raw.get("homeTeam") if isinstance(raw, dict) else None
        away_team = raw.get("visitingTeam") if isinstance(raw, dict) else None
        if isinstance(home_team, dict):
            home_info = home_team.get("info") if isinstance(home_team.get("info"), dict) else {}
        if isinstance(away_team, dict):
            away_info = away_team.get("info") if isinstance(away_team.get("info"), dict) else {}

        goals: List[Dict[str, Any]] = []
        penalties: List[Dict[str, Any]] = []

        periods = raw.get("periods") if isinstance(raw, dict) else None
        if not isinstance(periods, list):
            periods = []

        def _full_name(obj: Any) -> str:
            if isinstance(obj, dict):
                first = str(obj.get("firstName") or "").strip()
                last = str(obj.get("lastName") or "").strip()
                name = f"{first} {last}".strip()
                if name:
                    return name
                return str(obj.get("name") or "").strip()
            return str(obj or "").strip()

        for period in periods:
            if not isinstance(period, dict):
                continue

            period_info = period.get("info") if isinstance(period.get("info"), dict) else {}
            try:
                period_num = int(period_info.get("id") or 0)
            except Exception:
                period_num = 0

            for g in (period.get("goals") or []):
                if not isinstance(g, dict):
                    continue
                try:
                    g_period = period_num
                    g_period_info = g.get("period") if isinstance(g.get("period"), dict) else {}
                    if not g_period and g_period_info.get("id") is not None:
                        g_period = int(g_period_info.get("id") or 0)

                    time_str = str(g.get("time") or "").strip()
                    team_obj = g.get("team") if isinstance(g.get("team"), dict) else {}
                    team_name = str(team_obj.get("name") or "").strip()

                    scorer = g.get("scoredBy") if isinstance(g.get("scoredBy"), dict) else {}
                    scorer_name = _full_name(scorer) or "Unknown"

                    assists = g.get("assists") if isinstance(g.get("assists"), list) else []
                    assist1 = _full_name(assists[0]) if len(assists) > 0 else ""
                    assist2 = _full_name(assists[1]) if len(assists) > 1 else ""

                    props = g.get("properties") if isinstance(g.get("properties"), dict) else {}
                    is_pp = str(props.get("isPowerPlay") or "0") == "1"
                    is_sh = str(props.get("isShortHanded") or "0") == "1"
                    is_en = str(props.get("isEmptyNet") or "0") == "1"
                    special = "PP" if is_pp else "SH" if is_sh else "EN" if is_en else ""

                    goals.append(
                        {
                            "period": int(g_period or 0),
                            "time": time_str or "00:00",
                            "team": team_name,
                            "goal": {"name": scorer_name},
                            "assist1": {"name": assist1} if assist1 else {},
                            "assist2": {"name": assist2} if assist2 else {},
                            "plus_minus": special,
                        }
                    )
                except Exception:
                    continue

            for p in (period.get("penalties") or []):
                if not isinstance(p, dict):
                    continue
                try:
                    p_period = period_num
                    p_period_info = p.get("period") if isinstance(p.get("period"), dict) else {}
                    if not p_period and p_period_info.get("id") is not None:
                        p_period = int(p_period_info.get("id") or 0)

                    time_str = str(p.get("time") or "").strip()
                    against = p.get("againstTeam")
                    team_name = ""
                    if isinstance(against, dict):
                        team_name = str(against.get("name") or "").strip()
                    else:
                        team_name = str(against or "").strip()

                    taken = p.get("takenBy") if isinstance(p.get("takenBy"), dict) else None
                    served = p.get("servedBy") if isinstance(p.get("servedBy"), dict) else None
                    player_obj = taken or served or {}
                    player_name = _full_name(player_obj) or "Unknown"
                    player_number = player_obj.get("jerseyNumber")
                    try:
                        player_number = int(player_number) if player_number is not None else None
                    except Exception:
                        player_number = None

                    minutes = p.get("minutes")
                    try:
                        minutes = int(minutes) if minutes is not None else 0
                    except Exception:
                        minutes = 0
                    if minutes <= 0:
                        minutes = 2

                    description = str(p.get("description") or "").strip()

                    penalties.append(
                        {
                            "period": int(p_period or 0),
                            "time": time_str or "00:00",
                            "team": team_name,
                            "player": {"name": player_name, "number": player_number},
                            "description": description,
                            "minutes": minutes,
                        }
                    )
                except Exception:
                    continue

        return {
            "SiteKit": {
                "Gamesummary": {
                    "meta": {
                        "game_id": str(details.get("id") or game_id or ""),
                        "date": date_str,
                        "home_team": str(home_info.get("name") or ""),
                        "away_team": str(away_info.get("name") or ""),
                        "league": str(league or ""),
                    },
                    "goals": goals,
                    "penalties": penalties,
                }
            }
        }

    def extract_events(self, box_score: Dict) -> List[Dict]:
        """
        Extract goals and penalties from box score

        Args:
            box_score: Box score dictionary from API

        Returns:
            List of event dictionaries with standardized format
        """
        events = []

        try:
            # Navigate to the game data structure
            site_kit = box_score.get('SiteKit', {})
            if not isinstance(site_kit, dict):
                raise ValueError(f"'SiteKit' is not a dictionary: {type(site_kit).__name__}")

            if 'Gamesummary' not in site_kit:
                logger.warning(
                    f"'Gamesummary' key not found in SiteKit. "
                    f"Available keys: {list(site_kit.keys())}"
                )
                return []

            game_data = site_kit.get('Gamesummary', {})

            # Extract goals
            goals = game_data.get('goals', []) or game_data.get('scoring_plays', [])
            for goal in goals:
                try:
                    special_raw = str(goal.get('plus_minus', '') or goal.get('special', '') or '')
                    special_norm = special_raw.strip().upper()
                    power_play = 'PP' in special_norm or 'POWER PLAY' in special_norm
                    short_handed = 'SH' in special_norm or 'SHORT HANDED' in special_norm
                    empty_net = 'EN' in special_norm or 'EMPTY NET' in special_norm

                    events.append({
                        'type': 'goal',
                        'period': int(goal.get('period', 0)),
                        'time': goal.get('time', '00:00'),
                        'team': goal.get('team', ''),
                        'scorer': goal.get('goal', {}).get('name', '') if isinstance(goal.get('goal'), dict) else goal.get('scorer_name', ''),
                        'assist1': goal.get('assist1', {}).get('name', '') if isinstance(goal.get('assist1'), dict) else '',
                        'assist2': goal.get('assist2', {}).get('name', '') if isinstance(goal.get('assist2'), dict) else '',
                        'special': special_raw,  # PP, SH, EN, etc.
                        'power_play': power_play,
                        'short_handed': short_handed,
                        'empty_net': empty_net,
                        'video_time': None  # Will be filled by event matcher
                    })
                except Exception as e:
                    logger.warning(f"Failed to parse goal: {e}")

            # Extract penalties
            penalties = game_data.get('penalties', []) or game_data.get('penalty_plays', [])
            for penalty in penalties:
                try:
                    player_obj = penalty.get('player') if isinstance(penalty.get('player'), dict) else {}
                    player_name = player_obj.get('name') or penalty.get('player_name', '') or ''
                    player_number = player_obj.get('number')
                    if player_number is None:
                        player_number = penalty.get('player_number')
                    try:
                        player_number = int(player_number) if player_number is not None else None
                    except Exception:
                        player_number = None

                    minutes_val = penalty.get('minutes', 0)
                    try:
                        minutes_val = int(minutes_val) if minutes_val is not None else 0
                    except Exception:
                        minutes_val = 0
                    if minutes_val <= 0:
                        minutes_val = 2

                    events.append({
                        'type': 'penalty',
                        'period': int(penalty.get('period', 0)),
                        'time': penalty.get('time', '00:00'),
                        'team': penalty.get('team', ''),
                        'player': {'name': player_name, 'number': player_number},
                        'infraction': penalty.get('description', '') or penalty.get('infraction', ''),
                        'minutes': minutes_val,
                        'video_time': None
                    })
                except Exception as e:
                    logger.warning(f"Failed to parse penalty: {e}")

            # HockeyTech/box score feeds report *time elapsed* in the period (not time remaining).
            # Sort chronologically within each period (ascending elapsed time).
            events.sort(key=lambda e: (e['period'], self._time_to_seconds(e['time'])))

            logger.info(f"Extracted {len(events)} events from box score")

            # Log event summary
            goal_count = sum(1 for e in events if e['type'] == 'goal')
            penalty_count = sum(1 for e in events if e['type'] == 'penalty')
            logger.info(f"  Goals: {goal_count}, Penalties: {penalty_count}")

            return events

        except Exception as e:
            logger.error(f"Failed to extract events from box score: {e}")
            return []

    def _team_name_matches(self, name1: str, name2: str) -> bool:
        """
        Check if two team names match (fuzzy matching)

        Args:
            name1: First team name
            name2: Second team name

        Returns:
            True if names match
        """
        # Simple fuzzy matching - could be improved
        n1 = name1.lower().strip()
        n2 = name2.lower().strip()

        return n1 in n2 or n2 in n1 or n1 == n2

    def _time_to_seconds(self, time_str: str) -> int:
        """
        Convert MM:SS time string to seconds

        Args:
            time_str: Time string in MM:SS format

        Returns:
            Time in seconds
        """
        return time_string_to_seconds(time_str)

    def get_goals(self, box_score: Dict) -> List[Goal]:
        """
        Extract typed Goal objects from box score.

        This is the preferred method for goal extraction, providing
        type-safe Goal objects with validation.

        Args:
            box_score: Box score dictionary from API

        Returns:
            List of Goal objects
        """
        return self.parser.parse_goals(box_score)

    def get_goal_summary(
        self,
        box_score: Dict,
        home_team: str,
        away_team: str
    ) -> GoalSummary:
        """
        Extract goals with team context as a GoalSummary.

        Args:
            box_score: Box score dictionary from API
            home_team: Home team name
            away_team: Away team name

        Returns:
            GoalSummary with all goals and team information
        """
        return self.parser.parse_goal_summary(box_score, home_team, away_team)

    def get_goals_as_events(self, box_score: Dict) -> List[Dict]:
        """
        Extract goals as event dictionaries for backward compatibility.

        This method returns goals in the same format as extract_events()
        but using the new typed parsing internally.

        Args:
            box_score: Box score dictionary from API

        Returns:
            List of event dictionaries compatible with EventMatcher
        """
        goals = self.get_goals(box_score)
        return self.parser.goals_to_event_dicts(goals)

    def get_cached_box_scores(self) -> List[Path]:
        """
        Get list of cached box score files

        Returns:
            List of cache file paths
        """
        if not self.cache_dir or not self.cache_dir.exists():
            return []

        return list(self.cache_dir.glob("*_boxscore.json"))
