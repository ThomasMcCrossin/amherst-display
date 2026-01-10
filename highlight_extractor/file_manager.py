"""
File Manager - Handles folder organization, filename parsing, and file operations
"""

import re
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class FileManager:
    """Manages file operations, folder structure, and game metadata parsing"""

    def __init__(self, config):
        """
        Initialize FileManager with configuration

        Args:
            config: Configuration module with paths and settings
        """
        self.config = config
        self.teams_data = self._load_teams_data()

    def _load_teams_data(self) -> Dict:
        """Load teams.json data"""
        try:
            teams_file = self.config.TEAMS_FILE
            if teams_file.exists():
                with open(teams_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.warning(f"Teams file not found: {teams_file}")
                return {"teams": [], "league_meta": {}}
        except Exception as e:
            logger.error(f"Failed to load teams data: {e}")
            return {"teams": [], "league_meta": {}}

    def parse_mhl_filename(self, filename: str) -> Optional[Dict]:
        """
        Parse MHL-formatted filename

        Expected format: YYYY-MM-DD HomeTeam vs AwayTeam Home/Away HH.MMam/pm.ext
        Example: 2025-01-15 Amherst Ramblers vs Truro Bearcats Home 7.00pm.ts

        Args:
            filename: Video filename to parse

        Returns:
            Dictionary with game info or None if parsing fails
        """
        # Pattern for MHL format
        pattern = r'^(\d{4}-\d{2}-\d{2})\s+(.+?)\s+vs\s+(.+?)\s+(Home|Away)\s+(\d{1,2}\.\d{2}(?:am|pm))'

        match = re.match(pattern, filename, re.IGNORECASE)
        if not match:
            return None

        date_str, home_team, away_team, perspective, time_str = match.groups()

        # Parse date
        try:
            game_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            logger.warning(f"Invalid date format in filename: {date_str}")
            return None

        # Determine league by looking up teams
        league = self._determine_league(home_team, away_team)

        return {
            'date': game_date.strftime('%Y-%m-%d'),
            'date_formatted': game_date.strftime('%B %d, %Y'),
            'home_team': home_team.strip(),
            'away_team': away_team.strip(),
            'home_away': perspective.lower(),
            'time': time_str,
            'league': league,
            'filename': filename
        }

    def parse_generic_hockey_filename(self, filename: str) -> Dict:
        """
        Fallback parser for non-standard filenames

        Args:
            filename: Video filename

        Returns:
            Generic game info dictionary
        """
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'date_formatted': datetime.now().strftime('%B %d, %Y'),
            'home_team': 'Team1',
            'away_team': 'Team2',
            'home_away': 'unknown',
            'time': 'unknown',
            'league': 'Unknown',
            'filename': filename
        }

    def _determine_league(self, home_team: str, away_team: str) -> str:
        """
        Determine league by matching team names against teams.json

        Args:
            home_team: Home team name
            away_team: Away team name

        Returns:
            League identifier (MHL, BSHL, or Unknown)
        """
        teams = self.teams_data.get('teams', [])

        for team in teams:
            # Check if either team matches this entry
            team_name = team.get('name', '')
            aliases = team.get('aliases', [])

            # Check for match
            if (self._team_matches(home_team, team_name, aliases) or
                self._team_matches(away_team, team_name, aliases)):
                return team.get('league', 'Unknown')

        return 'Unknown'

    def _team_matches(self, input_name: str, team_name: str, aliases: List[str]) -> bool:
        """
        Check if input name matches team name or aliases

        Args:
            input_name: Team name from filename
            team_name: Official team name
            aliases: List of team aliases

        Returns:
            True if match found
        """
        input_lower = input_name.lower().strip()

        if input_lower == team_name.lower():
            return True

        for alias in aliases:
            if input_lower == alias.lower() or alias.lower() in input_lower:
                return True

        return False

    def create_game_folder(self, game_info: Dict) -> Dict[str, Path]:
        """
        Create organized folder structure for game

        Args:
            game_info: Dictionary with game metadata

        Returns:
            Dictionary with all folder paths
        """
        # Create folder name: YYYY-MM-DD_HomeTeam_vs_AwayTeam
        folder_name = f"{game_info['date']}_{game_info['home_team']}_vs_{game_info['away_team']}"
        folder_name = self._sanitize_folder_name(folder_name)

        # Base game directory
        game_dir = self.config.GAMES_DIR / folder_name

        # Create subdirectories
        folders = {
            'game_dir': game_dir,
            'output_dir': game_dir / 'output',
            'clips_dir': game_dir / 'clips',
            'source_dir': game_dir / 'source',
            'logs_dir': game_dir / 'logs',
            'data_dir': game_dir / 'data',
            'folder_name': folder_name
        }

        # Create all directories
        for key, path in folders.items():
            if key != 'folder_name' and isinstance(path, Path):
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.debug(f"Created directory: {path}")
                except Exception as e:
                    logger.error(f"Failed to create directory {path}: {e}")

        return folders

    def _sanitize_folder_name(self, name: str) -> str:
        """
        Sanitize folder name by removing invalid characters

        Args:
            name: Folder name to sanitize

        Returns:
            Sanitized folder name
        """
        # Replace invalid characters with underscores
        invalid_chars = r'[<>:"/\\|?*]'
        sanitized = re.sub(invalid_chars, '_', name)

        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        return sanitized

    def find_video_files(self, locations: Optional[List[Path]] = None) -> List[Path]:
        """
        Search for video files in specified locations

        Args:
            locations: List of directories to search (uses config defaults if None)

        Returns:
            List of video file paths
        """
        if locations is None:
            locations = [
                self.config.LOCAL_REPO_DIR,
                Path.home() / "Downloads",
                Path.home() / "Desktop",
            ]

            # Add Google Drive input if available
            if hasattr(self.config, 'GOOGLE_INPUT_DIR') and self.config.GOOGLE_INPUT_DIR:
                if self.config.GOOGLE_INPUT_DIR.exists():
                    locations.insert(1, self.config.GOOGLE_INPUT_DIR)

        video_files = []
        supported_formats = self.config.SUPPORTED_FORMATS

        for location in locations:
            if not location.exists():
                continue

            try:
                for ext in supported_formats:
                    video_files.extend(location.glob(f"*{ext}"))
            except Exception as e:
                logger.warning(f"Error searching {location}: {e}")

        return sorted(video_files, key=lambda p: p.stat().st_mtime, reverse=True)

    def save_game_metadata(self, game_folders: Dict, game_info: Dict, box_score: Optional[Dict] = None):
        """
        Save game metadata to JSON file

        Args:
            game_folders: Dictionary with folder paths
            game_info: Game information dictionary
            box_score: Optional box score data
        """
        metadata = {
            'game_info': game_info,
            'box_score': box_score,
            'processed_at': datetime.now().isoformat(),
            'version': '2.0.0'
        }

        metadata_file = game_folders['data_dir'] / 'game_metadata.json'

        try:
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, indent=2, fp=f)
            logger.info(f"Saved game metadata to {metadata_file}")
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")

    def save_events(self, game_folders: Dict, events: List[Dict]):
        """
        Save matched events to JSON file

        Args:
            game_folders: Dictionary with folder paths
            events: List of matched event dictionaries
        """
        events_file = game_folders['data_dir'] / 'matched_events.json'

        try:
            with open(events_file, 'w', encoding='utf-8') as f:
                json.dump(events, indent=2, fp=f)
            logger.info(f"Saved {len(events)} matched events to {events_file}")
        except Exception as e:
            logger.error(f"Failed to save events: {e}")
