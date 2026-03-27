import json
from pathlib import Path

from highlight_extractor.amherst_integration import AmherstBoxScoreProvider


def test_find_game_falls_back_to_remote_schedule(tmp_path, monkeypatch):
    games_path = tmp_path / "amherst-ramblers.json"
    games_path.write_text(json.dumps({"games": []}), encoding="utf-8")
    provider = AmherstBoxScoreProvider(str(games_path))

    monkeypatch.setattr(
        provider,
        "_fetch_remote_schedule",
        lambda: [
            {
                "game_id": "4948",
                "date_played": "2026-03-26",
                "home_team_name": "Amherst Ramblers",
                "visiting_team_name": "Summerside Western Capitals",
                "visiting_team_code": "SWC",
                "visiting_team": "10",
                "season_id": "44",
                "schedule_notes": "Game # 6, in the Best of 7, EastLink South Semi-Final 1 Series",
                "venue_name": "Amherst Stadium",
                "attendance": "2044",
                "home_goal_count": "3",
                "visiting_goal_count": "9",
                "status": "4",
                "game_status": "Final",
                "GameDateISO8601": "2026-03-26T19:00:00-03:00",
            }
        ],
    )
    monkeypatch.setattr(
        provider._live_fetcher,
        "fetch_box_score",
        lambda league, game_id: {
            "SiteKit": {
                "Gamesummary": {
                    "goals": [
                        {
                            "period": 1,
                            "time": "13:02",
                            "team": "Amherst Ramblers",
                            "goal": {"name": "Christian White"},
                            "assist1": {"name": "Zach Wheeler"},
                            "assist2": {},
                            "plus_minus": "",
                        }
                    ],
                    "penalties": [
                        {
                            "period": 1,
                            "time": "02:13",
                            "team": "Amherst Ramblers",
                            "player": {"name": "Christian White", "number": 4},
                            "description": "Delay Of Game - Minor",
                            "minutes": 2,
                        }
                    ],
                }
            }
        },
    )

    game = provider.find_game(game_date="2026-03-26")

    assert game is not None
    assert game["game_id"] == "4948"
    assert game["venue"] == "Amherst Stadium"
    assert game["attendance"] == 2044
    assert game["result"]["won"] is False
    assert game["scoring"][0]["scorer"]["name"] == "Christian White"
    assert game["penalties"][0]["player"]["name"] == "Christian White"
