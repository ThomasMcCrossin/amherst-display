import json
from pathlib import Path


def test_box_score_extract_events_sets_special_flags():
    from highlight_extractor.box_score import BoxScoreFetcher

    bs = {
        "SiteKit": {
            "Gamesummary": {
                "goals": [
                    {"period": 1, "time": "10:00", "team": "Amherst Ramblers", "goal": {"name": "A"}, "assist1": {}, "assist2": {}, "plus_minus": "PP"},
                    {"period": 1, "time": "05:00", "team": "Opponent", "goal": {"name": "B"}, "assist1": {}, "assist2": {}, "plus_minus": "SH"},
                    {"period": 3, "time": "01:00", "team": "Opponent", "goal": {"name": "C"}, "assist1": {}, "assist2": {}, "plus_minus": "EN"},
                ]
            }
        }
    }

    fetcher = BoxScoreFetcher()
    events = fetcher.extract_events(bs)
    goals = [e for e in events if e.get("type") == "goal"]

    assert len(goals) == 3
    by_scorer = {g["scorer"]: g for g in goals}
    assert by_scorer["A"]["power_play"] is True
    assert by_scorer["A"]["short_handed"] is False
    assert by_scorer["A"]["empty_net"] is False
    assert by_scorer["B"]["short_handed"] is True
    assert by_scorer["C"]["empty_net"] is True


def test_penalty_analyzer_links_pp_goal_to_penalty():
    from highlight_extractor.penalty_analyzer import analyze_game_penalties

    goals = [
        # Amherst-display / HockeyTech times are elapsed in period.
        {"type": "goal", "period": 2, "time": "6:30", "team": "Amherst Ramblers", "power_play": True},
    ]
    penalties = [
        {
            "period": 2,
            "time": "5:00",
            "team": "Truro Bearcats",
            "player": {"name": "John Smith", "number": None},
            "infraction": "Hooking - Minor",
            "minutes": 2,
        }
    ]

    result = analyze_game_penalties(goals, penalties, our_team="ramblers")
    assert 0 in result["pp_penalty_map"]
    p = result["pp_penalty_map"][0]
    assert p.team == "opponent"
    assert p.minutes == 2


def test_penalty_analyzer_ignores_misconduct_for_pp_linking():
    from highlight_extractor.penalty_analyzer import analyze_game_penalties

    goals = [
        {"type": "goal", "period": 1, "time": "10:00", "team": "Amherst Ramblers", "power_play": True},
    ]
    penalties = [
        {
            "period": 1,
            "time": "9:00",
            "team": "Opponent",
            "player": {"name": "Christian White", "number": 12},
            "infraction": "Misconduct",
            "minutes": 10,
        }
    ]

    result = analyze_game_penalties(goals, penalties, our_team="ramblers")
    assert result["pp_penalty_map"] == {}


def test_penalty_analyzer_prefers_minor_over_misconduct():
    from highlight_extractor.penalty_analyzer import analyze_game_penalties

    goals = [
        {"type": "goal", "period": 1, "time": "10:00", "team": "Amherst Ramblers", "power_play": True},
    ]
    penalties = [
        {
            "period": 1,
            "time": "9:00",
            "team": "Opponent",
            "player": {"name": "Christian White", "number": 12},
            "infraction": "Misconduct",
            "minutes": 10,
        },
        {
            "period": 1,
            "time": "9:30",
            "team": "Opponent",
            "player": {"name": "John Smith", "number": None},
            "infraction": "Hooking - Minor",
            "minutes": 2,
        },
    ]

    result = analyze_game_penalties(goals, penalties, our_team="ramblers")
    assert 0 in result["pp_penalty_map"]
    assert result["pp_penalty_map"][0].minutes == 2


def test_amherst_provider_emits_penalties_in_box_score(tmp_path: Path):
    from highlight_extractor.amherst_integration import AmherstBoxScoreProvider

    games_path = tmp_path / "amherst-ramblers.json"
    games_path.write_text(
        json.dumps(
            {
                "games": [
                    {
                        "game_id": "4820",
                        "date": "2026-01-10",
                        "home_game": True,
                        "opponent": {"team_name": "Miramichi Timberwolves"},
                        "result": {"won": True, "final_score": "4-2"},
                        "penalties": [
                            {
                                "period": 1,
                                "time": "5:00",
                                "team": "ramblers",
                                "player": {"name": "Test Player", "number": 9},
                                "infraction": "Hooking - Minor",
                                "minutes": 2,
                            }
                        ],
                        "box_score": {
                            "penalties": [
                                {
                                    "period": 2,
                                    "time": "10:00",
                                    "team": "opponent",
                                    "player": {"name": "Other Player", "number": 12},
                                    "infraction": "Tripping - Minor",
                                    "minutes": 2,
                                }
                            ]
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    provider = AmherstBoxScoreProvider(str(games_path))

    game = provider.find_game(game_date="2026-01-10", game_id="4820")
    assert game is not None
    top_level = game.get("penalties") or []
    nested = (game.get("box_score") or {}).get("penalties") or []
    assert len(top_level) > 0 or len(nested) > 0

    box_score = provider.get_box_score_for_game(game)
    penalties = box_score.get("SiteKit", {}).get("Gamesummary", {}).get("penalties") or []
    assert len(penalties) > 0
