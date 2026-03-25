from highlight_extractor.penalty_analyzer import parse_penalties, find_major_penalties


def test_find_major_penalties_only_five_minutes() -> None:
    penalties_data = [
        {
            "period": 1,
            "time": "10:00",
            "team": "ramblers",
            "player": {"name": "Cooper Cormier", "number": 7},
            "infraction": "Fighting",
            "minutes": 5,
        },
        {
            "period": 1,
            "time": "12:00",
            "team": "ramblers",
            "player": {"name": "Christian White", "number": 12},
            "infraction": "Misconduct",
            "minutes": 10,
        },
    ]

    penalties = parse_penalties(penalties_data, time_is_elapsed=True)
    majors = find_major_penalties(penalties)

    assert len(majors) == 1
    assert majors[0].player_name == "Cooper Cormier"
    assert majors[0].minutes == 5
