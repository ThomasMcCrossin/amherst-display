import config
from scorebug_profiles import resolve_scorebug_profile


def test_scorebug_profile_resolves_yarmouth_home_variant():
    profile, context = resolve_scorebug_profile(
        game_info={
            "league": "MHL",
            "home_team": "Yarmouth Mariners",
            "away_team": "Amherst Ramblers",
            "filename": "Replay - Amherst at Yarmouth.ts",
        }
    )

    assert profile.profile_id == "mhl_yarmouth_home"
    assert profile.execution_profile_name == "yarmouth_recording"
    assert context["home_team"] == "Yarmouth Mariners"


def test_scorebug_profile_resolves_amherst_home_variant():
    profile, _context = resolve_scorebug_profile(
        game_info={
            "league": "MHL",
            "home_team": "Amherst Ramblers",
            "away_team": "Summerside Western Capitals",
            "filename": "Replay - Summerside at Amherst.ts",
        }
    )

    assert profile.profile_id == "mhl_flo_strip"
    assert profile.execution_profile_name == "flo_strip_recording"


def test_scorebug_profile_resolves_summerside_home_variant():
    profile, _context = resolve_scorebug_profile(
        game_info={
            "league": "MHL",
            "home_team": "Summerside Western Capitals",
            "away_team": "Amherst Ramblers",
            "filename": "Replay - Amherst at Summerside.ts",
        }
    )

    assert profile.profile_id == "mhl_summerside_home_banner"
    assert profile.execution_profile_name == "mhl_summerside_recording"


def test_scorebug_profile_resolves_default_mhl_flo_strip_for_unknown_team():
    profile, _context = resolve_scorebug_profile(
        game_info={
            "league": "MHL",
            "home_team": "Pictou County Crushers",
            "away_team": "Truro Bearcats",
            "filename": "Replay - Truro at Pictou.ts",
        }
    )

    assert profile.profile_id == "mhl_flo_strip"
    assert profile.execution_profile_name == "flo_strip_recording"


def test_scorebug_profile_resolves_west_kent_stacked_box():
    profile, _context = resolve_scorebug_profile(
        game_info={
            "league": "MHL",
            "home_team": "West Kent Steamers",
            "away_team": "Amherst Ramblers",
        }
    )

    assert profile.profile_id == "mhl_flo_stacked_topleft"
    assert profile.execution_profile_name == "flo_stacked_recording"


def test_config_execution_selection_uses_scorebug_catalog():
    selection = config.resolve_highlight_execution_selection(
        "auto",
        game_info={
            "league": "MHL",
            "home_team": "Yarmouth Mariners",
            "away_team": "Amherst Ramblers",
            "filename": "Replay - Amherst at Yarmouth.ts",
        },
    )

    assert selection["execution_profile_name"] == "yarmouth_recording"
    assert selection["scorebug_profile"]["profile_id"] == "mhl_yarmouth_home"
    assert selection["execution_profile"]["broadcast_type"] == "yarmouth"


def test_config_execution_selection_uses_flo_strip_for_amherst_home():
    selection = config.resolve_highlight_execution_selection(
        "auto",
        game_info={
            "league": "MHL",
            "home_team": "Amherst Ramblers",
            "away_team": "Summerside Western Capitals",
            "filename": "Replay - Summerside at Amherst.ts",
        },
    )

    assert selection["execution_profile_name"] == "flo_strip_recording"
    assert selection["scorebug_profile"]["profile_id"] == "mhl_flo_strip"
    assert selection["execution_profile"]["broadcast_type"] == "flo_strip"
