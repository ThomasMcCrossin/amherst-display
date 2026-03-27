from drive_config import build_program_drive_layout, resolve_drive_config


def test_resolve_drive_config_prefers_generic_env_names():
    env = {
        "HIGHLIGHTS_DRIVE_ID": "drive-generic",
        "RAMBLERS_DRIVE_ID": "drive-legacy",
        "HIGHLIGHTS_GAMES_FOLDER_ID": "games-generic",
        "DRIVE_GAMES_FOLDER_ID": "games-legacy",
        "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/creds.json",
    }

    resolved = resolve_drive_config(env)

    assert resolved.drive_id == "drive-generic"
    assert resolved.games_folder_id == "games-generic"
    assert resolved.credentials_path == "/tmp/creds.json"


def test_resolve_drive_config_falls_back_to_legacy_aliases():
    env = {
        "RAMBLERS_DRIVE_ID": "drive-legacy",
        "DRIVE_INGEST_FOLDER_ID": "ingest-legacy",
        "DRIVE_HIGHLIGHTS_FOLDER_PATH": "Programs/MHL/Amherst Ramblers/2025-26/03_Reels/Games",
    }

    resolved = resolve_drive_config(env)

    assert resolved.drive_id == "drive-legacy"
    assert resolved.ingest_folder_id == "ingest-legacy"
    assert resolved.reels_folder_path.endswith("/03_Reels/Games")


def test_build_program_drive_layout_matches_canonical_tree():
    layout = build_program_drive_layout(
        league="MHL",
        team="Amherst Ramblers",
        season="2025-26",
    )

    assert layout.program_root_path == "Programs/MHL/Amherst Ramblers/2025-26"
    assert layout.ingest_inbox_path.endswith("/01_Ingest/Inbox")
    assert layout.games_root_path.endswith("/02_Games")
    assert layout.major_review_incoming_path.endswith("/04_Review/Major Penalties/Incoming")
