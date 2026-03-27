import json

from highlight_extractor.amherst_integration import find_amherst_display_path


def test_find_amherst_display_path_prefers_env_override(tmp_path, monkeypatch):
    env_repo = tmp_path / "portable-amherst-display"
    games_dir = env_repo / "games"
    games_dir.mkdir(parents=True)
    (games_dir / "amherst-ramblers.json").write_text(json.dumps({"games": []}), encoding="utf-8")

    monkeypatch.setenv("AMHERST_DISPLAY_DIR", str(env_repo))

    assert find_amherst_display_path() == env_repo.resolve()
