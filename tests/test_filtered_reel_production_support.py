import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FILTERED_REEL_PATH = REPO_ROOT / "scripts" / "build_filtered_reel.py"
PRODUCTION_REEL_PATH = REPO_ROOT / "scripts" / "build_production_highlight_reel.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_build_goal_score_lookup_and_annotation():
    module = _load_module(FILTERED_REEL_PATH, "build_filtered_reel_test")

    canonical_game_info = {
        "home_team": "Summerside Western Capitals",
        "away_team": "Amherst Ramblers",
    }
    game = {
        "scoring": [
            {
                "period": 1,
                "time": "2:00",
                "team": "opponent",
                "scorer": {"name": "Capital One"},
                "assists": [],
            },
            {
                "period": 1,
                "time": "5:00",
                "team": "amherst-ramblers",
                "scorer": {"name": "Rambler One"},
                "assists": [{"name": "Anthony Gaudet"}],
                "power_play": True,
            },
            {
                "period": 2,
                "time": "1:30",
                "team": "amherst-ramblers",
                "scorer": {"name": "Rambler Two"},
                "assists": [],
            },
        ]
    }

    lookup = module.build_goal_score_lookup(game, canonical_game_info)
    annotated = module.annotate_clip_entry(
        entry={
            "type": "goal",
            "period": 1,
            "time": "5:00",
            "team": "Amherst Ramblers",
            "scorer": "Rambler One",
            "assist1": "",
            "assist2": "",
        },
        score_lookup=lookup,
        series_title="EastLink South Semi-Final 1 vs Summerside Western Capitals",
        game_label="Game 1",
    )

    assert annotated["home_score"] == 1
    assert annotated["away_score"] == 1
    assert annotated["assist1"] == "Anthony Gaudet"
    assert annotated["power_play"] is True
    assert annotated["overlay_game_label"] == "Game 1"
    assert annotated["overlay_series_title"] == "EastLink South Semi-Final 1 vs Summerside Western Capitals"


def test_build_series_context():
    module = _load_module(FILTERED_REEL_PATH, "build_filtered_reel_series_context_test")

    context = module.build_series_context(
        source_index=1,
        game_date="2026-03-14",
        game={
            "opponent": {"team_name": "Summerside Western Capitals"},
            "venue": "Credit Union Place",
            "attendance": 2028,
            "schedule_notes": "Game # 1, EastLink South Semi-Final 1",
            "result": {"won": True, "final_score": "2-1"},
        },
        canonical_game_info={
            "home_team": "Summerside Western Capitals",
            "away_team": "Amherst Ramblers",
        },
        series_title="EastLink South Semi-Final 1 vs Summerside Western Capitals",
        game_label="Game 1",
        amherst_wins_before=0,
        opponent_wins_before=0,
    )

    assert context["series_record_before"] == "0-0"
    assert context["series_status"] == "Series tied 0-0"
    assert context["venue"] == "Credit Union Place"
    assert context["attendance"] == 2028
    assert context["game_date_display"] == "March 14, 2026"


def test_load_reel_manifest_items(tmp_path):
    module = _load_module(PRODUCTION_REEL_PATH, "build_production_reel_test")

    clip_path = tmp_path / "clip.mp4"
    clip_path.write_bytes(b"fake")
    manifest_path = tmp_path / "series.json"
    manifest_path.write_text(
        """
{
  "title": "EastLink South Semi-Final 1 vs Summerside Western Capitals",
  "clips": [
    {
      "index": 2,
      "clip_path": "%s",
      "game_label": "Game 3",
      "series_title": "EastLink South Semi-Final 1 vs Summerside Western Capitals",
      "series_context": {
        "game_date": "2026-03-19",
        "game_date_display": "March 19, 2026",
        "series_record_before": "1-1",
        "series_status": "Series tied 1-1",
        "venue": "Amherst Stadium",
        "attendance": 1500
      },
      "game_info": {
        "league": "MHL",
        "home_team": "Summerside Western Capitals",
        "away_team": "Amherst Ramblers"
      },
      "event": {
        "type": "goal",
        "period": 2,
        "time": "8:32",
        "team": "Amherst Ramblers",
        "scorer": "Mark Corbett",
        "home_score": 2,
        "away_score": 1
      }
    }
  ]
}
"""
        % str(clip_path),
        encoding="utf-8",
    )

    items = module._load_reel_manifest_items(manifest_path)
    assert len(items) == 1
    assert items[0].clip_path == clip_path
    assert items[0].overlay_game_label == "Game 3"
    assert items[0].overlay_series_title == "EastLink South Semi-Final 1 vs Summerside Western Capitals"
    assert items[0].series_context["series_status"] == "Series tied 1-1"
    assert items[0].event["home_score"] == 2


def test_insert_series_outro_card_appends_segment(tmp_path, monkeypatch):
    module = _load_module(PRODUCTION_REEL_PATH, "build_production_reel_outro_test")

    clip_path = tmp_path / "clip.mp4"
    clip_path.write_bytes(b"fake")
    items = [
        module.ClipItem(
            index=1,
            clip_path=clip_path,
            event={"type": "goal"},
            game_info={
                "league": "MHL",
                "home_team": "Amherst Ramblers",
                "away_team": "Summerside Western Capitals",
            },
            overlay_series_title="EastLink South Semi-Final 1 vs Summerside Western Capitals",
        )
    ]

    class DummyTeam:
        def __init__(self, name: str):
            self.name = name
            self.logo_path = REPO_ROOT / "assets" / "logos" / "fallback.png"

    monkeypatch.setattr(module, "_find_team_info", lambda name, league, db: DummyTeam(name))
    monkeypatch.setattr(module, "_render_series_outro_card_png", lambda *args, **kwargs: None)

    def _fake_render_static(image_path, out_path, *, duration_seconds, fps):
        out_path.write_bytes(b"fake")

    monkeypatch.setattr(module, "_render_static_card_video", _fake_render_static)

    rendered = module._insert_series_outro_card(
        items,
        output_dir=tmp_path,
        teams_db={},
        video_size=(1280, 720),
        fps="30",
        duration_seconds=4.5,
        series_status="Series tied 3-3",
        next_game_label="Game 7",
        datetime_label="Saturday, March 28, 2026 • 8:00 pm ADT",
        venue="Credit Union Place",
        location="511 Notre Dame St, Summerside, PE C1N 1T2",
        home_team_name="Summerside Western Capitals",
        away_team_name="Amherst Ramblers",
    )

    assert len(rendered) == 2
    assert rendered[-1].segment_kind == "series_outro"
    assert rendered[-1].clip_path.name == "series_outro.mp4"
