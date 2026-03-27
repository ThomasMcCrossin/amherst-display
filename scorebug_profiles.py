"""
Scorebug profile catalog.

This organizes known broadcast layouts without replacing the current OCR engine.
Resolved profiles feed the existing execution-profile / broadcast-type pipeline.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, Optional


def _norm(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("-", " ").split())


@dataclass(frozen=True)
class ScorebugProfile:
    profile_id: str
    description: str
    execution_profile_name: str
    broadcast_type: str
    roi_method: str
    preprocess_family: str
    known_layout: bool = True
    league: str = ""
    home_team: str = ""
    away_team: str = ""
    team: str = ""
    filename_hint: str = ""
    notes: str = ""

    def match_score(self, context: Dict[str, str]) -> int:
        score = 0
        if self.league:
            if _norm(context.get("league")) != _norm(self.league):
                return -1
            score += 30
        if self.home_team:
            if _norm(context.get("home_team")) != _norm(self.home_team):
                return -1
            score += 60
        if self.away_team:
            if _norm(context.get("away_team")) != _norm(self.away_team):
                return -1
            score += 40
        if self.team:
            teams = {_norm(context.get("home_team")), _norm(context.get("away_team"))}
            if _norm(self.team) not in teams:
                return -1
            score += 20
        if self.filename_hint:
            if _norm(self.filename_hint) not in _norm(context.get("filename")):
                return -1
            score += 10
        if self.known_layout:
            score += 1
        return score

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_scorebug_context(
    *,
    game_info: Optional[Dict[str, Any]] = None,
    source_game_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    game_info = game_info or {}
    source_game_info = source_game_info or {}
    return {
        "league": str(game_info.get("league") or source_game_info.get("league") or "").strip(),
        "home_team": str(game_info.get("home_team") or source_game_info.get("home_team") or "").strip(),
        "away_team": str(game_info.get("away_team") or source_game_info.get("away_team") or "").strip(),
        "home_away": str(game_info.get("home_away") or source_game_info.get("home_away") or "").strip(),
        "filename": str(game_info.get("filename") or source_game_info.get("filename") or "").strip(),
    }


SCOREBUG_PROFILES: tuple[ScorebugProfile, ...] = (
    ScorebugProfile(
        profile_id="mhl_summerside_home_banner",
        description="MHL Summerside home center-banner scorebug",
        execution_profile_name="mhl_summerside_recording",
        broadcast_type="mhl_summerside",
        roi_method="mhl_summerside",
        preprocess_family="flohockey",
        league="MHL",
        home_team="Summerside Western Capitals",
        notes="Centered black banner with a tighter right-side period/clock block.",
    ),
    ScorebugProfile(
        profile_id="mhl_amherst_home_strip",
        description="MHL Amherst home Flo strip scorebug",
        execution_profile_name="mhl_amherst_recording",
        broadcast_type="mhl_amherst",
        roi_method="mhl_amherst",
        preprocess_family="flohockey",
        league="MHL",
        home_team="Amherst Ramblers",
        notes="Wide white Flo strip used for Amherst home broadcasts in this series.",
    ),
    ScorebugProfile(
        profile_id="mhl_yarmouth_home",
        description="MHL Yarmouth home broadcast scorebug",
        execution_profile_name="yarmouth_recording",
        broadcast_type="yarmouth",
        roi_method="yarmouth",
        preprocess_family="yarmouth",
        league="MHL",
        home_team="Yarmouth Mariners",
        notes="Seeded from prior Amherst/Yarmouth tests. Expand with more fixtures later.",
    ),
    ScorebugProfile(
        profile_id="mhl_flohockey_default",
        description="Default FloHockey style scorebug for known MHL games",
        execution_profile_name="flohockey_recording",
        broadcast_type="flohockey",
        roi_method="flohockey",
        preprocess_family="flohockey",
        league="MHL",
        notes="Default MHL fallback while seeded non-standard layouts are limited.",
    ),
    ScorebugProfile(
        profile_id="generic_standard_fallback",
        description="Generic fallback for unknown layouts",
        execution_profile_name="generic_recording",
        broadcast_type="standard",
        roi_method="auto",
        preprocess_family="standard",
        known_layout=False,
        notes="Falls back to auto-probe and generic OCR settings.",
    ),
)


def iter_matching_scorebug_profiles(context: Dict[str, str]) -> Iterable[ScorebugProfile]:
    ranked = []
    for profile in SCOREBUG_PROFILES:
        score = profile.match_score(context)
        if score >= 0:
            ranked.append((score, profile))
    ranked.sort(key=lambda item: item[0], reverse=True)
    for _score, profile in ranked:
        yield profile


def resolve_scorebug_profile(
    *,
    game_info: Optional[Dict[str, Any]] = None,
    source_game_info: Optional[Dict[str, Any]] = None,
) -> tuple[ScorebugProfile, Dict[str, str]]:
    context = build_scorebug_context(game_info=game_info, source_game_info=source_game_info)
    profile = next(iter_matching_scorebug_profiles(context), SCOREBUG_PROFILES[-1])
    return profile, context
