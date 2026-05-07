"""Tests for ``parse_disc_title`` and ``final_kind_from_signals``."""

from __future__ import annotations

import pytest

from app.classify import (
    ParsedDiscTitle,
    final_kind_from_signals,
    parse_disc_title,
)


@pytest.mark.parametrize(
    "label,expected",
    [
        # Combined season/disc, all the common spellings.
        (
            "VAMPIRE_DIARIES_S2D1",
            ParsedDiscTitle(name="Vampire Diaries", kind="tv", season=2, disc=1, year=None, confidence=0.95),
        ),
        (
            "VAMPIRE_DIARIES_SEASON2_DISC2",
            ParsedDiscTitle(name="Vampire Diaries", kind="tv", season=2, disc=2, year=None, confidence=0.95),
        ),
        (
            "THE_VAMPIRE_DIARIES_S1_D2",
            ParsedDiscTitle(name="The Vampire Diaries", kind="tv", season=1, disc=2, year=None, confidence=0.95),
        ),
        (
            "BREAKING_BAD_S01D02",
            ParsedDiscTitle(name="Breaking Bad", kind="tv", season=1, disc=2, year=None, confidence=0.95),
        ),
        (
            "BREAKING_BAD_SEASON_1_DISC_2",
            ParsedDiscTitle(name="Breaking Bad", kind="tv", season=1, disc=2, year=None, confidence=0.95),
        ),
        (
            "STAR_TREK_TNG_S01_D03",
            ParsedDiscTitle(name="Star Trek Tng", kind="tv", season=1, disc=3, year=None, confidence=0.95),
        ),
        (
            "LOST_S5D1",
            ParsedDiscTitle(name="Lost", kind="tv", season=5, disc=1, year=None, confidence=0.95),
        ),
        # Season-only.
        (
            "PLANET_EARTH_SEASON_2",
            ParsedDiscTitle(name="Planet Earth", kind="tv", season=2, disc=None, year=None, confidence=0.8),
        ),
        # Disc-only (kind ambiguous; kept None so durations-fallback decides).
        (
            "INCEPTION_DISC_2",
            ParsedDiscTitle(name="Inception", kind=None, season=None, disc=2, year=None, confidence=0.8),
        ),
        # Movie with year, no season/disc -> kind=movie.
        (
            "THE_DARK_KNIGHT_2008",
            ParsedDiscTitle(name="The Dark Knight", kind="movie", season=None, disc=None, year=2008, confidence=0.6),
        ),
        # Numeric sequel suffix that is part of the title (NOT a disc number).
        (
            "DESPICABLE_ME_3",
            ParsedDiscTitle(name="Despicable Me 3", kind=None, season=None, disc=None, year=None, confidence=0.3),
        ),
        # Bare title - low confidence, kind unknown.
        (
            "INCEPTION",
            ParsedDiscTitle(name="Inception", kind=None, season=None, disc=None, year=None, confidence=0.3),
        ),
        # Noise tokens like NTSC / WS get stripped.
        (
            "PLANET_EARTH_SEASON_2_NTSC",
            ParsedDiscTitle(name="Planet Earth", kind="tv", season=2, disc=None, year=None, confidence=0.8),
        ),
        # Mixed case and dotted separators are normalized.
        (
            "the.office.s03.d01",
            ParsedDiscTitle(name="The Office", kind="tv", season=3, disc=1, year=None, confidence=0.95),
        ),
        # Two-digit season values pass through.
        (
            "FRIENDS_S10D01",
            ParsedDiscTitle(name="Friends", kind="tv", season=10, disc=1, year=None, confidence=0.95),
        ),
    ],
)
def test_parse_disc_title_examples(label, expected):
    got = parse_disc_title(label)
    assert got == expected


@pytest.mark.parametrize(
    "label",
    [
        None,
        "",
        "   ",
        "DVD_VIDEO",
        "LOGICAL_VOLUME_ID",
        "MOVIE_DVD",
        "_____",  # collapses to empty
    ],
)
def test_parse_disc_title_returns_none_for_empty_or_generic(label):
    assert parse_disc_title(label) is None


def test_final_kind_uses_parsed_when_set():
    parsed_tv = ParsedDiscTitle(name="X", kind="tv", season=1, disc=None, year=None, confidence=0.8)
    parsed_mv = ParsedDiscTitle(name="X", kind="movie", season=None, disc=None, year=2010, confidence=0.6)
    parsed_unk = ParsedDiscTitle(name="X", kind=None, season=None, disc=None, year=None, confidence=0.3)

    assert final_kind_from_signals(parsed_tv, "movie") == "tv"
    assert final_kind_from_signals(parsed_mv, "tv") == "movie"
    # Falls back to durations when parser can't tell.
    assert final_kind_from_signals(parsed_unk, "tv") == "tv"
    assert final_kind_from_signals(parsed_unk, "movie") == "movie"
    assert final_kind_from_signals(None, "movie") == "movie"
