"""Heuristic guess: is this disc more likely a movie or a TV season?

Two complementary signals:

- ``guess_movie_or_tv`` looks at title count + durations from MakeMKV. Robust
  when a disc has a generic / blank volume label.
- ``parse_disc_title`` reads the actual DVD volume name (e.g.
  ``VAMPIRE_DIARIES_SEASON2_DISC2``) and pulls out show name, season number,
  disc number, and year. Far more specific when present.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal


def guess_movie_or_tv(titles: list[dict[str, Any]]) -> str:
    """Return ``"movie"`` or ``"tv"`` for UI pre-selection."""
    if not titles:
        return "tv"
    durations = [int(t.get("duration_seconds") or 0) for t in titles]
    n = len(titles)
    if n == 1 and durations[0] >= 55 * 60:
        return "movie"
    if n >= 4:
        return "tv"
    non_zero = [d for d in durations if d > 0]
    if len(non_zero) >= 2:
        avg = sum(non_zero) / len(non_zero)
        if avg > 0 and all(abs(d - avg) < 3 * 60 for d in non_zero):
            return "tv"
    if n == 1:
        return "movie"
    return "tv"


@dataclass(frozen=True)
class ParsedDiscTitle:
    """Structured guess derived from a DVD's volume label."""

    name: str
    kind: Literal["tv", "movie"] | None
    season: int | None
    disc: int | None
    year: int | None
    confidence: float


_PAT_SEASON_DISC_LOOSE = re.compile(
    r"(?i)\bs(?:eason)?[\s_]*0*(\d{1,2})[\s_]+d(?:isc)?[\s_]*0*(\d{1,2})\b"
)
_PAT_SEASON_DISC_TIGHT = re.compile(r"(?i)\bs0*(\d{1,2})[\s_]*d0*(\d{1,2})\b")
_PAT_SEASON = re.compile(r"(?i)\bs(?:eason)?[\s_]*0*(\d{1,2})\b")
_PAT_DISC = re.compile(r"(?i)\bd(?:isc)?[\s_]*0*(\d{1,2})\b")
_PAT_YEAR = re.compile(r"\b(19[0-9]{2}|20[0-9]{2})\b")
_PAT_NOISE = re.compile(
    r"(?i)\b(ntsc|pal|widescreen|fullscreen|fs|ws|region[\s_]*[12345]|"
    r"dvd|bluray|blu[\s_]?ray|hd|sd|1080p|720p|480p|4k|uhd|"
    r"special[\s_]?features|special[\s_]?edition|side[\s_]?[ab]|"
    r"directors[\s_]?cut|extended|theatrical|unrated)\b"
)
# Common token-only volume labels MakeMKV reports for unbranded discs; if the
# whole label collapses to one of these, the parser yields no useful guess.
_GENERIC_LABELS = {
    "DVD VIDEO",
    "DVD VOLUME",
    "LOGICAL VOLUME ID",
    "VOLUME",
    "DISC",
    "DVD",
    "MOVIE",
    "MOVIE DVD",
    "UNTITLED",
}

_TITLE_LOWERCASE_SMALLS = {
    "a", "an", "and", "as", "at", "but", "by", "for", "from", "in",
    "into", "of", "on", "or", "the", "to", "vs", "with",
}


def _title_case_smart(s: str) -> str:
    """Title-case while keeping small words (``the``, ``of``, ``and``) lowercase.

    Always capitalizes the first word (English title case). Pure ASCII; the
    DVD volume names we see are all uppercase ASCII anyway.
    """
    parts = s.split()
    out: list[str] = []
    for i, p in enumerate(parts):
        lp = p.lower()
        if i > 0 and lp in _TITLE_LOWERCASE_SMALLS:
            out.append(lp)
        else:
            out.append(p[:1].upper() + p[1:].lower())
    return " ".join(out)


def parse_disc_title(disc_title: str | None) -> ParsedDiscTitle | None:
    """Extract show/movie metadata from a DVD volume label.

    Returns ``None`` when the input is empty or collapses to a generic label
    like ``DVD_VIDEO``. The caller should fall back to duration-based
    heuristics in that case.

    Confidence rough scale:

    - 0.95 : combined ``Sxx[ _]Dxx`` or ``Season N Disc M`` matched
    - 0.80 : season-only or disc-only marker
    - 0.60 : a plausible 4-digit year + name (likely a movie)
    - 0.30 : just a name (no season, disc, or year markers)
    """
    if not disc_title:
        return None
    s = disc_title.replace("_", " ").replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    if s.upper() in _GENERIC_LABELS:
        return None

    season: int | None = None
    disc: int | None = None
    year: int | None = None
    confidence = 0.3

    m = _PAT_SEASON_DISC_LOOSE.search(s) or _PAT_SEASON_DISC_TIGHT.search(s)
    if m:
        season = int(m.group(1))
        disc = int(m.group(2))
        s = s[: m.start()] + " " + s[m.end():]
        confidence = 0.95
    else:
        m_s = _PAT_SEASON.search(s)
        if m_s:
            season = int(m_s.group(1))
            s = s[: m_s.start()] + " " + s[m_s.end():]
            confidence = 0.8
        m_d = _PAT_DISC.search(s)
        if m_d:
            disc = int(m_d.group(1))
            s = s[: m_d.start()] + " " + s[m_d.end():]
            confidence = max(confidence, 0.8)

    m_y = _PAT_YEAR.search(s)
    if m_y:
        year = int(m_y.group(1))
        s = s[: m_y.start()] + " " + s[m_y.end():]

    s = _PAT_NOISE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    name = _title_case_smart(s) if s else ""

    if season is not None:
        kind: Literal["tv", "movie"] | None = "tv"
    elif disc is not None and year is None:
        # Disc number alone without season is ambiguous (movies ship multi-
        # disc too); leave kind unset and let durations decide.
        kind = None
    elif year is not None:
        kind = "movie"
        if confidence < 0.6:
            confidence = 0.6
    else:
        kind = None

    if not name:
        return None

    return ParsedDiscTitle(
        name=name, kind=kind, season=season, disc=disc, year=year, confidence=confidence,
    )


def final_kind_from_signals(parsed: ParsedDiscTitle | None, durations_guess: str) -> str:
    """Pick the final kind, preferring a confident disc-title verdict."""
    if parsed and parsed.kind in ("tv", "movie"):
        return parsed.kind
    return durations_guess
