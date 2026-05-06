"""Heuristic guess: is this disc more likely a movie or a TV season?"""

from __future__ import annotations

from typing import Any


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
