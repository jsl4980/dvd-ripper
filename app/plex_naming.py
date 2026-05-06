"""Plex folder / file naming helpers (with {tmdb-} / {tvdb-} id folders)."""

from __future__ import annotations

import re
from pathlib import Path

_INVALID = re.compile(r'[<>:"/\\|?*]+')


def sanitize_segment(name: str) -> str:
    cleaned = _INVALID.sub("-", name)
    return " ".join(cleaned.split()).strip() or "Unknown"


def movie_destination(
    library_root: Path,
    *,
    title: str,
    year: int | None,
    tmdb_id: int,
) -> tuple[Path, Path]:
    """Return ``(folder, file_path)`` under ``library_root/Movies``."""
    y = year or 1900
    show_folder = sanitize_segment(f"{title} ({y}) {{tmdb-{tmdb_id}}}")
    fname = sanitize_segment(f"{title} ({y}).mkv")
    folder = library_root / "Movies" / show_folder
    return folder, folder / fname


def tv_destination(
    library_root: Path,
    *,
    show: str,
    year: int | None,
    tvdb_id: int,
    season: int,
    episode: int,
    episode_title: str,
) -> tuple[Path, Path]:
    """Return ``(season_folder, file_path)`` under ``library_root/TV Shows``."""
    y = year or 1900
    show_folder = sanitize_segment(f"{show} ({y}) {{tvdb-{tvdb_id}}}")
    season_folder = library_root / "TV Shows" / show_folder / f"Season {season:02d}"
    fname = sanitize_segment(
        f"{show} ({y}) - S{season:02d}E{episode:02d} - {episode_title}.mkv"
    )
    return season_folder, season_folder / fname


def plex_section_id_for_path(
    local_file: Path,
    library_root: Path,
    *,
    movies_section_id: int,
    tv_section_id: int,
) -> int | None:
    """Map a library file path to the configured Plex library section id."""
    try:
        rel = local_file.relative_to(library_root)
    except ValueError:
        return None
    parts = rel.parts
    if not parts:
        return None
    if parts[0] == "Movies" and movies_section_id > 0:
        return movies_section_id
    if parts[0] == "TV Shows" and tv_section_id > 0:
        return tv_section_id
    return None
