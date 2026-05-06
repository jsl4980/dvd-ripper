"""Apply review decisions: DB rows, output filenames, final library paths."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from app.config import settings
from app.db import (
    get_job,
    list_titles,
    mark_title_skipped,
    merge_job_metadata,
    set_job_kind_and_id,
    update_job_status,
    update_title_assignment,
)
from app.metadata import tvdb
from app.plex_naming import movie_destination, tv_destination
from app.state import JobKind, JobStatus, TitleAssignment


async def apply_approval(
    job_id: int,
    *,
    kind: str,
    titles_payload: list[dict[str, Any]],
    tmdb_id: int | None = None,
    movie_title: str | None = None,
    movie_year: int | None = None,
    tvdb_id: int | None = None,
    show_title: str | None = None,
    show_year: int | None = None,
    season: int | None = None,
    start_episode: int = 1,
) -> None:
    job = await get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["status"] != JobStatus.NEEDS_REVIEW.value:
        raise HTTPException(400, "job is not awaiting review")

    db_titles = {int(t["id"]): t for t in await list_titles(job_id)}
    ordered: list[tuple[int, bool]] = []
    for row in titles_payload:
        tid = int(row["id"])
        if tid not in db_titles:
            raise HTTPException(400, f"unknown title id {tid}")
        ordered.append((tid, bool(row.get("skip"))))

    non_skip = [tid for tid, sk in ordered if not sk]
    if not non_skip:
        raise HTTPException(400, "at least one non-skipped title is required")

    dest_paths: dict[str, str] = {}

    if kind == "movie":
        if tmdb_id is None or not movie_title:
            raise HTTPException(400, "movie requires tmdb_id and movie_title")
        if len(non_skip) != 1:
            raise HTTPException(400, "movie workflow expects exactly one main title")
        _, dest_file = movie_destination(
            settings.library_root,
            title=movie_title,
            year=movie_year,
            tmdb_id=tmdb_id,
        )
        for tid, skip in ordered:
            if skip:
                await mark_title_skipped(tid)
                continue
            await update_title_assignment(
                tid,
                assignment_kind=TitleAssignment.MOVIE.value,
                season=None,
                episode=None,
                episode_title=None,
                output_filename=dest_file.name,
            )
            dest_paths[str(tid)] = str(dest_file.resolve())

        await set_job_kind_and_id(job_id, JobKind.MOVIE, str(tmdb_id))
        await merge_job_metadata(
            job_id,
            {
                "tmdb_id": tmdb_id,
                "movie_title": movie_title,
                "movie_year": movie_year,
                "dest_paths": dest_paths,
            },
        )

    elif kind == "tv":
        if tvdb_id is None or season is None or not show_title:
            raise HTTPException(400, "tv requires tvdb_id, show_title, and season")
        eps = await tvdb.get_episodes_for_season(tvdb_id, season)
        ep_names: dict[int, str] = {}
        for ep in eps:
            try:
                num = int(ep.get("number") or ep.get("episodeNumber") or 0)
            except (TypeError, ValueError):
                continue
            name = str(ep.get("name") or f"Episode {num}")
            ep_names[num] = name

        ep_cursor = start_episode
        for tid, skip in ordered:
            if skip:
                await mark_title_skipped(tid)
                continue
            ep_title = ep_names.get(ep_cursor, f"Episode {ep_cursor}")
            _, dest_file = tv_destination(
                settings.library_root,
                show=show_title,
                year=show_year,
                tvdb_id=tvdb_id,
                season=season,
                episode=ep_cursor,
                episode_title=ep_title,
            )
            await update_title_assignment(
                tid,
                assignment_kind=TitleAssignment.EPISODE.value,
                season=season,
                episode=ep_cursor,
                episode_title=ep_title,
                output_filename=dest_file.name,
            )
            dest_paths[str(tid)] = str(dest_file.resolve())
            ep_cursor += 1

        await set_job_kind_and_id(job_id, JobKind.TV, str(tvdb_id))
        await merge_job_metadata(
            job_id,
            {
                "tvdb_id": tvdb_id,
                "show_title": show_title,
                "show_year": show_year,
                "season": season,
                "start_episode": start_episode,
                "dest_paths": dest_paths,
            },
        )
    else:
        raise HTTPException(400, f"unknown kind {kind}")

    await update_job_status(job_id, JobStatus.APPROVED)
