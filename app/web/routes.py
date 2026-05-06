"""HTTP routes for the web UI (htmx) and JSON API."""

from __future__ import annotations

import asyncio
import logging
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.classify import guess_movie_or_tv
from app.config import settings
from app.db import (
    create_job,
    delete_titles_for_job,
    get_job,
    list_jobs,
    list_titles,
    update_job_status,
)
from app.metadata import tmdb, tvdb
from app.state import JobStatus
from app.web.approve import apply_approval

log = logging.getLogger("web")

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

router = APIRouter()


class TitleRow(BaseModel):
    id: int
    skip: bool = False


class ApproveBody(BaseModel):
    kind: Literal["movie", "tv"]
    titles: list[TitleRow]
    tmdb_id: int | None = None
    movie_title: str | None = None
    movie_year: int | None = None
    tvdb_id: int | None = None
    show_title: str | None = None
    show_year: int | None = None
    season: int | None = Field(default=None, ge=0)
    start_episode: int = Field(default=1, ge=1)


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "env": settings.app_env}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    jobs = await list_jobs()
    review_cards: list[dict[str, Any]] = []
    other: list[dict[str, Any]] = []
    for j in jobs:
        if j["status"] == JobStatus.NEEDS_REVIEW.value:
            t = await list_titles(int(j["id"]))
            review_cards.append(
                {
                    "job": j,
                    "titles": t,
                    "guess": guess_movie_or_tv(t),
                }
            )
        else:
            other.append(j)
    return TEMPLATES.TemplateResponse(
        "index.html",
        {
            "request": request,
            "jobs": jobs,
            "review_cards": review_cards,
            "other_jobs": other,
            "settings": settings,
        },
    )


@router.post("/api/jobs", status_code=201)
async def create_rip_job(request: Request) -> JSONResponse:
    """Enqueue a new rip (udev hook or manual / dev button)."""
    payload: dict[str, Any] = {}
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            payload = await request.json()
    except Exception:
        payload = {}
    disc_label = payload.get("disc_label")

    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    staging_dir = settings.staging_dir / "incoming" / timestamp
    staging_dir.mkdir(parents=True, exist_ok=True)

    job_id = await create_job(str(staging_dir), disc_label)
    log.info("queued rip job %d (%s)", job_id, staging_dir)
    return JSONResponse({"id": job_id, "staging_dir": str(staging_dir)}, status_code=201)


@router.post("/api/jobs/{job_id}/retry")
async def retry_job(job_id: int) -> dict[str, str]:
    """Re-queue a failed rip from scratch (deletes per-title rows)."""
    job = await get_job(job_id)
    if job is None:
        raise HTTPException(404)
    if job["status"] != JobStatus.FAILED.value:
        raise HTTPException(400, "only failed jobs can be retried for rip")
    await delete_titles_for_job(job_id)
    await update_job_status(job_id, JobStatus.PENDING_RIP, error_message=None)
    return {"status": "pending_rip"}


@router.get("/api/jobs")
async def api_list_jobs() -> list[dict[str, Any]]:
    return await list_jobs()


@router.get("/api/jobs/{job_id}")
async def api_get_job(job_id: int) -> dict[str, Any]:
    job = await get_job(job_id)
    if job is None:
        raise HTTPException(404)
    titles = await list_titles(job_id)
    guess = guess_movie_or_tv(titles)
    return {"job": job, "titles": titles, "guess": guess}


@router.post("/api/jobs/{job_id}/approve")
async def api_approve(job_id: int, body: ApproveBody) -> dict[str, str]:
    await apply_approval(
        job_id,
        kind=body.kind,
        titles_payload=[t.model_dump() for t in body.titles],
        tmdb_id=body.tmdb_id,
        movie_title=body.movie_title,
        movie_year=body.movie_year,
        tvdb_id=body.tvdb_id,
        show_title=body.show_title,
        show_year=body.show_year,
        season=body.season,
        start_episode=body.start_episode,
    )
    return {"status": "approved"}


@router.get("/api/search/movies")
async def search_movies(q: str) -> list[dict[str, Any]]:
    return await tmdb.search_movies(q)


@router.get("/api/search/tv")
async def search_tv(q: str) -> list[dict[str, Any]]:
    return await tvdb.search_series(q)


@router.get("/api/tv/{series_id}/seasons")
async def tv_seasons(series_id: int) -> dict[str, Any]:
    seasons = await tvdb.list_season_numbers(series_id)
    if not seasons:
        seasons = [1]
    return {"seasons": seasons}


@router.get("/api/jobs/{job_id}/titles/{title_id}/preview.mp4")
async def title_preview(job_id: int, title_id: int) -> FileResponse:
    """~10s h264 preview clip (cached under ``staging/previews``)."""
    job = await get_job(job_id)
    if job is None:
        raise HTTPException(404)
    titles = {t["id"]: t for t in await list_titles(job_id)}
    if title_id not in titles:
        raise HTTPException(404)
    title = titles[title_id]
    src = Path(job["staging_dir"]) / str(title["source_filename"])
    if not src.is_file():
        raise HTTPException(404, "source file missing")

    cache_dir = settings.staging_dir / "previews"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = cache_dir / f"{job_id}-{title_id}.mp4"
    if not cache.is_file() or cache.stat().st_mtime < src.stat().st_mtime:

        def run() -> None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            tmp = cache.with_suffix(".tmp.mp4")
            cmd = [
                settings.ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ss",
                "120",
                "-i",
                str(src),
                "-t",
                "12",
                "-vf",
                "scale=-2:360",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "28",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-movflags",
                "+faststart",
                str(tmp),
            ]
            subprocess.run(cmd, check=True)
            tmp.replace(cache)

        try:
            await asyncio.to_thread(run)
        except Exception as e:
            log.warning("preview ffmpeg failed for title %s: %s", title_id, e)
            raise HTTPException(500, "could not generate preview clip") from e

    return FileResponse(cache, media_type="video/mp4", filename=cache.name)
