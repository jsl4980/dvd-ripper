"""Publish worker: move encoded MKVs into the Plex library tree + scan."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Any

import httpx

from app.config import settings
from app.db import (
    all_titles_finished,
    claim_title_for_publish,
    get_job,
    list_jobs,
    set_title_publish_result,
    update_job_status,
)
from app.plex_naming import plex_section_id_for_path
from app.state import JobStatus, StageStatus

log = logging.getLogger("publish")


async def _plex_refresh(section_id: int, host_path: str) -> None:
    if not settings.plex_token or section_id <= 0:
        return
    base = settings.plex_url.rstrip("/")
    url = f"{base}/library/sections/{section_id}/refresh"
    params = {"path": host_path, "X-Plex-Token": settings.plex_token}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, params=params)
        if r.status_code >= 400:
            log.warning("plex refresh failed %s: %s", r.status_code, r.text[:500])
        else:
            log.info("plex refresh ok section=%s path=%s", section_id, host_path)


def _encoded_path(job_id: int, filename: str) -> Path:
    return settings.staging_dir / "encoding" / str(job_id) / "encoded" / filename


async def publish_one_title(job: dict[str, Any], title: dict[str, Any]) -> Path:
    """Move one encoded MKV into ``library_root``; return final path."""
    job_id = int(job["id"])
    meta = json.loads(job["metadata_json"] or "{}")
    dest_path = Path(str(meta["dest_paths"][str(title["id"])]))

    src = _encoded_path(job_id, str(title["encoded_filename"]))
    if not src.is_file():
        raise FileNotFoundError(f"missing encode output: {src}")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest_path))
    return dest_path


async def publish_loop() -> None:
    log.info("publish worker started")
    while True:
        try:
            jobs = await list_jobs(JobStatus.PUBLISHING)
            if not jobs:
                await asyncio.sleep(settings.poll_interval_seconds)
                continue
            job = jobs[0]
            job_id = job["id"]
            scan_dirs: dict[tuple[int, str], None] = {}

            try:
                while True:
                    title = await claim_title_for_publish(int(job_id))
                    if title is None:
                        break
                    try:
                        final = await publish_one_title(job, title)
                        await set_title_publish_result(
                            int(title["id"]),
                            status=StageStatus.DONE,
                            published_path=str(final),
                        )
                        section = plex_section_id_for_path(
                            final,
                            settings.library_root,
                            movies_section_id=settings.plex_movies_section_id,
                            tv_section_id=settings.plex_tv_section_id,
                        )
                        if section:
                            host_dir = settings.translate_to_plex_host(final.parent)
                            scan_dirs[(section, host_dir)] = None
                    except Exception as e:
                        log.exception("title %s publish failed", title["id"])
                        await set_title_publish_result(
                            int(title["id"]),
                            status=StageStatus.FAILED,
                            error=str(e),
                        )
                        await update_job_status(
                            int(job_id),
                            JobStatus.FAILED,
                            error_message=f"publish: {e}",
                        )
                        scan_dirs.clear()
                        break

                job_row = await get_job(int(job_id))
                if job_row and job_row["status"] == JobStatus.FAILED.value:
                    await asyncio.sleep(settings.poll_interval_seconds)
                    continue

                all_done, any_failed = await all_titles_finished(int(job_id), "publish")
                if all_done:
                    if any_failed:
                        await update_job_status(
                            int(job_id),
                            JobStatus.FAILED,
                            error_message="one or more titles failed publish",
                        )
                    else:
                        await update_job_status(int(job_id), JobStatus.PUBLISHED)
                        for (section_id, host_dir) in scan_dirs:
                            await _plex_refresh(section_id, host_dir)
            except Exception as e:
                log.exception("job %s: publish loop failed", job_id)
                await update_job_status(
                    int(job_id), JobStatus.FAILED, error_message=str(e)
                )
        except asyncio.CancelledError:
            log.info("publish worker stopping")
            return
