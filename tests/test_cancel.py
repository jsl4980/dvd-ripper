"""Rip cancellation (web UI / DB flag)."""

from __future__ import annotations

import sys

import pytest


def _reset_app_modules() -> None:
    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        del sys.modules[mod]


@pytest.mark.asyncio
async def test_cancel_pending_job(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    _reset_app_modules()

    from app.db import create_job, get_job, init_db, request_cancel_rip
    from app.state import JobStatus

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "x"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "X")

    assert (await get_job(job_id))["status"] == JobStatus.PENDING_RIP.value
    assert await request_cancel_rip(job_id) == "cancelled"
    job = await get_job(job_id)
    assert job["status"] == JobStatus.CANCELLED.value
    assert job["error_message"] == "Cancelled by user"
    _reset_app_modules()


@pytest.mark.asyncio
async def test_cancel_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    _reset_app_modules()

    from app.db import create_job, init_db, request_cancel_rip

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "y"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "Y")
    assert await request_cancel_rip(job_id) == "cancelled"
    assert await request_cancel_rip(job_id) == "cancelled"
    _reset_app_modules()


@pytest.mark.asyncio
async def test_cancel_rejects_needs_review(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    _reset_app_modules()

    from app.db import create_job, init_db, request_cancel_rip, update_job_status
    from app.state import JobStatus

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "z"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "Z")
    await update_job_status(job_id, JobStatus.NEEDS_REVIEW)
    with pytest.raises(ValueError, match="cannot be cancelled"):
        await request_cancel_rip(job_id)
    _reset_app_modules()
