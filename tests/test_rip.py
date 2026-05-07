"""End-to-end smoke test for the rip pipeline using the mock makemkvcon."""

from __future__ import annotations

import asyncio
import contextlib
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
MOCK_PY = REPO / "tests" / "fixtures" / "mock_makemkvcon" / "mock_makemkvcon.py"


def _reset_app_modules() -> None:
    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        del sys.modules[mod]


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    monkeypatch.setenv("MAKEMKVCON_PATH", str(MOCK_PY))
    monkeypatch.setenv("DVD_DEVICE", "disc:0")
    monkeypatch.setenv("MOCK_TITLE_COUNT", "3")
    monkeypatch.setenv("MOCK_TITLE_SECONDS", "2")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0.2")
    _reset_app_modules()
    yield tmp_path
    _reset_app_modules()


def test_robot_line_parser():
    from app.workers.rip import duration_to_seconds, parse_robot_line

    assert parse_robot_line("TCOUNT:8") == ("TCOUNT", ["8"])
    assert parse_robot_line('TINFO:0,9,0,"0:43:21"') == ("TINFO", ["0", "9", "0", "0:43:21"])
    assert parse_robot_line("not a robot line") is None
    assert duration_to_seconds("1:02:03") == 3723
    assert duration_to_seconds("0:43:21") == 43 * 60 + 21
    assert duration_to_seconds("") is None


def test_normalize_makemkv_source():
    from app.workers.rip import normalize_makemkv_source

    assert normalize_makemkv_source("/dev/sr0") == "dev:/dev/sr0"
    assert normalize_makemkv_source("dev:/dev/sr1") == "dev:/dev/sr1"
    assert normalize_makemkv_source("disc:0") == "disc:0"
    assert normalize_makemkv_source("D:") == "file:D:/"
    assert normalize_makemkv_source("movie.iso") == "iso:movie.iso"


def test_disc_index_for_drive_letter_from_drv_robot():
    from app.workers.rip import disc_index_for_drive_letter_from_drv_robot

    blob = (
        'DRV:0,0,999,0,"DVD HL-DT-ST","","D:"\n'
        'DRV:1,256,999,0,"","",""\n'
        "TCOUNT:0\n"
    )
    assert disc_index_for_drive_letter_from_drv_robot(blob, "D") == 0
    assert disc_index_for_drive_letter_from_drv_robot(blob, "E") is None


def test_infer_combined_play_all_title_indices():
    from app.workers.rip import ATTR_DURATION, infer_combined_play_all_title_indices

    # One marathon title ≈ sum of five ~42 min episodes (TV DVD layout).
    meta = {
        0: {ATTR_DURATION: "3:30:10"},
        1: {ATTR_DURATION: "0:41:45"},
        2: {ATTR_DURATION: "0:42:21"},
        3: {ATTR_DURATION: "0:42:06"},
        4: {ATTR_DURATION: "0:42:19"},
        5: {ATTR_DURATION: "0:41:39"},
    }
    skip = infer_combined_play_all_title_indices(meta, min_length_seconds=120)
    assert skip == {0}

    assert infer_combined_play_all_title_indices({0: {ATTR_DURATION: "1:30:00"}}, min_length_seconds=120) == set()
    assert infer_combined_play_all_title_indices(
        {0: {ATTR_DURATION: "1:00:00"}, 1: {ATTR_DURATION: "1:00:00"}}, min_length_seconds=120
    ) == set()


@pytest.mark.asyncio
async def test_rip_worker_skips_play_all_with_mock_info(tmp_path, monkeypatch):
    """Info pass reports a marathon title 0; worker should rip titles 1..N only."""
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    monkeypatch.setenv("MAKEMKVCON_PATH", str(MOCK_PY))
    monkeypatch.setenv("DVD_DEVICE", "disc:0")
    monkeypatch.setenv("MOCK_INFO_PLAY_ALL", "1")
    monkeypatch.setenv("MOCK_TITLE_COUNT", "6")
    monkeypatch.setenv("MOCK_TITLE_SECONDS", "2")
    monkeypatch.setenv("MOCK_EPISODE_SECONDS", "2700")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0.2")
    _reset_app_modules()

    from app.db import create_job, get_job, init_db, list_titles
    from app.state import JobStatus
    from app.workers.rip import rip_loop

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "playalltest"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "PLAYALL")

    task = asyncio.create_task(rip_loop())
    try:
        for _ in range(120):
            await asyncio.sleep(0.25)
            job = await get_job(job_id)
            assert job is not None
            if job["status"] in (JobStatus.NEEDS_REVIEW.value, JobStatus.FAILED.value):
                break
        else:
            pytest.fail("rip worker timed out")
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    job = await get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.NEEDS_REVIEW.value, job

    titles = await list_titles(job_id)
    assert len(titles) == 5
    assert {t["title_index"] for t in titles} == {1, 2, 3, 4, 5}
    _reset_app_modules()


@pytest.mark.asyncio
async def test_rip_worker_e2e(env):
    from app.db import create_job, get_job, init_db, list_titles
    from app.state import JobStatus
    from app.workers.rip import rip_loop

    await init_db()
    staging = env / "staging" / "incoming" / "test"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "TEST_DISC")

    task = asyncio.create_task(rip_loop())
    try:
        for _ in range(80):
            await asyncio.sleep(0.25)
            job = await get_job(job_id)
            assert job is not None
            if job["status"] in (JobStatus.NEEDS_REVIEW.value, JobStatus.FAILED.value):
                break
        else:
            pytest.fail("rip worker timed out")
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    job = await get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.NEEDS_REVIEW.value, job

    titles = await list_titles(job_id)
    assert len(titles) == 3
    for t in titles:
        assert t["duration_seconds"] == 2
        assert t["source_filename"].startswith("title")
        assert t["source_filename"].endswith(".mkv")
