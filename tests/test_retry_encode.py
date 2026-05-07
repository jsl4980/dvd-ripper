"""Retry-encode flow: DB requeue + web route + HandBrake error capture."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest


def _reset_app_modules() -> None:
    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        del sys.modules[mod]


def _write_python_stub(path: Path, body: str) -> Path:
    """Create a python script the encode worker can launch via subprocess.

    On Windows the worker spawns ``HandBrakeCLI.exe`` directly, so we need to
    wrap the python interpreter in a `.bat` shim. On POSIX a ``#!/usr/bin/env
    python`` shebang is fine. The path returned is what gets stuffed into
    ``settings.handbrakecli_path`` / ``settings.ffprobe_path``.
    """
    py_path = path.with_suffix(".py")
    py_path.write_text(textwrap.dedent(body))
    if sys.platform == "win32":
        bat_path = path.with_suffix(".bat")
        py = sys.executable.replace("/", "\\")
        bat_path.write_text(f'@echo off\r\n"{py}" "{py_path}" %*\r\n')
        return bat_path
    py_path.chmod(0o755)
    py_path.write_text("#!" + sys.executable + "\n" + py_path.read_text())
    return py_path


def _common_env(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    _reset_app_modules()


@pytest.mark.asyncio
async def test_requeue_failed_encode_resets_failed_titles(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from app.db import (
        add_title,
        create_job,
        get_job,
        init_db,
        list_titles,
        requeue_failed_encode,
        set_title_encode_result,
        update_job_status,
        update_title_assignment,
    )
    from app.state import JobStatus, StageStatus

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "VAMPIRE_DIARIES_S2D1")

    t1 = await add_title(job_id, title_index=1, source_filename="C1_t01.mkv", duration_seconds=2500, size_bytes=10)
    t2 = await add_title(job_id, title_index=2, source_filename="C2_t02.mkv", duration_seconds=2500, size_bytes=10)
    t3 = await add_title(job_id, title_index=3, source_filename="C3_t03.mkv", duration_seconds=2500, size_bytes=10)
    skip = await add_title(job_id, title_index=4, source_filename="extra.mkv", duration_seconds=200, size_bytes=10)

    for tid, ep in ((t1, 1), (t2, 2), (t3, 3)):
        await update_title_assignment(
            tid, assignment_kind="episode", season=1, episode=ep,
            episode_title=f"E{ep}", output_filename=f"S01E0{ep}.mkv",
        )
    await update_title_assignment(
        skip, assignment_kind="skip", season=None, episode=None,
        episode_title=None, output_filename=None,
    )

    await set_title_encode_result(t1, status=StageStatus.DONE, encoded_filename="S01E01.mkv")
    await set_title_encode_result(t2, status=StageStatus.FAILED, error="boom")
    await update_job_status(job_id, JobStatus.FAILED, error_message="encode: boom")

    summary = await requeue_failed_encode(job_id)
    assert summary["reset_titles"] == 1

    job = await get_job(job_id)
    assert job["status"] == JobStatus.APPROVED.value
    assert job["error_message"] is None

    titles = {t["id"]: t for t in await list_titles(job_id)}
    assert titles[t1]["encode_status"] == StageStatus.DONE.value
    assert titles[t1]["encoded_filename"] == "S01E01.mkv"
    assert titles[t2]["encode_status"] == StageStatus.PENDING.value
    assert titles[t2]["encode_error"] is None
    assert titles[t3]["encode_status"] == StageStatus.PENDING.value
    _reset_app_modules()


@pytest.mark.asyncio
async def test_retry_encode_route_rejects_when_not_failed(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import create_job, init_db, set_job_metadata
    from app.main import app
    from app.state import JobKind

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j2"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "DISC")
    await set_job_metadata(job_id, kind=JobKind.TV, metadata_id="123", metadata={"tvdb_id": 123})

    with TestClient(app) as client:
        r = client.post(f"/api/jobs/{job_id}/retry-encode")
        assert r.status_code == 400
        assert "only failed jobs" in r.text

        r2 = client.post("/api/jobs/9999/retry-encode")
        assert r2.status_code == 404
    _reset_app_modules()


def test_format_handbrake_error_includes_tail(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.workers.encode import _format_handbrake_error

    msg = _format_handbrake_error(3, ["Cannot load nvcuda.dll", "encavcodecInit: avcodec_open failed"])
    assert "exited with code 3" in msg
    assert "Cannot load nvcuda.dll" in msg
    assert "avcodec_open failed" in msg

    bare = _format_handbrake_error(3, [])
    assert bare == "HandBrakeCLI exited with code 3"
    _reset_app_modules()


@pytest.mark.asyncio
async def test_encode_one_title_surfaces_handbrake_output_on_failure(tmp_path, monkeypatch):
    """Failure path: RuntimeError must include the last HandBrake output lines."""
    _common_env(tmp_path, monkeypatch)

    src_dir = tmp_path / "staging" / "incoming" / "j_fail"
    src_dir.mkdir(parents=True)
    src = src_dir / "C1_t01.mkv"
    src.write_bytes(b"not really an mkv")

    ffprobe_stub = _write_python_stub(
        tmp_path / "ffprobe_stub",
        """
        import sys
        print("progressive")
        sys.exit(0)
        """,
    )
    hb_stub = _write_python_stub(
        tmp_path / "hb_stub",
        """
        import sys
        print("[00:00:00] Starting Task: Encoding Pass")
        print("[h264_nvenc @ 0xdead] Cannot load nvcuda.dll")
        print("[00:00:00] encavcodecInit: avcodec_open failed")
        print("ERROR: Failure to initialise thread 'FFMPEG encoder (libavcodec)'")
        print("[00:00:00] libhb: work result = 3")
        print("Encode failed (error 3).")
        sys.exit(3)
        """,
    )

    monkeypatch.setenv("HANDBRAKECLI_PATH", str(hb_stub))
    monkeypatch.setenv("FFPROBE_PATH", str(ffprobe_stub))
    _reset_app_modules()

    from app.workers.encode import encode_one_title

    title = {
        "id": 42,
        "source_filename": "C1_t01.mkv",
        "output_filename": "ep1.mkv",
    }
    with pytest.raises(RuntimeError) as ei:
        await encode_one_title(1, src_dir, title)
    msg = str(ei.value)
    assert "exited with code 3" in msg
    assert "Cannot load nvcuda.dll" in msg
    assert "avcodec_open failed" in msg
    _reset_app_modules()


@pytest.mark.asyncio
async def test_encode_one_title_succeeds_when_handbrake_writes_output(tmp_path, monkeypatch):
    """Success path: a stubbed HB that writes the expected output exits 0."""
    _common_env(tmp_path, monkeypatch)

    src_dir = tmp_path / "staging" / "incoming" / "j_ok"
    src_dir.mkdir(parents=True)
    src = src_dir / "C1_t01.mkv"
    src.write_bytes(b"src bytes")

    ffprobe_stub = _write_python_stub(
        tmp_path / "ffprobe_ok",
        """
        import sys
        print("progressive")
        sys.exit(0)
        """,
    )
    hb_stub = _write_python_stub(
        tmp_path / "hb_ok",
        """
        import sys, argparse
        p = argparse.ArgumentParser()
        p.add_argument("--input", required=True)
        p.add_argument("--output", required=True)
        args, _ = p.parse_known_args()
        with open(args.output, "wb") as f:
            f.write(b"encoded")
        print("Encode done!")
        sys.exit(0)
        """,
    )

    monkeypatch.setenv("HANDBRAKECLI_PATH", str(hb_stub))
    monkeypatch.setenv("FFPROBE_PATH", str(ffprobe_stub))
    _reset_app_modules()

    from app.workers.encode import encode_one_title

    title = {
        "id": 42,
        "source_filename": "C1_t01.mkv",
        "output_filename": "ep1.mkv",
    }
    out_path = await encode_one_title(7, src_dir, title)
    assert out_path.is_file()
    assert out_path.read_bytes() == b"encoded"
    _reset_app_modules()


@pytest.mark.asyncio
async def test_retry_encode_route_requires_metadata(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import create_job, init_db, update_job_status
    from app.main import app
    from app.state import JobStatus

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j3"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), "DISC")
    await update_job_status(job_id, JobStatus.FAILED, error_message="rip: boom")

    with TestClient(app) as client:
        r = client.post(f"/api/jobs/{job_id}/retry-encode")
        assert r.status_code == 400
        assert "approve" in r.text.lower()
    _reset_app_modules()
