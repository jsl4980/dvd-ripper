"""Encode worker: wraps ``HandBrakeCLI`` with the configured encoder profile."""

from __future__ import annotations

import asyncio
import logging
import sys
from collections import deque
from pathlib import Path

from app.config import settings
from app.db import (
    all_titles_finished,
    claim_job,
    claim_title_for_encode,
    set_title_encode_result,
    update_job_status,
)
from app.state import JobStatus, StageStatus

log = logging.getLogger("encode")


def _handbrake_binary() -> str:
    hb = settings.handbrakecli_path
    if sys.platform == "win32" and not hb.lower().endswith(".exe"):
        return hb  # user may have HB on PATH as HandBrakeCLI
    return hb


async def probe_field_order(video_path: Path) -> str:
    """Return ffprobe ``field_order`` for the first video stream (may be empty)."""
    proc = await asyncio.create_subprocess_exec(
        settings.ffprobe_path,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=field_order",
        "-of",
        "default=nw=1:nk=1",
        str(video_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, _err = await proc.communicate()
    if proc.returncode != 0:
        return ""
    return out.decode("utf-8", errors="replace").strip().lower()


def _encoder_flag() -> str:
    return {
        "nvenc_h264": "nvenc_h264",
        "nvenc_h265": "nvenc_h265",
        "x264": "x264",
        "x265": "x265",
    }[settings.encoder_profile]


_HB_TAIL_LINES = 80


def _format_handbrake_error(rc: int, tail: list[str]) -> str:
    """Build a RuntimeError message including the last HB output lines.

    Without this, a non-zero exit would surface as just "exited with code 3"
    in the UI, hiding the actual reason (e.g. ``Cannot load nvcuda.dll``).
    """
    if not tail:
        return f"HandBrakeCLI exited with code {rc}"
    body = "\n".join(tail).strip()
    return f"HandBrakeCLI exited with code {rc}:\n{body}"


async def encode_one_title(job_id: int, staging_dir: Path, title: dict[str, object]) -> Path:
    """Run HandBrake and return the path to the encoded MKV."""
    src = staging_dir / str(title["source_filename"])
    if not src.is_file():
        raise FileNotFoundError(f"missing source rip: {src}")

    field = await probe_field_order(src)
    if field:
        log.info(
            "title %s field_order=%s (Decomb still enabled for DVD-safe handling)",
            title["id"],
            field,
        )

    out_dir = settings.staging_dir / "encoding" / str(job_id) / "encoded"
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / str(title["output_filename"])
    if dest.exists():
        dest.unlink()

    hb = _handbrake_binary()
    cmd: list[str] = [
        hb,
        "--input",
        str(src),
        "--output",
        str(dest),
        "--format",
        "av_mkv",
        "--encoder",
        _encoder_flag(),
        "--quality",
        str(settings.encoder_quality),
        "--encoder-preset",
        settings.encoder_preset,
        "--comb-detect",
        "--decomb",
        "--all-audio",
        "--audio-copy-mask",
        "aac,ac3,eac3,mp3",
        "--audio-fallback",
        "av_aac",
        "--ab",
        str(settings.audio_fallback_bitrate),
        "--all-subtitles",
        "--subtitle-forced",
        "1",
    ]

    log.info("encoding title %s -> %s", title["id"], dest.name)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    tail: deque[str] = deque(maxlen=_HB_TAIL_LINES)
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        text = line.decode(errors="replace").rstrip()
        tail.append(text)
        log.debug("handbrake: %s", text)
    rc = await proc.wait()
    if rc != 0:
        for entry in tail:
            log.error("handbrake: %s", entry)
        raise RuntimeError(_format_handbrake_error(rc, list(tail)))
    if not dest.is_file():
        raise RuntimeError(f"HandBrake finished but output missing: {dest}")
    return dest


async def encode_loop() -> None:
    log.info("encode worker started (profile=%s)", settings.encoder_profile)
    while True:
        try:
            job = await claim_job(JobStatus.APPROVED, JobStatus.ENCODING)
            if job is None:
                await asyncio.sleep(settings.poll_interval_seconds)
                continue
            job_id = job["id"]
            staging_dir = Path(str(job["staging_dir"]))
            log.info("job %s: encoding started", job_id)
            try:
                while True:
                    title = await claim_title_for_encode(job_id)
                    if title is None:
                        all_done, any_failed = await all_titles_finished(job_id, "encode")
                        if all_done:
                            if any_failed:
                                await update_job_status(
                                    job_id,
                                    JobStatus.FAILED,
                                    error_message="one or more titles failed encoding",
                                )
                            else:
                                await update_job_status(job_id, JobStatus.PUBLISHING)
                        break
                    try:
                        await encode_one_title(job_id, staging_dir, title)
                        await set_title_encode_result(
                            int(title["id"]),
                            status=StageStatus.DONE,
                            encoded_filename=str(title["output_filename"]),
                        )
                    except Exception as e:
                        log.exception("title %s encode failed", title["id"])
                        await set_title_encode_result(
                            int(title["id"]),
                            status=StageStatus.FAILED,
                            error=str(e),
                        )
                        await update_job_status(
                            job_id, JobStatus.FAILED, error_message=f"encode: {e}"
                        )
                        break
            except Exception as e:
                log.exception("job %s: encode loop failed", job_id)
                await update_job_status(job_id, JobStatus.FAILED, error_message=str(e))
        except asyncio.CancelledError:
            log.info("encode worker stopping")
            return
