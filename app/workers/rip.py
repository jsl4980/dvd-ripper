"""Rip worker: drives `makemkvcon` and writes per-title rows to the DB.

MakeMKV's `--robot` output is a stream of single-line records:

    MSG:1005,0,1,"MakeMKV v1.17.7 win(x64-release) started",...
    TCOUNT:8
    TINFO:0,9,0,"0:43:21"
    TINFO:0,11,0,"5283364864"
    TINFO:0,27,0,"title00.mkv"
    PRGV:142,166,65536

Each line is `KIND:csv-payload`. We only need TINFO for title metadata
(durations, sizes, chapter counts, output filenames). MSG lines get logged.

The poll loop claims one `pending_rip` job at a time, ensuring a single rip
is in flight (the disc drive is the bottleneck).
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import settings
from app.db import (
    add_title,
    claim_job,
    update_job_status,
)
from app.state import JobStatus

log = logging.getLogger("rip")

# TINFO attribute IDs we care about (from MakeMKV's apdefs.h).
ATTR_NAME = 2
ATTR_CHAPTER_COUNT = 8
ATTR_DURATION = 9
ATTR_DISK_SIZE_HUMAN = 10
ATTR_DISK_SIZE_BYTES = 11
ATTR_OUTPUT_FILENAME = 27

ROBOT_LINE = re.compile(r"^(MSG|TCOUNT|CINFO|TINFO|SINFO|PRGV|PRGT|PRGC|DRV):(.*)$")


def _split_csv(payload: str) -> list[str]:
    """Parse a robot-line payload (CSV-ish with embedded quoted strings)."""
    reader = csv.reader(
        io.StringIO(payload),
        quotechar='"',
        doublequote=True,
        skipinitialspace=False,
    )
    try:
        return next(reader)
    except StopIteration:
        return []


@dataclass(frozen=True)
class TitleAttr:
    title_idx: int
    attr_id: int
    value: str


def parse_robot_line(line: str) -> tuple[str, list[str]] | None:
    """Return (kind, fields) or None for non-record lines."""
    m = ROBOT_LINE.match(line)
    if not m:
        return None
    kind, payload = m.group(1), m.group(2)
    return kind, _split_csv(payload)


def duration_to_seconds(s: str) -> int | None:
    """Convert "H:MM:SS" or "MM:SS" to integer seconds."""
    if not s:
        return None
    parts = s.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if len(nums) == 3:
        h, m, sec = nums
    elif len(nums) == 2:
        h, m, sec = 0, nums[0], nums[1]
    else:
        return None
    return h * 3600 + m * 60 + sec


def normalize_makemkv_source(device_or_source: str) -> str:
    """Map a configured DVD path/identifier to a makemkvcon source string.

    Already-prefixed sources (`dev:`, `disc:`, `file:`, `iso:`) pass through.
    `/dev/sr0` -> `dev:/dev/sr0`. A bare Windows drive letter like `D:`
    becomes `disc:0` (the user can override with an explicit `disc:N` if
    they have multiple optical drives).
    """
    s = device_or_source.strip()
    if not s:
        return s
    for prefix in ("dev:", "disc:", "file:", "iso:"):
        if s.startswith(prefix):
            return s
    if s.startswith("/dev/"):
        return f"dev:{s}"
    if s.lower().endswith(".iso"):
        return f"iso:{s}"
    if len(s) == 2 and s[1] == ":":
        return "disc:0"
    return s


async def run_makemkvcon(
    source: str,
    staging_dir: Path,
    *,
    min_length_seconds: int = 120,
) -> dict[int, dict[int, str]]:
    """Run `makemkvcon mkv all` and return collected TINFO attrs per title.

    Output goes to `staging_dir`. The minimum title length filters out
    short filler tracks (DVD menus, FBI warnings) that aren't worth ripping.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    binary = settings.makemkvcon_path
    prefix: list[str] = []
    # Convenience for dev: if MAKEMKVCON_PATH points at our mock .py file,
    # invoke it through the current Python interpreter.
    if binary.lower().endswith(".py"):
        prefix = [sys.executable]
    cmd = [
        *prefix,
        binary,
        "-r",
        "--noscan",
        f"--minlength={min_length_seconds}",
        "mkv",
        source,
        "all",
        str(staging_dir),
    ]
    log.info("running %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    titles: dict[int, dict[int, str]] = {}
    assert proc.stdout is not None
    while True:
        raw = await proc.stdout.readline()
        if not raw:
            break
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        parsed = parse_robot_line(line)
        if parsed is None:
            continue
        kind, fields = parsed
        if kind == "MSG":
            if len(fields) >= 4:
                log.info("makemkv: %s", fields[3])
        elif kind == "TINFO" and len(fields) >= 4:
            try:
                t_idx = int(fields[0])
                attr_id = int(fields[1])
            except ValueError:
                continue
            value = fields[3]
            titles.setdefault(t_idx, {})[attr_id] = value
        elif kind == "PRGT" and len(fields) >= 3:
            log.info("phase: %s", fields[2])

    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"makemkvcon exited with code {rc}")
    return titles


async def rip_one_disc(job: dict[str, Any]) -> None:
    """Drive `makemkvcon` for one job; populate titles rows on success."""
    job_id = job["id"]
    staging_dir = Path(job["staging_dir"])
    source = normalize_makemkv_source(settings.dvd_device)
    log.info("job %d: starting rip from %s -> %s", job_id, source, staging_dir)

    titles_attrs = await run_makemkvcon(source, staging_dir)

    written = sorted(staging_dir.glob("*.mkv"))
    if not written:
        raise RuntimeError("makemkvcon finished but no .mkv files were produced")

    name_to_attrs: dict[str, dict[int, str]] = {}
    for t_idx, attrs in titles_attrs.items():
        fname = attrs.get(ATTR_OUTPUT_FILENAME)
        if fname:
            name_to_attrs[fname] = {**attrs, "_title_idx": str(t_idx)}  # type: ignore[dict-item]

    inserted = 0
    for path in written:
        attrs = name_to_attrs.get(path.name, {})
        try:
            title_idx = int(attrs.get("_title_idx", "0"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            title_idx = inserted
        duration = duration_to_seconds(attrs.get(ATTR_DURATION, ""))
        try:
            size_bytes = int(attrs.get(ATTR_DISK_SIZE_BYTES, "")) if attrs.get(ATTR_DISK_SIZE_BYTES) else None
        except ValueError:
            size_bytes = None
        try:
            chapters = int(attrs.get(ATTR_CHAPTER_COUNT, "")) if attrs.get(ATTR_CHAPTER_COUNT) else None
        except ValueError:
            chapters = None
        await add_title(
            job_id,
            title_index=title_idx,
            source_filename=path.name,
            duration_seconds=duration,
            size_bytes=size_bytes,
            chapter_count=chapters,
        )
        inserted += 1
    log.info("job %d: ripped %d titles", job_id, inserted)


async def rip_loop() -> None:
    """Poll for `pending_rip` jobs and drive them through to `needs_review`."""
    log.info("rip worker started")
    while True:
        try:
            job = await claim_job(JobStatus.PENDING_RIP, JobStatus.RIPPING)
            if job is None:
                await asyncio.sleep(settings.poll_interval_seconds)
                continue
            try:
                await rip_one_disc(job)
                await update_job_status(job["id"], JobStatus.NEEDS_REVIEW)
                log.info("job %d: ready for review", job["id"])
            except Exception as e:
                log.exception("job %d: rip failed", job["id"])
                await update_job_status(
                    job["id"], JobStatus.FAILED, error_message=f"rip: {e}"
                )
        except asyncio.CancelledError:
            log.info("rip worker stopping")
            return
        except Exception:
            log.exception("rip loop unhandled error; sleeping before retry")
            await asyncio.sleep(5.0)
