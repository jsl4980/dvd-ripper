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

# `makemkvcon info <invalid>` still prints a `DRV:` table on Windows; use a
# slot that is never a real drive so we never need an open disc for probing.
_FAKE_DISC_SLOT_FOR_DRIVE_PROBE = "disc:99"

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
    `/dev/sr0` -> `dev:/dev/sr0`. A bare Windows drive letter like ``D:``
    becomes ``file:D:/``; on Windows the rip worker then maps that to
    ``disc:N`` using MakeMKV's ``DRV:`` table (``file:`` volume access often
    fails with exit 10 on real DVD drives).
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
    if len(s) == 2 and s[1] == ":" and s[0].isalpha():
        letter = s[0].upper()
        return f"file:{letter}:/"
    return s


_WIN_FILE_VOLUME = re.compile(r"^file:([A-Za-z]):/?$")


def disc_index_for_drive_letter_from_drv_robot(blob: str, letter: str) -> int | None:
    """Parse ``makemkvcon -r info …`` robot output; return index where last DRV field is ``X:``."""
    want = f"{letter.strip().upper()}:"
    for line in blob.splitlines():
        parsed = parse_robot_line(line)
        if parsed is None or parsed[0] != "DRV":
            continue
        fields = parsed[1]
        if len(fields) < 2:
            continue
        try:
            idx = int(fields[0])
        except ValueError:
            continue
        letter_field = fields[-1].strip() if fields else ""
        if letter_field.upper() == want.upper():
            return idx
    return None


async def makemkv_disc_index_for_windows_drive(letter: str) -> int | None:
    """Return MakeMKV ``disc:`` index for a Windows drive letter (e.g. ``D``)."""
    binary = settings.makemkvcon_path
    prefix: list[str] = []
    if binary.lower().endswith(".py"):
        prefix = [sys.executable]
    cmd = [*prefix, binary, "-r", "--noscan", "info", _FAKE_DISC_SLOT_FOR_DRIVE_PROBE]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    blob, _ = await proc.communicate()
    return disc_index_for_drive_letter_from_drv_robot(
        blob.decode("utf-8", errors="replace"),
        letter,
    )


async def map_windows_file_volume_to_disc_if_needed(source: str) -> str:
    """On Windows, replace ``file:X:/`` with ``disc:N`` when MakeMKV lists ``X:``."""
    if sys.platform != "win32":
        return source
    m = _WIN_FILE_VOLUME.match(source)
    if not m:
        return source
    letter = m.group(1)
    idx = await makemkv_disc_index_for_windows_drive(letter)
    if idx is None:
        log.warning(
            "MakeMKV drive table has no %s: entry; using %s (may fail for DVD video)",
            letter,
            source,
        )
        return source
    mapped = f"disc:{idx}"
    log.info("Windows DVD drive %s: -> %s (direct disc access)", letter, mapped)
    return mapped


def infer_combined_play_all_title_indices(
    titles_attrs: dict[int, dict[int, str]],
    *,
    min_length_seconds: int,
) -> set[int]:
    """Detect a TV \"play all\" title whose length ≈ the sum of episode titles.

    Many TV DVDs expose one long title (all episodes concatenated) plus each
    episode as its own title. Ripping that marathon track wastes space and
    review noise; we skip ripping it when the duration heuristic matches.
    """
    entries: list[tuple[int, int]] = []
    for idx, attrs in titles_attrs.items():
        ds = duration_to_seconds(attrs.get(ATTR_DURATION, ""))
        if ds is None or ds < min_length_seconds:
            continue
        entries.append((idx, ds))
    if len(entries) < 4:
        return set()
    entries.sort(key=lambda x: x[1], reverse=True)
    max_idx, max_d = entries[0]
    rest_sum = sum(d for _, d in entries[1:])
    if rest_sum == 0:
        return set()
    ratio = max_d / rest_sum
    # Allow slack for rounding and slightly short \"play all\" edits vs sum of eps.
    if 0.88 <= ratio <= 1.18 and max_d >= 45 * 60:
        log.info(
            "skipping suspected play-all title index %d (%ds ≈ %.0f%% of other "
            "titles' total %ds)",
            max_idx,
            max_d,
            100.0 * ratio,
            rest_sum,
        )
        return {max_idx}
    return set()


def _makemkv_cmd_prefix(*, min_length_seconds: int) -> list[str]:
    binary = settings.makemkvcon_path
    prefix: list[str] = []
    if binary.lower().endswith(".py"):
        prefix = [sys.executable]
    return [
        *prefix,
        binary,
        "-r",
        "--noscan",
        f"--minlength={min_length_seconds}",
    ]


async def _drain_makemkv_robot_stdout(proc: asyncio.subprocess.Process) -> dict[int, dict[int, str]]:
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
        if kind == "MSG" and len(fields) >= 4:
            log.info("makemkv: %s", fields[3])
        elif kind == "TINFO" and len(fields) >= 4:
            try:
                t_idx = int(fields[0])
                attr_id = int(fields[1])
            except ValueError:
                continue
            titles.setdefault(t_idx, {})[attr_id] = fields[3]
        elif kind == "PRGT" and len(fields) >= 3:
            log.info("phase: %s", fields[2])
    return titles


async def run_makemkv_info(
    source: str,
    *,
    min_length_seconds: int = 120,
) -> dict[int, dict[int, str]]:
    """Run ``makemkvcon info`` and return TINFO attrs per title index (robot output)."""
    cmd = [
        *_makemkv_cmd_prefix(min_length_seconds=min_length_seconds),
        "info",
        source,
    ]
    log.info("running %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    titles = await _drain_makemkv_robot_stdout(proc)
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"makemkvcon info exited with code {rc}")
    return titles


async def run_makemkv_mkv_one_title(
    source: str,
    staging_dir: Path,
    title_idx: int,
    *,
    min_length_seconds: int = 120,
) -> dict[int, dict[int, str]]:
    """Run ``makemkvcon mkv`` for a single title index."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        *_makemkv_cmd_prefix(min_length_seconds=min_length_seconds),
        "mkv",
        source,
        str(title_idx),
        str(staging_dir),
    ]
    log.info("running %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    titles = await _drain_makemkv_robot_stdout(proc)
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"makemkvcon exited with code {rc}")
    return titles


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
    cmd = [
        *_makemkv_cmd_prefix(min_length_seconds=min_length_seconds),
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
    titles = await _drain_makemkv_robot_stdout(proc)
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"makemkvcon exited with code {rc}")
    return titles


async def rip_one_disc(job: dict[str, Any]) -> None:
    """Drive `makemkvcon` for one job; populate titles rows on success."""
    job_id = job["id"]
    staging_dir = Path(job["staging_dir"])
    source = normalize_makemkv_source(settings.dvd_device)
    source = await map_windows_file_volume_to_disc_if_needed(source)
    log.info("job %d: starting rip from %s -> %s", job_id, source, staging_dir)

    min_len = 120
    info_attrs = await run_makemkv_info(source, min_length_seconds=min_len)
    skip_idx = infer_combined_play_all_title_indices(info_attrs, min_length_seconds=min_len)

    if skip_idx:
        candidates = sorted(
            idx
            for idx, attrs in info_attrs.items()
            if (ds := duration_to_seconds(attrs.get(ATTR_DURATION, ""))) is not None
            and ds >= min_len
            and idx not in skip_idx
        )
        if not candidates:
            raise RuntimeError("after play-all filtering, no titles left to rip")
        log.info(
            "job %d: ripping %d title(s) individually (skipped indices %s)",
            job_id,
            len(candidates),
            sorted(skip_idx),
        )
        titles_attrs: dict[int, dict[int, str]] = {}
        for t_idx in candidates:
            part = await run_makemkv_mkv_one_title(
                source, staging_dir, t_idx, min_length_seconds=min_len
            )
            titles_attrs.update(part)
    else:
        titles_attrs = await run_makemkvcon(source, staging_dir, min_length_seconds=min_len)

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
