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
import contextlib
import csv
import io
import logging
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import settings
from app.db import (
    add_title,
    claim_job,
    is_job_cancel_requested,
    set_job_disc_info,
    update_job_status,
)
from app.state import JobStatus

log = logging.getLogger("rip")

# Current makemkv subprocess for cooperative cancel + optional SIGKILL from API.
_active_mkv: dict[int, asyncio.subprocess.Process] = {}


class RipCancelled(Exception):
    """Raised when the user cancels the rip from the web UI."""


def register_rip_subprocess(job_id: int, proc: asyncio.subprocess.Process) -> None:
    _active_mkv[job_id] = proc


def clear_rip_subprocess(job_id: int) -> None:
    _active_mkv.pop(job_id, None)


async def kill_rip_subprocess_if_running(job_id: int) -> None:
    """Hard-stop makemkv for this job (used by HTTP cancel after DB flag is set)."""
    proc = _active_mkv.get(job_id)
    if proc is None or proc.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.kill()
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(proc.wait(), timeout=30)

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

# CINFO attribute IDs (disc-level metadata).
CINFO_TYPE = 1            # e.g. "DVD disc"
CINFO_NAME_CODE = 2       # volume name (matches CINFO_VOLUME_NAME on most discs)
CINFO_VOLUME_NAME = 30    # raw volume name
CINFO_DISC_TITLE = 32     # MakeMKV-derived display name

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


def filter_short_tv_extras_below_half_median(
    info_attrs: dict[int, dict[int, str]],
    candidate_indices: list[int],
    *,
    min_length_seconds: int,
) -> list[int]:
    """Drop titles far shorter than typical episodes (menus/extras) on TV discs.

    MakeMKV's ``--minlength`` only removes very short tracks; a ~3 min promo can
    still appear beside ~42 min episodes. After play-all removal, require each
    rip to be at least half the **median** candidate duration when that median
    looks like a TV episode block (≥ 20 min).
    """
    pairs: list[tuple[int, int]] = []
    for idx in candidate_indices:
        attrs = info_attrs.get(idx, {})
        ds = duration_to_seconds(attrs.get(ATTR_DURATION, ""))
        if ds is None or ds < min_length_seconds:
            continue
        pairs.append((idx, ds))
    if len(pairs) < 4:
        return list(candidate_indices)
    durs = sorted(d for _, d in pairs)
    median = int(statistics.median(durs))
    if median < 20 * 60:
        return list(candidate_indices)
    floor = max(min_length_seconds, median // 2)
    kept_idx = [idx for idx, d in pairs if d >= floor]
    if len(kept_idx) < 2:
        return list(candidate_indices)
    dropped = sorted(set(candidate_indices) - set(kept_idx))
    if dropped:
        log.info(
            "skipping extra-short titles vs ~half of median episode (%ds): indices %s",
            median,
            dropped,
        )
    return sorted(kept_idx)


def _makemkv_cmd_prefix(*, min_length_seconds: int) -> list[str]:
    binary = settings.makemkvcon_path
    binary_norm = binary.replace("\\", "/").lower()
    if "tests/fixtures/mock_makemkvcon/" in binary_norm and not settings.allow_mock_makemkvcon:
        raise RuntimeError(
            "MAKEMKVCON_PATH points to the test mock binary. "
            "Install real makemkvcon or set ALLOW_MOCK_MAKEMKVCON=true for dev-only testing."
        )
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


async def _readline_with_cancel(
    proc: asyncio.subprocess.Process,
    job_id: int | None,
) -> bytes:
    assert proc.stdout is not None
    if job_id is None:
        return await proc.stdout.readline()

    async def poll_cancel() -> None:
        while True:
            await asyncio.sleep(0.5)
            if await is_job_cancel_requested(job_id):
                return

    read_task = asyncio.create_task(proc.stdout.readline())
    poll_task = asyncio.create_task(poll_cancel())
    done, pending = await asyncio.wait(
        {read_task, poll_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()
    for t in pending:
        with contextlib.suppress(asyncio.CancelledError):
            await t

    if poll_task in done:
        read_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await read_task
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        raise RipCancelled()

    return read_task.result()


def _record_cinfo_field(cinfo_out: dict[int, str], fields: list[str]) -> None:
    """Store a CINFO ``attr_id -> value`` from a parsed robot record."""
    if len(fields) < 3:
        return
    try:
        attr_id = int(fields[0])
    except ValueError:
        return
    cinfo_out[attr_id] = fields[-1]


def cinfo_from_robot_blob(blob: str) -> dict[int, str]:
    """Parse CINFO records from MakeMKV ``-r`` output and return ``attr_id -> value``."""
    out: dict[int, str] = {}
    for line in blob.splitlines():
        parsed = parse_robot_line(line)
        if parsed is None or parsed[0] != "CINFO":
            continue
        _record_cinfo_field(out, parsed[1])
    return out


def disc_title_from_cinfo(cinfo: dict[int, str]) -> str | None:
    """Return the best human-readable disc title from CINFO attrs (or ``None``)."""
    for attr in (CINFO_DISC_TITLE, CINFO_VOLUME_NAME, CINFO_NAME_CODE):
        v = cinfo.get(attr)
        if v:
            return v
    return None


async def _drain_makemkv_robot_stdout(
    proc: asyncio.subprocess.Process,
    job_id: int | None,
    *,
    cinfo_out: dict[int, str] | None = None,
) -> dict[int, dict[int, str]]:
    titles: dict[int, dict[int, str]] = {}
    assert proc.stdout is not None
    while True:
        raw = await _readline_with_cancel(proc, job_id)
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
        elif kind == "CINFO" and cinfo_out is not None:
            _record_cinfo_field(cinfo_out, fields)
        elif kind == "PRGT" and len(fields) >= 3:
            log.info("phase: %s", fields[2])
    return titles


async def run_makemkv_info(
    source: str,
    *,
    min_length_seconds: int = 120,
    job_id: int | None = None,
) -> tuple[dict[int, dict[int, str]], dict[int, str]]:
    """Run ``makemkvcon info`` and return ``(titles, cinfo)`` from robot output."""
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
    if job_id is not None:
        register_rip_subprocess(job_id, proc)
    cinfo: dict[int, str] = {}
    try:
        titles = await _drain_makemkv_robot_stdout(proc, job_id, cinfo_out=cinfo)
    except RipCancelled:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=120)
        raise
    finally:
        if job_id is not None:
            clear_rip_subprocess(job_id)
    rc = await proc.wait()
    if job_id is not None and await is_job_cancel_requested(job_id):
        raise RipCancelled()
    if rc != 0:
        raise RuntimeError(f"makemkvcon info exited with code {rc}")
    return titles, cinfo


async def run_makemkv_mkv_one_title(
    source: str,
    staging_dir: Path,
    title_idx: int,
    *,
    min_length_seconds: int = 120,
    job_id: int | None = None,
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
    if job_id is not None:
        register_rip_subprocess(job_id, proc)
    try:
        titles = await _drain_makemkv_robot_stdout(proc, job_id)
    except RipCancelled:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=120)
        raise
    finally:
        if job_id is not None:
            clear_rip_subprocess(job_id)
    rc = await proc.wait()
    if job_id is not None and await is_job_cancel_requested(job_id):
        raise RipCancelled()
    if rc != 0:
        raise RuntimeError(f"makemkvcon exited with code {rc}")
    return titles


async def run_makemkvcon(
    source: str,
    staging_dir: Path,
    *,
    min_length_seconds: int = 120,
    job_id: int | None = None,
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
    if job_id is not None:
        register_rip_subprocess(job_id, proc)
    try:
        titles = await _drain_makemkv_robot_stdout(proc, job_id)
    except RipCancelled:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=120)
        raise
    finally:
        if job_id is not None:
            clear_rip_subprocess(job_id)
    rc = await proc.wait()
    if job_id is not None and await is_job_cancel_requested(job_id):
        raise RipCancelled()
    if rc != 0:
        raise RuntimeError(f"makemkvcon exited with code {rc}")
    return titles


async def _ensure_not_cancelled(job_id: int) -> None:
    if await is_job_cancel_requested(job_id):
        raise RipCancelled()


async def rip_one_disc(job: dict[str, Any]) -> None:
    """Drive `makemkvcon` for one job; populate titles rows on success."""
    job_id = job["id"]
    staging_dir = Path(job["staging_dir"])
    source = normalize_makemkv_source(settings.dvd_device)
    source = await map_windows_file_volume_to_disc_if_needed(source)
    log.info("job %d: starting rip from %s -> %s", job_id, source, staging_dir)

    await _ensure_not_cancelled(job_id)
    min_len = 120
    info_attrs, disc_cinfo = await run_makemkv_info(
        source, min_length_seconds=min_len, job_id=job_id
    )
    disc_title = disc_title_from_cinfo(disc_cinfo)
    disc_type = disc_cinfo.get(CINFO_TYPE)
    if disc_title or disc_type:
        await set_job_disc_info(
            job_id, disc_title=disc_title, disc_type=disc_type
        )
        if disc_title:
            log.info("job %d: disc title %r (%s)", job_id, disc_title, disc_type or "?")
    skip_idx = infer_combined_play_all_title_indices(info_attrs, min_length_seconds=min_len)

    selected_indices: list[int] | None = None
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
        candidates = filter_short_tv_extras_below_half_median(
            info_attrs, candidates, min_length_seconds=min_len
        )
        if not candidates:
            raise RuntimeError("after short-title filtering, no titles left to rip")
        selected_indices = candidates
        log.info(
            "job %d: ripping %d title(s) individually (skipped play-all %s)",
            job_id,
            len(candidates),
            sorted(skip_idx),
        )
        for t_idx in candidates:
            await _ensure_not_cancelled(job_id)
            await run_makemkv_mkv_one_title(
                source, staging_dir, t_idx, min_length_seconds=min_len, job_id=job_id
            )
    else:
        await _ensure_not_cancelled(job_id)
        titles_attrs = await run_makemkvcon(
            source, staging_dir, min_length_seconds=min_len, job_id=job_id
        )

    written = sorted(staging_dir.glob("*.mkv"))
    if not written:
        raise RuntimeError("makemkvcon finished but no .mkv files were produced")

    inserted = 0
    if selected_indices is not None:
        # In per-title mode, trust the original `info` metadata for selected
        # indices only. `mkv <idx>` often re-emits TINFO for many titles.
        for t_idx in selected_indices:
            attrs = info_attrs.get(t_idx, {})
            fname = attrs.get(ATTR_OUTPUT_FILENAME)
            if not fname:
                continue
            path = staging_dir / fname
            if not path.is_file():
                continue
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
                title_index=t_idx,
                source_filename=path.name,
                duration_seconds=duration,
                size_bytes=size_bytes,
                chapter_count=chapters,
            )
            inserted += 1
    else:
        name_to_attrs: dict[str, dict[int, str]] = {}
        for t_idx, attrs in titles_attrs.items():
            fname = attrs.get(ATTR_OUTPUT_FILENAME)
            if fname:
                name_to_attrs[fname] = {**attrs, "_title_idx": str(t_idx)}  # type: ignore[dict-item]
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
            except RipCancelled:
                log.info("job %d: rip cancelled by user", job["id"])
                await update_job_status(
                    job["id"], JobStatus.CANCELLED, error_message="Cancelled by user"
                )
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
