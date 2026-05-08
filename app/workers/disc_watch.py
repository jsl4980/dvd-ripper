"""Disc-insertion watcher.

The rip worker is event-driven only via the DB (it dequeues ``pending_rip``
jobs). A **udev hook** may POST to ``/api/jobs`` on Linux; this worker polls
when enabled and auto-queues a job when a video disc transitions from absent
to present (or sees a disc already loaded at startup).

**Windows**: ``DVD_DEVICE`` is a drive letter; we probe ``D:/VIDEO_TS`` etc.

**Linux**: ``DVD_DEVICE`` is a block path (e.g. ``/dev/sr0``, ``/dev/cdrom``).
We resolve its current filesystem mount via ``/proc/mounts`` and probe under
that path (automount/Udisks strongly recommended).

Probing avoids MakeMKV and heavy enumeration — those can hang the optical drive
while a rip is in flight.

State machine:

    unknown --present-->  queue (if no in-flight rip exists; covers
                                 startup-with-disc-already-loaded)
    unknown --absent-->   absent
    absent  --present-->  queue (insertion event)
    present --absent-->   absent (eject; no action)
    present --present-->  no-op (debounce; one queue per insertion cycle)

While a rip subprocess is active for any job, polling pauses entirely; the
optical drive is single-user, so any check would either block or lie.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from app.config import settings
from app.db import create_job, list_jobs
from app.state import JobStatus
from app.workers.rip import _active_mkv

log = logging.getLogger("disc_watch")

WatchState = Literal["unknown", "present", "absent"]
Action = Literal["queue_startup", "queue_insert", "log_eject", "noop"]

_DVD_MARKERS = ("VIDEO_TS", "BDMV")
_DETECT_TIMEOUT_SECONDS = 5.0


def parse_drive_letter(dvd_device: str) -> str | None:
    """Extract a single uppercase drive letter from ``D:`` / ``D:\\`` / ``d:/``.

    Returns ``None`` for non-Windows-style values (POSIX ``/dev/sr0``,
    ``disc:0``, ``iso:...``, etc.).
    """
    if not dvd_device:
        return None
    s = dvd_device.strip()
    if len(s) >= 2 and s[1] == ":" and s[0].isalpha():
        return s[0].upper()
    return None


def parse_linux_disc_device(dvd_device: str) -> str | None:
    """``/dev/sr0`` / ``/dev/cdrom`` (symlinks OK via ``realpath``); else ``None``."""
    if not dvd_device:
        return None
    s = dvd_device.strip()
    if not s.startswith("/dev/"):
        return None
    try:
        return os.path.realpath(s)
    except OSError:
        return None


def _unescape_proc_fs_token(tok: str) -> str:
    """``/proc/mounts`` encodes spaces etc. using backslash escapes (e.g. ``\\040``)."""
    return tok.encode("utf-8").decode("unicode_escape")


def find_linux_mountpoint_for_device(device_path: str) -> str | None:
    """Filesystem mount prefix for ``/dev/sr0``, from ``/proc/mounts``."""
    try:
        wanted = os.path.realpath(device_path)
    except OSError:
        return None
    try:
        with open("/proc/mounts", encoding="utf-8") as f:
            for line in f:
                fields = line.split()
                if len(fields) < 4:
                    continue
                try:
                    dev_tok = _unescape_proc_fs_token(fields[0])
                    mount_tok = _unescape_proc_fs_token(fields[1])
                except UnicodeDecodeError:
                    continue
                try:
                    if os.path.realpath(dev_tok) != wanted:
                        continue
                except OSError:
                    continue
                return mount_tok
    except OSError as e:
        log.debug("find_linux_mountpoint_for_device could not read /proc/mounts: %s", e)
        return None
    return None


def is_linux_dvd_present_sync(device_path: str) -> bool | None:
    """Mounted ``/dev/sr*`` probe. ``False`` when unmounted; ``None`` on IO errors."""
    mp = find_linux_mountpoint_for_device(device_path)
    if not mp:
        return False
    try:
        return any(
            os.path.isdir(os.path.join(mp, marker)) for marker in _DVD_MARKERS
        )
    except OSError as e:
        log.debug("is_linux_dvd_present_sync %s: %s", device_path, e)
        return None


async def is_dvd_present_linux(device_path: str) -> bool | None:
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(is_linux_dvd_present_sync, device_path),
            timeout=_DETECT_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        log.debug("linux dvd probe on %s: timed out", device_path)
        return None


def is_dvd_present_sync(drive_letter: str) -> bool | None:
    """Sync filesystem probe. Returns ``None`` on unexpected error.

    Stable for a non-loaded optical drive on modern Windows: ``os.path.isdir``
    on a drive with no media silently returns ``False`` without prompting.
    """
    try:
        return any(
            os.path.isdir(f"{drive_letter}:/{marker}") for marker in _DVD_MARKERS
        )
    except OSError as e:
        log.debug("is_dvd_present_sync %s: %s", drive_letter, e)
        return None


async def is_dvd_present(drive_letter: str) -> bool | None:
    """Async wrapper around the sync probe with a timeout.

    Returns ``None`` if the stat hangs (e.g. another process has the drive
    locked), so the watcher can leave state unchanged for that tick.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(is_dvd_present_sync, drive_letter),
            timeout=_DETECT_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        log.debug("dvd presence probe on %s: timed out", drive_letter)
        return None


def decide_action(
    prev_state: WatchState,
    current_present: bool,
    has_in_flight_rip: bool,
) -> tuple[Action, WatchState]:
    """Pure state-transition decision; returns ``(action, new_state)``.

    Tested independently of the IO so the polling cadence isn't part of the
    contract that gets verified.
    """
    if current_present:
        if prev_state == "present":
            return ("noop", "present")
        if prev_state == "unknown":
            if has_in_flight_rip:
                return ("noop", "present")
            return ("queue_startup", "present")
        return ("queue_insert", "present")
    if prev_state == "present":
        return ("log_eject", "absent")
    return ("noop", "absent")


@dataclass
class DiscWatcher:
    """Loop body separated from the lifetime owner so it can be unit-tested.

    ``poll_once`` runs one detect-decide-act cycle and returns the action
    taken; tests drive it directly without spinning up the asyncio loop.

    Exactly one of ``drive_letter`` or ``linux_device`` must be set.
    """

    drive_letter: str | None = None
    linux_device: str | None = None
    state: WatchState = "unknown"

    def __post_init__(self) -> None:
        n = sum(1 for x in (self.drive_letter, self.linux_device) if x is not None)
        if n != 1:
            raise ValueError("DiscWatcher requires exactly one of drive_letter, linux_device")

    def _probe_target_desc(self) -> str:
        return self.linux_device if self.linux_device is not None else (self.drive_letter or "")

    async def _has_in_flight_rip(self) -> bool:
        if _active_mkv:
            return True
        for status in (JobStatus.PENDING_RIP, JobStatus.RIPPING):
            if await list_jobs(status):
                return True
        return False

    async def _queue_job(self, *, reason: str) -> int:
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        staging_dir = settings.staging_dir / "incoming" / timestamp
        staging_dir.mkdir(parents=True, exist_ok=True)
        label = f"auto-detected ({reason})"
        job_id = await create_job(str(staging_dir), label)
        log.info(
            "auto-queued rip job %d (%s) on %s:",
            job_id,
            reason,
            self._probe_target_desc(),
        )
        return job_id

    async def poll_once(self) -> Action:
        if _active_mkv:
            return "noop"
        if self.linux_device is not None:
            present = await is_dvd_present_linux(self.linux_device)
        else:
            present = await is_dvd_present(self.drive_letter or "")
        if present is None:
            return "noop"
        action, new_state = decide_action(
            self.state, present, await self._has_in_flight_rip()
        )
        self.state = new_state
        if action == "queue_startup":
            await self._queue_job(reason="startup")
        elif action == "queue_insert":
            await self._queue_job(reason="insert")
        elif action == "log_eject":
            log.info("disc ejected from %s:", self._probe_target_desc())
        return action


async def disc_watch_loop() -> None:
    """Poll when enabled; no-op if ``DVD_DEVICE`` is not a supported device form."""
    if not settings.disc_watch_enabled:
        log.info("disc watcher disabled")
        return

    watcher: DiscWatcher | None = None
    probe_desc = ""

    if sys.platform == "win32":
        letter = parse_drive_letter(settings.dvd_device)
        if letter is None:
            log.info(
                "disc watcher: DVD_DEVICE=%r is not a Windows drive letter; skipping",
                settings.dvd_device,
            )
            return
        watcher = DiscWatcher(drive_letter=letter)
        probe_desc = letter
    else:
        dev = parse_linux_disc_device(settings.dvd_device)
        if dev is None:
            log.info(
                "disc watcher: DVD_DEVICE=%r is not a Linux optical device (/dev/...); skipping",
                settings.dvd_device,
            )
            return
        watcher = DiscWatcher(linux_device=dev)
        probe_desc = dev

    assert watcher is not None
    log.info(
        "disc watcher started on %s (poll every %.1fs)",
        probe_desc,
        settings.disc_watch_poll_seconds,
    )
    while True:
        try:
            await watcher.poll_once()
            await asyncio.sleep(settings.disc_watch_poll_seconds)
        except asyncio.CancelledError:
            log.info("disc watcher stopping")
            return
        except Exception:
            log.exception("disc watcher tick failed; backing off")
            await asyncio.sleep(15.0)
