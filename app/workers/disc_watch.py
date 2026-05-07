"""Disc-insertion watcher.

The rip worker is event-driven only via the DB (it dequeues ``pending_rip``
jobs). On Linux a udev hook is expected to POST to ``/api/jobs`` when a disc
is inserted; on Windows there is no equivalent hook, so this worker polls
the configured DVD drive and auto-queues a job when a new disc shows up.

Detection strategy: check for the standard video-disc directories
(``VIDEO_TS`` for DVD, ``BDMV`` for Blu-ray) at the drive root. Probing
those paths is essentially a single file stat and does not trigger Windows
auto-play UI. We *avoid* shelling out to MakeMKV or running long enumerations
(those can hang the optical drive while a rip is in flight).

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


def is_dvd_present_sync(drive_letter: str) -> bool | None:
    """Sync filesystem probe. Returns ``None`` on unexpected error.

    Stable for a non-loaded optical drive on modern Windows: ``os.path.isdir``
    on a drive with no media silently returns ``False`` without prompting.
    """
    try:
        for marker in _DVD_MARKERS:
            if os.path.isdir(f"{drive_letter}:/{marker}"):
                return True
        return False
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
    """

    drive_letter: str
    state: WatchState = "unknown"

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
            "auto-queued rip job %d (%s) on drive %s:",
            job_id,
            reason,
            self.drive_letter,
        )
        return job_id

    async def poll_once(self) -> Action:
        if _active_mkv:
            return "noop"
        present = await is_dvd_present(self.drive_letter)
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
            log.info("disc ejected from drive %s:", self.drive_letter)
        return action


async def disc_watch_loop() -> None:
    """Main entrypoint; quietly no-ops if disabled or DVD_DEVICE isn't a drive letter."""
    if not settings.disc_watch_enabled:
        log.info("disc watcher disabled")
        return
    if sys.platform != "win32":
        log.info("disc watcher only supports Windows drive letters; skipping")
        return
    letter = parse_drive_letter(settings.dvd_device)
    if letter is None:
        log.info(
            "disc watcher: DVD_DEVICE=%r is not a Windows drive letter; skipping",
            settings.dvd_device,
        )
        return

    watcher = DiscWatcher(drive_letter=letter)
    log.info(
        "disc watcher started on %s: (poll every %.1fs)",
        letter,
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
