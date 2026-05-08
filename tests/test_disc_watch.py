"""Disc-insertion watcher: parsing, decision logic, and full poll cycle."""

from __future__ import annotations

import sys

import pytest


def _reset_app_modules() -> None:
    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        del sys.modules[mod]


def _common_env(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("LIBRARY_ROOT", str(tmp_path / "library"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "pipeline.db"))
    _reset_app_modules()


def test_parse_drive_letter(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.workers.disc_watch import parse_drive_letter

    assert parse_drive_letter("D:") == "D"
    assert parse_drive_letter("d:") == "D"
    assert parse_drive_letter("E:\\") == "E"
    assert parse_drive_letter("F:/") == "F"
    assert parse_drive_letter("/dev/sr0") is None
    assert parse_drive_letter("disc:0") is None
    assert parse_drive_letter("") is None
    assert parse_drive_letter("C") is None
    _reset_app_modules()


def test_parse_linux_disc_device(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.workers.disc_watch import parse_linux_disc_device

    assert parse_linux_disc_device("D:") is None
    assert parse_linux_disc_device("disc:0") is None
    assert parse_linux_disc_device("") is None
    assert parse_linux_disc_device("/dev/sr0") is not None
    assert parse_linux_disc_device("  /dev/sr1  ") is not None
    _reset_app_modules()


def test_unescape_proc_fs_token(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.workers.disc_watch import _unescape_proc_fs_token

    assert _unescape_proc_fs_token("/run/foo\\040bar") == "/run/foo bar"
    _reset_app_modules()


def test_is_linux_dvd_present_under_mount(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.workers import disc_watch as dw

    root = tmp_path / "disc"
    root.mkdir()
    (root / "VIDEO_TS").mkdir()
    monkeypatch.setattr(dw, "find_linux_mountpoint_for_device", lambda _p: str(root))
    assert dw.is_linux_dvd_present_sync("/dev/sr0") is True
    (root / "VIDEO_TS").rmdir()
    assert dw.is_linux_dvd_present_sync("/dev/sr0") is False
    _reset_app_modules()


def test_disc_watcher_requires_single_target(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.workers.disc_watch import DiscWatcher

    with pytest.raises(ValueError, match="exactly one"):
        DiscWatcher()
    with pytest.raises(ValueError, match="exactly one"):
        DiscWatcher(drive_letter="D", linux_device="/dev/sr0")
    _reset_app_modules()


@pytest.mark.parametrize(
    "prev,present,in_flight,expected_action,expected_state",
    [
        # Startup: disc already loaded, no rip in flight -> queue once.
        ("unknown", True, False, "queue_startup", "present"),
        # Startup: disc already loaded but rip already running -> don't double-queue.
        ("unknown", True, True, "noop", "present"),
        # Startup: drive empty.
        ("unknown", False, False, "noop", "absent"),
        # Real insertion event.
        ("absent", True, False, "queue_insert", "present"),
        # Insertion while a rip is in flight (e.g. user swapping drives) - still queue;
        # the new job will wait its turn in the queue.
        ("absent", True, True, "queue_insert", "present"),
        # Steady state: disc still present.
        ("present", True, False, "noop", "present"),
        ("present", True, True, "noop", "present"),
        # Eject: log it once, then quiet.
        ("present", False, False, "log_eject", "absent"),
        ("absent", False, False, "noop", "absent"),
    ],
)
def test_decide_action(
    tmp_path, monkeypatch, prev, present, in_flight, expected_action, expected_state
):
    _common_env(tmp_path, monkeypatch)
    from app.workers.disc_watch import decide_action

    action, new_state = decide_action(prev, present, in_flight)
    assert action == expected_action
    assert new_state == expected_state
    _reset_app_modules()


@pytest.mark.asyncio
async def test_poll_once_queues_on_first_present_at_startup(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.db import init_db, list_jobs
    from app.workers import disc_watch as dw
    from app.workers.disc_watch import DiscWatcher

    await init_db()
    monkeypatch.setattr(dw, "is_dvd_present", _const_async(True))

    watcher = DiscWatcher(drive_letter="D")
    action = await watcher.poll_once()
    assert action == "queue_startup"
    assert watcher.state == "present"

    jobs = await list_jobs()
    assert len(jobs) == 1
    assert jobs[0]["disc_label"] == "auto-detected (startup)"
    assert "incoming" in jobs[0]["staging_dir"]

    # Steady state: still present -> no new job.
    action2 = await watcher.poll_once()
    assert action2 == "noop"
    assert len(await list_jobs()) == 1
    _reset_app_modules()


@pytest.mark.asyncio
async def test_poll_once_queues_on_first_present_at_startup_linux(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.db import init_db, list_jobs
    from app.workers import disc_watch as dw
    from app.workers.disc_watch import DiscWatcher

    await init_db()
    monkeypatch.setattr(dw, "is_dvd_present_linux", _const_async(True))

    watcher = DiscWatcher(linux_device="/dev/sr0")
    action = await watcher.poll_once()
    assert action == "queue_startup"
    assert watcher.state == "present"
    jobs = await list_jobs()
    assert len(jobs) == 1
    _reset_app_modules()


@pytest.mark.asyncio
async def test_poll_once_queues_on_insertion_after_eject(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.db import init_db, list_jobs
    from app.workers import disc_watch as dw
    from app.workers.disc_watch import DiscWatcher

    await init_db()
    states = iter([True, False, True])

    async def stub(*_args, **_kw):
        return next(states)

    monkeypatch.setattr(dw, "is_dvd_present", stub)

    watcher = DiscWatcher(drive_letter="D")
    assert (await watcher.poll_once()) == "queue_startup"
    assert (await watcher.poll_once()) == "log_eject"
    assert (await watcher.poll_once()) == "queue_insert"

    jobs = await list_jobs()
    assert len(jobs) == 2
    labels = sorted(j["disc_label"] for j in jobs)
    assert labels == ["auto-detected (insert)", "auto-detected (startup)"]
    _reset_app_modules()


@pytest.mark.asyncio
async def test_poll_once_skips_when_makemkv_subprocess_active(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)
    from app.db import init_db, list_jobs
    from app.workers import disc_watch as dw
    from app.workers import rip as rip_worker
    from app.workers.disc_watch import DiscWatcher

    await init_db()

    presence_calls: list[bool] = []

    async def stub(*_args):
        presence_calls.append(True)
        return True

    monkeypatch.setattr(dw, "is_dvd_present", stub)

    rip_worker._active_mkv[999] = object()  # type: ignore[assignment]
    try:
        watcher = DiscWatcher(drive_letter="D")
        action = await watcher.poll_once()
        assert action == "noop"
        assert presence_calls == []  # never even probed the drive
        assert await list_jobs() == []
    finally:
        rip_worker._active_mkv.pop(999, None)
    _reset_app_modules()


@pytest.mark.asyncio
async def test_poll_once_noops_when_probe_returns_none(tmp_path, monkeypatch):
    """A drive-locked timeout must not flip state or enqueue anything."""
    _common_env(tmp_path, monkeypatch)
    from app.db import init_db, list_jobs
    from app.workers import disc_watch as dw
    from app.workers.disc_watch import DiscWatcher

    await init_db()
    monkeypatch.setattr(dw, "is_dvd_present", _const_async(None))

    watcher = DiscWatcher(drive_letter="D")
    action = await watcher.poll_once()
    assert action == "noop"
    assert watcher.state == "unknown"
    assert await list_jobs() == []

    monkeypatch.setattr(dw, "is_dvd_present_linux", _const_async(None))
    lw = DiscWatcher(linux_device="/dev/sr0")
    action2 = await lw.poll_once()
    assert action2 == "noop"
    assert lw.state == "unknown"
    _reset_app_modules()


def _const_async(value):
    async def _fn(*_a, **_kw):
        return value

    return _fn
