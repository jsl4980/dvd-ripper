"""SQLite-backed job queue.

Plain `aiosqlite` with SQL strings. No ORM - the schema is small and stable
enough that an ORM would be more friction than help. Foreign-key cascade
delete on titles makes job deletion safe.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import aiosqlite

from app.config import settings
from app.state import JobKind, JobStatus, StageStatus

SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'unknown',
    disc_label TEXT,
    staging_dir TEXT NOT NULL,
    metadata_id TEXT,
    metadata_json TEXT,
    error_message TEXT,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

CREATE TABLE IF NOT EXISTS titles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    title_index INTEGER NOT NULL,
    source_filename TEXT NOT NULL,
    duration_seconds INTEGER,
    size_bytes INTEGER,
    chapter_count INTEGER,

    -- assignment after review
    assignment_kind TEXT,
    season INTEGER,
    episode INTEGER,
    episode_title TEXT,
    output_filename TEXT,

    -- per-stage status
    encode_status TEXT NOT NULL DEFAULT 'pending',
    encode_error TEXT,
    encoded_filename TEXT,
    publish_status TEXT NOT NULL DEFAULT 'pending',
    publish_error TEXT,
    published_path TEXT,

    UNIQUE(job_id, title_index)
);

CREATE INDEX IF NOT EXISTS idx_titles_job ON titles(job_id);
CREATE INDEX IF NOT EXISTS idx_titles_encode ON titles(encode_status);
CREATE INDEX IF NOT EXISTS idx_titles_publish ON titles(publish_status);

CREATE TABLE IF NOT EXISTS metadata_cache (
    cache_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


async def init_db() -> None:
    """Create the schema if missing. Idempotent."""
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.executescript(SCHEMA)
        await conn.commit()
    async with connect() as conn:
        cur = await conn.execute("PRAGMA table_info(jobs)")
        cols = {row[1] for row in await cur.fetchall()}
        if "cancel_requested" not in cols:
            await conn.execute(
                "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"
            )
            await conn.commit()
        if "disc_title" not in cols:
            await conn.execute("ALTER TABLE jobs ADD COLUMN disc_title TEXT")
            await conn.commit()
        if "disc_type" not in cols:
            await conn.execute("ALTER TABLE jobs ADD COLUMN disc_type TEXT")
            await conn.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    async with aiosqlite.connect(settings.db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        yield conn


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


# ----------------------- jobs -----------------------


async def create_job(staging_dir: str, disc_label: str | None) -> int:
    async with connect() as conn:
        cur = await conn.execute(
            "INSERT INTO jobs (status, kind, staging_dir, disc_label, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                JobStatus.PENDING_RIP.value,
                JobKind.UNKNOWN.value,
                staging_dir,
                disc_label,
                _now(),
                _now(),
            ),
        )
        await conn.commit()
        assert cur.lastrowid is not None
        return cur.lastrowid


async def get_job(job_id: int) -> dict[str, Any] | None:
    async with connect() as conn:
        cur = await conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def list_jobs(status: JobStatus | None = None) -> list[dict[str, Any]]:
    async with connect() as conn:
        if status is None:
            cur = await conn.execute("SELECT * FROM jobs ORDER BY created_at DESC")
        else:
            cur = await conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC",
                (status.value,),
            )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def claim_job(from_status: JobStatus, to_status: JobStatus) -> dict[str, Any] | None:
    """Grab one job in `from_status` and move it to `to_status`.

    Each stage has a single worker so there is no contention; SQLite's WAL
    mode + per-connection serialization is enough to avoid double-claims.
    """
    async with connect() as conn:
        cur = await conn.execute(
            "SELECT * FROM jobs WHERE status = ? ORDER BY id LIMIT 1",
            (from_status.value,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        await conn.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (to_status.value, _now(), row["id"]),
        )
        await conn.commit()
        return dict(row)


async def update_job_status(
    job_id: int,
    status: JobStatus,
    *,
    error_message: str | None = None,
) -> None:
    async with connect() as conn:
        await conn.execute(
            "UPDATE jobs SET status = ?, error_message = ?, cancel_requested = 0, "
            "updated_at = ? WHERE id = ?",
            (status.value, error_message, _now(), job_id),
        )
        await conn.commit()


async def set_job_disc_info(
    job_id: int,
    *,
    disc_title: str | None,
    disc_type: str | None = None,
) -> None:
    """Save disc metadata gathered from MakeMKV's CINFO records."""
    async with connect() as conn:
        await conn.execute(
            "UPDATE jobs SET disc_title = COALESCE(?, disc_title), "
            "disc_type = COALESCE(?, disc_type), updated_at = ? WHERE id = ?",
            (disc_title, disc_type, _now(), job_id),
        )
        await conn.commit()


async def is_job_cancel_requested(job_id: int) -> bool:
    job = await get_job(job_id)
    if job is None:
        return False
    return bool(job.get("cancel_requested", 0))


async def request_cancel_rip(job_id: int) -> str:
    """Cancel a rip: immediate if ``pending_rip``, else set flag if ``ripping``.

    Returns ``cancelled`` (already done or was pending) or ``signaled`` (ripping).
    """
    job = await get_job(job_id)
    if job is None:
        raise LookupError("job not found")
    st = job["status"]
    if st == JobStatus.CANCELLED.value:
        return "cancelled"
    if st == JobStatus.PENDING_RIP.value:
        await update_job_status(job_id, JobStatus.CANCELLED, error_message="Cancelled by user")
        return "cancelled"
    if st == JobStatus.RIPPING.value:
        async with connect() as conn:
            await conn.execute(
                "UPDATE jobs SET cancel_requested = 1, updated_at = ? WHERE id = ?",
                (_now(), job_id),
            )
            await conn.commit()
        return "signaled"
    raise ValueError(f"job status {st!r} cannot be cancelled from the web UI")


async def set_job_metadata(
    job_id: int,
    *,
    kind: JobKind,
    metadata_id: str,
    metadata: dict[str, Any],
) -> None:
    async with connect() as conn:
        await conn.execute(
            "UPDATE jobs SET kind = ?, metadata_id = ?, metadata_json = ?, updated_at = ? "
            "WHERE id = ?",
            (kind.value, metadata_id, json.dumps(metadata), _now(), job_id),
        )
        await conn.commit()


async def merge_job_metadata(job_id: int, patch: dict[str, Any]) -> None:
    """Shallow-merge ``patch`` into the existing ``metadata_json`` blob."""
    async with connect() as conn:
        cur = await conn.execute("SELECT metadata_json FROM jobs WHERE id = ?", (job_id,))
        row = await cur.fetchone()
        meta: dict[str, Any] = {}
        if row and row["metadata_json"]:
            try:
                meta = json.loads(row["metadata_json"])
            except json.JSONDecodeError:
                meta = {}
        meta.update(patch)
        await conn.execute(
            "UPDATE jobs SET metadata_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(meta), _now(), job_id),
        )
        await conn.commit()


async def set_job_kind_and_id(job_id: int, kind: JobKind, metadata_id: str) -> None:
    async with connect() as conn:
        await conn.execute(
            "UPDATE jobs SET kind = ?, metadata_id = ?, updated_at = ? WHERE id = ?",
            (kind.value, metadata_id, _now(), job_id),
        )
        await conn.commit()


async def delete_job(job_id: int) -> None:
    async with connect() as conn:
        await conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        await conn.commit()


async def delete_titles_for_job(job_id: int) -> None:
    async with connect() as conn:
        await conn.execute("DELETE FROM titles WHERE job_id = ?", (job_id,))
        await conn.commit()


# ---------------------- titles ----------------------


async def add_title(
    job_id: int,
    *,
    title_index: int,
    source_filename: str,
    duration_seconds: int | None,
    size_bytes: int | None,
    chapter_count: int | None = None,
) -> int:
    async with connect() as conn:
        cur = await conn.execute(
            "INSERT INTO titles (job_id, title_index, source_filename, duration_seconds, "
            "size_bytes, chapter_count) VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, title_index, source_filename, duration_seconds, size_bytes, chapter_count),
        )
        await conn.commit()
        assert cur.lastrowid is not None
        return cur.lastrowid


async def list_titles(job_id: int) -> list[dict[str, Any]]:
    async with connect() as conn:
        cur = await conn.execute(
            "SELECT * FROM titles WHERE job_id = ? ORDER BY title_index", (job_id,)
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def update_title_assignment(
    title_id: int,
    *,
    assignment_kind: str | None,
    season: int | None,
    episode: int | None,
    episode_title: str | None,
    output_filename: str | None,
) -> None:
    async with connect() as conn:
        await conn.execute(
            "UPDATE titles SET assignment_kind = ?, season = ?, episode = ?, "
            "episode_title = ?, output_filename = ? WHERE id = ?",
            (assignment_kind, season, episode, episode_title, output_filename, title_id),
        )
        await conn.commit()


async def claim_title_for_encode(job_id: int) -> dict[str, Any] | None:
    """Grab one approved title that hasn't started encoding."""
    async with connect() as conn:
        cur = await conn.execute(
            "SELECT * FROM titles WHERE job_id = ? AND assignment_kind IS NOT NULL "
            "AND assignment_kind != 'skip' AND encode_status = ? "
            "ORDER BY title_index LIMIT 1",
            (job_id, StageStatus.PENDING.value),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        await conn.execute(
            "UPDATE titles SET encode_status = ? WHERE id = ?",
            (StageStatus.RUNNING.value, row["id"]),
        )
        await conn.commit()
        return dict(row)


async def claim_title_for_publish(job_id: int) -> dict[str, Any] | None:
    """Grab one encoded title that hasn't published."""
    async with connect() as conn:
        cur = await conn.execute(
            "SELECT * FROM titles WHERE job_id = ? AND encode_status = ? "
            "AND publish_status = ? ORDER BY title_index LIMIT 1",
            (job_id, StageStatus.DONE.value, StageStatus.PENDING.value),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        await conn.execute(
            "UPDATE titles SET publish_status = ? WHERE id = ?",
            (StageStatus.RUNNING.value, row["id"]),
        )
        await conn.commit()
        return dict(row)


async def set_title_encode_result(
    title_id: int,
    *,
    status: StageStatus,
    encoded_filename: str | None = None,
    error: str | None = None,
) -> None:
    async with connect() as conn:
        await conn.execute(
            "UPDATE titles SET encode_status = ?, encoded_filename = ?, encode_error = ? "
            "WHERE id = ?",
            (status.value, encoded_filename, error, title_id),
        )
        await conn.commit()


async def set_title_publish_result(
    title_id: int,
    *,
    status: StageStatus,
    published_path: str | None = None,
    error: str | None = None,
) -> None:
    async with connect() as conn:
        await conn.execute(
            "UPDATE titles SET publish_status = ?, published_path = ?, publish_error = ? "
            "WHERE id = ?",
            (status.value, published_path, error, title_id),
        )
        await conn.commit()


async def all_titles_finished(job_id: int, stage: str) -> tuple[bool, bool]:
    """Return (all_done, any_failed) for a stage column.

    `stage` is ``encode`` or ``publish``. Rows must be fully reviewed first
    (``assignment_kind`` not NULL). Skipped titles should already be marked
    ``done`` for both stages during approval.
    """
    column = f"{stage}_status"
    async with connect() as conn:
        cur = await conn.execute(
            f"SELECT assignment_kind, {column} AS s FROM titles WHERE job_id = ?",
            (job_id,),
        )
        rows = await cur.fetchall()
    if not rows:
        return False, False
    if any(r["assignment_kind"] is None for r in rows):
        return False, False
    any_failed = any(
        r["assignment_kind"] != "skip" and r["s"] == StageStatus.FAILED.value for r in rows
    )
    all_done = all(r["s"] == StageStatus.DONE.value for r in rows)
    return all_done, any_failed


async def mark_title_skipped(title_id: int) -> None:
    """Mark a title as skipped end-to-end (no encode/publish work)."""
    async with connect() as conn:
        await conn.execute(
            "UPDATE titles SET assignment_kind = 'skip', encode_status = ?, "
            "publish_status = ? WHERE id = ?",
            (StageStatus.DONE.value, StageStatus.DONE.value, title_id),
        )
        await conn.commit()


# ------------------- metadata cache -------------------


async def cache_get(key: str) -> dict[str, Any] | None:
    async with connect() as conn:
        cur = await conn.execute(
            "SELECT payload_json FROM metadata_cache WHERE cache_key = ?", (key,)
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return json.loads(row["payload_json"])


async def cache_put(key: str, payload: dict[str, Any] | list[Any]) -> None:
    async with connect() as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO metadata_cache (cache_key, payload_json, fetched_at) "
            "VALUES (?, ?, ?)",
            (key, json.dumps(payload), _now()),
        )
        await conn.commit()


__all__: Iterable[str] = (
    "add_title",
    "all_titles_finished",
    "cache_get",
    "cache_put",
    "claim_job",
    "claim_title_for_encode",
    "claim_title_for_publish",
    "create_job",
    "delete_job",
    "delete_titles_for_job",
    "get_job",
    "init_db",
    "is_job_cancel_requested",
    "list_jobs",
    "list_titles",
    "mark_title_skipped",
    "merge_job_metadata",
    "request_cancel_rip",
    "set_job_disc_info",
    "set_job_kind_and_id",
    "set_job_metadata",
    "set_title_encode_result",
    "set_title_publish_result",
    "update_job_status",
    "update_title_assignment",
)
