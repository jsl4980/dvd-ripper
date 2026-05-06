"""State-machine constants for jobs and per-title assignments.

Job status flow:
    pending_rip -> ripping -> needs_review -> approved
                -> encoding -> publishing -> published
    (any state) -> failed (with error_message populated)

Per-title encode/publish status is tracked separately on the `titles` row
because a single job (one disc) emits N titles that encode/publish independently.
"""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    PENDING_RIP = "pending_rip"
    RIPPING = "ripping"
    NEEDS_REVIEW = "needs_review"
    APPROVED = "approved"
    ENCODING = "encoding"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED = "failed"


class JobKind(StrEnum):
    UNKNOWN = "unknown"
    MOVIE = "movie"
    TV = "tv"


class TitleAssignment(StrEnum):
    EPISODE = "episode"
    MOVIE = "movie"
    SPECIAL = "special"
    SKIP = "skip"


class StageStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


TERMINAL_JOB_STATUSES = frozenset({JobStatus.PUBLISHED, JobStatus.FAILED})
