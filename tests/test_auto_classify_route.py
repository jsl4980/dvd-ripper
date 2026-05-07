"""Tests for ``GET /api/jobs/{id}/auto-classify``."""

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


@pytest.mark.asyncio
async def test_auto_classify_tv_uses_disc_title_and_returns_top_candidate(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import add_title, create_job, init_db, set_job_disc_info
    from app.main import app
    from app.metadata import tvdb

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j1"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), None)
    await set_job_disc_info(job_id, disc_title="VAMPIRE_DIARIES_SEASON2_DISC2", disc_type="DVD disc")
    for i in range(1, 6):
        await add_title(job_id, title_index=i, source_filename=f"C{i}.mkv", duration_seconds=2500, size_bytes=1)

    captured: dict[str, str] = {}

    async def fake_search_series(query: str):
        captured["q"] = query
        return [
            {
                "objectID": "series-95491",
                "tvdb_id": 95491,
                "name": "The Vampire Diaries",
                "first_air_time": "2009-09-10",
            },
            {
                "objectID": "series-9999",
                "tvdb_id": 9999,
                "name": "Vampire Chronicles",
                "first_air_time": "2003-01-01",
            },
        ]

    monkeypatch.setattr(tvdb, "search_series", fake_search_series)

    with TestClient(app) as client:
        r = client.get(f"/api/jobs/{job_id}/auto-classify")
        assert r.status_code == 200
        body = r.json()

    assert captured["q"] == "Vampire Diaries"
    assert body["kind"] == "tv"
    assert body["parsed"]["name"] == "Vampire Diaries"
    assert body["parsed"]["season"] == 2
    assert body["parsed"]["disc"] == 2
    assert body["candidates"][0]["id"] == 95491
    assert body["candidates"][0]["name"] == "The Vampire Diaries"
    assert body["candidates"][0]["year"] == 2009
    _reset_app_modules()


@pytest.mark.asyncio
async def test_auto_classify_movie_with_year(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import add_title, create_job, init_db, set_job_disc_info
    from app.main import app
    from app.metadata import tmdb

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j2"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), None)
    await set_job_disc_info(job_id, disc_title="THE_DARK_KNIGHT_2008", disc_type="DVD disc")
    await add_title(job_id, title_index=1, source_filename="C1.mkv", duration_seconds=9120, size_bytes=1)

    captured: dict[str, str] = {}

    async def fake_search_movies(query: str):
        captured["q"] = query
        return [
            {"id": 155, "title": "The Dark Knight", "release_date": "2008-07-18", "overview": "x"},
        ]

    monkeypatch.setattr(tmdb, "search_movies", fake_search_movies)

    with TestClient(app) as client:
        r = client.get(f"/api/jobs/{job_id}/auto-classify")
        assert r.status_code == 200
        body = r.json()

    assert captured["q"] == "The Dark Knight"
    assert body["kind"] == "movie"
    assert body["parsed"]["year"] == 2008
    assert body["candidates"][0] == {
        "id": 155, "name": "The Dark Knight", "year": 2008, "overview": "x",
    }
    _reset_app_modules()


@pytest.mark.asyncio
async def test_auto_classify_falls_back_to_durations_when_disc_title_missing(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import add_title, create_job, init_db
    from app.main import app

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j3"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), None)
    for i in range(1, 6):
        await add_title(job_id, title_index=i, source_filename=f"C{i}.mkv", duration_seconds=2500, size_bytes=1)

    with TestClient(app) as client:
        r = client.get(f"/api/jobs/{job_id}/auto-classify")
        assert r.status_code == 200
        body = r.json()

    assert body["parsed"] is None
    assert body["kind"] == "tv"  # 5 evenly-paced titles -> tv from durations
    assert body["candidates"] == []
    _reset_app_modules()


@pytest.mark.asyncio
async def test_auto_classify_swallows_metadata_lookup_failure(tmp_path, monkeypatch):
    """Network-side failure must still return a usable payload (no candidates)."""
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import create_job, init_db, set_job_disc_info
    from app.main import app
    from app.metadata import tvdb

    await init_db()
    staging = tmp_path / "staging" / "incoming" / "j4"
    staging.mkdir(parents=True)
    job_id = await create_job(str(staging), None)
    await set_job_disc_info(job_id, disc_title="LOST_S5D1", disc_type=None)

    async def boom(_q):
        raise RuntimeError("TVDB down")

    monkeypatch.setattr(tvdb, "search_series", boom)

    with TestClient(app) as client:
        r = client.get(f"/api/jobs/{job_id}/auto-classify")
        assert r.status_code == 200
        body = r.json()

    assert body["parsed"]["name"] == "Lost"
    assert body["parsed"]["season"] == 5
    assert body["candidates"] == []
    _reset_app_modules()


@pytest.mark.asyncio
async def test_auto_classify_404_for_unknown_job(tmp_path, monkeypatch):
    _common_env(tmp_path, monkeypatch)

    from fastapi.testclient import TestClient

    from app.db import init_db
    from app.main import app

    await init_db()
    with TestClient(app) as client:
        r = client.get("/api/jobs/9999/auto-classify")
        assert r.status_code == 404
    _reset_app_modules()
