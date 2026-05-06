"""Thin async client for The Movie Database (TMDB) v3 API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import settings
from app.db import cache_get, cache_put

log = logging.getLogger("tmdb")

TMDB_BASE = "https://api.themoviedb.org/3"


async def search_movies(query: str) -> list[dict[str, Any]]:
    """Return TMDB movie search hits (id, title, release_date, ...)."""
    q = query.strip()
    if not q:
        return []
    if not settings.tmdb_api_key:
        log.warning("TMDB_API_KEY missing; movie search disabled")
        return []

    cache_key = f"tmdb:search:movie:{q.lower()}"
    cached = await cache_get(cache_key)
    if cached is not None and "results" in cached:
        return cached["results"]

    params = {"api_key": settings.tmdb_api_key, "query": q}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{TMDB_BASE}/search/movie", params=params)
        r.raise_for_status()
        data = r.json()

    await cache_put(cache_key, data)
    return data.get("results", [])


async def get_movie(movie_id: int) -> dict[str, Any] | None:
    """Fetch a single movie record."""
    if not settings.tmdb_api_key:
        return None
    cache_key = f"tmdb:movie:{movie_id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        return cached

    params = {"api_key": settings.tmdb_api_key}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{TMDB_BASE}/movie/{movie_id}", params=params)
        r.raise_for_status()
        data = r.json()

    await cache_put(cache_key, data)
    return data
