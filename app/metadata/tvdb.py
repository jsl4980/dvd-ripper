"""Thin async client for TheTVDB API v4."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import settings
from app.db import cache_get, cache_put

log = logging.getLogger("tvdb")

TVDB_BASE = "https://api4.thetvdb.com/v4"

_tvdb_token: str | None = None


async def _login() -> str:
    global _tvdb_token
    if _tvdb_token:
        return _tvdb_token
    if not settings.tvdb_api_key:
        raise RuntimeError("TVDB_API_KEY is not configured")

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{TVDB_BASE}/login",
            json={"apikey": settings.tvdb_api_key},
            headers={"accept": "application/json"},
        )
        r.raise_for_status()
        body = r.json()
    token = body.get("data", {}).get("token")
    if not token:
        raise RuntimeError(f"TVDB login failed: {body}")
    _tvdb_token = token
    return token


async def _headers() -> dict[str, str]:
    token = await _login()
    return {"accept": "application/json", "Authorization": f"Bearer {token}"}


async def search_series(query: str) -> list[dict[str, Any]]:
    q = query.strip()
    if not q:
        return []
    if not settings.tvdb_api_key:
        log.warning("TVDB_API_KEY missing; TV search disabled")
        return []

    cache_key = f"tvdb:search:series:{q.lower()}"
    cached = await cache_get(cache_key)
    if cached is not None and "results" in cached:
        return cached["results"]

    headers = await _headers()
    params = {"query": q, "type": "series"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{TVDB_BASE}/search", params=params, headers=headers)
        if r.status_code == 401:
            global _tvdb_token
            _tvdb_token = None
            headers = await _headers()
            r = await client.get(f"{TVDB_BASE}/search", params=params, headers=headers)
        r.raise_for_status()
        data = r.json()

    raw = data.get("data", data)
    results = raw if isinstance(raw, list) else []
    await cache_put(cache_key, {"results": results})
    return results


async def get_series_extended(series_id: int) -> dict[str, Any] | None:
    cache_key = f"tvdb:series:extended:{series_id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        return cached

    headers = await _headers()
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{TVDB_BASE}/series/{series_id}/extended", headers=headers)
        if r.status_code == 401:
            global _tvdb_token
            _tvdb_token = None
            headers = await _headers()
            r = await client.get(f"{TVDB_BASE}/series/{series_id}/extended", headers=headers)
        r.raise_for_status()
        data = r.json()

    payload = data.get("data") or data
    await cache_put(cache_key, payload if isinstance(payload, dict) else {"raw": payload})
    return payload if isinstance(payload, dict) else None


async def list_season_numbers(series_id: int) -> list[int]:
    """Return sorted unique season numbers from extended metadata."""
    ext = await get_series_extended(series_id)
    if not ext:
        return []
    seasons = ext.get("seasons") or []
    nums: set[int] = set()
    for s in seasons:
        try:
            n = int(s.get("number", s.get("seasonNumber", -1)))
        except (TypeError, ValueError):
            continue
        if n >= 0:
            nums.add(n)
    return sorted(nums)


async def get_episodes_for_season(series_id: int, season_number: int) -> list[dict[str, Any]]:
    """All episodes for a season (handles pagination).

    TVDB v4: ``GET /series/{id}/episodes/default?page=0&season=N``
    """
    cache_key = f"tvdb:episodes:{series_id}:default:{season_number}"
    cached = await cache_get(cache_key)
    if cached is not None and "episodes" in cached:
        return cached["episodes"]

    headers = await _headers()
    episodes: list[dict[str, Any]] = []
    page = 0
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            url = f"{TVDB_BASE}/series/{series_id}/episodes/default"
            r = await client.get(
                url,
                headers=headers,
                params={"page": page, "season": season_number},
            )
            if r.status_code == 401:
                global _tvdb_token
                _tvdb_token = None
                headers = await _headers()
                r = await client.get(
                    url,
                    headers=headers,
                    params={"page": page, "season": season_number},
                )
            r.raise_for_status()
            body = r.json()
            data = body.get("data", {})
            batch = data.get("episodes", []) if isinstance(data, dict) else []
            if not batch:
                break
            episodes.extend(batch)
            links = body.get("links") or {}
            if not links.get("next"):
                break
            page += 1
            if page > 500:
                log.warning("TVDB pagination exceeded 500 pages for series %s", series_id)
                break

    by_num: dict[int, dict[str, Any]] = {}
    for ep in episodes:
        try:
            num = int(ep.get("number") or ep.get("episodeNumber") or 0)
        except (TypeError, ValueError):
            continue
        by_num[num] = ep
    ordered = [by_num[k] for k in sorted(by_num)]
    await cache_put(cache_key, {"episodes": ordered})
    return ordered
