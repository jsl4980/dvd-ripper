"""FastAPI entrypoint and worker lifecycle.

The three workers (rip, encode, publish) run as background asyncio tasks
for the lifetime of the app. Each is a simple poll-the-DB loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import init_db
from app.web.routes import router as web_router
from app.workers.disc_watch import disc_watch_loop
from app.workers.encode import encode_loop
from app.workers.publish import publish_loop
from app.workers.rip import rip_loop

STATIC_DIR = Path(__file__).parent / "web" / "static"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    log.info("starting in env=%s", settings.app_env)
    await init_db()
    settings.staging_dir.mkdir(parents=True, exist_ok=True)
    settings.library_root.mkdir(parents=True, exist_ok=True)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    workers = [
        asyncio.create_task(rip_loop(), name="rip-worker"),
        asyncio.create_task(encode_loop(), name="encode-worker"),
        asyncio.create_task(publish_loop(), name="publish-worker"),
        asyncio.create_task(disc_watch_loop(), name="disc-watcher"),
    ]
    log.info("workers started: rip, encode, publish, disc_watch")
    try:
        yield
    finally:
        log.info("shutting down workers")
        for t in workers:
            t.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


app = FastAPI(lifespan=lifespan, title="DVD to Plex Pipeline")
app.include_router(web_router)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
