# DVD to Plex Pipeline

A small Python orchestrator that wraps **MakeMKV** + **HandBrake** CLIs with a single-page web UI for the per-disc episode/movie confirmation step. Designed for low volume (10s of discs + ongoing trickle), one USB DVD drive, and a Plex server on a separate host.

## Pipeline

```
Insert disc -> Rip (makemkvcon) -> Review (web UI, ~30s) -> Encode (HandBrakeCLI, NVENC H.264) -> Publish (Plex naming + SMB move + Plex scan)
```

Each stage is a worker pulling jobs from a SQLite-backed queue. State lives in `pipeline.db`; files move through `staging/` before landing in the SMB-mounted Plex library at `/mnt/plex`.

## Quick start (dev on Windows laptop)

Install host tools:

```powershell
winget install astral-sh.uv
winget install GuinpinSoft.MakeMKV
winget install HandBrake.HandBrake.CLI
winget install Gyan.FFmpeg
```

Set up the project:

```powershell
uv sync
copy .env.example .env
# Edit .env: set MAKEMKVCON_PATH, HANDBRAKECLI_PATH, FFPROBE_PATH to the .exe paths
# (or leave as-is if those binaries are on PATH)
```

Run the dev server:

```powershell
uv run uvicorn app.main:app --reload --port 8000
```

Then open http://localhost:8000.

## Dev without a real DVD drive

The mock MakeMKV binary at `tests/fixtures/mock_makemkvcon/mock_makemkvcon.py` emits realistic output and copies sample MKVs into staging. Point `MAKEMKVCON_PATH` at it in `.env` and trigger a fake rip via the web UI ("Add mock job" button in dev mode).

## Production (Linux desktop with NVIDIA 1050 Ti)

See [`deploy/README.md`](deploy/README.md) for systemd unit, udev rule, SMB mount, and install script details.

## Project layout

```
app/
  main.py            FastAPI entrypoint + worker lifecycle
  config.py          pydantic-settings, env-driven
  db.py              SQLite schema + helpers
  state.py           Job/title state-machine enums
  workers/
    rip.py           makemkvcon wrapper
    encode.py        HandBrakeCLI wrapper
    publish.py       Plex naming + SMB move + Plex scan
  metadata/
    tmdb.py          Movies
    tvdb.py          TV shows
  web/
    routes.py
    templates/
      index.html     The one htmx page
tests/
  fixtures/
    mock_makemkvcon/ Fake binary for dev
    sample_mkvs/     Tiny pre-ripped clips
deploy/
  dvd-pipeline.service
  99-dvd-insert.rules
  install.sh
```

## Encoder default

NVENC H.264, CQ 20, `slow` preset. Tuned for DVD source (480p/576p) and Plex client compatibility (Roku, Chromecast, mobile). DTS audio is transcoded to AAC at encode time so Chromecast/mobile direct-play works without on-the-fly transcoding by Plex. AC3/AAC/MP3 audio is passed through verbatim. Forced subs are burned in; full bitmap subs kept as soft tracks.

Switch encoders in `.env`: `ENCODER_PROFILE=nvenc_h265` or `x265`.
