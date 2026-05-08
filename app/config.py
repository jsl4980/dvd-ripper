"""Application configuration, loaded from `.env` or environment.

All paths are resolved to absolute `Path` objects at load time so workers
can pass them straight to subprocess calls without further normalization.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

EncoderProfile = Literal["nvenc_h264", "nvenc_h265", "x264", "x265"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: Literal["dev", "prod"] = "dev"

    staging_dir: Path = Path("./staging")
    library_root: Path = Path("./library")
    db_path: Path = Path("./pipeline.db")

    makemkvcon_path: str = "makemkvcon"
    handbrakecli_path: str = "HandBrakeCLI"
    ffprobe_path: str = "ffprobe"
    ffmpeg_path: str = "ffmpeg"

    dvd_device: str = "/dev/sr0"

    encoder_profile: EncoderProfile = "nvenc_h264"
    encoder_quality: int = 20
    encoder_preset: str = "slow"
    audio_fallback_bitrate: int = 160

    tmdb_api_key: str = ""
    tvdb_api_key: str = ""

    plex_url: str = "http://localhost:32400"
    plex_token: str = ""
    plex_movies_section_id: int = 0
    plex_tv_section_id: int = 0

    plex_path_local: Path = Path("/mnt/plex")
    plex_path_remote: Path = Path("/mnt/plex")

    host: str = "0.0.0.0"
    port: int = 8000

    poll_interval_seconds: float = 2.0

    # Auto-queue a rip when a fresh disc appears (Windows: drive letter in
    # ``DVD_DEVICE``; Linux: ``/dev/sr*`` via mount + VIDEO_TS / BDMV probe).
    disc_watch_enabled: bool = True
    disc_watch_poll_seconds: float = 5.0

    @field_validator("staging_dir", "library_root", "db_path", mode="after")
    @classmethod
    def _resolve_path(cls, v: Path) -> Path:
        return v.expanduser().resolve()

    def translate_to_plex_host(self, local_path: Path) -> str:
        """Convert this machine's view of a library file to the Plex host's view.

        Example: /mnt/plex/TV Shows/Foo/... -> /srv/media/TV Shows/Foo/...
        Used to build the `?path=` query parameter for Plex's scan API.
        """
        try:
            relative = local_path.relative_to(self.plex_path_local)
        except ValueError:
            return str(local_path)
        return str(self.plex_path_remote / relative).replace("\\", "/")


settings = Settings()
