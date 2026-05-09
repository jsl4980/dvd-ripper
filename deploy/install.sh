#!/usr/bin/env bash
# Deployment preflight helper for Linux desktop/server (Ubuntu/Debian-oriented).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="/opt/dvd-ripper"
SERVICE_FILE="/etc/systemd/system/dvd-pipeline.service"

echo "==> Repository: $ROOT"
[[ -f "$ROOT/pyproject.toml" ]] || { echo "ERROR: pyproject.toml not found at $ROOT" >&2; exit 1; }
command -v sudo >/dev/null 2>&1 || { echo "ERROR: sudo not found" >&2; exit 1; }

echo "==> Checking required binaries"
for bin in HandBrakeCLI ffmpeg ffprobe; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "ERROR: $bin not found on PATH" >&2
    exit 1
  }
done

if HandBrakeCLI --help 2>/dev/null | grep -q nvenc_h264; then
  echo "    HandBrakeCLI lists nvenc_h264 — OK"
else
  echo "    WARNING: nvenc_h264 not found in HandBrakeCLI --help (check GPU drivers or use x265)"
fi

if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi --query-gpu=name --format=csv,noheader | head -1 || true
else
  echo "    nvidia-smi not found — OK if you use CPU x265 profile instead"
fi

if ! command -v makemkvcon >/dev/null 2>&1; then
  echo "ERROR: native makemkvcon not on PATH (snap MakeMKV is not supported)." >&2
  echo "       Install from https://www.makemkv.com/download/ or distro packages." >&2
  exit 1
fi

echo "==> Checking dvdrip user and target tree"
if getent passwd dvdrip >/dev/null 2>&1; then
  echo "    dvdrip user exists"
else
  echo "ERROR: dvdrip user does not exist. Create it first:" >&2
  echo "       sudo useradd -r -m -d $TARGET_DIR dvdrip" >&2
  exit 1
fi

[[ -d "$TARGET_DIR" ]] || {
  echo "ERROR: $TARGET_DIR does not exist" >&2
  exit 1
}

if [[ ! -x "$TARGET_DIR/.venv/bin/uvicorn" ]]; then
  echo "ERROR: missing $TARGET_DIR/.venv/bin/uvicorn" >&2
  echo "       Run as dvdrip: cd $TARGET_DIR && uv sync" >&2
  exit 1
fi

[[ -f /etc/dvd-pipeline.env ]] || {
  echo "ERROR: /etc/dvd-pipeline.env missing" >&2
  echo "       Create with: sudo install -m 600 -o root -g root $TARGET_DIR/.env.example /etc/dvd-pipeline.env" >&2
  exit 1
}

[[ -f "$SERVICE_FILE" ]] || {
  echo "ERROR: $SERVICE_FILE missing" >&2
  echo "       Install with: sudo cp $TARGET_DIR/deploy/dvd-pipeline.service $SERVICE_FILE" >&2
  exit 1
}

echo "==> Preflight passed"
echo "Next commands:"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl restart dvd-pipeline.service"
echo "  sudo systemctl status dvd-pipeline.service --no-pager"
echo "  curl -sS http://127.0.0.1:8000/healthz"
