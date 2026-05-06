#!/usr/bin/env bash
# Install helper for the Linux desktop (Ubuntu/Debian-oriented).
set -euo pipefail

echo "==> Checking HandBrake NVENC support"
if command -v HandBrakeCLI >/dev/null 2>&1; then
  if HandBrakeCLI --help 2>/dev/null | grep -q nvenc_h264; then
    echo "    HandBrakeCLI lists nvenc_h264 — OK"
  else
    echo "    WARNING: nvenc_h264 not found in HandBrakeCLI --help (check GPU drivers)"
  fi
else
  echo "    ERROR: HandBrakeCLI not on PATH"
  exit 1
fi

echo "==> NVIDIA driver (optional check)"
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi --query-gpu=name --format=csv,noheader | head -1 || true
else
  echo "    nvidia-smi not found — OK if you use CPU x265 profile instead"
fi

echo "==> Done. Next steps:"
echo "    1. Copy project to /opt/dvd-ripper, run 'uv sync' as dvdrip user"
echo "    2. Create /etc/dvd-pipeline.env from .env.example"
echo "    3. sudo cp deploy/dvd-pipeline.service /etc/systemd/system/"
echo "    4. sudo cp deploy/99-dvd-insert.rules /etc/udev/rules.d/"
echo "    5. sudo cp deploy/dvd-insert@.service /etc/systemd/system/"
echo "    6. sudo udevadm control --reload-rules && sudo systemctl daemon-reload"
echo "    7. sudo systemctl enable --now dvd-pipeline.service"
