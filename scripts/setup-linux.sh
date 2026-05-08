#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Repository: $ROOT"
[[ -f "$ROOT/pyproject.toml" ]] || { echo "error: pyproject.toml not found" >&2; exit 1; }
[[ "$(id -u)" -ne 0 ]] || { echo "error: do not run as root; sudo is used internally" >&2; exit 1; }
command -v sudo >/dev/null || { echo "error: sudo required" >&2; exit 1; }

export DEBIAN_FRONTEND=noninteractive

echo "==> apt packages"
sudo apt-get update -y
sudo apt-get install -y \
  ca-certificates curl git \
  ffmpeg handbrake-cli \
  cifs-utils \
  python3 python3-venv \
  build-essential pkg-config \
  libdvd-pkg || true

if dpkg -l libdvd-pkg &>/dev/null; then
  sudo debconf-set-selections <<<"libdvd-pkg libdvd-pkg/build boolean true" || true
  sudo dpkg-reconfigure -f noninteractive libdvd-pkg 2>/dev/null || true
fi

echo "==> uv"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="${HOME}/.local/bin:${PATH}"
command -v uv >/dev/null || { echo "error: uv missing (~/.local/bin not on PATH)" >&2; exit 1; }

echo "==> uv sync"
uv sync

ENV_FILE="$ROOT/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  cp "$ROOT/.env.example" "$ENV_FILE"
  echo "==> Created .env from .env.example"
else
  echo "==> .env already exists (left unchanged)"
fi

echo "==> NOTE: setup does not auto-enable the mock ripper."
echo "         Keep MAKEMKVCON_PATH=makemkvcon for real discs."
echo "         Use the mock only for explicit dev/testing."

if ! command -v makemkvcon >/dev/null 2>&1; then
  echo "==> makemkvcon not found; attempting apt install (makemkv-bin makemkv-oss)"
  sudo apt-get install -y makemkv-bin makemkv-oss || true
fi
if ! command -v makemkvcon >/dev/null 2>&1; then
  echo "==> apt did not provide makemkvcon; attempting snap install (Ubuntu)"
  if command -v snap >/dev/null 2>&1; then
    sudo snap install makemkv || true
    # Best-effort interface connections for optical drive + external media.
    sudo snap connect makemkv:optical-drive || true
    sudo snap connect makemkv:removable-media || true
    if command -v makemkvcon >/dev/null 2>&1; then
      :
    elif command -v makemkv.makemkvcon >/dev/null 2>&1; then
      if ! grep -qE '^MAKEMKVCON_PATH=' "$ENV_FILE"; then
        echo "MAKEMKVCON_PATH=makemkv.makemkvcon" >> "$ENV_FILE"
      else
        sed -i 's#^MAKEMKVCON_PATH=.*#MAKEMKVCON_PATH=makemkv.makemkvcon#' "$ENV_FILE"
      fi
      echo "==> Set MAKEMKVCON_PATH=makemkv.makemkvcon in .env"
    fi
  else
    echo "==> snap is not available; skipping snap fallback"
  fi
fi
if ! command -v makemkvcon >/dev/null 2>&1 && ! command -v makemkv.makemkvcon >/dev/null 2>&1; then
  cat <<'EOF'
ERROR: makemkvcon is still not installed.

Install MakeMKV manually, then rerun this script:
  1) Download package or source from https://www.makemkv.com/download/
  2) Install so `makemkvcon` is available on PATH (or use `makemkv.makemkvcon`)
  3) Verify with: command -v makemkvcon || command -v makemkv.makemkvcon

Then set MAKEMKVCON_PATH in .env if needed.
EOF
  exit 1
fi
command -v nvidia-smi >/dev/null 2>&1 || echo "NOTE: nvidia-smi missing — use ENCODER_PROFILE=x265 for CPU-only encodes."

echo ""
echo "Done. Start:"
echo "  uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"
