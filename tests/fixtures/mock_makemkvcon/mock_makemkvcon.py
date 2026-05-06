#!/usr/bin/env python3
"""Mock `makemkvcon` for dev/testing without a real DVD drive.

Implements just enough of the robot-mode CLI surface for our rip worker:

  mock_makemkvcon -r [--noscan] [--minlength=N] mkv <source> all <out_dir>

When invoked, it:

1. Optionally generates a small set of sample MKVs in `samples/` (alongside
   this script) using ffmpeg if they are missing.
2. Copies the samples into <out_dir> as title00.mkv, title01.mkv, ...
3. Emits MakeMKV-style robot output (TCOUNT, TINFO, MSG, PRGT) describing
   the "rip" so the rip worker's parser exercises the real code path.

You can fix the number of fake titles via the env var MOCK_TITLE_COUNT
(default 4) and the per-title duration via MOCK_TITLE_SECONDS (default 30).
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SAMPLES_DIR = HERE / "samples"


def _ffmpeg() -> str | None:
    return shutil.which("ffmpeg")


def _ensure_samples(count: int, seconds: int) -> list[Path]:
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    samples = [SAMPLES_DIR / f"sample_{i:02d}.mkv" for i in range(count)]
    missing = [p for p in samples if not p.exists() or p.stat().st_size == 0]
    if not missing:
        return samples
    ffmpeg = _ffmpeg()
    if ffmpeg is None:
        for p in missing:
            p.write_bytes(b"\x1aE\xdf\xa3" + b"\x00" * 1024)
        return samples
    for i, p in enumerate(missing):
        cmd = [
            ffmpeg, "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", f"testsrc=duration={seconds}:size=720x480:rate=24",
            "-f", "lavfi", "-i", f"sine=frequency={440 + 110 * i}:duration={seconds}",
            "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
            "-c:a", "ac3", "-b:a", "192k",
            str(p),
        ]
        subprocess.run(cmd, check=True)
    return samples


def _emit(line: str) -> None:
    print(line, flush=True)


def _emit_msg(code: int, text: str) -> None:
    _emit(f'MSG:{code},0,1,"{text}","%1","{text}"')


def _emit_tinfo(idx: int, attr: int, value: str) -> None:
    _emit(f'TINFO:{idx},{attr},0,"{value}"')


def _h_m_s(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}:{m:02d}:{s:02d}"


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n //= 1024  # type: ignore[assignment]
    return f"{n} PB"


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-r", action="store_true")
    parser.add_argument("--noscan", action="store_true")
    parser.add_argument("--minlength", type=int, default=120)
    parser.add_argument("--decrypt", action="store_true")
    parser.add_argument("verb", nargs="?")
    parser.add_argument("source", nargs="?")
    parser.add_argument("selection", nargs="?")
    parser.add_argument("dest", nargs="?")
    args, _unknown = parser.parse_known_args()

    if args.verb != "mkv":
        _emit_msg(1, f"mock supports only `mkv` verb (got {args.verb})")
        return 2
    if not args.dest:
        _emit_msg(1, "mock: missing destination directory")
        return 2

    title_count = int(os.environ.get("MOCK_TITLE_COUNT", "4"))
    title_seconds = int(os.environ.get("MOCK_TITLE_SECONDS", "30"))

    out_dir = Path(args.dest)
    out_dir.mkdir(parents=True, exist_ok=True)

    samples = _ensure_samples(title_count, title_seconds)

    _emit_msg(1005, "MakeMKV mock started")
    _emit_msg(3007, f"Using direct disc access mode for {args.source!r}")
    _emit(f"TCOUNT:{title_count}")
    _emit('CINFO:1,6209,"MockShow Season 1"')
    _emit('CINFO:2,0,"MOCK_DVD"')

    for i, sample in enumerate(samples):
        out_path = out_dir / f"title{i:02d}.mkv"
        size = sample.stat().st_size if sample.exists() else 0
        _emit_tinfo(i, 2, f"MockShow S01D01 T{i:02d}")
        _emit_tinfo(i, 8, "6")
        _emit_tinfo(i, 9, _h_m_s(title_seconds))
        _emit_tinfo(i, 10, _human_size(size))
        _emit_tinfo(i, 11, str(size))
        _emit_tinfo(i, 27, out_path.name)

    _emit(f'PRGT:5018,0,"Saving {title_count} titles into MKV files"')
    for i, sample in enumerate(samples):
        out_path = out_dir / f"title{i:02d}.mkv"
        _emit(f'PRGC:5017,0,"Title #{i + 1}"')
        time.sleep(0.05)
        if sample.exists():
            shutil.copy2(sample, out_path)
        else:
            out_path.write_bytes(b"\x1aE\xdf\xa3")
        _emit(f"PRGV:{i + 1},{title_count},65536")

    _emit_msg(5036, f"Copy complete. {title_count} titles saved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
