#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

function Refresh-Path {
  $machine = [Environment]::GetEnvironmentVariable("Path", "Machine")
  $user = [Environment]::GetEnvironmentVariable("Path", "User")
  $env:Path = "$machine;$user"
}

Write-Host "==> Repository: $RepoRoot"

if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
  throw "winget not found. Install App Installer (Windows Package Manager), then re-run."
}

$wingetPkgs = @(
  @{ Id = "astral-sh.uv"; Name = "uv" },
  @{ Id = "GuinpinSoft.MakeMKV"; Name = "MakeMKV" },
  @{ Id = "HandBrake.HandBrake.CLI"; Name = "HandBrake CLI" },
  @{ Id = "Gyan.FFmpeg"; Name = "FFmpeg" },
  @{ Id = "Git.Git"; Name = "Git" }
)

foreach ($p in $wingetPkgs) {
  Write-Host "==> winget: $($p.Name) ($($p.Id))"
  winget install --id $p.Id --accept-source-agreements --accept-package-agreements --silent --disable-interactivity
  Refresh-Path
}

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
  throw "uv not on PATH. Open a new terminal after winget installs, then re-run this script."
}

Write-Host "==> uv sync"
uv sync

$envFile = Join-Path $RepoRoot ".env"
$example = Join-Path $RepoRoot ".env.example"
if (-not (Test-Path $envFile)) {
  if (-not (Test-Path $example)) { throw ".env.example missing" }
  Copy-Item $example $envFile
  Write-Host "==> Created .env from .env.example"
} else {
  Write-Host "==> .env already exists (left unchanged)"
}

$mockRel = "tests/fixtures/mock_makemkvcon/mock_makemkvcon.py"
$mockAbs = Join-Path $RepoRoot $mockRel
if (Test-Path $mockAbs) {
  $raw = Get-Content $envFile -Raw
  if ($raw -match '(?m)^MAKEMKVCON_PATH=makemkvcon' -or $raw -match '(?m)^MAKEMKVCON_PATH=makemkvcon64\.exe') {
    $raw = $raw -replace '(?m)^MAKEMKVCON_PATH=.*$', "MAKEMKVCON_PATH=$mockRel"
    $raw = $raw -replace '(?m)^DVD_DEVICE=.*$', "DVD_DEVICE=disc:0"
    if ($raw -match '(?m)^ALLOW_MOCK_MAKEMKVCON=') {
      $raw = $raw -replace '(?m)^ALLOW_MOCK_MAKEMKVCON=.*$', "ALLOW_MOCK_MAKEMKVCON=true"
    } else {
      $raw = $raw.TrimEnd() + "`r`nALLOW_MOCK_MAKEMKVCON=true`r`n"
    }
    [System.IO.File]::WriteAllText($envFile, $raw)
    Write-Host "==> Set MAKEMKVCON_PATH -> $mockRel, DVD_DEVICE=disc:0, ALLOW_MOCK_MAKEMKVCON=true"
  }
}

Write-Host ""
Write-Host "Done. Start:"
Write-Host "  uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000"
