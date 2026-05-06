# Linux production deployment

The pipeline runs on the **rip/encode host** (Linux desktop with the DVD drive and GPU). Plex stays on a separate machine; this host mounts the Plex library share at `/mnt/plex` (see `.env.example`).

## Packages (Debian/Ubuntu)

```bash
sudo apt update
sudo apt install -y python3.12 python3.12-venv cifs-utils handbrake-cli ffmpeg \
    libdvd-pkg git curl
sudo dpkg-reconfigure libdvd-pkg   # builds libdvdcss
```

MakeMKV is not packaged officially — install from [makemkv.com](https://www.makemkv.com/) or build from source so `makemkvcon` is on `PATH`.

## SMB mount (`/mnt/plex`)

Create `/etc/plex-smb.creds` (mode `600`):

```
username=plexwriter
password=secret
```

`/etc/fstab`:

```
//plex.example.com/media  /mnt/plex  cifs  credentials=/etc/plex-smb.creds,uid=1000,gid=1000,iocharset=utf8,vers=3.0,nofail,_netdev  0  0
```

## App user + tree

```bash
sudo useradd -r -m -d /opt/dvd-ripper dvdrip
sudo mkdir -p /opt/dvd-ripper
sudo chown -R dvdrip:dvdrip /opt/dvd-ripper
```

Copy the repo into `/opt/dvd-ripper`, then as `dvdrip`:

```bash
cd /opt/dvd-ripper
uv sync
cp .env.example /etc/dvd-pipeline.env   # root-owned secrets; edit values
```

Point `LIBRARY_ROOT=/mnt/plex`, set `MAKEMKVCON_PATH`, `HANDBRAKECLI_PATH`, API keys, and Plex settings.

## systemd

`dvd-pipeline.service` expects:

- `WorkingDirectory=/opt/dvd-ripper`
- venv at `/opt/dvd-ripper/.venv`
- `EnvironmentFile=/etc/dvd-pipeline.env` (optional `-` prefix in unit file)

```bash
sudo cp deploy/dvd-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dvd-pipeline.service
```

`RequiresMountsFor=/mnt/plex` prevents startup before the share is mounted.

## Disc insert hook

```bash
sudo cp deploy/99-dvd-insert.rules /etc/udev/rules.d/
sudo cp deploy/dvd-insert@.service /etc/systemd/system/
sudo udevadm control --reload-rules
```

Adjust `dvd-insert@.service` if the pipeline listens on a non-loopback address.

## Smoke test

```bash
curl -sS -X POST http://127.0.0.1:8000/api/jobs -H 'Content-Type: application/json' -d '{}'
curl -sS http://127.0.0.1:8000/healthz
```
