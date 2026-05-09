# Linux production deployment

This host is the rip/encode machine (optical drive + optional NVIDIA GPU).
Plex can stay on another machine; this host should mount the Plex share at
`/mnt/plex` (or change all related env paths consistently).

## 1) Install base packages (Debian/Ubuntu)

Project runtime requires Python 3.11+.

```bash
sudo apt update
sudo apt install -y python3 python3-venv cifs-utils handbrake-cli ffmpeg \
  libdvd-pkg git curl
sudo dpkg-reconfigure libdvd-pkg   # builds libdvdcss
```

Install `uv` if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 2) Install MakeMKV CLI (`makemkvcon`)

Preferred: install MakeMKV so `makemkvcon` is on `PATH`.
On some Ubuntu releases, these packages work:

```bash
sudo apt install -y makemkv-bin makemkv-oss
```

Fallback: snap (less reliable for optical-device passthrough):

```bash
sudo snap install makemkv
sudo snap connect makemkv:optical-drive
sudo snap connect makemkv:removable-media
```

Verify:

```bash
command -v makemkvcon || command -v makemkv.makemkvcon
```

If neither command exists, install from [makemkv.com](https://www.makemkv.com/download/).

**systemd / snap:** `dvd-pipeline.service` sets `PATH` to include `/snap/bin` so
`MAKEMKVCON_PATH=makemkvcon` works when MakeMKV is installed via snap. If you
override `PATH` in a drop-in, keep `/snap/bin` or set `MAKEMKVCON_PATH` to the
full path from `command -v makemkvcon`.

## 3) Configure SMB mount (`/mnt/plex`)

Create `/etc/plex-smb.creds` (mode `600`):

```
username=plexwriter
password=secret
```

`/etc/fstab` example:

```
//plex.example.com/media  /mnt/plex  cifs  credentials=/etc/plex-smb.creds,uid=1000,gid=1000,iocharset=utf8,vers=3.0,nofail,_netdev  0  0
```

Mount and verify:

```bash
sudo mkdir -p /mnt/plex
sudo mount -a
mount | grep /mnt/plex
```

## 4) Create service user and app directory

```bash
sudo useradd -r -m -d /opt/dvd-ripper dvdrip || true
sudo mkdir -p /opt/dvd-ripper
sudo chown -R dvdrip:dvdrip /opt/dvd-ripper
```

Copy this repo to `/opt/dvd-ripper` (or clone there), then create venv as
`dvdrip`:

```bash
sudo -u dvdrip -H bash -lc 'cd /opt/dvd-ripper && uv sync'
sudo -u dvdrip test -x /opt/dvd-ripper/.venv/bin/uvicorn
```

## 5) Create environment file used by systemd

```bash
sudo install -m 600 -o root -g root /opt/dvd-ripper/.env.example /etc/dvd-pipeline.env
sudoedit /etc/dvd-pipeline.env
```

Minimum critical values:

- `APP_ENV=prod`
- `LIBRARY_ROOT=/mnt/plex`
- `DVD_DEVICE=/dev/sr0`
- `MAKEMKVCON_PATH=makemkvcon` (or `makemkv.makemkvcon` for snap)
- `ALLOW_MOCK_MAKEMKVCON=false` (or unset)
- Plex/TMDB/TVDB credentials

## 6) Install and start `systemd` unit

```bash
sudo cp /opt/dvd-ripper/deploy/dvd-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dvd-pipeline.service
```

The unit expects:

- `WorkingDirectory=/opt/dvd-ripper`
- `/opt/dvd-ripper/.venv/bin/uvicorn` exists
- `User=Group=dvdrip`
- `/etc/dvd-pipeline.env` exists
- Uvicorn binds `--host 0.0.0.0 --port 8000` (reachable on the LAN; open firewall if needed)

## 7) Optional: auto-queue job on disc insert (udev)

```bash
sudo cp /opt/dvd-ripper/deploy/99-dvd-insert.rules /etc/udev/rules.d/
sudo cp /opt/dvd-ripper/deploy/dvd-insert@.service /etc/systemd/system/
sudo udevadm control --reload-rules
sudo systemctl daemon-reload
```

`dvd-insert@.service` posts to `http://127.0.0.1:8000/...` on this host only.
Change that URL if you move the API to another host/port.

## 8) Verify runtime

```bash
sudo systemctl status dvd-pipeline.service --no-pager
sudo journalctl -u dvd-pipeline.service -n 100 --no-pager
curl -sS http://127.0.0.1:8000/healthz
curl -sS -X POST http://127.0.0.1:8000/api/jobs -H 'Content-Type: application/json' -d '{}'
```

From another machine on the network, use `http://<this-host-ip>:8000/` (not
`localhost`). If the page does not load, check `sudo ufw status` and allow
`8000/tcp` when using UFW.

If service shows `status=217/USER`, the `dvdrip` user/group is missing or invalid.
