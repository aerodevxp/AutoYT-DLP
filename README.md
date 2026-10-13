# auto_ytdlp

Bulk playlist downloader built on [yt-dlp](https://github.com/yt-dlp/yt-dlp),
with cross-folder content dedupe, so you never end up with the same video twice
in different folders.

## Features

- **Playlist list file** — one line per playlist, comments and blanks allowed.
- **Audio or video per entry** — audio downloads to `.m4a`, video to `.mkv`.
- **Content dedupe** — fingerprints existing files so re-downloads of things you
  already own get culled, regardless of filename or folder.
- **Global ID archive** — known videos are skipped outright, across all playlists.
- **Never overwrites** — existing files are left alone.
- **Stale `.part` cleanup** — killed runs self-heal.

## Requirements

- Python 3.10+
- `ffmpeg` on `PATH` (only for merging video / converting audio)
- A JS runtime for YouTube's n-sig challenge — see setup below

## Setup

```bash
git clone <this repo> auto_ytdlp && cd auto_ytdlp

python -m venv venv
./venv/bin/pip install -U "yt-dlp[default]" "yt-dlp[curl-cffi]" python-dotenv

# local JS runtime (stays in the project dir)
curl -fsSL https://deno.land/install.sh | DENO_INSTALL="$PWD/.deno" sh

cp .env.example .env
$EDITOR .env          # set PLAYLIST_FILE, SCAN_ROOTS, etc.
