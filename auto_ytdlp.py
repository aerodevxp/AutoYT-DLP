#!/usr/bin/env python3
"""
auto_ytdlp.py — Bulk playlist downloader built on yt-dlp, with cross-folder
content dedupe, timestamped logging, and a fast "already synced" pre-check.

Config is loaded from a .env file (see .env.example).

Playlist list file, one entry per line:

    customcomment;url;videoORaudio;downloaddir

  - Blank lines and full-line comments (leading '#') are ignored.
  - Inline comments (' # ...') are stripped.
  - 'kind' defaults to "video" if empty.
  - 'downloaddir' defaults to a folder named after the comment if empty.

Fast pre-check (always on):
  For playlist/channel URLs, the entry list is fetched in FLAT mode (one
  request). If every ID is already in the archive, the playlist is skipped
  without yt-dlp ever resolving a single video. If only some are missing,
  yt-dlp is told to process just those. If flat extraction looks unsupported
  (no entries, or a single entry for something that is clearly a collection),
  it falls back to a normal full run.

Dedupe (folder-independent):
  - A GLOBAL archive records known video IDs so yt-dlp skips them outright.
  - A GLOBAL content-hash library catches same-content files whose names
    don't carry an ID: on download, a duplicate is deleted and its ID archived.
  - Seed it with:  ./auto_ytdlp.py --scan [DIR ...]

Every run also prunes dead library entries and indexes any new files (local,
no network). Use --no-sync to skip that.
"""

import argparse
import atexit
import datetime
import fnmatch
import hashlib
import json
import os
import re
import shutil
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

try:
    import yt_dlp
except ImportError:
    sys.exit("yt-dlp not installed. Run: pip install -U 'yt-dlp[default]'")

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("python-dotenv not installed. Run: pip install -U python-dotenv")


def _bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

COOKIES_FILE     = os.getenv("COOKIES_FILE", "")
PLAYLIST_FILE    = os.getenv("PLAYLIST_FILE", "playlists.txt")
VIDEO_TEMPLATE   = os.getenv(
    "VIDEO_TEMPLATE",
    "%(playlist_index)03d - %(title)s [%(id)s].%(ext)s",
)
AUDIO_TEMPLATE   = os.getenv(
    "AUDIO_TEMPLATE",
    "%(playlist_index)03d - %(title)s [%(id)s].%(ext)s",
)
MERGE_FORMAT     = os.getenv("MERGE_FORMAT", "mp4")
FORMAT_SELECTOR  = os.getenv("FORMAT_SELECTOR", "b[ext=mp4]/b")
AUDIO_FORMAT     = os.getenv("AUDIO_FORMAT", "bestaudio[ext=m4a]/bestaudio")
JS_RUNTIME_DIR   = os.getenv("JS_RUNTIME_DIR", "").strip()
CLEAN_PART_FILES = _bool("CLEAN_PART_FILES", "true")

# --- Post-processing toggles (all off by default = no ffmpeg) ---
AUDIO_CONVERT    = _bool("AUDIO_CONVERT", "false")
EMBED_THUMBNAIL  = _bool("EMBED_THUMBNAIL", "false")
EMBED_METADATA   = _bool("EMBED_METADATA", "false")
AUDIO_CODEC      = os.getenv("AUDIO_CODEC", "mp3")
AUDIO_QUALITY    = os.getenv("AUDIO_QUALITY", "192")

REMOTE_COMPONENTS = [c.strip() for c in
                     os.getenv("REMOTE_COMPONENTS", "ejs:github").split(",")
                     if c.strip()]
CACHE_DIR  = os.getenv("CACHE_DIR", "./.cache/yt-dlp")
VERBOSE    = _bool("VERBOSE", "false")
IMPERSONATE = os.getenv("IMPERSONATE", "").strip()
EXTRACTOR_ARGS_RAW = os.getenv("EXTRACTOR_ARGS", "")

# --- Logging ---
LOG_FILE = os.getenv("LOG_FILE", "./auto_ytdlp.log")
LOG_PROGRESS_INTERVAL = float(os.getenv("LOG_PROGRESS_INTERVAL", "30"))
HEARTBEAT_INTERVAL    = float(os.getenv("HEARTBEAT_INTERVAL", "60"))
SCAN_LOG_EVERY        = int(os.getenv("SCAN_LOG_EVERY", "200"))

# --- Rate limiting / politeness ---
SLEEP_INTERVAL          = float(os.getenv("SLEEP_INTERVAL", "0"))
MAX_SLEEP_INTERVAL      = float(os.getenv("MAX_SLEEP_INTERVAL", "0"))
SLEEP_INTERVAL_REQUESTS = float(os.getenv("SLEEP_INTERVAL_REQUESTS", "0"))
CONCURRENT_FRAGMENTS    = int(os.getenv("CONCURRENT_FRAGMENTS", "4"))

# --- Dedupe ---
DEDUPE         = _bool("DEDUPE", "true")
GLOBAL_ARCHIVE = os.getenv("GLOBAL_ARCHIVE", "./.global_archive.txt")
LIBRARY_FILE   = os.getenv("LIBRARY_FILE", "./.dedupe_library.json")
DEDUPE_KEEP    = os.getenv("DEDUPE_KEEP", "existing").strip().lower()

# --- Scan ---
SCAN_ROOTS = [r.strip() for r in os.getenv("SCAN_ROOTS", "").split(";")
              if r.strip()]
SCAN_EXCLUDE = [e.strip() for e in os.getenv("SCAN_EXCLUDE", "").split(";")
                if e.strip()]
SCAN_NEW_ONLY = False   # flipped on for the automatic pre-run sync

_DEFAULT_SCAN_EXTS = "mp4,mkv,webm,mp3,m4a,opus,flac,wav,ogg,aac"
SCAN_EXTS = {
    "." + e.strip().lstrip(".").lower()
    for e in (os.getenv("SCAN_EXTS", "").strip() or _DEFAULT_SCAN_EXTS).split(",")
    if e.strip()
}
if not SCAN_EXTS:
    SCAN_EXTS = {"." + e for e in _DEFAULT_SCAN_EXTS.split(",")}
# ---------------------------------------------------------------------------

PART_PATTERNS = ("*.part", "*.part-Frag*", "*.ytdl")

ID_PATTERNS = [
    re.compile(r"[\[\(]([A-Za-z0-9_-]{11})[\]\)]"),
    re.compile(r"\s-\s([A-Za-z0-9_-]{11})(?=\.[a-z0-9]+$)"),
    re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})"),
]

# URL fragments that suggest a playlist/channel rather than a single video
COLLECTION_HINTS = ("list=", "/playlist", "/playlists", "/@", "/channel/",
                    "/c/", "/user/", "/videos", "/model/", "/pornstar/")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_log_lock = threading.Lock()
_log_fh = None
_last_activity = time.time()
_hb_stop = threading.Event()


def log(msg: str, console: bool = True):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    with _log_lock:
        if _log_fh:
            _log_fh.write(line + "\n")
            _log_fh.flush()
        if console:
            print(line, flush=True)


def touch():
    global _last_activity
    _last_activity = time.time()


def init_log():
    global _log_fh
    if not LOG_FILE:
        return
    p = os.path.abspath(os.path.expanduser(LOG_FILE))
    os.makedirs(os.path.dirname(p), exist_ok=True)
    _log_fh = open(p, "a", encoding="utf-8")
    atexit.register(_log_fh.close)


def _heartbeat_loop():
    last = time.time()
    while not _hb_stop.wait(5):
        now = time.time()
        if now - last >= HEARTBEAT_INTERVAL:
            last = now
            log(f"[heartbeat] idle {now - _last_activity:.0f}s — still running")


def start_heartbeat():
    threading.Thread(target=_heartbeat_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_video_id(name: str):
    for pat in ID_PATTERNS:
        m = pat.search(name)
        if m:
            return m.group(1)
    return None


def looks_like_collection(url: str) -> bool:
    u = url.lower()
    return any(h in u for h in COLLECTION_HINTS)


def parse_extractor_args(raw: str) -> dict:
    result: dict = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        name, _, kv = part.partition(":")
        opts = result.setdefault(name.strip(), {})
        for pair in kv.split(","):
            pair = pair.strip()
            if "=" not in pair:
                continue
            k, _, v = pair.partition("=")
            opts.setdefault(k.strip(), []).append(v.strip())
    return result


def base_ydl_opts(cookie_file: str | None = None) -> dict:
    """Shared quiet options for metadata-only extraction."""
    opts = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "remote_components": REMOTE_COMPONENTS,
        "cachedir": os.path.abspath(os.path.expanduser(CACHE_DIR)),
    }
    if cookie_file:
        opts["cookiefile"] = os.path.expanduser(cookie_file)
    if IMPERSONATE:
        opts["impersonate"] = IMPERSONATE
    return opts


# ---------------------------------------------------------------------------
# Content-hash library
# ---------------------------------------------------------------------------
class DedupeLibrary:
    """Fingerprint -> [paths] map, persisted to JSON. Global, folder-agnostic."""

    def __init__(self, path: str):
        self.path = os.path.abspath(os.path.expanduser(path))
        self.fps: dict[str, list[str]] = {}
        self.path_fp: dict[str, str] = {}
        self.load()

    def load(self):
        if os.path.isfile(self.path):
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                data = {}
            self.fps = data.get("fingerprints", {})
            for fp, paths in self.fps.items():
                for p in paths:
                    self.path_fp[p] = fp

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"fingerprints": self.fps}, f, indent=1)
        os.replace(tmp, self.path)

    @staticmethod
    def fingerprint(path: str, chunk: int = 1 << 20) -> str:
        h = hashlib.sha256()
        size = os.path.getsize(path)
        h.update(str(size).encode())
        with open(path, "rb") as f:
            h.update(f.read(chunk))
            if size > chunk * 2:
                f.seek(-chunk, os.SEEK_END)
                h.update(f.read(chunk))
        return h.hexdigest()

    def find_dup(self, path: str) -> list[str]:
        ap = os.path.abspath(path)
        if ap in self.path_fp:
            return []
        fp = self.fingerprint(ap)
        return list(self.fps.get(fp, []))

    def register(self, path: str):
        ap = os.path.abspath(path)
        fp = self.fingerprint(ap)
        self.fps.setdefault(fp, [])
        if ap not in self.fps[fp]:
            self.fps[fp].append(ap)
        self.path_fp[ap] = fp

    def unregister(self, path: str):
        ap = os.path.abspath(path)
        fp = self.path_fp.pop(ap, None)
        if fp and ap in self.fps.get(fp, []):
            self.fps[fp].remove(ap)
            if not self.fps[fp]:
                del self.fps[fp]


# ---------------------------------------------------------------------------
# JS runtime / cache
# ---------------------------------------------------------------------------
def setup_js_runtime() -> None:
    if JS_RUNTIME_DIR:
        d = os.path.abspath(os.path.expanduser(JS_RUNTIME_DIR))
        if os.path.isdir(d):
            os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
        else:
            log(f"[warn] JS_RUNTIME_DIR not found: {d}")

    runtime = shutil.which("deno") or shutil.which("node")
    if runtime:
        log(f"[ok] JS runtime: {runtime}")
    else:
        log("[warn] no JS runtime found (deno/node). YouTube n-sig will fail.")


def setup_cache() -> None:
    os.makedirs(os.path.abspath(os.path.expanduser(CACHE_DIR)), exist_ok=True)


# ---------------------------------------------------------------------------
# Archive helpers
# ---------------------------------------------------------------------------
def load_archive_ids(archive_path: str) -> set:
    ids = set()
    archive_path = os.path.abspath(os.path.expanduser(archive_path))
    if os.path.isfile(archive_path):
        for line in open(archive_path, encoding="utf-8"):
            parts = line.strip().split()
            if len(parts) == 2:
                ids.add(parts[1])
    return ids


def remove_from_archive(archive_path: str, vid: str) -> bool:
    archive_path = os.path.abspath(os.path.expanduser(archive_path))
    if not os.path.isfile(archive_path):
        return False
    lines = open(archive_path, encoding="utf-8").read().splitlines()
    kept = [l for l in lines if not l.strip().endswith(" " + vid)]
    if len(kept) != len(lines):
        with open(archive_path, "w", encoding="utf-8") as f:
            f.write("\n".join(kept) + "\n")
        return True
    return False


# ---------------------------------------------------------------------------
# Flat playlist pre-check
# ---------------------------------------------------------------------------
def fetch_playlist_entries(url: str, cookie_file: str | None):
    """Return list of (playlist_index, video_id) via flat extraction.

    Returns [] if the extractor produced nothing usable.
    """
    opts = base_ydl_opts(cookie_file)
    opts["extract_flat"] = "in_playlist"

    out = []
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False) or {}

    def walk(node):
        for e in node.get("entries") or []:
            if not e:
                continue
            if e.get("id"):
                out.append((e.get("playlist_index"), e["id"]))
            if e.get("entries"):
                walk(e)

    walk(info)
    return out


def plan_fast_skip(url: str, archive_ids: set, cookie_file: str | None):
    """Decide whether to skip a collection URL.

    Returns (skip: bool, playlist_items: str|None, note: str).
    """
    if not looks_like_collection(url):
        return False, None, "single item"

    try:
        entries = fetch_playlist_entries(url, cookie_file)
    except Exception as e:
        return False, None, f"flat fetch failed ({e})"

    if not entries:
        return False, None, "flat returned nothing (unsupported?)"

    # A collection resolving to a single entry is suspicious -> full run
    if len(entries) <= 1:
        return False, None, "flat returned 1 entry (unsupported?)"

    missing = [(i, v) for i, v in entries if v not in archive_ids]
    if not missing:
        return True, None, f"all {len(entries)} item(s) already present"

    idxs = [str(i) for i, _ in missing if i is not None]
    if len(idxs) == len(missing) and len(missing) < len(entries):
        return False, ",".join(idxs), f"{len(missing)}/{len(entries)} missing"
    return False, None, f"{len(missing)}/{len(entries)} missing (full pass)"


# ---------------------------------------------------------------------------
# Stale partial cleanup
# ---------------------------------------------------------------------------
def clean_stale_parts(directory: str) -> int:
    root = Path(os.path.expanduser(directory))
    if not root.is_dir():
        return 0
    removed = 0
    for pattern in PART_PATTERNS:
        for f in root.rglob(pattern):
            try:
                size = f.stat().st_size
                f.unlink()
                removed += 1
                log(f"    [clean] removed stale {f.relative_to(root)} ({size} bytes)")
            except OSError as e:
                log(f"    [clean] could not remove {f}: {e}")
    return removed


# ---------------------------------------------------------------------------
# Playlist file parsing
# ---------------------------------------------------------------------------
def _strip_inline_comment(line: str) -> str:
    for i, ch in enumerate(line):
        if ch == "#" and (i == 0 or line[i - 1] in " \t"):
            return line[:i]
    return line


def parse_playlist_file(path: Path):
    entries = []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        line = _strip_inline_comment(raw).strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 2 or not parts[1]:
            log(f"  [warn] line {lineno}: need at least 'comment;url', skipping: {raw}")
            continue
        comment, url = parts[0], parts[1]
        kind = parts[2].lower() if len(parts) > 2 and parts[2] else "video"
        download_path = parts[3] if len(parts) > 3 and parts[3] else (comment or ".")
        if kind not in ("video", "audio"):
            log(f"  [warn] line {lineno}: type must be 'video' or 'audio', "
                f"got '{kind}' — defaulting to 'video'")
            kind = "video"
        entries.append((comment, url, kind, download_path))
    return entries


# ---------------------------------------------------------------------------
# Dedupe post-hook + progress/pp hooks
# ---------------------------------------------------------------------------
def make_post_hook(lib: DedupeLibrary):
    def hook(filename, *args, **kwargs):
        touch()
        if not DEDUPE or not filename or not os.path.isfile(filename):
            return
        try:
            dups = lib.find_dup(filename)
            if dups:
                if DEDUPE_KEEP == "new":
                    for old in dups:
                        if (os.path.abspath(old) != os.path.abspath(filename)
                                and os.path.isfile(old)):
                            os.remove(old)
                            lib.unregister(old)
                            log(f"    [dedupe] removed older copy: {old}")
                    lib.register(filename)
                else:
                    os.remove(filename)
                    log(f"    [dedupe] duplicate content, removed new copy: "
                        f"{os.path.basename(filename)}")
            else:
                lib.register(filename)
            lib.save()
        except OSError as e:
            log(f"    [dedupe] {e}")
    return hook


def make_progress_hook():
    state = {"last": 0.0}

    def hook(d):
        touch()
        status = d.get("status")
        name = os.path.basename(d.get("filename", ""))
        if status == "finished":
            log(f"    [dl] finished: {name}")
            state["last"] = 0.0
            return
        if status != "downloading":
            return
        now = time.time()
        if now - state["last"] < LOG_PROGRESS_INTERVAL:
            return
        state["last"] = now
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        done = d.get("downloaded_bytes", 0)
        pct = (done / total * 100) if total else 0
        speed = d.get("speed") or 0
        eta = d.get("eta")
        extra = f" @ {speed/1e6:.2f}MB/s" if speed else ""
        if eta:
            extra += f" eta {eta:.0f}s"
        log(f"    [dl] {pct:5.1f}% {done/1e6:.1f}MB{extra} | {name}")

    return hook


def make_pp_hook():
    def hook(d):
        touch()
        status = d.get("status")
        pp = d.get("postprocessor", "?")
        if status == "started":
            log(f"    [pp] {pp} started")
        elif status == "finished":
            log(f"    [pp] {pp} done")
    return hook


# ---------------------------------------------------------------------------
# yt-dlp options
# ---------------------------------------------------------------------------
def build_ydl_opts(download_path: str, kind: str, cookie_file: str | None,
                   post_hook, playlist_items: str | None = None) -> dict:
    download_path = os.path.expanduser(download_path)
    os.makedirs(download_path, exist_ok=True)

    archive_path = os.path.abspath(os.path.expanduser(GLOBAL_ARCHIVE))
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)

    template = AUDIO_TEMPLATE if kind == "audio" else VIDEO_TEMPLATE

    opts = {
        "download_archive": archive_path,
        "nooverwrites": True,
        "outtmpl": os.path.join(download_path, template),
        "writethumbnail": False,

        "ignoreerrors": True,
        "noplaylist": False,
        "continuedl": True,
        "retries": 5,
        "fragment_retries": 5,
        "concurrent_fragment_downloads": CONCURRENT_FRAGMENTS,

        "sleep_interval": SLEEP_INTERVAL,
        "max_sleep_interval": MAX_SLEEP_INTERVAL,
        "sleep_interval_requests": SLEEP_INTERVAL_REQUESTS,

        "remote_components": REMOTE_COMPONENTS,
        "cachedir": os.path.abspath(os.path.expanduser(CACHE_DIR)),

        "verbose": VERBOSE,
        "quiet": not VERBOSE,
        "no_warnings": False,
        "progress": True,

        "progress_hooks": [make_progress_hook()],
        "postprocessor_hooks": [make_pp_hook()],
    }

    if playlist_items:
        opts["playlist_items"] = playlist_items
    if post_hook:
        opts["post_hooks"] = [post_hook]
    if IMPERSONATE:
        opts["impersonate"] = IMPERSONATE

    extractor_args = parse_extractor_args(EXTRACTOR_ARGS_RAW)
    if extractor_args:
        opts["extractor_args"] = extractor_args

    pps = []
    if kind == "audio":
        opts["format"] = AUDIO_FORMAT
        if AUDIO_CONVERT:
            pps.append({"key": "FFmpegExtractAudio",
                        "preferredcodec": AUDIO_CODEC,
                        "preferredquality": AUDIO_QUALITY})
    else:
        opts["format"] = FORMAT_SELECTOR
        opts["merge_output_format"] = MERGE_FORMAT

    if EMBED_THUMBNAIL:
        pps.append({"key": "EmbedThumbnail"})
    if EMBED_METADATA:
        pps.append({"key": "FFmpegMetadata"})
    opts["postprocessors"] = pps

    if cookie_file:
        opts["cookiefile"] = os.path.expanduser(cookie_file)

    return opts


# ---------------------------------------------------------------------------
# Scan / seed
# ---------------------------------------------------------------------------
def dedupe_nested_roots(roots):
    roots = sorted(set(os.path.abspath(os.path.expanduser(r)) for r in roots))
    out = []
    for r in roots:
        if not any(r == o or r.startswith(o + os.sep) for o in out):
            out.append(r)
    return out


def make_excluder(patterns):
    abs_pats, name_pats = [], []
    for p in patterns:
        p = os.path.expanduser(p.strip())
        if os.sep in p or os.path.isabs(p):
            abs_pats.append(os.path.abspath(p))
        else:
            name_pats.append(p)

    def excluded(full):
        ap = os.path.abspath(full)
        for pat in abs_pats:
            if ap == pat or ap.startswith(pat + os.sep):
                return True
        base = os.path.basename(full)
        return any(fnmatch.fnmatch(base, pat) for pat in name_pats)

    return excluded


def scan_roots(roots, lib: DedupeLibrary, archive_path: str):
    archive_path = os.path.abspath(os.path.expanduser(archive_path))
    known = set()
    if os.path.isfile(archive_path):
        with open(archive_path, encoding="utf-8") as f:
            known = {l.strip() for l in f if l.strip()}

    excluded = make_excluder(SCAN_EXCLUDE)
    log(f"[scan] SCAN_EXTS = {sorted(SCAN_EXTS)}"
        f"{'  (new-only)' if SCAN_NEW_ONLY else ''}")
    if SCAN_EXCLUDE:
        log(f"[scan] SCAN_EXCLUDE = {SCAN_EXCLUDE}")

    indexed = 0
    already = 0
    new_ids = 0
    seen_exts = {}
    started = time.time()

    def _onerror(e):
        log(f"  [scan] WALK ERROR: {e}")

    for root in roots:
        r = os.path.expanduser(root)
        if not os.path.isdir(r):
            log(f"  [scan] skip (no such dir): {r}")
            continue

        count = 0
        skip_known = 0
        skipped_ext = {}
        pruned = []
        log(f"  [scan] walking {r}")
        last_log = time.time()

        for dirpath, dirs, files in os.walk(r, onerror=_onerror):
            keep = []
            for d in dirs:
                full = os.path.join(dirpath, d)
                if excluded(full):
                    pruned.append(full)
                else:
                    keep.append(d)
            dirs[:] = keep

            for name in files:
                ext = Path(name).suffix.lower()
                seen_exts[ext] = seen_exts.get(ext, 0) + 1
                if ext not in SCAN_EXTS:
                    skipped_ext[ext] = skipped_ext.get(ext, 0) + 1
                    continue

                p = os.path.join(dirpath, name)
                if excluded(p):
                    continue

                vid = extract_video_id(name)
                if vid:
                    line = f"youtube {vid}"
                    if line not in known:
                        known.add(line)
                        new_ids += 1

                if SCAN_NEW_ONLY and os.path.abspath(p) in lib.path_fp:
                    already += 1
                    skip_known += 1
                    continue

                try:
                    lib.register(p)
                    indexed += 1
                    count += 1
                except OSError as e:
                    log(f"  [scan] {p}: {e}")
                    continue

                touch()
                now = time.time()
                if count % max(SCAN_LOG_EVERY, 1) == 0:
                    log(f"  [scan] ... {count} indexed in this root", console=False)
                if now - last_log >= 1.0:
                    last_log = now
                    elapsed = now - started
                    rate = indexed / elapsed if elapsed else 0
                    log(f"  [scan] {indexed} indexed ({rate:.0f}/s) | "
                        f"{os.path.relpath(p, r)}")

        lib.save()
        summary = f"  [scan] {r}: {count} indexed"
        if skip_known:
            summary += f", {skip_known} already known"
        if skipped_ext:
            summary += f"  (skipped exts: {skipped_ext})"
        log(summary)
        if pruned:
            log(f"  [scan] excluded {len(pruned)} dir(s): "
                f"{', '.join(os.path.relpath(x, r) for x in pruned[:8])}"
                f"{' ...' if len(pruned) > 8 else ''}")

    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    with open(archive_path, "w", encoding="utf-8") as f:
        for line in sorted(known):
            f.write(line + "\n")

    elapsed = time.time() - started
    log(f"[scan] indexed {indexed} new file(s) in {elapsed:.1f}s; "
        f"{already} already known; archive now has {len(known)} id(s).")


def prune_dead_entries(lib: DedupeLibrary) -> int:
    """Drop library entries whose files no longer exist."""
    dead = 0
    for fp in list(lib.fps):
        live = [p for p in lib.fps[fp] if os.path.isfile(p)]
        if live:
            lib.fps[fp] = live
        else:
            for p in lib.fps[fp]:
                lib.path_fp.pop(p, None)
            del lib.fps[fp]
            dead += 1
    if dead:
        lib.save()
    return dead


def run_sync(lib: DedupeLibrary, entries):
    """Prune dead entries, then register any new files. Local, no network."""
    dead = prune_dead_entries(lib)
    log(f"[sync] pruned {dead} dead library entr(ies)")

    roots = list(SCAN_ROOTS) + [p for _, _, _, p in entries]
    roots = dedupe_nested_roots(roots)
    if not roots:
        return

    global SCAN_NEW_ONLY
    prev = SCAN_NEW_ONLY
    SCAN_NEW_ONLY = True
    log("[sync] scanning for new files...")
    scan_roots(roots, lib, GLOBAL_ARCHIVE)
    SCAN_NEW_ONLY = prev


# ---------------------------------------------------------------------------
# Prune-removed (playlist deletions)
# ---------------------------------------------------------------------------
def video_url_for(playlist_url: str, vid: str):
    host = urlparse(playlist_url).netloc.lower()
    if "youtube" in host or "youtu.be" in host:
        return f"https://www.youtube.com/watch?v={vid}"
    if "pornhub" in host:
        return f"https://www.pornhub.com/view_video.php?viewkey={vid}"
    if "spankbang" in host:
        return f"https://spankbang.com/{vid}/video/"
    return None


def video_still_exists(video_url: str, cookie_file: str | None) -> bool:
    try:
        with yt_dlp.YoutubeDL(base_ydl_opts(cookie_file)) as ydl:
            return bool(ydl.extract_info(video_url, download=False))
    except Exception:
        return False


def prune_removed(entries, lib: DedupeLibrary, cookie_file: str | None,
                  dry_run=False):
    excluded = make_excluder(SCAN_EXCLUDE)
    removed = kept_gone = 0

    for comment, url, kind, download_path in entries:
        root = Path(os.path.expanduser(download_path))
        if not root.is_dir():
            continue

        log(f"[prune] {url} -> {root}")
        try:
            current = {v for _, v in fetch_playlist_entries(url, cookie_file)}
        except Exception as e:
            log(f"[prune]   playlist fetch failed: {e}")
            continue
        log(f"[prune]   {len(current)} item(s) currently in playlist")

        for f in sorted(root.rglob("*")):
            if not f.is_file() or f.suffix.lower() not in SCAN_EXTS:
                continue
            if excluded(str(f)):
                continue
            vid = extract_video_id(f.name)
            if not vid or vid in current:
                continue

            vurl = video_url_for(url, vid)
            if not vurl:
                log(f"[prune]   ? {f.name} (no url pattern, kept)")
                continue
            if dry_run:
                log(f"[prune]   would check: {f.name}")
                continue

            touch()
            if video_still_exists(vurl, cookie_file):
                try:
                    f.unlink()
                    lib.unregister(str(f))
                    remove_from_archive(GLOBAL_ARCHIVE, vid)
                    removed += 1
                    log(f"[prune]   removed (still on site): {f.name}")
                except OSError as e:
                    log(f"[prune]   delete failed {f.name}: {e}")
            else:
                kept_gone += 1
                log(f"[prune]   kept (gone from site): {f.name}")

            time.sleep(SLEEP_INTERVAL_REQUESTS or 1)

    lib.save()
    log(f"[prune] done. removed {removed}, kept {kept_gone} (unavailable).")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global VERBOSE, SCAN_EXCLUDE, SCAN_NEW_ONLY

    ap = argparse.ArgumentParser(
        description="Auto-download playlists with yt-dlp (new items only), "
                    "content dedupe, and a fast already-synced pre-check.")
    ap.add_argument("-e", "--env", default=".env")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-clean", action="store_true")
    ap.add_argument("--no-sync", action="store_true",
                    help="Skip the pre-run dead-entry prune + new-file scan.")
    ap.add_argument("--no-fast", action="store_true",
                    help="Disable the flat pre-check; always run full extraction.")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--exclude", action="append", default=[], metavar="NAME")
    ap.add_argument("--scan", nargs="*", metavar="DIR")
    ap.add_argument("--prune-removed", action="store_true",
                    help="Delete files removed from a playlist, but only if the "
                         "video still exists on the site.")
    args = ap.parse_args()

    if args.env != ".env":
        load_dotenv(args.env, override=True)

    if args.verbose:
        VERBOSE = True
    if args.exclude:
        SCAN_EXCLUDE = SCAN_EXCLUDE + args.exclude

    init_log()
    start_heartbeat()

    log(f"=== start pid={os.getpid()} cwd={os.getcwd()} "
        f"argv={' '.join(sys.argv[1:]) or '(none)'} ===")

    setup_js_runtime()
    setup_cache()

    lib = DedupeLibrary(LIBRARY_FILE)

    playlist_file = Path(os.path.expanduser(PLAYLIST_FILE))
    entries = []
    if playlist_file.is_file():
        entries = parse_playlist_file(playlist_file)
    elif not args.scan:
        log(f"Playlist file not found: {playlist_file}")
        sys.exit(1)

    # ---- scan mode ----
    if args.scan is not None:
        roots = list(args.scan) if args.scan else list(SCAN_ROOTS)
        if not args.scan:
            roots += [p for _, _, _, p in entries]
        roots = dedupe_nested_roots(roots)
        log(f"Scanning {len(roots)} root(s)...")
        scan_roots(roots, lib, GLOBAL_ARCHIVE)
        log("=== done (scan) ===")
        return

    if not entries:
        log("No valid entries found in playlist file.")
        sys.exit(1)

    cookie_file = COOKIES_FILE or None
    if cookie_file and not os.path.isfile(os.path.expanduser(cookie_file)):
        log(f"[warn] cookie file not found: {cookie_file} — running without cookies")
        cookie_file = None

    # ---- prune-removed mode ----
    if args.prune_removed:
        log("Pruning files removed from playlists (checking availability)...")
        prune_removed(entries, lib, cookie_file, dry_run=args.dry_run)
        log("=== done (prune-removed) ===")
        return

    # ---- pre-run sync ----
    if not args.no_sync and not args.dry_run:
        run_sync(lib, entries)

    post_hook = make_post_hook(lib) if DEDUPE else None
    archive_ids = load_archive_ids(GLOBAL_ARCHIVE)

    log(f"Loaded {len(entries)} entry(ies). Dedupe={'on' if DEDUPE else 'off'} "
        f"(library: {len(lib.fps)} fingerprint(s)).")

    for idx, (comment, url, kind, download_path) in enumerate(entries, 1):
        label = f" — {comment}" if comment else ""
        log(f"[{idx}/{len(entries)}] ({kind}) {url}{label}")
        log(f"    -> {download_path}")

        if args.dry_run:
            continue

        playlist_items = None
        if not args.no_fast:
            skip, playlist_items, note = plan_fast_skip(url, archive_ids, cookie_file)
            log(f"    [fast] {note}")
            if skip:
                continue

        if CLEAN_PART_FILES and not args.no_clean:
            clean_stale_parts(download_path)

        entry_start = time.time()
        touch()
        try:
            with yt_dlp.YoutubeDL(
                    build_ydl_opts(download_path, kind, cookie_file,
                                   post_hook, playlist_items)) as ydl:
                ydl.download([url])
            log(f"    [done] entry {idx} in {time.time() - entry_start:.1f}s")
        except Exception as e:
            log(f"    [error] entry {idx}: {e}")

        archive_ids = load_archive_ids(GLOBAL_ARCHIVE)
        log("")

    log("=== done ===")


if __name__ == "__main__":
    main()
