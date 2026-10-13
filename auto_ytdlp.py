#!/usr/bin/env python3
"""
auto_ytdlp.py — Bulk playlist downloader built on yt-dlp, with cross-folder
content dedupe.

Config is loaded from a .env file (see .env.example).

Playlist list file, one entry per line:

    customcomment;url;videoORaudio;downloaddir

  - Blank lines and full-line comments (leading '#') are ignored.
  - Inline comments (' # ...') are stripped.
  - 'kind' defaults to "video" if empty.
  - 'downloaddir' defaults to a folder named after the comment if empty.

Dedupe (folder-independent):
  - A GLOBAL archive records known video IDs so yt-dlp skips them outright.
  - A GLOBAL content-hash library catches same-content files whose names
    don't carry an ID: on download, a duplicate is deleted and its ID archived.
  - Seed it with:  ./auto_ytdlp.py --scan [DIR ...]
"""

import argparse
import fnmatch
import hashlib
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

try:
    import yt_dlp
except ImportError:
    sys.exit("yt-dlp not installed. Run: pip install -U 'yt-dlp[default]'")

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("python-dotenv not installed. Run: pip install -U python-dotenv")


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
FORMAT_SELECTOR  = os.getenv("FORMAT_SELECTOR", "bv*+ba/b")
AUDIO_FORMAT     = os.getenv("AUDIO_FORMAT", "bestaudio/best")
AUDIO_CODEC      = os.getenv("AUDIO_CODEC", "mp3")
AUDIO_QUALITY    = os.getenv("AUDIO_QUALITY", "192")
JS_RUNTIME_DIR   = os.getenv("JS_RUNTIME_DIR", "").strip()
CLEAN_PART_FILES = os.getenv("CLEAN_PART_FILES", "true").strip().lower() in (
    "1", "true", "yes", "on")

REMOTE_COMPONENTS = [c.strip() for c in
                     os.getenv("REMOTE_COMPONENTS", "ejs:github").split(",")
                     if c.strip()]
CACHE_DIR  = os.getenv("CACHE_DIR", "./.cache/yt-dlp")
VERBOSE    = os.getenv("VERBOSE", "false").strip().lower() in (
    "1", "true", "yes", "on")
EXTRACTOR_ARGS_RAW = os.getenv("EXTRACTOR_ARGS", "")

# --- Dedupe ---
DEDUPE         = os.getenv("DEDUPE", "true").strip().lower() in (
    "1", "true", "yes", "on")
GLOBAL_ARCHIVE = os.getenv("GLOBAL_ARCHIVE", "./.global_archive.txt")
LIBRARY_FILE   = os.getenv("LIBRARY_FILE", "./.dedupe_library.json")
DEDUPE_KEEP    = os.getenv("DEDUPE_KEEP", "existing").strip().lower()

# --- Scan ---
SCAN_ROOTS = [r.strip() for r in os.getenv("SCAN_ROOTS", "").split(";")
              if r.strip()]
SCAN_EXCLUDE = [e.strip() for e in os.getenv("SCAN_EXCLUDE", "").split(";")
                if e.strip()]

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

# Best-effort YouTube ID extraction from filenames
ID_PATTERNS = [
    re.compile(r"[\[\(]([A-Za-z0-9_-]{11})[\]\)]"),
    re.compile(r"\s-\s([A-Za-z0-9_-]{11})(?=\.[a-z0-9]+$)"),
    re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})"),
]


def extract_video_id(name: str):
    for pat in ID_PATTERNS:
        m = pat.search(name)
        if m:
            return m.group(1)
    return None


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
        """Fast fingerprint: size + first & last 1 MiB (hashed)."""
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
            print(f"[warn] JS_RUNTIME_DIR not found: {d}")

    runtime = shutil.which("deno") or shutil.which("node")
    if runtime:
        print(f"[ok] JS runtime: {runtime}")
    else:
        print("[warn] no JS runtime found (deno/node). YouTube n-sig will fail.")
        print("       fix: curl -fsSL https://deno.land/install.sh | "
              'DENO_INSTALL="$PWD/.deno" sh')


def setup_cache() -> None:
    os.makedirs(os.path.abspath(os.path.expanduser(CACHE_DIR)), exist_ok=True)


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
                print(f"    [clean] removed stale {f.relative_to(root)} ({size} bytes)")
            except OSError as e:
                print(f"    [clean] could not remove {f}: {e}")
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
            print(f"  [warn] line {lineno}: need at least 'comment;url', skipping: {raw}")
            continue
        comment, url = parts[0], parts[1]
        kind = parts[2].lower() if len(parts) > 2 and parts[2] else "video"
        download_path = parts[3] if len(parts) > 3 and parts[3] else (comment or ".")
        if kind not in ("video", "audio"):
            print(f"  [warn] line {lineno}: type must be 'video' or 'audio', "
                  f"got '{kind}' — defaulting to 'video'")
            kind = "video"
        entries.append((comment, url, kind, download_path))
    return entries


# ---------------------------------------------------------------------------
# Dedupe post-hook
# ---------------------------------------------------------------------------
def make_post_hook(lib: DedupeLibrary):
    def hook(filename, *args, **kwargs):
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
                            print(f"    [dedupe] removed older copy: {old}")
                    lib.register(filename)
                else:
                    os.remove(filename)
                    print(f"    [dedupe] duplicate content, removed new copy: "
                          f"{os.path.basename(filename)}")
            else:
                lib.register(filename)
            lib.save()
        except OSError as e:
            print(f"    [dedupe] {e}")
    return hook


# ---------------------------------------------------------------------------
# yt-dlp options
# ---------------------------------------------------------------------------
def build_ydl_opts(download_path: str, kind: str, cookie_file: str | None,
                   post_hook) -> dict:
    download_path = os.path.expanduser(download_path)
    os.makedirs(download_path, exist_ok=True)

    archive_path = os.path.abspath(os.path.expanduser(GLOBAL_ARCHIVE))
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)

    template = AUDIO_TEMPLATE if kind == "audio" else VIDEO_TEMPLATE

    opts = {
        "download_archive": archive_path,
        "nooverwrites": True,
        "outtmpl": os.path.join(download_path, template),
        "writethumbnail": False,          # embed only, no stray image files

        "ignoreerrors": True,
        "noplaylist": False,
        "continuedl": True,
        "retries": 5,
        "fragment_retries": 5,
        "concurrent_fragment_downloads": 4,

        "remote_components": REMOTE_COMPONENTS,
        "cachedir": os.path.abspath(os.path.expanduser(CACHE_DIR)),

        "verbose": VERBOSE,
        "quiet": not VERBOSE,
        "no_warnings": False,
        "progress": True,
    }

    if post_hook:
        opts["post_hooks"] = [post_hook]

    extractor_args = parse_extractor_args(EXTRACTOR_ARGS_RAW)
    if extractor_args:
        opts["extractor_args"] = extractor_args

    if kind == "audio":
        opts["format"] = AUDIO_FORMAT
        opts["postprocessors"] = [
            {"key": "FFmpegExtractAudio", "preferredcodec": AUDIO_CODEC,
             "preferredquality": AUDIO_QUALITY},
            {"key": "EmbedThumbnail"},
            {"key": "FFmpegMetadata"},
        ]
    else:
        opts["format"] = FORMAT_SELECTOR
        opts["merge_output_format"] = MERGE_FORMAT
        opts["postprocessors"] = [
            {"key": "EmbedThumbnail"},
            {"key": "FFmpegMetadata"},
        ]

    if cookie_file:
        opts["cookiefile"] = os.path.expanduser(cookie_file)

    return opts


# ---------------------------------------------------------------------------
# Scan / seed
# ---------------------------------------------------------------------------
def dedupe_nested_roots(roots):
    """Drop any root that's already inside another root."""
    roots = sorted(set(os.path.abspath(os.path.expanduser(r)) for r in roots))
    out = []
    for r in roots:
        if not any(r == o or r.startswith(o + os.sep) for o in out):
            out.append(r)
    return out


def make_excluder(patterns):
    """Predicate matching dirs/files to skip (path prefix or basename glob)."""
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
    print(f"[scan] SCAN_EXTS = {sorted(SCAN_EXTS)}")
    if SCAN_EXCLUDE:
        print(f"[scan] SCAN_EXCLUDE = {SCAN_EXCLUDE}")

    indexed = 0
    new_ids = 0
    seen_exts = {}
    started = time.time()

    def _onerror(e):
        print(f"\n  [scan] WALK ERROR: {e}")

    for root in roots:
        r = os.path.expanduser(root)
        if not os.path.isdir(r):
            print(f"  [scan] skip (no such dir): {r}")
            continue

        count = 0
        skipped_ext = {}
        pruned = []
        print(f"  [scan] walking {r}")
        last = time.time()

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
                try:
                    lib.register(p)
                    indexed += 1
                    count += 1
                except OSError as e:
                    print(f"\n  [scan] {p}: {e}")
                    continue

                vid = extract_video_id(name)
                if vid:
                    line = f"youtube {vid}"
                    if line not in known:
                        known.add(line)
                        new_ids += 1

                now = time.time()
                if now - last >= 0.3:
                    last = now
                    elapsed = now - started
                    rate = indexed / elapsed if elapsed else 0
                    rel = os.path.relpath(p, r)
                    line = f"\r  [scan] {indexed} indexed ({rate:.0f}/s) | {rel}"
                    print(f"{line[:110]:<110}", end="", flush=True)

        lib.save()
        summary = f"\r  [scan] {r}: {count} indexed"
        if skipped_ext:
            summary += f"  (skipped exts: {skipped_ext})"
        print(f"{summary}{' ' * 20}")
        if pruned:
            print(f"  [scan] excluded {len(pruned)} dir(s): "
                  f"{', '.join(os.path.relpath(x, r) for x in pruned[:8])}"
                  f"{' ...' if len(pruned) > 8 else ''}")

    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    with open(archive_path, "w", encoding="utf-8") as f:
        for line in sorted(known):
            f.write(line + "\n")

    elapsed = time.time() - started
    print(f"\n[scan] indexed {indexed} file(s) in {elapsed:.1f}s; "
          f"archive now has {len(known)} known id(s) (+{new_ids}).")
    print(f"[scan] extensions seen: {seen_exts}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global VERBOSE, SCAN_EXCLUDE

    ap = argparse.ArgumentParser(
        description="Auto-download playlists with yt-dlp (new items only) "
                    "+ cross-folder content dedupe.")
    ap.add_argument("-e", "--env", default=".env")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-clean", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--exclude", action="append", default=[], metavar="NAME",
                    help="Exclude a folder name/glob or path from --scan "
                         "(repeatable)")
    ap.add_argument("--scan", nargs="*", metavar="DIR",
                    help="Index existing files + seed the global ID archive, "
                         "then exit. With no DIRs, uses SCAN_ROOTS and every "
                         "downloaddir from the playlist file.")
    args = ap.parse_args()

    if args.env != ".env":
        load_dotenv(args.env, override=True)

    if args.verbose:
        VERBOSE = True
    if args.exclude:
        SCAN_EXCLUDE = SCAN_EXCLUDE + args.exclude

    setup_js_runtime()
    setup_cache()

    lib = DedupeLibrary(LIBRARY_FILE)

    playlist_file = Path(os.path.expanduser(PLAYLIST_FILE))
    entries = []
    if playlist_file.is_file():
        entries = parse_playlist_file(playlist_file)
    elif not args.scan:
        sys.exit(f"Playlist file not found: {playlist_file}")

    # ---- scan mode ----
    if args.scan is not None:
        roots = list(args.scan) if args.scan else list(SCAN_ROOTS)
        if not args.scan:
            roots += [p for _, _, _, p in entries]
        roots = dedupe_nested_roots(roots)
        print(f"Scanning {len(roots)} root(s)...")
        scan_roots(roots, lib, GLOBAL_ARCHIVE)
        return

    if not entries:
        sys.exit("No valid entries found in playlist file.")

    cookie_file = COOKIES_FILE or None
    if cookie_file and not os.path.isfile(os.path.expanduser(cookie_file)):
        print(f"[warn] cookie file not found: {cookie_file} — continuing without cookies")
        cookie_file = None

    post_hook = make_post_hook(lib) if DEDUPE else None

    print(f"Loaded {len(entries)} entry(ies). "
          f"Dedupe={'on' if DEDUPE else 'off'} "
          f"(library: {len(lib.fps)} fingerprint(s)).\n")

    for idx, (comment, url, kind, download_path) in enumerate(entries, 1):
        label = f" — {comment}" if comment else ""
        print(f"[{idx}/{len(entries)}] ({kind}) {url}{label}")
        print(f"    -> {download_path}")

        if args.dry_run:
            continue

        if CLEAN_PART_FILES and not args.no_clean:
            clean_stale_parts(download_path)

        try:
            with yt_dlp.YoutubeDL(
                    build_ydl_opts(download_path, kind, cookie_file, post_hook)) as ydl:
                ydl.download([url])
        except Exception as e:
            print(f"    [error] {e}")

        print()

    print("Done.")


if __name__ == "__main__":
    main()
