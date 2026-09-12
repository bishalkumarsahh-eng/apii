import os
import re
import time
import asyncio
import sqlite3
import threading
import logging
import urllib.request
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, Header, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv
import yt_dlp
from ytmusicapi import YTMusic


# =========================================================
# LOAD ENVIRONMENT VARIABLES
# =========================================================

load_dotenv()


# =========================================================
# CONFIGURATION
# =========================================================

DOWNLOAD_DIR = os.getenv(
    "DOWNLOAD_DIR",
    "downloads"
)

CACHE_EXPIRE_HOURS = float(
    os.getenv(
        "CACHE_EXPIRE_HOURS",
        "24"
    )
)

MAX_VIDEO_QUALITY = os.getenv(
    "MAX_VIDEO_QUALITY",
    "720"
)

PORT = int(
    os.getenv(
        "PORT",
        "8000"
    )
)

COOKIE_URL = os.getenv("COOKIE_URL", "")

# YouTube player clients. Avoid the deprecated/problematic tv_downgraded
# client that can cause "The page needs to be reloaded" errors.
YOUTUBE_PLAYER_CLIENTS = os.getenv(
    "YOUTUBE_PLAYER_CLIENTS",
    "default,web_embedded"
).strip()

# YouTube can currently downgrade logged-in cookie sessions to the
# tv_downgraded client, which may return "The page needs to be reloaded".
# Public music/video downloads normally do not need account cookies.
YOUTUBE_USE_COOKIES = os.getenv(
    "YOUTUBE_USE_COOKIES",
    "false"
).strip().lower() in ("1", "true", "yes", "on")

COOKIES_FILE = "cookies.txt"

DB_FILE = "cache.db"

# =========================================================
# API KEY AUTHENTICATION
# =========================================================
# Set API_KEY in Heroku Config Vars. Keep this value secret.
# Client requests should send: X-API-Key: <your-key>
# Authorization: Bearer <your-key> is also accepted.
# For compatibility, ?api_key=<your-key> is also accepted.

API_KEY = os.getenv("API_KEY", "").strip()

# Expose the header in Swagger UI so protected endpoints can be tested
# with the Authorize button. Query and Bearer authentication remain supported.
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    x_api_key: Optional[str] = Security(api_key_header),
    authorization: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None, description="API key (legacy/query compatibility)")
):
    """Protect API endpoints with a server-side API key."""

    if not API_KEY:
        logger.error("API_KEY is not configured on the server.")
        raise HTTPException(
            status_code=503,
            detail="API authentication is not configured on the server."
        )

    # Prefer the HTTP header. Also accept ?api_key=... for compatibility
    # with existing Music Bot clients.
    supplied_key = (x_api_key or api_key or "").strip()

    # Also accept Authorization: Bearer <key> for clients that prefer it.
    if not supplied_key and authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer":
            supplied_key = token.strip()

    if not supplied_key or supplied_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key."
        )

    return True


# =========================================================
# DOWNLOAD PERFORMANCE SETTINGS
# =========================================================

CONCURRENT_FRAGMENT_DOWNLOADS = int(
    os.getenv(
        "CONCURRENT_FRAGMENT_DOWNLOADS",
        "15"
    )
)

HTTP_CHUNK_SIZE = int(
    os.getenv(
        "HTTP_CHUNK_SIZE",
        "10485760"
    )
)

SOCKET_TIMEOUT = int(
    os.getenv(
        "SOCKET_TIMEOUT",
        "15"
    )
)

RETRIES = int(
    os.getenv(
        "RETRIES",
        "5"
    )
)

FRAGMENT_RETRIES = int(
    os.getenv(
        "FRAGMENT_RETRIES",
        "5"
    )
)


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =========================================================
# DIRECT STREAM CACHE
# =========================================================
# YouTube media URLs are signed and expire. Keep them only briefly so a
# repeated play request can skip the yt-dlp extraction step.
DIRECT_URL_CACHE: Dict[str, tuple] = {}
DIRECT_URL_CACHE_TTL = int(os.getenv("DIRECT_URL_CACHE_TTL", "900"))
DIRECT_URL_CACHE_MAX = int(os.getenv("DIRECT_URL_CACHE_MAX", "2048"))
DIRECT_CACHE_LOCK = threading.Lock()
DIRECT_RESOLVE_LOCKS: Dict[str, threading.Lock] = {}
DIRECT_RESOLVE_LOCKS_GUARD = threading.Lock()


def _get_direct_cached(video_id: str):
    with DIRECT_CACHE_LOCK:
        item = DIRECT_URL_CACHE.get(video_id)
        if not item:
            return None
        url, created = item
        if time.time() - created >= DIRECT_URL_CACHE_TTL:
            DIRECT_URL_CACHE.pop(video_id, None)
            return None
        return url


def _set_direct_cached(video_id: str, media_url: str):
    with DIRECT_CACHE_LOCK:
        if len(DIRECT_URL_CACHE) >= DIRECT_URL_CACHE_MAX and video_id not in DIRECT_URL_CACHE:
            oldest_id = min(DIRECT_URL_CACHE, key=lambda key: DIRECT_URL_CACHE[key][1])
            DIRECT_URL_CACHE.pop(oldest_id, None)
        DIRECT_URL_CACHE[video_id] = (media_url, time.time())


def _get_direct_resolve_lock(video_id: str) -> threading.Lock:
    # Coalesce simultaneous requests for the same song without serializing different songs.
    with DIRECT_RESOLVE_LOCKS_GUARD:
        lock = DIRECT_RESOLVE_LOCKS.get(video_id)
        if lock is None:
            lock = threading.Lock()
            DIRECT_RESOLVE_LOCKS[video_id] = lock
        return lock


# =========================================================
# DOWNLOAD DIRECTORY
# =========================================================

os.makedirs(
    DOWNLOAD_DIR,
    exist_ok=True
)


# =========================================================
# DATABASE & CACHE SYSTEM
# =========================================================

def init_db():

    """Initializes the SQLite database for caching metadata safely."""

    try:

        with sqlite3.connect(
            DB_FILE,
            timeout=15.0
        ) as conn:

            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT,
                    title TEXT,
                    file_name TEXT,
                    file_path TEXT,
                    file_type TEXT,
                    file_size INTEGER,
                    duration INTEGER,
                    created_time REAL,
                    thumbnail TEXT,
                    UNIQUE(video_id, file_type)
                )
                '''
            )

            conn.commit()

        logger.info(
            "SQLite database initialized."
        )

    except Exception as e:

        logger.error(
            f"Database initialization failed: {e}"
        )


def get_cached_metadata(
    video_id: str,
    file_type: str
) -> Optional[Dict[str, Any]]:

    """Retrieves cached metadata from SQLite and verifies file existence."""

    try:

        with sqlite3.connect(
            DB_FILE,
            timeout=15.0
        ) as conn:

            conn.row_factory = sqlite3.Row

            cur = conn.cursor()

            cur.execute(
                """
                SELECT *
                FROM downloads
                WHERE video_id = ?
                AND file_type = ?
                """,
                (
                    video_id,
                    file_type
                )
            )

            row = cur.fetchone()

            if row:

                if (
                    os.path.isfile(
                        row["file_path"]
                    )
                    and
                    os.path.getsize(
                        row["file_path"]
                    ) > 0
                ):

                    return dict(row)

                else:

                    logger.warning(
                        f"File {row['file_name']} "
                        "missing from disk. "
                        "Removing DB entry."
                    )

                    cur.execute(
                        """
                        DELETE FROM downloads
                        WHERE id = ?
                        """,
                        (
                            row["id"],
                        )
                    )

                    conn.commit()

            return None

    except Exception as e:

        logger.error(
            f"Error accessing cache DB: {e}"
        )

        return None


def save_cached_metadata(
    data: Dict[str, Any],
    file_type: str
):

    """Saves download metadata to SQLite."""

    try:

        with sqlite3.connect(
            DB_FILE,
            timeout=15.0
        ) as conn:

            conn.execute(
                '''
                INSERT OR REPLACE INTO downloads
                (
                    video_id,
                    title,
                    file_name,
                    file_path,
                    file_type,
                    file_size,
                    duration,
                    created_time,
                    thumbnail
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    data["videoId"],
                    data["title"],
                    data["filename"],
                    data["path"],
                    file_type,
                    data["filesize"],
                    data["duration"],
                    time.time(),
                    data["thumbnail"]
                )
            )

            conn.commit()

    except Exception as e:

        logger.error(
            f"Error saving to cache DB: {e}"
        )


def find_legacy_cached_file(
    video_id: str,
    ext: str
) -> Optional[str]:

    """Fallback to check un-indexed files downloaded before SQLite was added."""

    if not video_id:

        return None

    suffix = f"_{video_id}.{ext}"

    try:

        with os.scandir(
            DOWNLOAD_DIR
        ) as entries:

            for entry in entries:

                if entry.name.endswith(
                    suffix
                ):

                    return entry.name

    except Exception as e:

        logger.error(
            f"Error reading {DOWNLOAD_DIR}: {e}"
        )

    return None


# =========================================================
# CACHE CLEANUP
# =========================================================

async def cache_cleanup_task():

    """Background task to delete old files and clean up database."""

    while True:

        try:

            logger.info(
                "Running advanced cache cleanup..."
            )

            expiry_time = (
                time.time()
                -
                (
                    CACHE_EXPIRE_HOURS
                    * 3600
                )
            )

            def perform_cleanup():

                deleted_files = 0
                db_cleaned = 0

                with sqlite3.connect(
                    DB_FILE,
                    timeout=15.0
                ) as conn:

                    conn.row_factory = sqlite3.Row

                    cur = conn.cursor()

                    # -----------------------------------------
                    # 1. Scan disk for expired files
                    # -----------------------------------------

                    if os.path.exists(
                        DOWNLOAD_DIR
                    ):

                        for entry in os.scandir(
                            DOWNLOAD_DIR
                        ):

                            if entry.is_file():

                                file_stat = entry.stat()

                                if (
                                    file_stat.st_mtime
                                    <
                                    expiry_time
                                ):

                                    try:

                                        os.remove(
                                            entry.path
                                        )

                                        deleted_files += 1

                                        cur.execute(
                                            """
                                            DELETE FROM downloads
                                            WHERE file_name = ?
                                            """,
                                            (
                                                entry.name,
                                            )
                                        )

                                    except Exception as e:

                                        logger.warning(
                                            f"Could not delete old "
                                            f"file {entry.name}: {e}"
                                        )

                    # -----------------------------------------
                    # 2. Remove phantom DB records
                    # -----------------------------------------

                    cur.execute(
                        """
                        SELECT id, file_path
                        FROM downloads
                        """
                    )

                    all_records = cur.fetchall()

                    for record in all_records:

                        if not os.path.exists(
                            record["file_path"]
                        ):

                            cur.execute(
                                """
                                DELETE FROM downloads
                                WHERE id = ?
                                """,
                                (
                                    record["id"],
                                )
                            )

                            db_cleaned += 1

                    conn.commit()

                return (
                    deleted_files,
                    db_cleaned
                )

            deleted_files, db_cleaned = (
                await asyncio.to_thread(
                    perform_cleanup
                )
            )

            if (
                deleted_files > 0
                or
                db_cleaned > 0
            ):

                logger.info(
                    f"Cleanup complete: "
                    f"Deleted {deleted_files} "
                    f"old files on disk, "
                    f"cleared {db_cleaned} "
                    f"orphaned DB records."
                )

            else:

                logger.info(
                    "Cleanup complete: "
                    "No expired files found."
                )

        except Exception as e:

            logger.error(
                "Cache cleanup encountered an error "
                f"(will retry next cycle): {e}"
            )

        await asyncio.sleep(
            3600
        )


# =========================================================
# FASTAPI LIFESPAN
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    logger.info(
        "Starting MAGMA Music API..."
    )

    init_db()

    # -----------------------------------------
    # Download cookies
    # -----------------------------------------

    if COOKIE_URL:

        try:

            urllib.request.urlretrieve(
                COOKIE_URL,
                COOKIES_FILE
            )

            logger.info(
                "Successfully downloaded "
                "cookies.txt from COOKIE_URL"
            )

        except Exception as e:

            logger.error(
                f"Failed to download cookies "
                f"from COOKIE_URL: {e}"
            )

    # -----------------------------------------
    # Start cleanup worker
    # -----------------------------------------

    cleanup_worker = asyncio.create_task(
        cache_cleanup_task()
    )

    yield

    # -----------------------------------------
    # Shutdown
    # -----------------------------------------

    logger.info(
        "Shutting down MAGMA Music API..."
    )

    cleanup_worker.cancel()


# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI(
    title="YouTube Downloader & Search API",
    version="3.0.0-UltraFast",
    lifespan=lifespan
)

# Keep a visible startup diagnostic so Heroku logs immediately show which
# audio endpoints are actually loaded by the running process.
@app.on_event("startup")
async def _log_audio_routes():
    routes = {getattr(route, "path", "") for route in app.routes}
    logger.info("🎵 Audio routes loaded: /stream=%s /download=%s",
                "/stream" in routes, "/download" in routes)


# =========================================================
# CORS
# =========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)


# =========================================================
# MAGMA.HTML DEVELOPER PORTAL
# =========================================================

HTML_FILE = os.path.join(
    os.path.dirname(
        os.path.abspath(__file__)
    ),
    "Magma.html"
)

try:

    with open(
        HTML_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        DEVELOPER_PORTAL_HTML = f.read()

    logger.info(
        "Magma.html loaded successfully."
    )

except Exception as e:

    logger.error(
        f"Failed to load Magma.html: {e}"
    )

    DEVELOPER_PORTAL_HTML = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>MAGMA API</title>
    </head>
    <body>
        <h1>MAGMA API</h1>
        <p>
            Developer portal could not be loaded.
        </p>
    </body>
    </html>
    """


# =========================================================
# YOUTUBE MUSIC
# =========================================================

ytmusic = YTMusic()

SEARCH_CACHE: Dict[str, tuple] = {}
SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", "120"))
SEARCH_CACHE_MAX = int(os.getenv("SEARCH_CACHE_MAX", "512"))


def _get_search_cached(key: str):
    item = SEARCH_CACHE.get(key)
    if not item:
        return None
    value, created = item
    if time.time() - created >= SEARCH_CACHE_TTL:
        SEARCH_CACHE.pop(key, None)
        return None
    return value


def _set_search_cached(key: str, value):
    if len(SEARCH_CACHE) >= SEARCH_CACHE_MAX and key not in SEARCH_CACHE:
        SEARCH_CACHE.pop(next(iter(SEARCH_CACHE)), None)
    SEARCH_CACHE[key] = (value, time.time())


# =========================================================
# VIDEO ID EXTRACTION
# =========================================================

def extract_video_id(
    url: str
) -> Optional[str]:

    """Extracts the 11-character YouTube Video ID."""

    if not url:

        return None

    if re.match(
        r"^[0-9A-Za-z_-]{11}$",
        url
    ):

        return url

    pattern = (
        r"(?:youtu\.be\/|v=|\/shorts\/|"
        r"\/embed\/|\/v\/)"
        r"([0-9A-Za-z_-]{11})"
    )

    match = re.search(
        pattern,
        url
    )

    if match:

        return match.group(1)

    match = re.search(
        r"[0-9A-Za-z_-]{11}",
        url
    )

    return (
        match.group(0)
        if match
        else None
    )


# =========================================================
# BASE YT-DLP OPTIONS
# =========================================================

def get_base_ydl_opts() -> Dict[str, Any]:

    opts = {

        "outtmpl":
            f"{DOWNLOAD_DIR}/%(title).150s_%(id)s.%(ext)s",

        "restrictfilenames":
            True,

        "noplaylist":
            True,

        "quiet":
            False,

        "no_warnings":
            False,

        "retries":
            RETRIES,

        "fragment_retries":
            FRAGMENT_RETRIES,

        "socket_timeout":
            SOCKET_TIMEOUT,

        "continuedl":
            True,

        "js_runtimes":
            {
                "node": {}
            },

        # yt-dlp-ejs is installed locally; avoid a GitHub fetch on every download.
    }

    if YOUTUBE_USE_COOKIES and os.path.exists(
        COOKIES_FILE
    ):

        opts["cookiefile"] = (
            COOKIES_FILE
        )

        logger.info(
            f"Loaded cookies from "
            f"{COOKIES_FILE}"
        )
    elif os.path.exists(COOKIES_FILE):
        logger.info(
            "cookies.txt found but disabled for YouTube downloads "
            "(YOUTUBE_USE_COOKIES=false)"
        )

    return opts


# =========================================================
# THUMBNAIL
# =========================================================

def fetch_thumbnail_sync(
    url: str
) -> Dict[str, Any]:

    opts = get_base_ydl_opts()

    opts["skip_download"] = True

    try:

        with yt_dlp.YoutubeDL(
            opts
        ) as ydl:

            info = ydl.extract_info(
                url,
                download=False
            )

            return {

                "title":
                    info.get("title"),

                "thumbnail":
                    info.get("thumbnail"),

                "videoId":
                    info.get("id")
            }

    except Exception as e:

        logger.error(
            f"Thumbnail fetch error: {e}"
        )

        raise RuntimeError(
            f"Failed to fetch thumbnail: {str(e)}"
        )


# =========================================================
# ULTRA-FAST DIRECT AUDIO RESOLVER
# =========================================================

def _resolve_direct_audio_uncached(video_id: str) -> Dict[str, Any]:
    canonical = f"https://www.youtube.com/watch?v={video_id}"
    use_cookies = YOUTUBE_USE_COOKIES and os.path.isfile(COOKIES_FILE)
    started = time.perf_counter()

    common = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": max(SOCKET_TIMEOUT, 8),
        "retries": 1,
        "fragment_retries": 1,
        "extractor_retries": 2,
        "check_formats": False,
        "format": "bestaudio/best",
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.8",
        },
    }

    # Try several current YouTube clients.  The previous code accidentally
    # supplied extractor_args in the wrong shape (a list instead of the
    # documented player_client mapping), which could make the fast resolver
    # fail and unnecessarily send the bot to cookies.
    client_names = ["default", "android", "web", "web_embedded"]
    attempts = []
    if use_cookies:
        attempts.append(("default-cookies", "default", True))
    for name in client_names:
        attempts.append((name, name, False))

    last_error = None
    for name, client, with_cookies in attempts:
        # Do not make repeated extraction calls when another request has just
        # populated the cache.
        cached = _get_direct_cached(video_id)
        if cached:
            return {"status": True, "videoId": video_id, "url": cached,
                    "cached": True, "resolve_time": 0}

        opts = dict(common)
        opts["extractor_args"] = {"youtube": {"player_client": [client]}}
        if with_cookies:
            opts["cookiefile"] = COOKIES_FILE
            opts["js_runtimes"] = {"node": {}}

        attempt_started = time.perf_counter()
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(canonical, download=False)

            media_url = info.get("url")
            if not media_url:
                audio = [
                    f for f in (info.get("formats") or [])
                    if f.get("url") and f.get("acodec") not in (None, "none")
                    and f.get("vcodec") in (None, "none")
                ]
                audio.sort(key=lambda item: (item.get("abr") or 0), reverse=True)
                if audio:
                    media_url = audio[0]["url"]

            if not media_url:
                raise RuntimeError("No direct audio stream was returned")

            elapsed = round(time.perf_counter() - started, 3)
            _set_direct_cached(video_id, media_url)
            logger.info(
                "FAST audio resolved in %ss for %s using %s cookies=%s",
                elapsed, video_id, name, with_cookies,
            )
            return {
                "status": True,
                "videoId": video_id,
                "title": info.get("title", ""),
                "duration": info.get("duration", 0),
                "thumbnail": info.get("thumbnail", ""),
                "url": media_url,
                "cached": False,
                "resolve_time": elapsed,
            }
        except Exception as exc:
            last_error = exc
            logger.warning(
                "FAST resolver %s failed after %ss: %s",
                name, round(time.perf_counter() - attempt_started, 3), exc,
            )

    raise RuntimeError(str(last_error) if last_error else "Unable to resolve YouTube audio")


def resolve_direct_audio_sync(url: str) -> Dict[str, Any]:
    """Resolve a signed YouTube audio URL and coalesce duplicate requests."""
    video_id = extract_video_id(url)
    if not video_id:
        raise RuntimeError("Invalid YouTube URL or video ID")

    with _get_direct_resolve_lock(video_id):
        cached = _get_direct_cached(video_id)
        if cached:
            return {"status": True, "videoId": video_id, "url": cached,
                    "cached": True, "resolve_time": 0}
        return _resolve_direct_audio_uncached(video_id)

# =========================================================
# AUDIO DOWNLOAD
# =========================================================

def download_audio_sync(
    url: str
) -> Dict[str, Any]:

    video_id = extract_video_id(
        url
    )

    # Accept both full YouTube URLs and the plain video IDs used by
    # older Music Bot clients. yt-dlp needs a real URL to extract media.
    if video_id and not re.match(r"^https?://", url):
        url = f"https://www.youtube.com/watch?v={video_id}"

    # -----------------------------------------
    # DATABASE CACHE
    # -----------------------------------------

    if video_id:

        cached_data = get_cached_metadata(
            video_id,
            "mp3"
        )

        if cached_data:

            logger.info(
                f"Database cache hit! "
                f"Returning audio for {video_id}"
            )

            return {

                "status":
                    True,

                "title":
                    cached_data["title"],

                "duration":
                    cached_data["duration"],

                "thumbnail":
                    cached_data["thumbnail"],

                "filename":
                    cached_data["file_name"],

                "path":
                    cached_data["file_path"],

                "download_url":
                    f"/files/"
                    f"{cached_data['file_name']}",

                "videoId":
                    video_id,

                "uploader":
                    "Cached",

                "filesize":
                    cached_data["file_size"]
            }

        # -----------------------------------------
        # LEGACY CACHE
        # -----------------------------------------

        legacy_file = find_legacy_cached_file(
            video_id,
            "mp3"
        )

        if legacy_file:

            path = os.path.join(
                DOWNLOAD_DIR,
                legacy_file
            )

            if (
                os.path.isfile(path)
                and
                os.path.getsize(path) > 0
            ):

                logger.info(
                    f"Legacy disk cache hit "
                    f"for {video_id}. "
                    "Saving to DB."
                )

                data = {

                    "videoId":
                        video_id,

                    "title":
                        legacy_file[
                            :-
                            len(
                                f"_{video_id}.mp3"
                            )
                        ],

                    "filename":
                        legacy_file,

                    "path":
                        path,

                    "type":
                        "mp3",

                    "filesize":
                        os.path.getsize(path),

                    "duration":
                        0,

                    "thumbnail":
                        f"https://i.ytimg.com/vi/"
                        f"{video_id}/hqdefault.jpg"
                }

                save_cached_metadata(
                    data,
                    "mp3"
                )

                data["status"] = True

                data["download_url"] = (
                    f"/files/{legacy_file}"
                )

                data["uploader"] = "Cached"

                return data

    # -----------------------------------------
    # ACTUAL DOWNLOAD
    # -----------------------------------------

    logger.info(
        f"Starting audio download for: {url}"
    )

    opts = get_base_ydl_opts()

    opts.update({

        "format":
            # Prefer any available audio format; FFmpeg converts it to MP3.
            "bestaudio/best",

        "writethumbnail":
            False,

        "postprocessors": [

            {

                "key":
                    "FFmpegExtractAudio",

                "preferredcodec":
                    "mp3",

                "preferredquality":
                    "192"
            }
        ],

        "extractor_args": {

            "youtube": [
                f"player_client={YOUTUBE_PLAYER_CLIENTS}"
            ]
        },

        # -----------------------------------------
        # ENV CONFIGURABLE SPEED SETTINGS
        # -----------------------------------------

        "concurrent_fragment_downloads":
            CONCURRENT_FRAGMENT_DOWNLOADS,

        "http_chunk_size":
            HTTP_CHUNK_SIZE,

        "nocheckcertificate":
            True,

        "noprogress":
            True,

        "quiet":
            True,

        "no_warnings":
            True,

        "updatetime":
            False,

        "clean_infojson":
            False,

        "retries":
            RETRIES,

        "fragment_retries":
            FRAGMENT_RETRIES,

        "socket_timeout":
            SOCKET_TIMEOUT,

        "postprocessor_args": [

            "-threads",
            "0",

            "-vn",
            "-sn"
        ]
    })

    try:

        with yt_dlp.YoutubeDL(
            opts
        ) as ydl:

            info = ydl.extract_info(
                url,
                download=True
            )

            filename = ydl.prepare_filename(
                info
            )

            base_path, _ = os.path.splitext(
                filename
            )

            final_path = (
                f"{base_path}.mp3"
            )

            if (
                not os.path.isfile(
                    final_path
                )
                or
                os.path.getsize(
                    final_path
                ) == 0
            ):

                raise RuntimeError(
                    "Downloaded file is missing "
                    "or empty."
                )

            logger.info(
                f"Successfully downloaded audio: "
                f"{final_path}"
            )

            response_data = {

                "status":
                    True,

                "title":
                    info.get(
                        "title",
                        ""
                    ),

                "duration":
                    info.get(
                        "duration",
                        0
                    ),

                "thumbnail":
                    info.get(
                        "thumbnail",
                        ""
                    ),

                "filename":
                    os.path.basename(
                        final_path
                    ),

                "path":
                    final_path,

                "download_url":
                    f"/files/"
                    f"{os.path.basename(final_path)}",

                "videoId":
                    info.get("id"),

                "uploader":
                    info.get("uploader"),

                "filesize":
                    os.path.getsize(
                        final_path
                    )
            }

            save_cached_metadata(
                response_data,
                "mp3"
            )

            return response_data

    except yt_dlp.utils.DownloadError as e:

        logger.error(
            f"yt-dlp error downloading audio "
            f"for {url}: {e}"
        )

        raise RuntimeError(
            f"Download Error: {str(e)}"
        )

    except Exception as e:

        logger.error(
            f"Unexpected error downloading audio "
            f"for {url}: {e}"
        )

        raise RuntimeError(
            f"Internal Server Error: {str(e)}"
        )


# =========================================================
# VIDEO DOWNLOAD
# =========================================================

def download_video_sync(
    url: str
) -> Dict[str, Any]:

    video_id = extract_video_id(
        url
    )

    # Accept both full YouTube URLs and the plain video IDs used by
    # older Music Bot clients. yt-dlp needs a real URL to extract media.
    if video_id and not re.match(r"^https?://", url):
        url = f"https://www.youtube.com/watch?v={video_id}"

    # -----------------------------------------
    # DATABASE CACHE
    # -----------------------------------------

    if video_id:

        cached_data = get_cached_metadata(
            video_id,
            "mp4"
        )

        if cached_data:

            logger.info(
                f"Database cache hit! "
                f"Returning video for {video_id}"
            )

            return {

                "status":
                    True,

                "title":
                    cached_data["title"],

                "thumbnail":
                    cached_data["thumbnail"],

                "filename":
                    cached_data["file_name"],

                "path":
                    cached_data["file_path"],

                "download_url":
                    f"/files/"
                    f"{cached_data['file_name']}",

                "duration":
                    cached_data["duration"],

                "videoId":
                    video_id,

                "uploader":
                    "Cached",

                "filesize":
                    cached_data["file_size"]
            }

        # -----------------------------------------
        # LEGACY CACHE
        # -----------------------------------------

        legacy_file = find_legacy_cached_file(
            video_id,
            "mp4"
        )

        if legacy_file:

            path = os.path.join(
                DOWNLOAD_DIR,
                legacy_file
            )

            if (
                os.path.isfile(path)
                and
                os.path.getsize(path) > 0
            ):

                logger.info(
                    f"Legacy disk cache hit "
                    f"for {video_id}. "
                    "Saving to DB."
                )

                data = {

                    "videoId":
                        video_id,

                    "title":
                        legacy_file[
                            :-
                            len(
                                f"_{video_id}.mp4"
                            )
                        ],

                    "filename":
                        legacy_file,

                    "path":
                        path,

                    "type":
                        "mp4",

                    "filesize":
                        os.path.getsize(path),

                    "duration":
                        0,

                    "thumbnail":
                        f"https://i.ytimg.com/vi/"
                        f"{video_id}/hqdefault.jpg"
                }

                save_cached_metadata(
                    data,
                    "mp4"
                )

                data["status"] = True

                data["download_url"] = (
                    f"/files/{legacy_file}"
                )

                data["uploader"] = "Cached"

                return data

    # -----------------------------------------
    # ACTUAL DOWNLOAD
    # -----------------------------------------

    logger.info(
        f"Starting video download for: {url}"
    )

    opts = get_base_ydl_opts()

    opts.update({

        "format":
            # Do not require MP4/M4A streams; YouTube often exposes
            # WebM or other formats depending on the player client.
            f"bestvideo[height<={MAX_VIDEO_QUALITY}]"
            f"+bestaudio/best[height<={MAX_VIDEO_QUALITY}]/best",

        "merge_output_format":
            "mp4",

        "writethumbnail":
            False,

        "embedthumbnail":
            False,

        "extractor_args": {

            "youtube": [
                f"player_client={YOUTUBE_PLAYER_CLIENTS}"
            ]
        },

        # -----------------------------------------
        # ENV CONFIGURABLE SPEED SETTINGS
        # -----------------------------------------

        "concurrent_fragment_downloads":
            CONCURRENT_FRAGMENT_DOWNLOADS,

        "http_chunk_size":
            HTTP_CHUNK_SIZE,

        "nocheckcertificate":
            True,

        "noprogress":
            True,

        "quiet":
            True,

        "no_warnings":
            True,

        "updatetime":
            False,

        "clean_infojson":
            False,

        "retries":
            RETRIES,

        "fragment_retries":
            FRAGMENT_RETRIES,

        "socket_timeout":
            SOCKET_TIMEOUT,

        "postprocessor_args": [

            "-threads",
            "0"
        ]
    })

    try:

        with yt_dlp.YoutubeDL(
            opts
        ) as ydl:

            info = ydl.extract_info(
                url,
                download=True
            )

            filename = ydl.prepare_filename(
                info
            )

            base_path, _ = os.path.splitext(
                filename
            )

            final_path = (
                f"{base_path}.mp4"
            )

            # -----------------------------------------
            # Check possible output extensions
            # -----------------------------------------

            for ext in [
                ".mp4",
                ".webm",
                ".mkv"
            ]:

                test_path = (
                    f"{base_path}{ext}"
                )

                if (
                    os.path.isfile(
                        test_path
                    )
                    and
                    os.path.getsize(
                        test_path
                    ) > 0
                ):

                    final_path = test_path

                    break

            if not (
                os.path.isfile(
                    final_path
                )
                and
                os.path.getsize(
                    final_path
                ) > 0
            ):

                raise RuntimeError(
                    "Downloaded file not found "
                    "or is empty."
                )

            logger.info(
                f"Successfully downloaded video: "
                f"{final_path}"
            )

            response_data = {

                "status":
                    True,

                "title":
                    info.get(
                        "title",
                        ""
                    ),

                "thumbnail":
                    info.get(
                        "thumbnail",
                        ""
                    ),

                "filename":
                    os.path.basename(
                        final_path
                    ),

                "path":
                    final_path,

                "download_url":
                    f"/files/"
                    f"{os.path.basename(final_path)}",

                "duration":
                    info.get(
                        "duration",
                        0
                    ),

                "videoId":
                    info.get("id"),

                "uploader":
                    info.get("uploader"),

                "filesize":
                    os.path.getsize(
                        final_path
                    )
            }

            save_cached_metadata(
                response_data,
                "mp4"
            )

            return response_data

    except yt_dlp.utils.DownloadError as e:

        logger.error(
            f"yt-dlp error downloading video "
            f"for {url}: {e}"
        )

        raise RuntimeError(
            f"Download Error: {str(e)}"
        )

    except Exception as e:

        logger.error(
            f"Unexpected error downloading video "
            f"for {url}: {e}"
        )

        raise RuntimeError(
            f"Internal Server Error: {str(e)}"
        )


# =========================================================
# ROOT — DEVELOPER PORTAL
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
async def root():

    return HTMLResponse(
        content=DEVELOPER_PORTAL_HTML,
        status_code=200
    )


# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
async def health_check():

    return {

        "status":
            "healthy",

        "version":
            "2.3.0",

        "yt_dlp_version":
            yt_dlp.version.__version__,

        "cache_expiry_hours":
            CACHE_EXPIRE_HOURS
    }


# =========================================================
# SEARCH
# =========================================================

@app.get("/search")
async def search_youtube_music(

    _: bool = Depends(require_api_key),

    q: str = Query(
        ...,
        description="Search query"
    ),

    limit: int = Query(
        1,
        description=
            "Number of results to return (max 20)"
    )
):

    try:

        logger.info(
            f"Received search request "
            f"for query '{q}' "
            f"with limit {limit}"
        )

        actual_limit = min(
            max(
                1,
                limit
            ),
            20
        )

        cache_key = f"{q.strip().casefold()}::{actual_limit}"
        cached_results = _get_search_cached(cache_key)
        if cached_results is not None:
            if actual_limit == 1:
                return cached_results[0] if cached_results else {}
            return cached_results

        def perform_search():

            return ytmusic.search(
                q,
                filter="songs",
                limit=actual_limit
            )

        results = await asyncio.to_thread(
            perform_search
        )

        formatted_results = []

        for r in results:

            artists = ", ".join(
                [
                    a.get(
                        "name",
                        ""
                    )
                    for a in r.get(
                        "artists",
                        []
                    )
                ]
            )

            thumbnails = r.get(
                "thumbnails",
                []
            )

            thumbnail_url = (
                thumbnails[-1].get(
                    "url"
                )
                if thumbnails
                else None
            )

            formatted_results.append({

                "title":
                    r.get("title"),

                "artist":
                    artists,

                "videoId":
                    r.get("videoId"),

                "duration":
                    r.get("duration"),

                "thumbnail":
                    thumbnail_url
            })

        _set_search_cached(cache_key, formatted_results)

        logger.info(
            f"Successfully completed search "
            f"for query '{q}', "
            f"returned "
            f"{len(formatted_results)} "
            f"result(s)"
        )

        if actual_limit == 1:

            return (
                formatted_results[0]
                if formatted_results
                else {}
            )

        return formatted_results

    except Exception as e:

        logger.error(
            f"Search error for query '{q}': {e}"
        )

        raise HTTPException(
            status_code=500,
            detail={

                "error":
                    "Search failed",

                "message":
                    str(e)
            }
        )


# =========================================================
# THUMBNAIL API
# =========================================================

@app.get("/thumbnail")
async def get_thumbnail(

    _: bool = Depends(require_api_key),

    url: str = Query(
        ...,
        description="YouTube URL"
    )
):

    try:

        result = await asyncio.to_thread(
            fetch_thumbnail_sync,
            url
        )

        return result

    except Exception as e:

        logger.error(
            f"Thumbnail API error: {e}"
        )

        raise HTTPException(
            status_code=500,
            detail={

                "error":
                    "Failed to fetch thumbnail",

                "message":
                    str(e)
            }
        )


# =========================================================
# DIRECT AUDIO API
# =========================================================

@app.get("/direct")
async def direct_audio(
    _: bool = Depends(require_api_key),
    url: str = Query(..., description="YouTube URL or video ID")
):
    try:
        return JSONResponse(await asyncio.to_thread(resolve_direct_audio_sync, url))
    except Exception as e:
        logger.error("Direct audio API error: %s", e)
        raise HTTPException(status_code=500, detail={"error": "Direct audio failed", "message": str(e)})


async def _proxy_direct_audio(url: str) -> StreamingResponse:
    """Resolve and proxy audio through this API server.

    YouTube signed media URLs can be rejected when the client fetching them
    has a different egress IP from the server that resolved them. Keeping the
    upstream connection on this dyno avoids that cross-host 403.
    """
    result = await asyncio.to_thread(resolve_direct_audio_sync, url)
    audio_url = result.get("url") if isinstance(result, dict) else None
    if not isinstance(audio_url, str) or not audio_url.startswith(("http://", "https://")):
        raise HTTPException(status_code=502, detail="YouTube did not return a usable audio URL")

    client = httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(60.0, connect=10.0),
    )
    upstream = await client.send(
        client.build_request(
            "GET",
            audio_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.8",
            },
        ),
        stream=True,
    )

    if upstream.status_code != 200:
        status = upstream.status_code
        await upstream.aclose()
        await client.aclose()

        # Signed YouTube URLs can expire before our short cache TTL.  Evict the
        # stale entry and resolve once more instead of making the bot fall back
        # to its own yt-dlp/cookie downloader.
        video_id = extract_video_id(url)
        if video_id:
            with DIRECT_CACHE_LOCK:
                DIRECT_URL_CACHE.pop(video_id, None)
            try:
                retry_result = await asyncio.to_thread(resolve_direct_audio_sync, video_id)
                retry_url = retry_result.get("url") if isinstance(retry_result, dict) else None
                if retry_url:
                    retry_client = httpx.AsyncClient(
                        follow_redirects=True,
                        timeout=httpx.Timeout(60.0, connect=10.0),
                    )
                    retry_upstream = await retry_client.send(
                        retry_client.build_request(
                            "GET",
                            retry_url,
                            headers={
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
                                "Accept-Language": "en-US,en;q=0.8",
                            },
                        ),
                        stream=True,
                    )
                    if retry_upstream.status_code == 200:
                        async def retry_body():
                            try:
                                async for chunk in retry_upstream.aiter_bytes(64 * 1024):
                                    if chunk:
                                        yield chunk
                            finally:
                                await retry_upstream.aclose()
                                await retry_client.aclose()

                        retry_headers = {
                            "Cache-Control": "no-store",
                            "X-API-Resolve-Time": str(retry_result.get("resolve_time", "retry")),
                        }
                        if retry_upstream.headers.get("content-length"):
                            retry_headers["Content-Length"] = retry_upstream.headers["content-length"]
                        retry_type = retry_upstream.headers.get("content-type", "audio/mpeg").split(";", 1)[0]
                        logger.info("Recovered stale signed URL for %s after upstream HTTP %s", video_id, status)
                        return StreamingResponse(retry_body(), media_type=retry_type, headers=retry_headers)
                    await retry_upstream.aclose()
                    await retry_client.aclose()
            except Exception as retry_exc:
                logger.warning("Signed URL refresh failed for %s: %s", video_id, retry_exc)

        raise HTTPException(
            status_code=502,
            detail=f"YouTube audio upstream returned HTTP {status}",
        )

    async def body():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
                if chunk:
                    yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    response_headers = {
        "Cache-Control": "no-store",
        "X-API-Resolve-Time": str(result.get("resolve_time", "cached")),
    }
    if upstream.headers.get("content-length"):
        response_headers["Content-Length"] = upstream.headers["content-length"]

    content_type = upstream.headers.get("content-type", "audio/mpeg").split(";", 1)[0]
    return StreamingResponse(body(), media_type=content_type, headers=response_headers)


@app.get("/stream")
async def stream_audio(
    _: bool = Depends(require_api_key),
    url: str = Query(..., description="YouTube URL or video ID"),
):
    """Resolve and proxy audio so clients never fetch the signed URL directly."""
    try:
        return await _proxy_direct_audio(url)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Audio proxy error: %s", e)
        raise HTTPException(status_code=502, detail={"error": "Audio proxy failed", "message": str(e)})


# =========================================================
# AUDIO DOWNLOAD API
# =========================================================

@app.get("/download")
async def download_audio(

    _: bool = Depends(require_api_key),

    url: str = Query(
        ...,
        description="YouTube URL or video ID"
    ),

    type: Optional[str] = Query(
        default=None,
        description="Legacy Music Bot mode: audio or video"
    )
):

    try:

        requested_type = (type or "").strip().lower()
        if requested_type not in ("", "audio", "video"):
            raise HTTPException(
                status_code=400,
                detail="type must be audio or video"
            )

        # Proxy audio through the API server. Returning a YouTube signed URL
        # directly can produce HTTP 403 when the bot uses a different egress IP.
        if requested_type == "audio":
            return await _proxy_direct_audio(url)

        # The legacy bot uses /download?type=video. Keep that contract
        # working without changing the modern JSON response by default.
        if requested_type == "video":
            result = await asyncio.to_thread(
                download_video_sync,
                url
            )
        else:
            result = await asyncio.to_thread(
                download_audio_sync,
                url
            )

        if requested_type:
            file_path = result.get("path") if isinstance(result, dict) else None
            if not file_path or not os.path.isfile(file_path):
                raise HTTPException(
                    status_code=500,
                    detail="Download completed without a readable file"
                )
            return FileResponse(
                path=file_path,
                filename=result.get("filename") or os.path.basename(file_path),
                media_type="video/mp4" if requested_type == "video" else "audio/mpeg"
            )

        return JSONResponse(
            content=result
        )

    except HTTPException:
        raise
    except Exception as e:

        logger.error(
            f"Audio download API error: {e}"
        )

        raise HTTPException(
            status_code=500,
            detail={

                "error":
                    "Audio download failed",

                "message":
                    str(e)
            }
        )


# =========================================================
# VIDEO DOWNLOAD API
# =========================================================

@app.get("/video")
async def download_video(

    _: bool = Depends(require_api_key),

    url: str = Query(
        ...,
        description="YouTube URL"
    )
):

    try:

        result = await asyncio.to_thread(
            download_video_sync,
            url
        )

        return JSONResponse(
            content=result
        )

    except Exception as e:

        logger.error(
            f"Video download API error: {e}"
        )

        raise HTTPException(
            status_code=500,
            detail={

                "error":
                    "Video download failed",

                "message":
                    str(e)
            }
        )


# =========================================================
# FILE SERVING
# =========================================================

@app.get("/files/{filename}")
async def get_file(
    filename: str,
    _: bool = Depends(require_api_key)
):

    filename = os.path.basename(
        filename
    )

    file_path = os.path.join(
        DOWNLOAD_DIR,
        filename
    )

    if not os.path.isfile(
        file_path
    ):

        logger.warning(
            f"Requested file not found: "
            f"{filename}"
        )

        raise HTTPException(
            status_code=404,
            detail="File not found"
        )

    return FileResponse(
        path=file_path,
        filename=filename
    )


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=PORT,
        reload=False
    )