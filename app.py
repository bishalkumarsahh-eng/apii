import os
# === SPEED OPTIMIZATION V2 ===
# Defaults are tuned for Heroku: moderate concurrency and low retry overhead.
import re
import time
import asyncio
import sqlite3
import logging
import urllib.request
import subprocess
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
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
    "web_embedded,default"
).strip()

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

# ShrutiBots audio downloader (Priority 1). Keep the key in Heroku Config Vars.
SHRUTI_API_URL = os.getenv(
    "SHRUTI_API_URL",
    "https://api01.shrutibots.site"
).rstrip("/")

SHRUTI_API_KEY = os.getenv(
    "SHRUTI_API_KEY",
    ""
).strip()



async def require_api_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
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
    version="2.3.0-Production",
    lifespan=lifespan
)


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

        "remote_components":
            [
                "ejs:github"
            ]
    }

    if os.path.exists(
        COOKIES_FILE
    ):

        opts["cookiefile"] = (
            COOKIES_FILE
        )

        logger.info(
            f"Loaded cookies from "
            f"{COOKIES_FILE}"
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
# AUDIO DOWNLOAD
# =========================================================


# =========================================================
# SHRUTIBOTS AUDIO DOWNLOADER
# =========================================================

def download_audio_shruti(
    video_id: str
) -> Optional[Dict[str, Any]]:
    """Try ShrutiBots first and save its returned audio into DOWNLOAD_DIR.

    Supports either a direct audio response or JSON containing a downloadable URL.
    Returns the same metadata shape used by the existing downloader, or None on failure.
    """
    if not SHRUTI_API_KEY:
        logger.warning("ShrutiBots API key is not configured; using yt-dlp fallback.")
        return None

    api_url = f"{SHRUTI_API_URL}/download"
    params = {
        "url": video_id,
        "type": "audio",
        "api_key": SHRUTI_API_KEY,
    }

    try:
        logger.info(
            f"🚀 [PRIORITY 1] Trying ShrutiBots API for {video_id} (type: audio)"
        )

        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(
            f"{api_url}?{query}",
            headers={"User-Agent": "Mozilla/5.0"},
        )

        with urllib.request.urlopen(request, timeout=45) as response:
            content_type = (response.headers.get("Content-Type") or "").lower()
            body = response.read()

        # Direct audio/file response.
        if body and (
            content_type.startswith("audio/")
            or "application/octet-stream" in content_type
        ):
            ext = "mp3" if "mpeg" in content_type or "mp3" in content_type else "audio"
            filename = f"{video_id}.{ext}"
            path = os.path.join(DOWNLOAD_DIR, filename)
            with open(path, "wb") as f:
                f.write(body)
            if os.path.getsize(path) > 0:
                logger.info(
                    f"✅ [SHRUTI SUCCESS] Downloaded: {path} "
                    f"({os.path.getsize(path) / 1024 / 1024:.2f} MB)"
                )
                data = {
                    "status": True,
                    "title": video_id,
                    "duration": 0,
                    "thumbnail": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                    "filename": filename,
                    "path": path,
                    "download_url": f"/files/{filename}",
                    "videoId": video_id,
                    "uploader": "ShrutiBots",
                    "filesize": os.path.getsize(path),
                }
                save_cached_metadata(data, "mp3")
                return data

        # JSON response containing a remote audio URL.
        payload = json.loads(body.decode("utf-8", errors="replace"))

        def find_url(obj):
            if isinstance(obj, str) and obj.startswith(("http://", "https://")):
                return obj
            if isinstance(obj, dict):
                for key in (
                    "download_url", "audio_url", "url", "link",
                    "download", "file", "audio", "result", "data"
                ):
                    if key in obj:
                        found = find_url(obj[key])
                        if found:
                            return found
                for value in obj.values():
                    found = find_url(value)
                    if found:
                        return found
            elif isinstance(obj, list):
                for value in obj:
                    found = find_url(value)
                    if found:
                        return found
            return None

        remote_url = find_url(payload)
        if not remote_url:
            raise RuntimeError("ShrutiBots response did not contain an audio URL")

        logger.info(f"📥 [SHRUTI] Downloading audio for {video_id}...")
        audio_request = urllib.request.Request(
            remote_url,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(audio_request, timeout=90) as response:
            audio_body = response.read()

        if not audio_body:
            raise RuntimeError("ShrutiBots returned an empty audio file")

        filename = f"{video_id}.mp3"
        path = os.path.join(DOWNLOAD_DIR, filename)
        with open(path, "wb") as f:
            f.write(audio_body)

        if os.path.getsize(path) <= 0:
            raise RuntimeError("Saved ShrutiBots audio file is empty")

        logger.info(
            f"📦 [SHRUTI] File size: {os.path.getsize(path) / 1024 / 1024:.2f} MB"
        )
        logger.info(f"✅ [SHRUTI SUCCESS] Downloaded: {path}")

        data = {
            "status": True,
            "title": video_id,
            "duration": 0,
            "thumbnail": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
            "filename": filename,
            "path": path,
            "download_url": f"/files/{filename}",
            "videoId": video_id,
            "uploader": "ShrutiBots",
            "filesize": os.path.getsize(path),
        }
        save_cached_metadata(data, "mp3")
        return data

    except Exception as e:
        logger.warning(f"⚠️ [SHRUTI FAILED] {video_id}: {e}")
        return None


def download_audio_fast(video_id: str) -> Optional[Dict[str, Any]]:
    """Fast audio path: resolve a single embeddable audio format first.

    This avoids a full MP3 post-processing download on the first attempt.
    If the video is not available through the fast client, callers should
    fall back to the normal yt-dlp path.
    """
    fast_clients = os.getenv("FAST_YOUTUBE_PLAYER_CLIENTS", "web_embedded").strip()
    if not fast_clients:
        return None

    source_path = None
    try:
        started = time.perf_counter()
        logger.info(f"⚡ [FAST PATH] Resolving audio for {video_id} using {fast_clients}")

        opts = get_base_ydl_opts()
        opts.update({
            "format": "ba[ext=m4a]/ba[ext=webm]/bestaudio",
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "nocheckcertificate": True,
            "socket_timeout": min(SOCKET_TIMEOUT, 10),
            "retries": 2,
            "fragment_retries": 2,
            "extractor_args": {"youtube": [f"player_client={fast_clients}"]},
        })

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}",
                download=False,
            )
            fmt = info
            if info.get("requested_formats"):
                fmt = info["requested_formats"][0]
            media_url = fmt.get("url")
            if not media_url:
                return None

            ext = (fmt.get("ext") or "m4a").lower()
            source_path = os.path.join(DOWNLOAD_DIR, f".{video_id}.source.{ext}")
            final_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp3")

            logger.info(f"⚡ [FAST PATH] URL resolved in {time.perf_counter() - started:.2f}s; downloading source")
            req = urllib.request.Request(
                media_url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                    "Connection": "keep-alive",
                },
            )
            with urllib.request.urlopen(req, timeout=45) as response, open(source_path, "wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)

        if not os.path.isfile(source_path) or os.path.getsize(source_path) <= 0:
            return None

        # Convert locally only after the source has arrived. This keeps the
        # network path fast while preserving the existing MP3 API contract.
        subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", source_path,
                "-vn", "-sn",
                "-codec:a", "libmp3lame",
                "-b:a", "192k",
                final_path,
            ],
            check=True,
            timeout=120,
        )

        if not os.path.isfile(final_path) or os.path.getsize(final_path) <= 0:
            return None

        data = {
            "status": True,
            "title": info.get("title", video_id),
            "duration": info.get("duration", 0),
            "thumbnail": info.get("thumbnail", f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"),
            "filename": os.path.basename(final_path),
            "path": final_path,
            "download_url": f"/files/{os.path.basename(final_path)}",
            "videoId": info.get("id", video_id),
            "uploader": info.get("uploader", "YouTube"),
            "filesize": os.path.getsize(final_path),
        }
        save_cached_metadata(data, "mp3")
        logger.info(
            f"🚀 [FAST SUCCESS] {video_id}: {os.path.getsize(final_path) / 1048576:.2f} MB "
            f"in {time.perf_counter() - started:.2f}s"
        )
        return data
    except Exception as e:
        logger.info(f"↩️ [FAST FALLBACK] {video_id}: {e}")
        return None
    finally:
        if source_path and os.path.exists(source_path):
            try:
                os.remove(source_path)
            except OSError:
                pass


def download_audio_sync(
    url: str
) -> Dict[str, Any]:

    video_id = extract_video_id(
        url
    )

    # FAST LOCAL RESOLUTION IS PRIMARY.
    # The remote ShrutiBots resolver can have 10-15s upstream preparation
    # time, so do not pay that latency before checking cache + fast yt-dlp.
    # ShrutiBots remains an automatic fallback below.

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
    # FAST DIRECT AUDIO PATH
    # -----------------------------------------

    if video_id:
        fast_result = download_audio_fast(video_id)
        if fast_result:
            return fast_result

        # Remote ShrutiBots fallback only after the fast local path fails.
        shruti_result = download_audio_shruti(video_id)
        if shruti_result:
            return shruti_result

    # -----------------------------------------
    # ACTUAL DOWNLOAD
    # -----------------------------------------

    logger.info(
        f"Starting audio download for: {url}"
    )

    opts = get_base_ydl_opts()

    opts.update({

        "format":
            "ba[ext=m4a]/ba[ext=webm]/bestaudio/best",

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
            f"bv*[height<={MAX_VIDEO_QUALITY}]"
            f"[ext=mp4]+ba[ext=m4a]/"
            f"b[height<={MAX_VIDEO_QUALITY}]"
            f"[ext=mp4]/best",

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
# AUDIO DOWNLOAD API
# =========================================================

@app.get("/download")
async def download_audio(

    _: bool = Depends(require_api_key),

    url: str = Query(
        ...,
        description="YouTube URL"
    )
):

    try:

        result = await asyncio.to_thread(
            download_audio_sync,
            url
        )

        return JSONResponse(
            content=result
        )

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
# DIRECT MEDIA STREAMING API
# =========================================================

@app.get("/stream")
async def stream_audio(
    _: bool = Depends(require_api_key),
    url: str = Query(..., description="YouTube URL or video ID")
):
    """Download audio on the server and stream the finished MP3 directly.

    This endpoint is optimized for music bots: one HTTP request and no
    intermediate JSON -> /files round trip.
    """
    try:
        result = await asyncio.to_thread(download_audio_sync, url)
        if not result or not result.get("status"):
            raise HTTPException(status_code=500, detail="Audio download failed")

        file_path = result.get("path")
        filename = os.path.basename(result.get("filename") or file_path or "audio.mp3")
        if not file_path or not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail="Downloaded audio file not found")

        file_size = os.path.getsize(file_path)
        if file_size <= 1024:
            raise HTTPException(status_code=500, detail="Downloaded audio file is empty")

        logger.info(f"Direct audio stream ready: {filename} ({file_size / 1048576:.2f} MB)")
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="audio/mpeg",
            headers={"Cache-Control": "no-store"}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Direct audio stream error for {url}: {e}")
        raise HTTPException(status_code=500, detail="Audio streaming failed")


@app.get("/video-stream")
async def stream_video(
    _: bool = Depends(require_api_key),
    url: str = Query(..., description="YouTube URL or video ID")
):
    """Download video on the server and stream the finished file directly."""
    try:
        result = await asyncio.to_thread(download_video_sync, url)
        if not result or not result.get("status"):
            raise HTTPException(status_code=500, detail="Video download failed")

        file_path = result.get("path")
        filename = os.path.basename(result.get("filename") or file_path or "video.mp4")
        if not file_path or not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail="Downloaded video file not found")

        file_size = os.path.getsize(file_path)
        if file_size <= 1024:
            raise HTTPException(status_code=500, detail="Downloaded video file is empty")

        logger.info(f"Direct video stream ready: {filename} ({file_size / 1048576:.2f} MB)")
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="video/mp4",
            headers={"Cache-Control": "no-store"}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Direct video stream error for {url}: {e}")
        raise HTTPException(status_code=500, detail="Video streaming failed")

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