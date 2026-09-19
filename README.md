# MAGMA Music API

Lean FastAPI YouTube/YouTube Music downloader API for music-bot backends.

## Audio path
1. Check SQLite cache.
2. If cached, return immediately.
3. Otherwise use one yt-dlp + FFmpeg download path.
4. Save the finished MP3 and metadata to cache.

There is no remote downloader dependency or duplicate audio fallback path. This avoids hidden upstream TTFB delays.

## Endpoints
- `GET /` — developer portal
- `GET /health` — health/status
- `GET /search?query=...` — YouTube Music search
- `GET /thumbnail?url=...` — thumbnail metadata
- `GET /download?url=...` — JSON metadata for downloaded MP3
- `GET /stream?url=...` — direct MP3 response
- `GET /video?url=...` — video download metadata
- `GET /video-stream?url=...` — direct video response
- `GET /files/{filename}` — cached file response

All protected endpoints require `X-API-Key`, `Authorization: Bearer ...`, or the legacy `api_key` query parameter.

## Docker
```bash
docker build -t magma-api .
docker run -d --name magma-api -p 8000:8000 --env-file .env magma-api
```

The container includes FFmpeg and Node.js for yt-dlp's JavaScript challenge support.
