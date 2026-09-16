# Music API performance update

This archive contains the repository files with the performance update applied.

Updated files:
- app.py: coalesces duplicate direct-audio resolutions, adds bounded search/direct URL caches, and avoids per-request GitHub EJS downloads.
- Procfile and start.sh: disable access logs and support WEB_CONCURRENCY.

Secrets intentionally excluded: .env and cookies.txt. Copy those from your existing deployment; do not commit them.

Validate after replacing files:
```bash
python -m py_compile app.py
git diff --check
```


## Direct bot streaming
Use `/stream?url=VIDEO_ID` for audio and `/video-stream?url=VIDEO_ID` for video. These endpoints return the media directly after the server finishes the yt-dlp download, avoiding the JSON metadata + second `/files/...` request used by `/download`.


## Fast audio resolver

The audio downloader now tries a lightweight `web_embedded` YouTube resolver before the legacy full download path. If the video is not available through that client, it automatically falls back to the existing downloader. The MP3 API contract is unchanged.
