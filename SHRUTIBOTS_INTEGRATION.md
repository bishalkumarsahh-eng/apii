# ShrutiBots integration

Audio `/download` now tries ShrutiBots first, then falls back to the existing
yt-dlp downloader when ShrutiBots fails.

Heroku Config Vars:
- `SHRUTI_API_URL=https://api01.shrutibots.site`
- `SHRUTI_API_KEY=<your key>`

The real API key is intentionally not stored in this ZIP.
