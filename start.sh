#!/usr/bin/env bash
set -e
exec uvicorn app:app --host 0.0.0.0 --port "${PORT:-8000}" --loop auto --no-access-log --workers "${WEB_CONCURRENCY:-1}"
