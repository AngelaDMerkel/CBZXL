FROM node:22-alpine AS frontend
WORKDIR /build
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN corepack enable && corepack prepare pnpm@11.9.0 --activate && pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm run build

FROM python:3.13-alpine
LABEL name="cbzxl-web" version="4.1.1"

RUN apk add --no-cache libjxl-tools tzdata imagemagick file

WORKDIR /app
RUN mkdir -p /data /config /dev/shm/cbzxl

COPY requirements.txt cbzxl.py app.py /app/
COPY cbzxl_web /app/cbzxl_web
COPY --from=frontend /build/dist /app/frontend/dist
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    CONFIG_DIR=/config \
    DB_PATH=/config/cbzxl-v3.sqlite3 \
    LIBRARY_PATH=/data \
    LOG_FILE=/config/cbzxl.log \
    SCRATCH_DIR=/dev/shm/cbzxl \
    RESUME_DIR=/config/resume \
    WORKER_STALL_SECONDS=900 \
    TMPDIR=/dev/shm \
    TEMP=/dev/shm \
    TMP=/dev/shm

VOLUME ["/data", "/config"]
EXPOSE 5000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]
