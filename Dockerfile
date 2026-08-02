FROM node:22-alpine AS frontend
WORKDIR /build
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN corepack enable && corepack prepare pnpm@11.9.0 --activate && pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm run build

FROM alpine:3.24 AS libjxl
ARG LIBJXL_VERSION=0.12.0
RUN apk add --no-cache curl lzip
RUN curl -fsSL \
    "https://github.com/libjxl/libjxl/releases/download/v${LIBJXL_VERSION}/jxl-linux-x86_64-static.tar.lz" \
    -o /tmp/libjxl.tar.lz
RUN sha256sum /tmp/libjxl.tar.lz \
    | grep -Fx "5318a1ea40adad76d023e0c17a03d4627f8282f83cb2b575e69be8e74f1ff456  /tmp/libjxl.tar.lz"
RUN mkdir -p /opt/libjxl \
    && lzip -dc /tmp/libjxl.tar.lz | tar -xf - -C /opt/libjxl
RUN /opt/libjxl/tools/cjxl --version 2>&1 | grep -F "v${LIBJXL_VERSION}" \
    && /opt/libjxl/tools/djxl --version 2>&1 | grep -F "v${LIBJXL_VERSION}"

FROM python:3.13-alpine
LABEL name="cbzxl-web" version="4.5.6"

RUN apk add --no-cache \
    tzdata \
    imagemagick \
    imagemagick-jpeg \
    imagemagick-tiff \
    imagemagick-webp \
    file
COPY --from=libjxl /opt/libjxl/tools/cjxl /usr/local/bin/cjxl
COPY --from=libjxl /opt/libjxl/tools/djxl /usr/local/bin/djxl

WORKDIR /app
RUN mkdir -p /data /config /dev/shm/cbzxl /dev/shm/cbzxl-runtime

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY cbzxl.py app.py /app/
COPY cbzxl_web /app/cbzxl_web
COPY --from=frontend /build/dist /app/frontend/dist
RUN python -m cbzxl_web.runtime_dependencies

ENV PYTHONUNBUFFERED=1 \
    CONFIG_DIR=/config \
    DB_PATH=/config/cbzxl-v3.sqlite3 \
    LIBRARY_PATH=/data \
    LOG_FILE= \
    SCRATCH_DIR=/dev/shm/cbzxl \
    WORKSPACE_DIR=/dev/shm/cbzxl/workspaces \
    RUNTIME_DIR=/dev/shm/cbzxl-runtime \
    DURABLE_CHECKPOINT_SECONDS=300 \
    DURABLE_CHECKPOINT_ARCHIVES=10000 \
    PAGE_DECISION_RETENTION_DAYS=180 \
    ARCHIVE_STABLE_SECONDS=90 \
    AUTO_RETRY_ATTEMPTS=1 \
    DATABASE_MAINTENANCE_SECONDS=604800 \
    MIGRATION_BACKUP_KEEP=3 \
    WORKER_STALL_SECONDS=900 \
    TMPDIR=/dev/shm \
    TEMP=/dev/shm \
    TMP=/dev/shm

VOLUME ["/data", "/config"]
EXPOSE 5000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000", "--no-access-log"]
