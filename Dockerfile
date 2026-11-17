FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS frontend
WORKDIR /build
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN corepack enable && corepack prepare pnpm@11.9.0 --activate \
    && pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm run build

FROM rust:1.96-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS backend
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked \
    && install -Dm755 target/release/cbzxl /out/cbzxl

FROM debian:bookworm@sha256:813017f3d62be4b5891a7acca6a01bdcd4b8513daa81b1ab99d3a50385b26931 AS libjxl
ARG LIBJXL_VERSION=0.12.0
ARG LIBJXL_COMMIT=a7a9c787341cf703dede03c2009fa460cae5e5df
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cmake git ninja-build pkg-config g++ libbrotli-dev \
    zlib1g-dev libpng-dev libjpeg-dev libgif-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --branch "v${LIBJXL_VERSION}" --depth 1 --recursive \
    https://github.com/libjxl/libjxl.git /src/libjxl \
    && test "$(git -C /src/libjxl rev-parse HEAD)" = "${LIBJXL_COMMIT}"
RUN cmake -S /src/libjxl -B /src/libjxl/build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/opt/libjxl \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF \
    -DJPEGXL_ENABLE_BENCHMARK=OFF \
    -DJPEGXL_ENABLE_EXAMPLES=OFF \
    -DJPEGXL_ENABLE_FUZZERS=OFF \
    -DJPEGXL_ENABLE_MANPAGES=OFF \
    -DJPEGXL_ENABLE_PLUGINS=OFF \
    -DJPEGXL_ENABLE_TOOLS=ON \
    && cmake --build /src/libjxl/build --target cjxl djxl -j2 \
    && install -Dm755 /src/libjxl/build/tools/cjxl /opt/libjxl/bin/cjxl \
    && install -Dm755 /src/libjxl/build/tools/djxl /opt/libjxl/bin/djxl

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241
ARG VERSION=development
ARG REVISION=unknown
ARG SOURCE=https://github.com/AngelaDMerkel/CBZXL
LABEL org.opencontainers.image.title="CBZXL" \
      org.opencontainers.image.description="Comic archive optimizer with a web UI and JPEG XL worker" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}"
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates gosu imagemagick tzdata libgif7 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=backend /out/cbzxl /usr/local/bin/cbzxl
COPY --from=libjxl /opt/libjxl/bin/ /usr/local/bin/
COPY --from=frontend /build/dist /app/frontend/dist
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN mkdir -p /data /config /runtime /dev/shm/cbzxl \
    && chmod 0755 /usr/local/bin/docker-entrypoint.sh \
    && cjxl --version \
    && djxl --version \
    && cbzxl --version

ENV DB_PATH=/config/cbzxl-v5.sqlite3 \
    LIBRARY_PATH=/data \
    SCRATCH_DIR=/dev/shm/cbzxl \
    RUNTIME_DIR=/runtime \
    FRONTEND_DIR=/app/frontend/dist \
    LISTEN_ADDR=0.0.0.0:5000 \
    CPU_LIMIT=4 \
    ARCHIVE_STABLE_SECONDS=90 \
    FINGERPRINT_AUDIT_DAYS=30 \
    PAGE_OUTCOME_RETENTION_DAYS=180 \
    MAX_ARCHIVE_UNCOMPRESSED_MB=8192 \
    TZ=UTC \
    RUST_LOG=cbzxl=info

VOLUME ["/data", "/config"]
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=20s \
    CMD ["cbzxl", "healthcheck"]
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
