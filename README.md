# CBZXL

CBZXL is a self-hosted CBZ-to-JPEG XL archive optimizer with a portable SQLite database, durable scheduling, a separate conversion worker, and a minimalist web workspace.

## What changed

- Application state lives in `/config/cbzxl-v2.sqlite3`.
- The web API and conversion worker are separate processes.
- Scan requests, progress, cancellation, results, reviews, schedules, and activity survive restarts.
- Schedules use daily, weekly, monthly, or repeating interval choices—no cron expressions.
- Existing `converted_archives`, `runs`, and `archive_results` tables can be imported from the previous SQLite database.
- The migration workspace reads and explains the legacy schema before building the new record set; the source stays read-only.
- Per-archive progress samples power smooth live progress, rolling throughput, ETA, momentum, and durable run statistics.
- Engine 3 uses structured JSON events, page-level progress, content signatures, persistent resume manifests, bounded libjxl scheduling, decode verification, and fsynced atomic commits.
- Database exports use SQLite's online backup mechanism to produce a consistent single-file snapshot.

## Conversion engine

The worker treats `threads` as a total CPU budget. It automatically divides that budget between concurrent pages and each native `cjxl` encoder, preventing the oversubscription caused by giving every encoder the entire machine. Set `RESUME_DIR` to durable storage (the Compose default is `/config/resume`) so interrupted archives can continue from verified JXL pages on the next attempt.

The engine validates ZIP paths before extraction, writes each page through a partial file, checks the JXL signature, verifies the completed CBZ and its CRCs, decodes representative pages with `djxl`, syncs the replacement, and only then performs the atomic replacement.

Archive paths, archive filenames, image directories, and image basenames are immutable. Conversion may only replace an image extension with `.jxl` (for example, `Chapter 2/004.jpg` becomes `Chapter 2/004.jxl`). Flattening and empty-archive deletion are disabled. If two inputs would require the same `.jxl` path, the archive fails safely and remains unchanged.

Smart mode is lossless-first and savings-gated per file. Every convertible page is first tested as lossless JXL. If that file is smaller, it is kept immediately. Only when lossless is not smaller and the configured file-size or resolution threshold is met does the engine try distance `0.5`; that lossy result is also kept only when smaller. Otherwise the original page remains byte-for-byte unchanged. Archive-level size does not override those page decisions; metadata is deflated during repacking while already-compressed image formats are stored directly.

## Run with Docker Compose

Update the two host paths in `docker-compose.yml`, then pull and run the published x86-64 image:

```sh
docker compose pull
docker compose up -d
```

Open `http://localhost:5000`. The web and worker containers share `/data` and `/config`; keep `/config` on storage local to the Docker host so SQLite WAL locking remains reliable.

## Import the existing database

In **Data & settings**, enter the old database path as it appears inside the container—by default `/data/converted_archives.db`—and choose **Inspect first**. CBZXL checks database integrity and reports the available record counts before import. The source file is opened read-only and is never changed.

The importer is idempotent. The same source snapshot will not be imported twice. A command-line dry run is also available:

```sh
docker compose run --rm cbzxl-web python -m cbzxl_web.legacy /data/converted_archives.db --dry-run
```

## Development checks

```sh
python -m unittest discover -s tests -v
cd frontend && pnpm install && pnpm run build
docker compose config
```

The published `angeladmerkel/cbzxl-web:latest` image targets `linux/amd64`. The production Dockerfile builds the React interface first, then packages it with the FastAPI application and Python worker.
