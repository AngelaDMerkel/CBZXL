# CBZXL Web 2.0

CBZXL is a self-hosted CBZ-to-JPEG XL archive optimizer with a portable SQLite database, durable scheduling, a separate conversion worker, and a responsive operations interface.

## What changed

- Application state lives in `/config/cbzxl-v2.sqlite3`.
- The web API and conversion worker are separate processes.
- Scan requests, progress, cancellation, results, reviews, schedules, and activity survive restarts.
- Schedules use daily, weekly, monthly, or repeating interval choices—no cron expressions.
- Existing `converted_archives`, `runs`, and `archive_results` tables can be imported from the previous SQLite database.
- Database exports use SQLite's online backup mechanism to produce a consistent single-file snapshot.

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
