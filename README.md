# CBZXL

CBZXL is a self-hosted CBZ-to-JPEG XL archive optimizer with a portable SQLite database, durable scheduling, a separate conversion worker, and a minimalist web workspace.

## What changed

- Application state lives in `/config/cbzxl-v3.sqlite3`. Earlier database files remain untouched and can be inspected and imported through the legacy importer.
- The web API and conversion worker are separate processes.
- Scan requests, durable checkpoints, cancellation, results, reviews, schedules, and activity survive restarts. Sub-checkpoint live progress is intentionally volatile.
- Schedules use daily, weekly, monthly, or repeating interval choices—no cron expressions.
- Existing `converted_archives`, `runs`, and `archive_results` tables can be imported from the previous SQLite database.
- The legacy importer reads and explains the old schema before building the new record set; the source stays read-only.
- RAM-backed structured events power smooth page-level progress without writing each update to the application database. Periodic checkpoints retain rolling throughput, ETA, momentum, and durable run statistics.
- Engine 3 uses content signatures, ephemeral RAM workspaces, bounded libjxl scheduling, decode verification, and atomic archive replacement.
- The streamlined interface centers the active conversion, lifetime statistics, an actionable attention queue, run history, and settings.
- Failed archives retain structured diagnostics and can be retried normally or in compatibility mode. Page-level failures can be previewed, preserved, replaced without changing their internal filename, or explicitly deleted.
- Database exports use SQLite's online backup mechanism to produce a consistent single-file snapshot.

## Conversion engine

The worker treats `threads` as a total CPU budget. It automatically divides that budget between concurrent pages and each native `cjxl` encoder, preventing the oversubscription caused by giving every encoder the entire machine.

Extraction, candidate images, conversion decisions, and live WebUI state remain in RAM. Interrupted archives restart cleanly from their unchanged source CBZ; no resume manifest is written to durable storage. Unchanged results and format inventory are committed in a single checkpoint every five minutes by default. Set `DURABLE_CHECKPOINT_SECONDS` between `300` and `600` to choose a five-to-ten-minute interval. Modified archives, failures requiring attention, explicit user actions, and run boundaries are committed immediately because those records protect data integrity or user intent. `CBZXL_SHM_SIZE` and `CBZXL_MEMORY_LIMIT` control the Compose RAM allocation and container limit.

The production server also disables routine HTTP access logging, so dashboard polling and health checks do not create a continuous Docker log stream. Startup, warning, and error logs remain available.

The engine validates ZIP paths before extraction, writes each page through a partial file, checks the JXL signature, verifies the completed CBZ and its CRCs, decodes representative pages with `djxl`, syncs the replacement, and only then performs the atomic replacement.

Archive paths, archive filenames, image directories, and image basenames are immutable. Conversion may only replace an image extension with `.jxl` (for example, `Chapter 2/004.jpg` becomes `Chapter 2/004.jxl`). Flattening and empty-archive deletion are disabled. If two inputs would require the same `.jxl` path, the archive fails safely and remains unchanged.

Every conversion is savings-gated per file: a JXL candidate is retained only when it is smaller than its source page. Smart mode remains lossless-first. Only when lossless is not smaller and the configured file-size or resolution threshold is met does the engine try distance `0.5`; that candidate must also be smaller. Otherwise the original page remains byte-for-byte unchanged. A final archive-level guard also rejects a non-repair result that is not smaller overall.

## Run with Docker Compose

Update the two host paths in `docker-compose.yml`, then pull and run the published x86-64 image:

```sh
docker compose pull
docker compose up -d
```

Open `http://localhost:5000`. The web and worker containers share `/data` and `/config`; keep `/config` on storage local to the Docker host so SQLite WAL locking remains reliable.

## Import the existing database

The one-time importer remains available from the command line rather than occupying permanent space in the web interface. Pass the old database path as it appears inside the container—by default `/data/converted_archives.db`. Begin with a dry run; CBZXL checks database integrity and reports the available record counts while opening the source read-only:

```sh
docker compose run --rm cbzxl-web python -m cbzxl_web.legacy /data/converted_archives.db --dry-run
```

Remove `--dry-run` to perform the import. The importer is idempotent, so the same source snapshot will not be imported twice.

## Homepage widget

CBZXL exposes a read-only summary at `/api/integrations/homepage` for
[Homepage's Custom API widget](https://gethomepage.dev/widgets/services/customapi/).
It reports exact net savings in bytes, the current run stage, smooth progress, the
current archive, attention count, throughput, and ETA. The endpoint continues to
return HTTP 200 with a `Degraded` status if the database is temporarily
unavailable.

Add this service to Homepage's `services.yaml`:

```yaml
- Media:
    - CBZXL:
        icon: mdi-archive-arrow-down-outline
        href: http://supertanker:5000
        description: Comic archive optimization
        widget:
          type: customapi
          url: http://supertanker:5000/api/integrations/homepage
          refreshInterval: 10000
          mappings:
            - field: net_savings_bytes
              label: Net Savings
              format: bytes
            - field: status
              label: Status
              format: text
            - field: progress_percent
              label: Progress
              format: float
              suffix: "%"
```

Homepage fetches service widgets from its server container. The `url` must
therefore be reachable from that container; use the CBZXL container name and
Docker network instead of `supertanker` if the containers share a network.

## Development checks

```sh
python -m unittest discover -s tests -v
cd frontend && pnpm install && pnpm run build
docker compose config
```

The published `angeladmerkel/cbzxl-web:latest` image targets `linux/amd64`. The production Dockerfile builds the React interface first, then packages it with the FastAPI application and Python worker.
