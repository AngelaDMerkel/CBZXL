# CBZXL 5

CBZXL is a self-hosted comic archive optimizer. It converts eligible pages in
CBZ files to JPEG XL, retains originals whenever conversion does not save
space, verifies every accepted candidate, and keeps an indexed lifetime history
in SQLite.

Version 5 is a ground-up Rust rebuild. It replaces the Python web/worker split
with one small binary while retaining the conservative archive and pixel-safety
contract established by CBZXL 4.

## What it does

- Converts JPEG, PNG, TIFF, and BMP pages to JPEG XL.
- Inventories and preserves WebP without ever submitting it to the JPEG XL encoder.
- Supports lossless, fixed lossy, and lossless-first Smart policies.
- Scans the entire library, only new/changed files, or only failed archives.
- Runs manual or daily, weekly, monthly, and interval schedules.
- Keeps complete, paginated outcomes for optimized, retained, skipped, audited,
  repaired, deferred, and failed archives.
- Reports per-page decisions and attempts without using text logs as state.
- Repairs damaged images and ZIP structures through explicit Attention actions.
- Exposes health, readiness, statistics, database export, and Homepage widget
  endpoints.
- Supports optional bearer-token authentication.

## CBZXL 4 feature parity

Version 5.2 retains the August 15, 2026 visual system and restores the complete compatibility surface:

- the original Overview, Attention, History, detailed statistics, policies,
  schedules, diagnostics, paired collision previews, reviews, and page history;
- legacy `/api/*` routes alongside `/api/v2/*`, including Homepage and API docs;
- normalized-input, single-thread, JPEG-reconstruction, and one-shot
  archive compatibility retries;
- effort 11 expert mode, stalled-command termination, worker self-restart, and
  progress shared through `RUNTIME_DIR` when web and worker run separately;
- ZIP64, Stored, Deflate, BZIP2, and LZMA recovery plus CP437 ZIP names;
- degraded database/runtime health behavior, backup rotation, optional file
  logging, configurable checkpoints, maintenance, retry, and retention controls.

The v5 API adds bearer-token authentication and cursor pagination without
removing the unversioned compatibility API.

## Safety contract

CBZXL never accepts a replacement merely because encoding finished.

1. The source archive must be stable before processing.
2. ZIP paths, duplicate names, symlinks, and expansion limits are validated.
3. A JPEG XL candidate must have a valid signature and be smaller than its
   source page before decode verification is attempted.
4. Lossless candidates must decode to the same canonical sRGB pixels.
5. Every rebuilt ZIP entry is read back through CRC verification.
6. Representative JXL pages are decoded from the final workspace.
7. Ordinary replacements must make the complete CBZ smaller.
8. The source identity and content fingerprint are checked again immediately
   before replacement.
9. The verified candidate is synced and atomically renamed from the same
   directory as the source.
10. A durable commit intent lets the next worker adopt or reject an interrupted
    filesystem replacement safely.

Page failures do not stop the rest of an archive. Failed originals remain in
place and appear individually in Attention.

## Architecture

The `cbzxl` binary has five commands:

| Command | Purpose |
| --- | --- |
| `cbzxl run` | Web server and embedded worker; the default container command |
| `cbzxl serve` | Web server only |
| `cbzxl worker` | Worker only |
| `cbzxl migrate SOURCE` | Inspect or import a CBZXL v2/v3 database |
| `cbzxl doctor` | SQLite integrity and schema report |
| `cbzxl healthcheck` | Lightweight HTTP readiness probe used by Docker |

The implementation is divided by responsibility:

| Path | Responsibility |
| --- | --- |
| `src/domain.rs` | Typed policies, jobs, outcomes, issues, and repairs |
| `src/schedule.rs` | DST-aware schedule validation and calculation |
| `src/db/` | Schema, queue, archives, imports, maintenance, and commits |
| `src/engine/` | Discovery, ZIP safety, media verification, repairs, and commits |
| `src/api/` | Versioned HTTP modules and authentication |
| `frontend/src/App.tsx` | Overview, Attention, History, Settings, and authentication workflows |
| `frontend/src/settings/` | Library, policy, and schedule editors |
| `legacy/python/` | Unshipped CBZXL 4 behavioral reference and tests |

SQLite remains the control plane because CBZXL has one local writer and many
short readers. The binary bundles SQLite 3.53.2 and uses WAL mode, a busy
timeout, batched outcomes, passive checkpoints, and online backups.

## Docker Compose

Edit the two host paths in `docker-compose.yml`:

```yaml
volumes:
  - /path/to/comics:/data
  - /path/to/cbzxl-config:/config
```

Then run:

```sh
docker compose build
docker compose up -d
```

Open `http://<host>:5000`.

The image builds libjxl 0.12.0 from its pinned release tag for the target
architecture. Both AMD64 and ARM64 builds use the same runtime validation.

Resource overrides now affect Compose directly:

```sh
CBZXL_CPU_LIMIT=12 \
CBZXL_MEMORY_LIMIT=24g \
CBZXL_SHM_SIZE=12g \
docker compose up -d
```

CBZXL intersects `CPU_LIMIT` with the container CPU quota and CPU set before
normalizing a policy.

## Importing an existing database

If `DB_PATH` points directly at a recognized v2 or v3 database, startup performs
a one-time, locked upgrade automatically. It creates and verifies a backup in
`/config/backups`, imports into a fresh v5 schema, verifies the result, and then
atomically replaces the configured database. This supports existing deployments
that keep `DB_PATH=/config/cbzxl-v3.sqlite3` while updating the image to v5.

The explicit importer below remains useful for dry runs or when preserving the
source at its original path is preferred.

The importer recognizes:

- legacy v2 `converted_archives`, `runs`, and `archive_results`; and
- current v3 schema version 15, including libraries, policies, schedules, jobs,
  archives, results, page decisions, issues, reviews, format inventory, events,
  and lifetime snapshots.

It creates an online SQLite snapshot, checks its integrity, computes a content
digest, imports transactionally, and verifies the target with `quick_check` and
`foreign_key_check`. Repeating the same snapshot is idempotent. Active source
jobs become interrupted, derived caches are rebuilt, and pending commit intents
are reported rather than copied blindly.

With the old and new databases mounted in `/config`:

```sh
docker compose run --rm cbzxl \
  migrate /config/cbzxl-v3.sqlite3 --dry-run

docker compose run --rm cbzxl \
  migrate /config/cbzxl-v3.sqlite3
```

The explicit importer never modifies its source. CBZXL refuses to merge into a
target that already contains archive or job history.

After import, start the service and check:

```sh
docker compose run --rm cbzxl doctor
docker compose up -d
```

Library roots are preserved by import and can be changed in Settings. In the
container, the normal root is `/data`.

## Authentication

Set a long random token if port 5000 is reachable by anything other than a
fully trusted private network:

```dotenv
CBZXL_API_TOKEN=replace-with-a-long-random-value
```

CBZXL logs a warning whenever it listens on a non-loopback address without a
token because repair and deletion routes are otherwise available to every
client that can reach port 5000.

The browser asks for the token and stores it locally. Application APIs require
`Authorization: Bearer <token>`. Liveness, readiness, and the small Homepage
integration remain available without credentials; destructive and database
operations do not.

## Configuration

| Variable | Default | Purpose |
| --- | ---: | --- |
| `DB_PATH` | `/config/cbzxl-v5.sqlite3` | Durable SQLite database |
| `LIBRARY_PATH` | `/data` | Initial library root |
| `SCRATCH_DIR` | `/dev/shm/cbzxl` | Per-archive workspace |
| `FRONTEND_DIR` | `/app/frontend/dist` | Built web application |
| `LISTEN_ADDR` | `0.0.0.0:5000` | HTTP listener |
| `TZ` | `UTC` | Schedule timezone |
| `CPU_LIMIT` | detected | Maximum policy CPU budget |
| `MAX_ARCHIVE_UNCOMPRESSED_MB` | `8192` | ZIP expansion ceiling |
| `ARCHIVE_STABLE_SECONDS` | `90` | Required source quiet period |
| `FINGERPRINT_AUDIT_DAYS` | `30` | Distributed content audit period |
| `PAGE_OUTCOME_RETENTION_DAYS` | `180` | Routine detailed page-history retention |
| `PAGE_DECISION_RETENTION_DAYS` | `180` | CBZXL 4 alias for page-history retention |
| `DURABLE_CHECKPOINT_SECONDS` | `300` | Maximum delay before unchanged outcomes are committed |
| `DURABLE_CHECKPOINT_ARCHIVES` | `10000` | Maximum unchanged outcomes per checkpoint |
| `DATABASE_MAINTENANCE_SECONDS` | `604800` | Integrity/optimization maintenance interval |
| `STATISTICS_CACHE_SECONDS` | `30` | In-memory detailed-statistics cache lifetime |
| `MIGRATION_BACKUP_KEEP` | `3` | Verified migration backups retained |
| `WORKER_STALL_SECONDS` | `900` | Maximum silent native command duration |
| `AUTO_RETRY_ATTEMPTS` | `1` | Conservative archive compatibility retries, capped at one |
| `ENCODER_MAX_SECONDS` | `0` | Per-encode timeout; zero disables it |
| `AUXILIARY_TIMEOUT_SECONDS` | `300` | Decode/inspection timeout |
| `WORKER_POLL_MS` | `1000` | Empty-queue poll interval |
| `CBZXL_API_TOKEN` | empty | Optional API bearer token |
| `CBZXL_UID` | `0` | Optional numeric runtime UID; set with a writable library mount |
| `CBZXL_GID` | `0` | Optional numeric runtime GID |
| `NTFY_URL` | empty | Full ntfy topic publish URL |
| `NTFY_TOKEN` | empty | Optional ntfy bearer token |
| `NTFY_TIMEOUT_SECONDS` | `10` | Notification timeout, clamped to 1–30 seconds |
| `RUST_LOG` | `cbzxl=info` | Structured log filter |
| `LOG_FILE` | empty | Optional additional text log path |

## API

Versioned endpoints use `/api/v2`.

The August 15 unversioned `/api/*` routes remain available for existing clients.
Interactive documentation is served at `/api/docs`, and both
`/api/integrations/homepage` and `/api/v2/integrations/homepage` are supported.

- `/health` and `/ready`
- `/api/v2/bootstrap` and `/api/v2/dashboard`
- `/api/v2/jobs` and `/api/v2/jobs/{id}`
- `/api/v2/jobs/{id}/events` for SSE progress
- `/api/v2/archives` and `/api/v2/archives/{id}`
- `/api/v2/issues` and `/api/v2/issues/summary`
- `/api/v2/libraries`, `/api/v2/policies`, and `/api/v2/schedules`
- `/api/v2/statistics`
- `/api/v2/database/export`, `/import`, and `/imports`
- `/api/v2/integrations/homepage`

Lists use cursor pagination rather than deep SQL offsets.

## Development

The backend requires Rust 1.96 or newer:

```sh
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

The frontend uses pnpm:

```sh
cd frontend
pnpm install --frozen-lockfile
pnpm run build
```

Validate deployment configuration and the production image:

```sh
docker compose config --quiet
docker build -t cbzxl:test .
```

The production worker performs end-to-end lossless JPEG, PNG, TIFF, BMP, and
BMP probes before accepting jobs. WebP is deliberately inventory-only.
