# CBZXL

CBZXL is a self-hosted comic archive optimizer. It converts eligible images
inside CBZ files to JPEG XL, keeps an indexed lifetime history in SQLite, and
provides a minimalist web interface for running conversions, monitoring
progress, reviewing statistics, and repairing problem archives.

The project is designed around a conservative rule: a conversion should never
make the library less usable merely to claim a conversion. Originals are kept
when JPEG XL is not smaller, candidates are decoded and verified before use,
archive replacement is atomic, and one damaged archive does not stop the rest
of a run.

## Feature overview

### Web interface

The interface is organized into four focused pages:

- **Overview** shows the active or most recent run, smooth archive-and-page
  progress, throughput, ETA, and the lifetime totals for Net Savings, Pages
  Optimized, Archives Optimized, and Need Attention.
- **Attention** groups unresolved items by failure type. Every failed image is
  retained and reported separately, while archive-level counts distinguish a
  single damaged page from an archive with many failures. Each item can be
  retried, inspected, or dismissed until the next full rescan.
- **History** explains the result of each run and distinguishes optimized,
  already-current, retained, and failed archives. It records page outcomes,
  bytes saved, prior processing context, encoder effort, and technical events.
- **Settings** manages conversion policies and schedules. Policies can be
  created, edited, made the default, or deleted. Schedules can be created,
  paused, assigned a policy, and deleted.

The web process remains available in a degraded state if database startup
fails, rather than making the interface disappear with the worker. `/health`
reports the degraded state and `/ready` returns HTTP 503 until SQLite is
available. The same readiness contract covers the conversion runtime: CBZXL
blocks new jobs and marks both services unhealthy if any supported input cannot
complete an end-to-end lossless JPEG XL verification.

### Manual and scheduled runs

Manual conversion defaults to **Unprocessed** and offers three scopes:

- **Unprocessed** converts new archives and archives whose recorded source has
  changed. Unchanged archives already known to CBZXL are skipped.
- **Entire Library** attempts every CBZ in the library, including previously
  processed archives. Reruns add to run history and processing-effort
  statistics without resetting each archive's lifetime size baseline.
- **Failed** retries archives currently recorded as failed.

Scheduled runs use the scope saved in their assigned conversion policy. The
worker persists the queue, claims one job at a time, detects stalled converter
processes, launches due daily, weekly, monthly, or interval schedules, and can
publish terminal run summaries to an ntfy topic.

An unreadable or unconvertible archive is recorded for Attention and processing
continues. A run reaches its terminal state only after every archive in scope
has been attempted, unless the user cancels it or the worker itself is
interrupted.

### Conversion policies

CBZXL includes four initial policies:

- **Balanced lossless** — lossless JPEG XL, effort 10, and the default
  Unprocessed scope.
- **Smart lossy fallback** — tries lossless first, then distance `0.5` only
  when the source page is larger than 10 MB or 5 megapixels.
- **Recheck without conversion** — scans and refreshes records without
  rewriting image data.
- **Integrity audit** — read-only CRC checking of every archive entry plus a
  full decode of every JXL page.

Policies expose scope, conversion strategy, encoder effort, total CPU budget,
backup behavior, and the thresholds relevant to lossy modes. Fixed-distance
lossy policies are also supported.

The `threads` setting is a total CPU budget, not a per-image thread count. The
engine divides it between concurrent pages and each native `cjxl` process to
avoid multiplying the requested concurrency. `CBZXL_CPU_LIMIT` sets both the
Compose worker quota and the maximum offered by the WebUI. Each policy can
select any smaller budget. The worker also intersects that configured ceiling
with its actual cgroup quota, CPU set, and process affinity, then clamps stale
or externally submitted policies before launching the converter.

## Conversion and naming guarantees

### Lossless-first, savings-gated conversion

Every eligible page is judged independently:

1. Encode a JPEG XL candidate.
2. Verify its signature and compare its size with the source page.
3. Discard a non-smaller candidate without spending time decoding it.
4. Decode every smaller candidate with `djxl`; for lossless conversion, also
   compare its canonical sRGB pixels with the original source image, including
   when a metadata-free normalization input was required.
5. Keep the candidate only after that verification succeeds. If a page cannot
   be decoded, encoded, or verified, retain that original page and continue
   attempting every other image in the archive. Commit the verified smaller
   pages only when the complete rebuilt CBZ is also smaller, then report every
   failed entry through Attention, run history, and ntfy. A verified
   non-smaller candidate remains a normal retention.

Smart lossy never bypasses that order. It first attempts lossless JPEG XL. A
lossy candidate is attempted only when the configured size or resolution
threshold is met and lossless did not save space; it must also be smaller to be
kept. A final archive-level guard rejects an ordinary conversion if container
overhead would still make the complete CBZ larger. Filename corrections may add
only their precisely calculated ZIP entry-name overhead; any excess growth
causes CBZXL to retain the original archive.

The production image contains libjxl 0.12.0. Encoding defaults to effort 10;
effort 11 is available as expert mode. If a lower configured effort produces a
valid but non-saving lossless result, CBZXL can retry that page at effort 10
before retaining the original.

### Filename behavior

CBZ and page names are treated as library data:

- CBZ files are never renamed.
- Image directories and basenames are never changed.
- Successful conversion changes only the image extension to `.jxl`.
- A mislabeled retained image can have only its extension corrected to its
  detected format, such as `001.jpg` becoming `001.webp`.
- Archives are never flattened and empty archives are never silently deleted.

If extension correction would collide with another page, the source archive is
left intact. Attention displays both images and lets the user choose which one
to keep; the resulting archive is rebuilt and verified before replacement.

### Archive safety

Before a source can be replaced, CBZXL:

- rejects unsafe ZIP paths and symlink entries;
- enforces a configurable maximum uncompressed archive size;
- extracts into an isolated RAM workspace;
- writes candidates through `.cbzxl-partial` files;
- checks every output entry and CRC;
- decodes representative JXL pages;
- syncs the completed candidate;
- rechecks that the source archive has not changed during processing; and
- atomically replaces the source only after all checks pass.

Newly discovered or changed archives must remain stable for 90 seconds by
default. This prevents CBZXL from opening a file that another process is still
copying to the library.

A small write-ahead commit intent is stored in SQLite immediately before an
atomic replacement. If the worker stops after the filesystem replacement but
before the result is committed, the next run recognizes and adopts the verified
candidate instead of converting it again.

Transient encoder, decoder, or verification failures receive at most one
conservative compatibility retry. Corrupt ZIP structures, unsafe paths,
permission errors, missing sources, source changes, and user-decision failures
are not automatically retried.

## Attention and archive recovery

Attention groups failures without changing the compact queue layout. Opening an
item provides context-aware actions.

Available recovery paths include:

- **CRC-32 failures** — inspect stored and calculated CRC values; recover and
  convert a decodable entry; keep that page unchanged while converting the
  rest; replace it; or explicitly delete it.
- **Unreadable or truncated ZIP files** — inspect recoverable local entries;
  salvage the complete archive when every entry verifies; or, with explicit
  confirmation, rebuild only the fully recovered entries before a truncated
  tail.
- **Image processing failures** — malformed ICC/EXIF metadata is normalized
  into canonical sRGB pixels automatically. Conversion continues after each
  failed image, retains every failed original, and reports all affected entry
  paths with the number of failures in that archive. Sources that are likely
  corrupt, truncated, or malformed offer an explicit tolerant pixel-recovery
  action; the recovered image must strictly decode and the rebuilt archive
  must verify before replacement. A page can instead be preserved, replaced,
  or explicitly deleted.
- **Filename collisions** — compare both conflicting images and choose which
  one the verified rebuild should retain.
- **Archives larger than their lifetime baseline** — retry lossless, try the
  conservative Smart Lossy policy, repack, restore an available original
  `.cbz.bak`, or accept the archive unchanged.
- **Lossless verification failures** — restore the conversion runtime and
  retry; CBZXL preserves the original, marks the archive failed, and includes
  the decoder diagnostic in the terminal ntfy summary.
- **System failures** — surface missing files, permissions, unavailable
  encoders, unsafe paths, and source files modified during conversion with
  specific diagnostic context.

The top-level Retry action uses the default conversion behavior. After a retry
also fails, the archive can be explicitly deleted from the library. Dismissed
items stay out of Attention until a full rescan rediscovers them.

Destructive actions require confirmation in the WebUI. Candidate archives are
verified before they replace a source; the one intentional exception is an
explicit request to delete an image or archive.

## Statistics and reporting

The four lifetime summary values remain visible on Overview. More expensive
reports are loaded only after **Detailed Statistics** is opened.

Detailed reporting includes:

- library coverage, average reduction, retained originals, and success rate;
- cumulative net savings and average percentage saved over 90 days, one year,
  or the full recorded lifetime, calculated only from accepted conversions
  whose rebuilt archive was smaller; rejected candidates that retained the
  original archive do not contribute a zero-value sample;
- archive count, page count, source size, current size, and JXL coverage;
- current image-format composition by both file count and compressed storage;
- compression effectiveness by source format;
- lossless, Smart Lossy, retained-original, skipped/current, and failed page
  outcomes;
- archive savings distribution with median, average, and middle 50%;
- median, 90th-percentile, and distributed archive processing times;
- efficiency by conversion policy;
- unresolved Attention items by failure category; and
- lifetime completed runs, archives attempted, pages examined, and encoder
  time.

Repeated Entire Library runs contribute to run history and lifetime effort.
Archive-level Net Savings remains based on the current library versus each
archive's established lifetime baseline, preventing reruns from resetting or
double-counting the library total.

If correcting a mislabeled image changes only its filename extension, the ZIP
headers can add a few bytes because each entry name is stored twice. CBZXL
proves that overhead from the page decisions before classifying it as expected:
the real byte increase remains included in Net Savings and is explained in run
history, but it does not create an Attention item. Any growth not fully
explained by filename records continues to require review.

## Low-write runtime model

Live progress is structured state, not a text log:

- extraction, candidate images, and converter workspaces live in `/dev/shm`;
- the web and worker share live job state through the RAM-backed `/runtime`
  mount;
- rolling throughput and ETA samples remain in that RAM-backed state and are
  never persisted to SQLite;
- dashboard polling does not write to SQLite;
- Uvicorn access logging is disabled;
- no durable per-archive resume manifest is written; an interrupted archive
  restarts from its unchanged source; and
- unchanged results and format inventory are batched into durable checkpoints
  every five minutes by default.

Modified archives, failures requiring attention, explicit user actions,
replacement commit intents, and run boundaries are written immediately because
they protect data integrity or user intent. Detailed page-decision records are
retained for 180 days by default; current archive state, run totals, and
unresolved Attention items remain durable.

## Docker Compose deployment

The published image is `angeladmerkel/cbzxl-web:latest` for `linux/amd64`.
CBZXL uses two containers built from the same image:

- `cbzxl-web` serves the React application and FastAPI API on port 5000.
- `cbzxl-worker` runs schedules and conversion jobs independently.

Edit the two host paths in the included `docker-compose.yml`:

```yaml
volumes:
  - /mnt/Array/Media/Manga:/data
  - /mnt/Array/appdata/cbzxl:/config
  - /dev/shm/cbzxl-runtime:/runtime
```

`/data` must be writable if conversions or repairs are enabled. Keep `/config`
on storage local to the Docker host so SQLite WAL locking is reliable. The
`/runtime` mount must be the same RAM-backed path for both services.

Then start both services:

```sh
docker compose pull
docker compose up -d
```

Open `http://<nas-host>:5000`.

The included Compose file gives each container a 32 GB memory/shm allowance by
default and applies an eight-CPU ceiling to the worker. Resource settings can be
overridden:

```sh
CBZXL_SHM_SIZE=48g CBZXL_MEMORY_LIMIT=48g CBZXL_CPU_LIMIT=12 docker compose up -d
```

With `CBZXL_CPU_LIMIT=12`, the WebUI offers budgets up to 12 threads and Compose
caps the worker at 12 CPUs. A policy can still choose 1, 2, 4, 6, 8, or 12.
External NAS limits can reduce the effective worker ceiling further.

### ntfy notifications

Set `CBZXL_NTFY_URL` to the full publish URL for a topic on your ntfy server.
If the topic requires authentication, create a dedicated ntfy access token and
set `CBZXL_NTFY_TOKEN`. For example, put these values in `stack.env` beside
`docker-compose.yml`:

```dotenv
CBZXL_NTFY_URL=https://ntfy.example.com/cbzxl
CBZXL_NTFY_TOKEN=tk_replace_with_a_dedicated_token
```

Restart both containers with
`docker compose --env-file stack.env up -d`. The worker sends one summary when
a scan succeeds, partially succeeds, fails, or is cancelled. Notification
delivery is best-effort and cannot change the result of a conversion job. The
token is optional for topics that allow anonymous publishing, and it is passed
only to the worker container. `stack.env` is ignored by Git to prevent a real
token from being committed.

### Important paths

| Container path | Purpose | Persistence |
| --- | --- | --- |
| `/data` | CBZ library | Durable library storage |
| `/config/cbzxl-v3.sqlite3` | Application database | Durable |
| `/config/backups` | Verified pre-migration database backups | Durable |
| `/config/replacements` | Staged user-uploaded replacement images | Durable until consumed |
| `/runtime` | Cross-container live progress | RAM only |
| `/dev/shm/cbzxl` | Extraction and conversion workspaces | RAM only |

### Environment variables

| Variable | Default | Purpose |
| --- | ---: | --- |
| `TZ` | `UTC` | Schedule and display timezone |
| `CONFIG_DIR` | `/config` | Durable application configuration directory |
| `DB_PATH` | `/config/cbzxl-v3.sqlite3` | SQLite database path |
| `LIBRARY_PATH` | `/data` | CBZ library root |
| `SCRATCH_DIR` | `/dev/shm/cbzxl` | RAM conversion scratch directory |
| `WORKSPACE_DIR` | `/dev/shm/cbzxl/workspaces` | Per-archive RAM workspaces |
| `RUNTIME_DIR` | `/dev/shm/cbzxl-runtime` | Volatile live-progress state |
| `CPU_LIMIT` | `8` | Shared WebUI/worker CPU ceiling; normally set through `CBZXL_CPU_LIMIT` |
| `LOG_FILE` | empty | Optional converter text log; empty avoids durable log writes |
| `DURABLE_CHECKPOINT_SECONDS` | `300` | Batch interval for unchanged results; minimum 300 seconds |
| `DURABLE_CHECKPOINT_ARCHIVES` | `10000` | Archive count that can also trigger a batch checkpoint |
| `PAGE_DECISION_RETENTION_DAYS` | `180` | Retention for detailed per-page decisions |
| `ARCHIVE_STABLE_SECONDS` | `90` | Required source quiet period before processing |
| `FINGERPRINT_AUDIT_DAYS` | `30` | Spreads periodic ZIP content-fingerprint audits across normal scans |
| `AUTO_RETRY_ATTEMPTS` | `1` | Conservative automatic compatibility retries; capped at one |
| `DATABASE_MAINTENANCE_SECONDS` | `604800` | SQLite maintenance interval; minimum one day |
| `MIGRATION_BACKUP_KEEP` | `3` | Number of verified pre-migration backups retained |
| `WORKER_STALL_SECONDS` | `900` | Time without converter heartbeat before worker intervention |
| `NTFY_URL` | empty | Full ntfy topic publish URL; normally set through `CBZXL_NTFY_URL` |
| `NTFY_TOKEN` | empty | Optional ntfy bearer token; normally set through `CBZXL_NTFY_TOKEN` |
| `NTFY_TIMEOUT_SECONDS` | `10` | ntfy publish timeout, clamped to 0.1–30 seconds |
| `MAX_ARCHIVE_UNCOMPRESSED_MB` | `8192` | Extraction safety ceiling |
| `ENCODER_MAX_SECONDS` | `0` | Optional per-encode timeout; zero disables it |
| `AUXILIARY_TIMEOUT_SECONDS` | `300` | Timeout for supporting image inspection commands |

Compose also recognizes `CBZXL_SHM_SIZE`, `CBZXL_MEMORY_LIMIT`, and
`CBZXL_CPU_LIMIT` for container resource configuration. ntfy configuration is
passed as `CBZXL_NTFY_URL`, `CBZXL_NTFY_TOKEN`, and
`CBZXL_NTFY_TIMEOUT_SECONDS`. Temporary-file variables should continue to point
to `/dev/shm`.

## Database lifecycle

Application state lives in `/config/cbzxl-v3.sqlite3`. Schema migrations run
automatically. Before a schema upgrade, CBZXL:

1. checks the current database;
2. creates a timestamped backup in `/config/backups`;
3. verifies the backup; and
4. retains the newest configured number of migration backups.

Normal maintenance runs a SQLite `quick_check` and passive WAL checkpoint once
per week by default.

The database can be exported from `/api/database/export`. Export uses SQLite's
online backup mechanism, producing a consistent single-file snapshot while the
application is running.

### Importing an older CBZXL database

The old schema importer remains a command-line migration tool instead of a
permanent WebUI page. It opens the source read-only, validates it, explains the
recognized schema, and imports earlier `converted_archives`, `runs`, and
`archive_results` records into the current database.

Pass the old database path as seen inside the container. Start with a dry run:

```sh
docker compose run --rm cbzxl-web \
  python -m cbzxl_web.legacy /data/converted_archives.db --dry-run
```

Remove `--dry-run` to import. Imports are idempotent for the same source
snapshot, and the legacy database is never modified.

## Homepage integration

CBZXL exposes `/api/integrations/homepage` for
[Homepage's Custom API widget](https://gethomepage.dev/widgets/services/customapi/).
It returns Net Savings, current run state, smooth progress, the current archive,
Attention count, throughput, and ETA. The endpoint continues to return HTTP 200
with a degraded status if SQLite is temporarily unavailable.

Example `services.yaml`:

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

Homepage requests widget data from its own server container. The widget URL
must therefore be reachable from that container; use a shared Docker network
and the CBZXL service name when host DNS is unavailable.

## API and health endpoints

Interactive API documentation is available at `/api/docs`.

Useful read-only endpoints include:

| Endpoint | Purpose |
| --- | --- |
| `/api/dashboard` | Summary totals and active/recent runs |
| `/api/statistics` | Detailed lifetime reporting |
| `/api/jobs` | Run history |
| `/api/issues` | Grouped unresolved Attention items |
| `/api/schedules` | Schedule definitions |
| `/api/archives` | Indexed archive records |
| `/api/integrations/homepage` | Homepage widget payload |
| `/api/database/export` | Consistent SQLite download |
| `/health` | Liveness plus database and conversion-runtime status |
| `/ready` | Readiness; HTTP 503 when the database or lossless conversion runtime is unavailable |

The WebUI also uses mutation endpoints for jobs, issue remedies, replacement
uploads, reviews, schedules, and conversion policies.

## Current code map

| Path | Responsibility |
| --- | --- |
| `frontend/src/App.tsx` | React page composition and client-side interactions |
| `frontend/src/lib/` | API transport, formatting, and policy presentation helpers |
| `frontend/src/components/` | Reusable visual components |
| `app.py` | FastAPI composition, reporting, and repair orchestration |
| `cbzxl_web/api/` | HTTP request contracts and independently testable routers |
| `cbzxl_web/worker.py` | Durable queue, scheduler, subprocess lifecycle, and stall detection |
| `cbzxl.py` | Conversion-run orchestration and persistence |
| `cbzxl_web/engine.py` | ZIP safety, recovery primitives, verification, and workspace mechanics |
| `cbzxl_web/conversion/` | Candidate verification policy and CPU parallelism |
| `cbzxl_web/domain/` | Conversion-policy defaults, scope, and shared invariants |
| `cbzxl_web/database.py` | Schema, migrations, transactions, schedules, and maintenance |
| `cbzxl_web/persistence/` | Reusable migration primitives |
| `cbzxl_web/repositories/` | Focused SQL persistence adapters |
| `cbzxl_web/services/` | UI-independent application rules such as Attention triage |
| `cbzxl_web/config.py` | Environment parsing and cgroup-aware runtime capabilities |
| `cbzxl_web/runtime_state.py` | RAM-backed live progress shared by web and worker |
| `cbzxl_web/protocol.py` | Structured converter event protocol |
| `cbzxl_web/legacy.py` | Read-only legacy database inspection and import |

The web and worker processes coordinate through SQLite for durable intent and
through `/runtime` for high-frequency volatile progress. They do not depend on
one another for HTTP availability.

## Development and validation

Run the Python test suite:

```sh
python -m unittest discover -s tests -v
```

Build the frontend:

```sh
cd frontend
corepack enable
pnpm install --frozen-lockfile
pnpm run build
```

Validate deployment configuration:

```sh
docker compose config
```

The production Dockerfile builds the React application, verifies the pinned
libjxl binary, and refuses to produce an image unless JPEG, PNG, TIFF, BMP, and
WebP each complete a lossless encode, decode, and canonical pixel-signature
comparison. It packages the verified runtime with FastAPI and the Python worker
in a `linux/amd64` image.
