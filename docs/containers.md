# Containers

## Image and supported platforms

`ghcr.io/angeladmerkel/cbzxl` is the production image for CBZXL 5. It includes the
Rust API, React frontend, embedded worker, ImageMagick, and libjxl 0.12.0. There
are no separate production web/worker images; `serve` and `worker` are alternate
commands of this same image. The legacy Python directory is not shipped.

The image supports **`linux/amd64` and `linux/arm64`**. The pinned Debian, Rust,
and Node base-image indexes support both; Rust dependencies (including bundled
SQLite and native compression libraries) and libjxl are compiled for each target.
CI builds and runs the same tests on native GitHub AMD64 and ARM64 Linux runners.
The libjxl build uses its portable defaults and runtime CPU dispatch, with no
host-specific `-march=native` tuning. No GPU is required.

32-bit ARM/x86 and other architectures are not built or tested. macOS and Windows
hosts require a Linux container engine, such as Docker Desktop; these are not
native macOS or Windows container images. Docker selects the matching architecture
automatically. Prefer native execution; emulation is slower and is not the CI
runtime test environment.

Images will be available only after the workflow is merged and a publishing
event succeeds. `dev` is published on pushes to `main`; `stable` and `latest`
require a stable GitHub Release. Historical releases are not republished.

## Docker Compose

Use the repository's `docker-compose.yml` with Docker Compose v2. Place comics in
`./comics`, or set `CBZXL_LIBRARY_PATH` to your library directory in a local `.env`.
The directory must be writable: CBZXL replaces successfully optimized archives
in place. Keep a separate backup of your library.

```sh
mkdir -p comics
docker compose pull
docker compose up -d
docker compose logs -f cbzxl
```

Open `http://localhost:5000`. Port **5000/tcp** is the only exposed application
port. Compose publishes it on loopback by default. To allow remote access, set
`CBZXL_BIND_ADDRESS=0.0.0.0` and configure `CBZXL_API_TOKEN` with a strong random
value. Use a trusted TLS reverse proxy when access crosses an untrusted network.
`/health`, `/ready`, and the Homepage widget are intentionally unauthenticated;
application APIs require the token when configured.

Compose overrides can be saved in `.env` (ignored by Git and Docker builds):

| Setting | Compose default | Meaning |
| --- | --- | --- |
| `CBZXL_IMAGE` | `ghcr.io/angeladmerkel/cbzxl:latest` | Set a published version or `image@sha256:…` for controlled updates |
| `CBZXL_LIBRARY_PATH` | `./comics` | Writable host library directory |
| `CBZXL_BIND_ADDRESS` | `127.0.0.1` | Host interface for the published port |
| `CBZXL_PORT` | `5000` | Host port; container continues to use 5000 |
| `CBZXL_DB_PATH` | `/config/cbzxl-v5.sqlite3` | Database path inside the container; preserve an existing path on upgrade |
| `CBZXL_UID` / `CBZXL_GID` | `0` / `0` | Numeric identity for the application process |
| `TZ` | `UTC` | Schedule timezone |
| `CBZXL_CPU_LIMIT` | `2` | Container CPU quota and application budget |
| `CBZXL_MEMORY_LIMIT` | `4g` | Container memory limit |
| `CBZXL_SHM_SIZE` | `1g` | Maximum RAM-backed conversion scratch space |

For a non-root process, set both `CBZXL_UID` and `CBZXL_GID` to an identity with
write permission on the library. The entrypoint starts as root, prepares
`/config`, `/runtime`, and scratch ownership, then uses `gosu` to drop privileges.
It does **not** change library ownership. Compose keeps the CHOWN, SETUID,
SETGID, and DAC_OVERRIDE capabilities needed for this initialization and disables
new privileges. Do not combine this entrypoint mode with Compose `user:` unless
you have independently prepared all writable directories.

| Container path | Storage | Purpose |
| --- | --- | --- |
| `/data` | Writable library bind mount | Comics, including optimized replacements |
| `/config` | `cbzxl-config` named volume | SQLite database, backups and durable history |
| `/runtime` | 64 MiB tmpfs in Compose | Transient progress; recreated at startup |
| `/dev/shm/cbzxl` | Shared memory, sized by `CBZXL_SHM_SIZE` | Temporary conversion workspaces |

Scratch usage counts toward the container memory limit. Allow room for decoded
pages, encoder memory, and parallel work in addition to scratch. Large archives
can exceed the starter defaults; increase memory/scratch together or place
`SCRATCH_DIR` on a writable disk-backed volume. The 8192 MiB archive expansion
ceiling is a safety maximum, not a promise that every allowed archive fits the
default memory budget. See the [configuration reference](../README.md#configuration)
for application settings.

## Plain Docker

This equivalent starting point uses a named configuration volume and a library
directory relative to the current shell:

```sh
mkdir -p comics
docker volume create cbzxl-config
docker run -d --name cbzxl --init --restart unless-stopped \
  -p 127.0.0.1:5000:5000 \
  --mount type=bind,src="$(pwd)/comics",dst=/data \
  --mount type=volume,src=cbzxl-config,dst=/config \
  --tmpfs /runtime:size=64m --shm-size=1g --memory=4g --cpus=2 \
  -e CPU_LIMIT=2 -e TZ=UTC \
  ghcr.io/angeladmerkel/cbzxl:latest
docker exec cbzxl cbzxl healthcheck
```

Pass authentication via a protected `--env-file` when enabling remote access.
Published public images require no registry credentials. To build from source,
use `docker build -t cbzxl:local .`, then run that image or set
`CBZXL_IMAGE=cbzxl:local docker compose up -d --pull never`. Local builds use
`development`/`unknown` OCI version/revision labels unless `VERSION`, `REVISION`,
and `SOURCE` build arguments are supplied.

## Tags and release process

| Tag | When it changes |
| --- | --- |
| `dev` | Successful tested push to `main`; never a stable release |
| `sha-<full 40-character commit>` | Successful main or release publication for that source commit |
| `5.2.0` (example) | A published stable GitHub Release matching the package version |
| `5.3.0-rc.1` (example) | A published prerelease; never changes `stable` or `latest` |
| `stable`, `latest` | Successful stable release publication only |
| `build-<run>-<attempt>-<arch>` | Internal per-platform staging tags used to assemble the tested index |

Reuse the project's unprefixed release-tag convention. An optional `v` prefix is
also accepted and produces both prefixed and unprefixed image tags. Release tags
must match the version in **Cargo.toml, Cargo.lock, and frontend/package.json**.
For a prerelease, use a version suffix such as `-rc.1` in all three files and
mark the GitHub Release as a prerelease. A suffix prevents stable promotion even
if the GitHub prerelease checkbox is accidentally left clear. A GitHub prerelease
without a version suffix is rejected. SemVer `+build` metadata is rejected because
Docker tags cannot contain `+`.

Maintainers publish through GitHub's **Publish release** action after reviewing
the versioned commit. Merely pushing a tag, saving a draft release, or manually
running CI does not publish. The `published` event handles releases published
from drafts too. Publish stable releases in the intended upgrade order: stable
aliases follow successful publication, rather than numerical version sorting.

Each image carries OCI source, full source revision, and version labels.
Development versions are labeled `<package-version>-dev.<commit>`. Tag names can
move on reruns (or when a main commit becomes a release); pin the reported
**multi-platform digest** for immutable deployments.

The workflow runs Rust formatting, Clippy and tests, frontend build/tests, release
metadata tests, and Compose validation. Each architecture's exported Docker image
is loaded and smoke-tested as root and UID 65532. Checks include readiness (which
executes JPEG/PNG/TIFF/BMP encode/decode probes), database diagnostics, the frontend
and its JS asset, API authentication, actual CBZ conversion, unchanged WebP bytes,
and history persistence after container recreation.

Only after all checks pass does the publishing job load the saved tested archives,
verify image IDs/labels, push them, and assemble the two platform digests into a
multi-platform index. **There is no rebuild between test and publication.** Job
summaries report per-platform and index digests. Tested archive artifacts expire
after two days; rerun the entire workflow if they have expired. Native follow-up
jobs pull the index by digest without credentials and smoke-test it again.

## One-time maintainer setup

1. Enable GitHub Actions and permit the pinned official Actions used in CI.
   The repository must allow `GITHUB_TOKEN` to publish packages. Only the publish
   job requests `packages: write`; PR builds have read-only repository access,
   never log in, and never push. No personal access token or registry secret is needed.
2. After the first successful push, open the `cbzxl` package's **Package settings**
   and change **Visibility** to **Public**. A public repository does not by itself
   make a new GHCR package public. The OCI source label links the package to this
   repository. If the package already existed, connect it to this repository and
   grant this repository **Write** under **Manage Actions access** (or enable the
   applicable inherited repository permissions).
3. The first anonymous-pull jobs may fail until visibility is changed. After
   setting Public, rerun the failed jobs. They use an empty Docker configuration
   and pull actual layers on fresh runners for both architectures. Treat failures
   as incomplete publication verification, even if the earlier push succeeded.
4. Protect `main` and release tags as appropriate. Use the `validate`, `metadata`,
   and both `container` jobs as required PR checks. Every Action is pinned to an
   upstream-verified full commit; update pins by resolving upstream tags/commits,
   reviewing the changes, and rerunning CI. Publishing is serialized, while
   superseded PR runs are cancelled. Build caches are architecture-specific.

To verify public access independently, use the digest from the workflow summary
on a fresh machine/runner (an empty Docker config removes credential helpers):

```sh
image='ghcr.io/angeladmerkel/cbzxl@sha256:REPLACE_WITH_REPORTED_DIGEST'
anonymous_config="$(mktemp -d)"
DOCKER_CONFIG="$anonymous_config" docker pull --platform linux/amd64 "$image"
DOCKER_CONFIG="$anonymous_config" docker pull --platform linux/arm64 "$image"
rm -rf "$anonymous_config"
```

See GitHub's [Container registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
and [package visibility settings](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

## Upgrading

1. Finish or stop active jobs, stop CBZXL, and back up the library and complete
   configuration volume. Stop the old web/worker deployment before starting v5;
   do not run two workers against the same library/database.
2. Preserve the existing configuration mount, library container paths, and
   `DB_PATH`. When adopting this Compose file, replace its new named configuration
   volume with your existing mount rather than accidentally starting an empty
   database. Set `CBZXL_DB_PATH` if the database has an older filename.
3. Choose a published version or digest in `CBZXL_IMAGE`, then run
   `docker compose pull && docker compose up -d`. Check `docker compose ps`, logs,
   and `docker compose exec cbzxl cbzxl healthcheck`.
4. Recognized v2/v3 databases are backed up and upgraded automatically. Review the
   [migration instructions](../README.md#importing-an-existing-database) before a
   major upgrade. Keep the pre-upgrade backup: downgrading an image does not undo a
   database migration or restore converted comics.

Do not use `docker compose down -v` when retaining data: it deletes named volumes.
Routine upgrades keep both `/data` and `/config`; runtime and scratch are disposable.
