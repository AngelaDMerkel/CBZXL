import hashlib
import time
import zipfile
from pathlib import Path

from cbzxl_web.engine import archive_fingerprint


class ArchiveSourceChanged(RuntimeError):
    kind = "source_changed"
    stage = "committing"


def capture_archive_identity(path: str | Path) -> dict[str, int]:
    """Return the inexpensive identity used to guard an atomic replacement."""
    stat_result = Path(path).stat()
    return {
        "device": stat_result.st_dev,
        "inode": stat_result.st_ino,
        "size": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
    }


def archive_guard_fingerprint(path: str | Path) -> str:
    """Fingerprint valid ZIP metadata, falling back to complete raw bytes."""
    try:
        return archive_fingerprint(path)
    except zipfile.BadZipFile:
        digest = hashlib.blake2b(digest_size=20)
        with Path(path).open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


def require_unchanged_archive(
    path: str | Path,
    expected_identity: dict[str, int],
    expected_fingerprint: str,
) -> None:
    """Refuse to overwrite a source that changed after conversion began."""
    current_identity = capture_archive_identity(path)
    if current_identity != expected_identity:
        raise ArchiveSourceChanged(
            "Source archive changed during conversion; the verified candidate "
            "was discarded"
        )
    try:
        current_fingerprint = archive_guard_fingerprint(path)
    except OSError as exc:
        raise ArchiveSourceChanged(
            "Source archive became unreadable during conversion; the verified "
            "candidate was discarded"
        ) from exc
    if current_fingerprint != expected_fingerprint:
        raise ArchiveSourceChanged(
            "Source archive contents changed during conversion; the verified "
            "candidate was discarded"
        )


def archive_is_stable(
    path: str | Path,
    minimum_age_seconds: int,
    *,
    now: float | None = None,
) -> bool:
    """Return whether an archive is old enough to no longer be an active copy."""
    minimum_age = max(0, int(minimum_age_seconds))
    if minimum_age == 0:
        return True
    stat_result = Path(path).stat()
    wall_time = time.time() if now is None else float(now)
    return wall_time - stat_result.st_mtime >= minimum_age
