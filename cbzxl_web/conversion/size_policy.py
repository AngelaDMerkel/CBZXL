import re
from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath
from typing import Any


EXTENSION_REASON = re.compile(
    r"filename extension corrected to (?P<extension>\.[A-Za-z0-9]+)",
    re.IGNORECASE,
)
EXPECTED_EXTENSION_OVERHEAD = "extension_overhead"


def zip_filename_growth(source: str, output: str) -> int:
    """Return structural ZIP growth caused only by a longer entry name.

    ZIP stores the entry name in both its local header and central directory.
    UTF-8 byte lengths are used because that is what contributes to archive
    size; extension corrections preserve the rest of the path.
    """
    difference = len(output.encode("utf-8")) - len(source.encode("utf-8"))
    return difference * 2


def extension_overhead_from_manifest(
    completed_pages: Mapping[str, Mapping[str, Any]] | None,
) -> int:
    overhead = 0
    for source, page in (completed_pages or {}).items():
        if page.get("operation") != "extension_corrected":
            continue
        output = page.get("output")
        if output:
            overhead += zip_filename_growth(str(source), str(output))
    return max(0, overhead)


def extension_overhead_from_decisions(
    decisions: Iterable[Mapping[str, Any]],
) -> int | None:
    """Recover proven extension overhead from durable page decisions.

    ``None`` means the evidence is incomplete or includes an outcome capable
    of increasing image content, so it must not be auto-classified.
    """
    rows = list(decisions)
    if not rows:
        return None

    def value(row: Mapping[str, Any], key: str) -> Any:
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            return None

    permitted = {"extension_corrected", "retained", "converted"}
    if any(str(value(row, "outcome") or "") not in permitted for row in rows):
        return None
    overhead = 0
    corrected = 0
    for row in rows:
        if value(row, "outcome") != "extension_corrected":
            continue
        match = EXTENSION_REASON.search(str(value(row, "reason") or ""))
        if not match:
            return None
        source = PurePosixPath(str(value(row, "entry_path") or ""))
        if not source.name:
            return None
        output = source.with_suffix(match.group("extension")).as_posix()
        overhead += zip_filename_growth(source.as_posix(), output)
        corrected += 1
    return max(0, overhead) if corrected else None


def is_expected_extension_growth(growth_bytes: int, overhead_bytes: int | None) -> bool:
    """Classify growth only when correction overhead fully explains it."""
    return bool(
        growth_bytes > 0
        and overhead_bytes is not None
        and overhead_bytes > 0
        and growth_bytes <= overhead_bytes
    )
