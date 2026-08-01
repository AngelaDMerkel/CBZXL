from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping


@dataclass(frozen=True)
class CandidateAssessment:
    """Result of the inexpensive size gate and, when needed, decode verification."""

    ready: bool
    verified: bool
    size: int | None
    reason: str


def assess_candidate(
    candidate: Path,
    source_size: int,
    *,
    signature_check: Callable[[Path], bool],
    decode_verify: Callable[[], None],
    verification_errors: tuple[type[Exception], ...] = (Exception,),
) -> CandidateAssessment:
    """Verify only candidates that could replace the source.

    A valid but non-smaller JPEG XL is sufficient to make a retention decision,
    so decoding it would add cost without improving safety. Smaller candidates
    must still pass the full decode/pixel check before they can be accepted.
    """

    try:
        size = candidate.stat().st_size
    except OSError:
        return CandidateAssessment(False, False, None, "encoder produced no candidate")
    if size <= 0 or not signature_check(candidate):
        return CandidateAssessment(
            False,
            False,
            size,
            "encoder did not produce a valid JPEG XL file",
        )
    if size >= source_size:
        return CandidateAssessment(
            True,
            False,
            size,
            "decode verification skipped because the candidate was not smaller",
        )
    try:
        decode_verify()
    except verification_errors as exc:
        return CandidateAssessment(False, False, size, str(exc))
    return CandidateAssessment(True, True, size, "verified")


def required_archive_decode_entries(
    *,
    original_files: Iterable[str],
    repair_action: str | None,
    repair_entries: Iterable[str],
) -> list[str]:
    """Return only pre-existing JXL pages that need mandatory final decoding.

    Newly encoded pages have already passed their page-level decode verification.
    The final archive check still validates ZIP CRCs, signatures, and representative
    samples, without decoding every accepted page a second time.
    """

    recovery_actions = {
        "salvage",
        "salvage_partial",
        "restore_original",
        "restore_reconvert",
    }
    entries = original_files if repair_action in recovery_actions else repair_entries
    return [
        str(path)
        for path in entries
        if Path(str(path)).suffix.lower() == ".jxl"
    ]
