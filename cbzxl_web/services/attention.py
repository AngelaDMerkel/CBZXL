"""Attention-queue taxonomy and context-aware remedy selection."""

from collections.abc import Mapping
from typing import Any


ISSUE_GROUPS = {
    "growth": {
        "label": "Archives larger after conversion",
        "triage": "Repair available",
        "recommended": "Retry with a smaller result or accept the current archive",
        "kinds": {"archive_grew"},
    },
    "crc": {
        "label": "CRC-32 failures",
        "triage": "Inspect repairability",
        "recommended": (
            "Inspect the failing entry, then recover, preserve, replace or "
            "explicitly delete it"
        ),
        "kinds": {"crc_mismatch"},
    },
    "images": {
        "label": "Image processing failures",
        "triage": "Needs your decision",
        "recommended": "Preserve, replace or explicitly delete the affected image",
        "kinds": {
            "page_encode_failed",
            "page_conversion_error",
            "image_corrupt",
            "unsupported_image",
        },
    },
    "unreadable": {
        "label": "Unreadable archives",
        "triage": "Needs your decision",
        "recommended": "Inspect the source archive before retrying",
        "kinds": {"archive_corrupt"},
    },
    "verification": {
        "label": "Lossless verification failures",
        "triage": "Conversion failed",
        "recommended": (
            "Restore the conversion runtime, then retry; the original was preserved"
        ),
        "kinds": {"verification_failed", "page_verification_failed"},
    },
    "paths": {
        "label": "Unsafe archive paths",
        "triage": "Needs your decision",
        "recommended": "Correct the source archive outside CBZXL",
        "kinds": {"unsafe_archive_path"},
    },
    "collisions": {
        "label": "Filename collisions",
        "triage": "Needs your decision",
        "recommended": (
            "Compare both images and choose which one the archive should keep"
        ),
        "kinds": {"filename_collision"},
    },
    "permissions": {
        "label": "Write or permission failures",
        "triage": "System problem",
        "recommended": "Restore library access, then retry the affected archives",
        "kinds": {"permission_denied"},
    },
    "missing": {
        "label": "Missing source archives",
        "triage": "System problem",
        "recommended": "Restore or rescan the missing source path",
        "kinds": {"file_missing"},
    },
    "dependency": {
        "label": "Missing conversion dependency",
        "triage": "System problem",
        "recommended": "Restore the encoder or image utility before retrying",
        "kinds": {"encoder_unavailable"},
    },
    "conversion": {
        "label": "Archive conversion failures",
        "triage": "Retryable",
        "recommended": "Retry normally or in compatibility mode",
        "kinds": {"archive_failed", "source_changed"},
    },
}


def remedies_for_issue(issue: Mapping[str, Any]) -> list[str]:
    kind = issue.get("kind") or "archive_failed"
    if kind == "archive_grew":
        if issue.get("backup_available"):
            return ["lossless", "smart_lossy", "restore_original", "repack"]
        return ["repack", "dismiss"]
    if kind == "crc_mismatch":
        return ["rebuild", "preserve", "replace", "delete", "dismiss"]
    if kind == "archive_corrupt":
        return [
            "salvage",
            "salvage_partial",
            "compatibility",
            "retry",
            "dismiss",
        ]
    if kind == "filename_collision":
        if issue.get("entry_path") and issue.get("related_entry_path"):
            return ["keep_primary", "keep_related", "dismiss"]
        return ["dismiss"]
    if issue.get("entry_path"):
        return ["compatibility", "preserve", "replace", "delete"]
    if kind == "unsafe_archive_path":
        return ["dismiss"]
    return ["compatibility", "preserve", "retry", "dismiss"]


def issue_group(issue: Mapping[str, Any]) -> dict[str, Any]:
    kind = issue.get("kind") or "archive_failed"
    for group_id, group in ISSUE_GROUPS.items():
        if kind in group["kinds"]:
            return {
                "id": group_id,
                **{key: value for key, value in group.items() if key != "kinds"},
            }
    return {
        "id": "other",
        "label": "Other conversion failures",
        "triage": "Inspect",
        "recommended": "Inspect the diagnostic detail before choosing a remedy",
    }


def issue_group_filter(group_id: str | None) -> tuple[str, list[str]] | None:
    """Return a parameterized SQL fragment, or ``None`` for an unknown group."""
    if not group_id:
        return "", []
    group = ISSUE_GROUPS.get(group_id)
    if group:
        kinds = sorted(group["kinds"])
        placeholders = ",".join("?" for _ in kinds)
        return f" AND ai.kind IN ({placeholders})", kinds
    if group_id == "other":
        known = sorted(
            {
                kind
                for group_definition in ISSUE_GROUPS.values()
                for kind in group_definition["kinds"]
            }
        )
        placeholders = ",".join("?" for _ in known)
        return f" AND ai.kind NOT IN ({placeholders})", known
    return None
