from collections.abc import Mapping
from typing import Any, Literal

from cbzxl_web.config import clamp_cpu_budget


ScanScope = Literal["library", "unprocessed", "failed"]

DEFAULT_OPTIONS: dict[str, Any] = {
    "dry_run": False,
    "backup": False,
    "recheck_all": False,
    "verbose": True,
    "no_convert": False,
    "no_flatten": True,
    "delete_empty": False,
    "smart_distance": False,
    "effort": 10,
    "threads": 4,
    "distance": None,
    "smart_mb": 10.0,
    "smart_mp": 5.0,
    "failed_only": False,
    "compatibility_mode": False,
    "continue_on_page_error": False,
    "integrity_audit": False,
    "repair_action": None,
    "repair_entry": None,
    "repair_entries": [],
    "repair_source": None,
}

BUILTIN_PROFILES = (
    {
        "id": "profile-balanced",
        "name": "Balanced lossless",
        "is_default": 1,
        "options": DEFAULT_OPTIONS,
    },
    {
        "id": "profile-smart",
        "name": "Smart lossy fallback",
        "is_default": 0,
        "options": {
            **DEFAULT_OPTIONS,
            "smart_distance": True,
            "distance": 0.5,
        },
    },
    {
        "id": "profile-recheck",
        "name": "Recheck without conversion",
        "is_default": 0,
        "options": {
            **DEFAULT_OPTIONS,
            "no_convert": True,
            "recheck_all": True,
        },
    },
    {
        "id": "profile-integrity",
        "name": "Integrity audit",
        "is_default": 0,
        "options": {
            **DEFAULT_OPTIONS,
            "no_convert": True,
            "recheck_all": True,
            "integrity_audit": True,
        },
    },
)


def normalize_policy_options(
    stored: Mapping[str, Any] | None = None,
    overrides: Mapping[str, Any] | None = None,
    *,
    clamp_threads: bool = True,
) -> dict[str, Any]:
    """Merge a policy while enforcing invariants shared by every entry point."""
    options = {
        key: list(value) if isinstance(value, list) else value
        for key, value in DEFAULT_OPTIONS.items()
    }
    if stored:
        options.update(stored)
    if overrides:
        options.update(
            {key: value for key, value in overrides.items() if value is not None}
        )
    options["no_flatten"] = True
    options["delete_empty"] = False
    if clamp_threads:
        options["threads"] = clamp_cpu_budget(options.get("threads", 4))
        if options.get("encoder_threads") is not None:
            options["encoder_threads"] = clamp_cpu_budget(
                options["encoder_threads"],
                limit=options["threads"],
            )
    return options


def apply_scope(options: Mapping[str, Any], scope: ScanScope) -> dict[str, Any]:
    normalized = dict(options)
    normalized["failed_only"] = scope == "failed"
    normalized["recheck_all"] = scope == "library"
    return normalized
