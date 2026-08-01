from typing import Literal

from pydantic import BaseModel, Field, model_validator

from cbzxl_web.config import clamp_cpu_budget


class ScanOptions(BaseModel):
    dry_run: bool = False
    backup: bool = False
    recheck_all: bool = False
    verbose: bool = True
    no_convert: bool = False
    no_flatten: bool = True
    delete_empty: bool = False
    smart_distance: bool = False
    effort: int = Field(10, ge=0, le=11)
    threads: int = Field(4, ge=1, le=32)
    distance: float | None = Field(None, ge=0, le=15)
    smart_mb: float = Field(10.0, ge=0.1, le=1000)
    smart_mp: float = Field(5.0, ge=0.1, le=1000)
    failed_only: bool = False
    compatibility_mode: bool = False
    continue_on_page_error: bool = False
    integrity_audit: bool = False
    only_path: str | None = None
    repair_action: Literal[
        "replace",
        "delete",
        "rebuild",
        "preserve",
        "salvage",
        "salvage_partial",
        "restore_original",
        "restore_reconvert",
        "repack",
    ] | None = None
    repair_entry: str | None = None
    repair_entries: list[str] = Field(default_factory=list)
    repair_source: str | None = None

    @model_validator(mode="after")
    def enforce_runtime_contract(self):
        # These two historical flags would violate CBZXL's filename guarantees.
        self.no_flatten = True
        self.delete_empty = False
        self.threads = clamp_cpu_budget(self.threads)
        return self


class JobCreate(BaseModel):
    library_id: str | None = None
    profile_id: str | None = None
    scope: Literal["library", "unprocessed", "failed"] = "unprocessed"
    options: ScanOptions | None = None


class SchedulePayload(BaseModel):
    name: str = Field("Library maintenance", min_length=1, max_length=80)
    enabled: bool = True
    cadence: Literal["daily", "weekly", "monthly", "interval"] = "daily"
    interval_value: int | None = Field(None, ge=1, le=365)
    interval_unit: Literal["hours", "days"] | None = None
    weekday: int | None = Field(None, ge=0, le=6)
    day_of_month: int | None = None
    hour: int = Field(4, ge=0, le=23)
    minute: int = Field(0, ge=0, le=59)
    library_id: str | None = None
    profile_id: str | None = None

    @model_validator(mode="after")
    def validate_cadence(self):
        if self.cadence == "interval" and (
            not self.interval_value or not self.interval_unit
        ):
            raise ValueError("Interval schedules need a quantity and unit")
        if self.cadence == "weekly" and self.weekday is None:
            raise ValueError("Weekly schedules need a weekday")
        if (
            self.cadence == "monthly"
            and self.day_of_month not in {-1, *range(1, 32)}
        ):
            raise ValueError(
                "Monthly schedules need a day from 1 to 31, or the last day"
            )
        return self


class ReviewPayload(BaseModel):
    action: Literal["reviewed", "reopen", "queue_recheck"]
    note: str = Field("", max_length=500)


class IssueActionPayload(BaseModel):
    action: Literal[
        "retry",
        "dismiss",
        "compatibility",
        "preserve",
        "delete",
        "delete_archive",
        "lossless",
        "smart_lossy",
        "restore_original",
        "repack",
        "rebuild",
        "salvage",
        "salvage_partial",
        "keep_primary",
        "keep_related",
    ]


class ProfilePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    options: ScanOptions
    is_default: bool = False


class LegacyImportPayload(BaseModel):
    path: str = Field(..., min_length=1)
    dry_run: bool = True

