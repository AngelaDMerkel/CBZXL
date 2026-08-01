"""Runtime configuration shared by the web, worker, and conversion engine.

Keep environment parsing and container-resource detection in one place so the
WebUI cannot advertise a CPU budget that the worker is unable to use.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


CGROUP_V2_CPU_MAX = Path("/sys/fs/cgroup/cpu.max")
CGROUP_V1_CPU_QUOTA = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
CGROUP_V1_CPU_PERIOD = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
CGROUP_CPUSET_FILES = (
    Path("/sys/fs/cgroup/cpuset.cpus.effective"),
    Path("/sys/fs/cgroup/cpuset/cpuset.cpus"),
)


def positive_int(value: object, default: int) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def parse_cpuset(value: str) -> int | None:
    """Return the number of CPUs represented by a Linux cpuset expression."""
    cpus: set[int] = set()
    try:
        for raw_part in value.strip().split(","):
            part = raw_part.strip()
            if not part:
                continue
            if "-" in part:
                start_text, end_text = part.split("-", 1)
                start = int(start_text)
                end = int(end_text)
                if start < 0 or end < start:
                    return None
                cpus.update(range(start, end + 1))
            else:
                cpu = int(part)
                if cpu < 0:
                    return None
                cpus.add(cpu)
    except ValueError:
        return None
    return len(cpus) or None


def quota_cpu_count(quota: int, period: int) -> int | None:
    """Convert a cgroup CPU quota into a conservative integer worker budget."""
    if quota <= 0 or period <= 0:
        return None
    return max(1, math.floor(quota / period))


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _cgroup_quota_limit() -> int | None:
    cpu_max = _read_text(CGROUP_V2_CPU_MAX)
    if cpu_max:
        parts = cpu_max.split()
        if len(parts) >= 2 and parts[0] != "max":
            try:
                return quota_cpu_count(int(parts[0]), int(parts[1]))
            except ValueError:
                pass
    quota_text = _read_text(CGROUP_V1_CPU_QUOTA)
    period_text = _read_text(CGROUP_V1_CPU_PERIOD)
    if quota_text and period_text:
        try:
            return quota_cpu_count(int(quota_text), int(period_text))
        except ValueError:
            return None
    return None


def _cpuset_limit(paths: Iterable[Path] = CGROUP_CPUSET_FILES) -> int | None:
    for path in paths:
        value = _read_text(path)
        if value:
            parsed = parse_cpuset(value)
            if parsed:
                return parsed
    return None


def _affinity_limit() -> int | None:
    get_affinity = getattr(os, "sched_getaffinity", None)
    if get_affinity is None:
        return None
    try:
        return len(get_affinity(0)) or None
    except OSError:
        return None


def cpu_limit() -> int:
    """Return the usable CPU ceiling visible to this container.

    ``CPU_LIMIT`` is the Compose contract shared with the web container. The
    worker also intersects it with affinity, cpuset, and cgroup quota so an
    external NAS limit can only reduce the effective budget.
    """
    configured_text = os.getenv("CPU_LIMIT")
    configured = (
        positive_int(configured_text, 1)
        if configured_text not in {None, ""}
        else None
    )
    detected = [
        configured,
        _affinity_limit(),
        _cpuset_limit(),
        _cgroup_quota_limit(),
        os.cpu_count(),
    ]
    usable = [int(value) for value in detected if value is not None and value > 0]
    return max(1, min(usable)) if usable else 1


def clamp_cpu_budget(value: object, *, limit: int | None = None) -> int:
    ceiling = max(1, int(limit if limit is not None else cpu_limit()))
    return min(positive_int(value, 1), ceiling)


@dataclass(frozen=True)
class RuntimeCapabilities:
    cpu_limit: int

    @classmethod
    def detect(cls) -> "RuntimeCapabilities":
        return cls(cpu_limit=cpu_limit())

    def as_dict(self) -> dict[str, int]:
        return {"cpu_limit": self.cpu_limit}

