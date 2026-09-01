"""End-to-end validation of the media conversion runtime."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from functools import lru_cache
from pathlib import Path

from PIL import Image


REQUIRED_TOOLS = ("cjxl", "djxl", "magick", "identify", "file")
SUPPORTED_INPUT_PROBES = (
    ("jpeg", "JPEG", ".jpg"),
    ("png", "PNG", ".png"),
    ("tiff", "TIFF", ".tiff"),
    ("bmp", "BMP", ".bmp"),
    ("webp", "WEBP", ".webp"),
)
NORMALIZED_INPUTS = {"tiff", "bmp", "webp"}


class RuntimeDependencyError(RuntimeError):
    """Raised when the installed tools cannot perform lossless conversion."""


def _run(command: list[str], label: str) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeDependencyError(f"{label} could not run: {exc}") from exc
    if result.returncode:
        detail = (result.stderr or result.stdout or "unknown error").strip()
        raise RuntimeDependencyError(f"{label} failed: {detail[:1000]}")
    return result


def _pixel_signature(path: Path) -> str:
    result = _run(
        [
            "magick",
            str(path),
            "-alpha",
            "on",
            "-colorspace",
            "sRGB",
            "-format",
            "%w %h rgba %#",
            "info:",
        ],
        f"Pixel verification for {path.name}",
    )
    signature = result.stdout.strip()
    if not signature:
        raise RuntimeDependencyError(
            f"Pixel verification returned no signature for {path.name}"
        )
    return signature


@lru_cache(maxsize=1)
def validate_conversion_runtime() -> dict[str, object]:
    """Prove that every supported input can complete the lossless JXL path."""
    missing = [tool for tool in REQUIRED_TOOLS if not shutil.which(tool)]
    if missing:
        raise RuntimeDependencyError(
            "Required conversion tools are unavailable: " + ", ".join(missing)
        )

    with tempfile.TemporaryDirectory(prefix="cbzxl-runtime-check-") as directory:
        root = Path(directory)
        pixels = [
            ((x * 29 + y * 7) % 256, (x * 11 + y * 31) % 256, (x * 17 + y * 13) % 256)
            for y in range(16)
            for x in range(16)
        ]
        for name, pillow_format, suffix in SUPPORTED_INPUT_PROBES:
            source = root / f"source-{name}{suffix}"
            image = Image.new("RGB", (16, 16))
            image.putdata(pixels)
            image.save(source, format=pillow_format)

            _run(
                ["magick", str(source), "null:"],
                f"ImageMagick {name.upper()} decode check",
            )
            encoder_input = source
            if name in NORMALIZED_INPUTS:
                encoder_input = root / f"normalized-{name}.png"
                _run(
                    ["magick", str(source), str(encoder_input)],
                    f"ImageMagick {name.upper()} normalization check",
                )

            candidate = root / f"candidate-{name}.jxl"
            decoded = root / f"decoded-{name}{'.jpg' if name == 'jpeg' else '.png'}"
            _run(
                [
                    "cjxl",
                    "-d",
                    "0",
                    "--effort=1",
                    "--num_threads=1",
                    "--quiet",
                    str(encoder_input),
                    str(candidate),
                ],
                f"Lossless {name.upper()} JPEG XL encode check",
            )
            _run(
                [
                    "djxl",
                    str(candidate),
                    str(decoded),
                    "--num_threads=1",
                    "--quiet",
                ],
                f"Lossless {name.upper()} JPEG XL decode check",
            )
            if _pixel_signature(encoder_input) != _pixel_signature(decoded):
                raise RuntimeDependencyError(
                    f"Lossless {name.upper()} JPEG XL pixel verification failed"
                )

    return {
        "status": "ok",
        "tools": list(REQUIRED_TOOLS),
        "formats": [probe[0] for probe in SUPPORTED_INPUT_PROBES],
    }


@lru_cache(maxsize=1)
def conversion_runtime_error() -> str | None:
    try:
        validate_conversion_runtime()
    except RuntimeDependencyError as exc:
        return str(exc)
    return None


def main() -> int:
    try:
        result = validate_conversion_runtime()
    except RuntimeDependencyError as exc:
        print(f"CBZXL conversion runtime check failed: {exc}")
        return 1
    print(
        "CBZXL conversion runtime check passed: "
        + ", ".join(str(value) for value in result["formats"])
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
