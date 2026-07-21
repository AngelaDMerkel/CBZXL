import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


PARTIAL_SUFFIX = ".cbzxl-partial"


class ConversionCancelled(RuntimeError):
    pass


class VerificationError(RuntimeError):
    pass


class CancellationToken:
    def __init__(self):
        self._event = threading.Event()

    @property
    def requested(self):
        return self._event.is_set()

    def request(self, *_args):
        self._event.set()

    def check(self):
        if self.requested:
            raise ConversionCancelled("Conversion cancelled")


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def run_cancellable(command, token, *, heartbeat=None, max_seconds=0, discard_stdout=False):
    """Run a child process with cooperative cancellation and periodic heartbeats."""
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL if discard_stdout else subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while True:
        try:
            stdout, stderr = process.communicate(timeout=0.5)
            return CommandResult(process.returncode, stdout or "", stderr or "")
        except subprocess.TimeoutExpired:
            if heartbeat:
                heartbeat()
            if token.requested or (max_seconds and time.monotonic() - started > max_seconds):
                process.terminate()
                try:
                    stdout, stderr = process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate()
                if token.requested:
                    raise ConversionCancelled("Conversion cancelled")
                return CommandResult(124, stdout or "", stderr or "Timed out")


def install_signal_handlers(token):
    signal.signal(signal.SIGTERM, token.request)
    signal.signal(signal.SIGINT, token.request)


def is_zip_symlink(info):
    return stat.S_ISLNK(info.external_attr >> 16)


def archive_fingerprint(path):
    """Fast content signature based on ZIP entry CRCs and metadata, independent of mtime."""
    source = Path(path)
    digest = hashlib.blake2b(digest_size=20)
    digest.update(str(source.stat().st_size).encode())
    with zipfile.ZipFile(source, "r") as archive:
        for info in archive.infolist():
            digest.update(info.filename.replace("\\", "/").encode("utf-8", "surrogateescape"))
            digest.update(f":{info.CRC}:{info.file_size}:{info.compress_size}:{info.compress_type};".encode())
    return digest.hexdigest()


def options_fingerprint(options):
    encoded = json.dumps(options, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.blake2b(encoded, digest_size=16).hexdigest()


def safe_extract_archive(source, destination, max_uncompressed_bytes, token=None):
    destination = Path(destination).resolve()
    total_uncompressed = 0
    extracted_files = []
    seen_paths = set()
    with zipfile.ZipFile(source, "r") as archive:
        for member in archive.infolist():
            if token:
                token.check()
            member_name = member.filename
            if "\\" in member_name:
                raise ValueError(f"Non-canonical archive path cannot be preserved: {member.filename}")
            member_path = Path(member_name)
            if member_name.startswith("/") or member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(f"Unsafe archive path: {member.filename}")
            canonical_name = PurePosixPath(member_name.rstrip("/")).as_posix()
            if canonical_name != member_name.rstrip("/"):
                raise ValueError(f"Non-canonical archive path cannot be preserved: {member.filename}")
            if is_zip_symlink(member):
                raise ValueError(f"Symlinks are not allowed in archives: {member.filename}")
            normalized_name = member_path.as_posix().rstrip("/")
            if normalized_name in seen_paths:
                raise ValueError(f"Duplicate archive path: {member.filename}")
            seen_paths.add(normalized_name)
            total_uncompressed += member.file_size
            if total_uncompressed > max_uncompressed_bytes:
                max_mb = max_uncompressed_bytes // (1024 * 1024)
                raise ValueError(f"Archive expands past configured limit of {max_mb} MB")
            target = (destination / member_path).resolve()
            if os.path.commonpath([destination, target]) != str(destination):
                raise ValueError(f"Unsafe archive path: {member.filename}")
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source_stream, open(target, "wb") as target_stream:
                shutil.copyfileobj(source_stream, target_stream, length=1024 * 1024)
            extracted_files.append(normalized_name)
    return extracted_files


def validate_conversion_paths(convertible_paths, all_files):
    """Reject any conversion that cannot preserve the path and basename exactly."""
    sources = {Path(path).resolve() for path in convertible_paths}
    occupied = {
        str(Path(path).resolve()).casefold(): Path(path)
        for path in all_files
        if Path(path).resolve() not in sources
    }
    planned = {}
    for source in convertible_paths:
        source = Path(source)
        output = source.with_suffix(".jxl")
        key = str(output.resolve()).casefold()
        if key in occupied:
            raise ValueError(
                f"Cannot preserve filename: {source.name} must become {output.name}, but that path already exists"
            )
        if key in planned:
            other = planned[key]
            raise ValueError(
                f"Cannot preserve filenames: both {other.name} and {source.name} would become {output.name}"
            )
        planned[key] = source


def verify_filename_invariants(original_files, final_files, completed_pages):
    """Ensure conversion changed only selected image extensions to .jxl."""
    original = set(original_files)
    expected = set(original)
    for source, page in completed_pages.items():
        output = page.get("output", "")
        source_path = PurePosixPath(source)
        required_output = source_path.with_suffix(".jxl").as_posix()
        if source not in original:
            raise VerificationError(f"Conversion manifest references an unknown source path: {source}")
        if output != required_output:
            raise VerificationError(
                f"Filename invariant violated: {source} may only become {required_output}"
            )
        if output in original and output != source:
            raise VerificationError(f"Filename invariant would overwrite an existing entry: {output}")
        expected.remove(source)
        expected.add(output)
    if set(final_files) != expected:
        raise VerificationError("Archive paths changed beyond permitted image extension changes")
    if len({path.casefold() for path in final_files}) != len(final_files):
        raise VerificationError("Archive contains case-insensitive filename collisions")


class ResumeWorkspace:
    def __init__(self, root, archive_path, fingerprint, option_key):
        self.root = Path(root) / fingerprint[:24]
        self.files = self.root / "files"
        self.manifest_path = self.root / "manifest.json"
        self.archive_path = str(Path(archive_path).resolve())
        self.fingerprint = fingerprint
        self.option_key = option_key
        self._lock = threading.Lock()
        self.manifest = {}

    def _write_manifest(self):
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self.manifest_path.with_suffix(".tmp")
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(self.manifest, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.manifest_path)

    def prepare(self, max_uncompressed_bytes, token):
        resumed = False
        if self.manifest_path.is_file() and self.files.is_dir():
            try:
                self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
                resumed = (
                    self.manifest.get("version") == 3
                    and self.manifest.get("archive_path") == self.archive_path
                    and self.manifest.get("fingerprint") == self.fingerprint
                    and self.manifest.get("options") == self.option_key
                )
            except (OSError, json.JSONDecodeError):
                resumed = False
        if resumed:
            for page in self.manifest.get("completed_pages", {}).values():
                output = self.files / page.get("output", "")
                if not output.is_file() or not jxl_has_valid_signature(output):
                    resumed = False
                    break
        if not resumed:
            if self.root.exists():
                shutil.rmtree(self.root)
            self.files.mkdir(parents=True)
            original_files = safe_extract_archive(self.archive_path, self.files, max_uncompressed_bytes, token)
            self.manifest = {
                "version": 3,
                "archive_path": self.archive_path,
                "fingerprint": self.fingerprint,
                "options": self.option_key,
                "stage": "extracted",
                "original_files": original_files,
                "completed_pages": {},
            }
            self._write_manifest()
        for partial in self.files.rglob(f"*{PARTIAL_SUFFIX}*"):
            partial.unlink(missing_ok=True)
        for pattern in ("*.cbzxl-input.*", "*.cbzxl.png"):
            for normalized in self.files.rglob(pattern):
                normalized.unlink(missing_ok=True)
        return resumed, len(self.manifest.get("completed_pages", {}))

    def set_stage(self, stage):
        with self._lock:
            self.manifest["stage"] = stage
            self._write_manifest()

    def record_page(self, source_relative, output_relative, output_size):
        with self._lock:
            self.manifest.setdefault("completed_pages", {})[source_relative] = {
                "output": output_relative,
                "bytes": output_size,
            }
            self._write_manifest()

    def complete(self):
        if self.root.exists():
            shutil.rmtree(self.root)


def jxl_has_valid_signature(path):
    with open(path, "rb") as handle:
        prefix = handle.read(12)
    return prefix.startswith(b"\xff\x0a") or prefix.startswith(b"\x00\x00\x00\x0cJXL ")


def verify_archive(
    candidate,
    workspace,
    expected_files,
    token,
    *,
    deep_samples=3,
    heartbeat=None,
    original_files=None,
    completed_pages=None,
):
    candidate = Path(candidate)
    with zipfile.ZipFile(candidate, "r") as archive:
        members = [info.filename for info in archive.infolist() if not info.is_dir()]
        if len(members) != len(set(members)):
            raise VerificationError("Replacement archive contains duplicate paths")
        expected = sorted(Path(path).relative_to(workspace).as_posix() for path in expected_files)
        if sorted(members) != expected:
            raise VerificationError("Replacement archive contents do not match the conversion workspace")
        if original_files is not None:
            verify_filename_invariants(original_files, members, completed_pages or {})
        bad_entry = archive.testzip()
        if bad_entry:
            raise VerificationError(f"Replacement archive failed CRC validation at {bad_entry}")
    jxl_paths = sorted(path for path in expected_files if path.suffix.lower() == ".jxl")
    for path in jxl_paths:
        token.check()
        if not jxl_has_valid_signature(path):
            raise VerificationError(f"Invalid JPEG XL signature: {path.name}")
    if jxl_paths and deep_samples:
        positions = sorted({0, len(jxl_paths) // 2, len(jxl_paths) - 1})[:deep_samples]
        for index in positions:
            token.check()
            result = run_cancellable(
                ["djxl", str(jxl_paths[index]), "-", "--output_format", "ppm", "--num_threads=1", "--quiet"],
                token,
                heartbeat=heartbeat,
                discard_stdout=True,
            )
            if result.returncode != 0:
                raise VerificationError(f"JPEG XL decode check failed for {jxl_paths[index].name}: {result.stderr.strip()}")
    return {"entries": len(expected_files), "jxl_entries": len(jxl_paths), "decoded_samples": min(deep_samples, len(jxl_paths))}


def plan_parallelism(cpu_budget, page_count, encoder_threads=None):
    budget = max(1, int(cpu_budget or 1))
    workers = max(1, min(int(page_count or 1), budget, 4))
    threads = max(1, int(encoder_threads or budget // workers))
    while workers * threads > budget and workers > 1:
        workers -= 1
    return workers, threads
