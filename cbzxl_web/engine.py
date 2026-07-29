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
import bz2
import binascii
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


PARTIAL_SUFFIX = ".cbzxl-partial"
LOCAL_FILE_HEADER = struct.Struct("<4s5H3L2H")
ZIP64_EXTRA_ID = 0x0001
IMAGE_SUFFIXES = {
    ".jpg", ".jpeg", ".png", ".jxl", ".tif", ".tiff", ".bmp",
    ".gif", ".webp", ".avif",
}


class ConversionCancelled(RuntimeError):
    pass


class VerificationError(RuntimeError):
    pass


class PageConversionError(RuntimeError):
    def __init__(
        self,
        message,
        *,
        entry_path=None,
        kind="page_encode_failed",
        stage="encoding",
        detail=None,
        related_entry_path=None,
    ):
        super().__init__(message)
        self.entry_path = entry_path
        self.kind = kind
        self.stage = stage
        self.detail = detail
        self.related_entry_path = related_entry_path


@dataclass(frozen=True)
class LocalZipEntry:
    filename: str
    flag_bits: int
    compress_type: int
    CRC: int
    compress_size: int
    file_size: int
    header_offset: int
    data_offset: int

    def is_dir(self):
        return self.filename.endswith("/")


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


def _decompress_zip_payload(compressed, method, filename, max_bytes=None):
    try:
        if method == zipfile.ZIP_STORED:
            recovered = compressed
        elif method == zipfile.ZIP_DEFLATED:
            decompressor = zlib.decompressobj(-15)
            recovered = decompressor.decompress(
                compressed,
                max_bytes + 1 if max_bytes else 0,
            )
            if max_bytes and (
                len(recovered) > max_bytes or decompressor.unconsumed_tail
            ):
                raise VerificationError(
                    f"Recovered ZIP entry exceeds the recovery limit: {filename}"
                )
            recovered += (
                decompressor.flush(max_bytes + 1 - len(recovered))
                if max_bytes
                else decompressor.flush()
            )
        elif method == zipfile.ZIP_BZIP2:
            decompressor = bz2.BZ2Decompressor()
            recovered = decompressor.decompress(
                compressed,
                max_length=max_bytes + 1 if max_bytes else -1,
            )
            if not decompressor.eof:
                raise VerificationError(
                    f"ZIP entry could not be decompressed completely: {filename}"
                )
        elif method == zipfile.ZIP_LZMA:
            decompressor = zipfile.LZMADecompressor()
            chunks = []
            recovered_size = 0
            for offset in range(0, len(compressed), 4096):
                chunk = decompressor.decompress(compressed[offset:offset + 4096])
                recovered_size += len(chunk)
                if max_bytes and recovered_size > max_bytes:
                    raise VerificationError(
                        f"Recovered ZIP entry exceeds the recovery limit: {filename}"
                    )
                chunks.append(chunk)
            recovered = b"".join(chunks)
        else:
            raise VerificationError(
                f"ZIP compression method {method} is not recoverable: {filename}"
            )
    except (OSError, EOFError, ValueError, zlib.error) as exc:
        raise VerificationError(f"ZIP entry could not be decompressed: {filename}") from exc
    if max_bytes and len(recovered) > max_bytes:
        raise VerificationError(f"Recovered ZIP entry exceeds the recovery limit: {filename}")
    return recovered


def _zip64_local_sizes(extra, compressed_size, file_size):
    if compressed_size != 0xFFFFFFFF and file_size != 0xFFFFFFFF:
        return compressed_size, file_size
    offset = 0
    while offset + 4 <= len(extra):
        field_id, field_size = struct.unpack_from("<HH", extra, offset)
        offset += 4
        payload = extra[offset:offset + field_size]
        offset += field_size
        if field_id != ZIP64_EXTRA_ID:
            continue
        payload_offset = 0
        if file_size == 0xFFFFFFFF and payload_offset + 8 <= len(payload):
            file_size = struct.unpack_from("<Q", payload, payload_offset)[0]
            payload_offset += 8
        if compressed_size == 0xFFFFFFFF and payload_offset + 8 <= len(payload):
            compressed_size = struct.unpack_from("<Q", payload, payload_offset)[0]
        break
    return compressed_size, file_size


def scan_local_zip_entries(source, *, max_entries=100000, allow_partial=False):
    """Read recoverable local ZIP headers without requiring a central directory."""
    entries = []
    source = Path(source)
    archive_size = source.stat().st_size
    with open(source, "rb") as handle:
        offset = 0
        while offset + 4 <= archive_size:
            handle.seek(offset)
            signature = handle.read(4)
            if signature in {b"PK\x01\x02", b"PK\x05\x06", b"PK\x06\x06", b"PK\x06\x07"}:
                break
            if signature != b"PK\x03\x04":
                if not entries:
                    raise VerificationError("No recoverable local ZIP headers were found")
                if allow_partial:
                    break
                raise VerificationError(
                    f"Unexpected data after local ZIP entry at byte {offset}"
                )
            handle.seek(offset)
            header = handle.read(LOCAL_FILE_HEADER.size)
            if len(header) != LOCAL_FILE_HEADER.size:
                if allow_partial and entries:
                    break
                raise VerificationError("A local ZIP header is truncated")
            (
                _signature,
                _version,
                flags,
                method,
                _time,
                _date,
                crc,
                compressed_size,
                file_size,
                name_len,
                extra_len,
            ) = LOCAL_FILE_HEADER.unpack(header)
            if flags & 0x1:
                raise VerificationError("Encrypted ZIP entries cannot be salvaged")
            if flags & 0x8:
                raise VerificationError(
                    "ZIP entries using trailing data descriptors require external repair"
                )
            raw_name = handle.read(name_len)
            extra = handle.read(extra_len)
            if len(raw_name) != name_len or len(extra) != extra_len:
                if allow_partial and entries:
                    break
                raise VerificationError("A local ZIP filename or extra field is truncated")
            try:
                filename = raw_name.decode("utf-8" if flags & 0x800 else "cp437")
            except UnicodeDecodeError as exc:
                raise VerificationError("A local ZIP filename cannot be decoded") from exc
            compressed_size, file_size = _zip64_local_sizes(
                extra,
                compressed_size,
                file_size,
            )
            if compressed_size == 0xFFFFFFFF or file_size == 0xFFFFFFFF:
                raise VerificationError(f"ZIP64 sizes are incomplete for {filename}")
            data_offset = offset + LOCAL_FILE_HEADER.size + name_len + extra_len
            if data_offset + compressed_size > archive_size:
                if allow_partial and entries:
                    break
                raise VerificationError(f"Compressed ZIP entry is truncated: {filename}")
            entries.append(
                LocalZipEntry(
                    filename=filename,
                    flag_bits=flags,
                    compress_type=method,
                    CRC=crc,
                    compress_size=compressed_size,
                    file_size=file_size,
                    header_offset=offset,
                    data_offset=data_offset,
                )
            )
            if len(entries) > max_entries:
                raise VerificationError("Archive contains too many entries to salvage safely")
            offset = data_offset + compressed_size
    if not entries:
        raise VerificationError("No recoverable local ZIP entries were found")
    return entries


def recover_local_zip_entry_bytes(
    source,
    entry,
    *,
    max_bytes=None,
    allow_size_mismatch=False,
):
    if max_bytes and entry.file_size > max_bytes and not allow_size_mismatch:
        raise VerificationError(f"ZIP entry exceeds the recovery limit: {entry.filename}")
    with open(source, "rb") as handle:
        handle.seek(entry.data_offset)
        compressed = handle.read(entry.compress_size)
    if len(compressed) != entry.compress_size:
        raise VerificationError(f"Compressed ZIP entry is truncated: {entry.filename}")
    recovered = _decompress_zip_payload(
        compressed,
        entry.compress_type,
        entry.filename,
        max_bytes,
    )
    if len(recovered) != entry.file_size and not allow_size_mismatch:
        raise VerificationError(f"Recovered ZIP entry has the wrong size: {entry.filename}")
    return recovered


def recover_zip_entry_bytes(
    source,
    info,
    *,
    max_bytes=None,
    allow_size_mismatch=False,
):
    """Recover one ZIP entry without trusting its stored CRC.

    This is deliberately limited to common CBZ compression methods. Callers
    must validate the recovered payload before offering an archive rebuild.
    """
    if info.flag_bits & 0x1:
        raise VerificationError(f"Encrypted ZIP entries cannot be recovered: {info.filename}")
    if max_bytes and info.file_size > max_bytes and not allow_size_mismatch:
        raise VerificationError(f"ZIP entry exceeds the recovery limit: {info.filename}")
    with open(source, "rb") as handle:
        handle.seek(info.header_offset)
        header = handle.read(LOCAL_FILE_HEADER.size)
        if len(header) != LOCAL_FILE_HEADER.size:
            raise VerificationError(f"Local ZIP header is truncated: {info.filename}")
        signature, _version, _flags, method, _time, _date, _crc, _compressed, _size, name_len, extra_len = LOCAL_FILE_HEADER.unpack(header)
        if signature != b"PK\x03\x04":
            raise VerificationError(f"Local ZIP header is invalid: {info.filename}")
        handle.seek(name_len + extra_len, os.SEEK_CUR)
        compressed = handle.read(info.compress_size)
    if len(compressed) != info.compress_size:
        raise VerificationError(f"Compressed ZIP entry is truncated: {info.filename}")
    recovered = _decompress_zip_payload(
        compressed,
        method,
        info.filename,
        max_bytes,
    )
    if len(recovered) != info.file_size and not allow_size_mismatch:
        raise VerificationError(f"Recovered ZIP entry has the wrong size: {info.filename}")
    if max_bytes and len(recovered) > max_bytes:
        raise VerificationError(f"Recovered ZIP entry exceeds the recovery limit: {info.filename}")
    return recovered


def safe_extract_salvage_archive(
    source,
    destination,
    max_uncompressed_bytes,
    token=None,
    *,
    allow_partial=False,
):
    destination = Path(destination).resolve()
    entries = scan_local_zip_entries(source, allow_partial=allow_partial)
    extracted_files = []
    seen_paths = set()
    total_uncompressed = 0
    for entry in entries:
        if token:
            token.check()
        member_name = entry.filename
        member_path = Path(member_name)
        if (
            member_name.startswith("/")
            or member_path.is_absolute()
            or ".." in member_path.parts
            or "\\" in member_name
        ):
            raise ValueError(f"Unsafe archive path: {member_name}")
        canonical_name = PurePosixPath(member_name.rstrip("/")).as_posix()
        if canonical_name != member_name.rstrip("/"):
            raise ValueError(
                f"Non-canonical archive path cannot be preserved: {member_name}"
            )
        normalized_name = member_path.as_posix().rstrip("/")
        if normalized_name in seen_paths:
            raise ValueError(f"Duplicate archive path: {member_name}")
        seen_paths.add(normalized_name)
        target = (destination / member_path).resolve()
        if os.path.commonpath([destination, target]) != str(destination):
            raise ValueError(f"Unsafe archive path: {member_name}")
        if entry.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        payload = recover_local_zip_entry_bytes(
            source,
            entry,
            max_bytes=max_uncompressed_bytes,
            allow_size_mismatch=True,
        )
        total_uncompressed += len(payload)
        if total_uncompressed > max_uncompressed_bytes:
            max_mb = max_uncompressed_bytes // (1024 * 1024)
            raise ValueError(f"Archive expands past configured limit of {max_mb} MB")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        extracted_files.append(normalized_name)
    return extracted_files


def crc32_hex(payload):
    return f"{binascii.crc32(payload) & 0xFFFFFFFF:08X}"


def safe_extract_archive(
    source,
    destination,
    max_uncompressed_bytes,
    token=None,
    *,
    recover_crc_entry=None,
    recover_crc_entries=None,
    recover_size_mismatch=False,
    defer_entries=None,
):
    destination = Path(destination).resolve()
    total_uncompressed = 0
    extracted_files = []
    seen_paths = set()
    recovery_entries = {
        PurePosixPath(path).as_posix()
        for path in (recover_crc_entries or [])
        if path
    }
    if recover_crc_entry:
        recovery_entries.add(PurePosixPath(recover_crc_entry).as_posix())
    deferred_entries = {
        PurePosixPath(path).as_posix()
        for path in (defer_entries or [])
        if path
    }
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
            if normalized_name in deferred_entries and not member.is_dir():
                extracted_files.append(normalized_name)
                continue
            recovered_payload = None
            if normalized_name in recovery_entries:
                recovered_payload = recover_zip_entry_bytes(
                    source,
                    member,
                    max_bytes=max_uncompressed_bytes,
                    allow_size_mismatch=recover_size_mismatch,
                )
            total_uncompressed += (
                len(recovered_payload)
                if recovered_payload is not None
                else member.file_size
            )
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
            if recovered_payload is not None:
                target.write_bytes(recovered_payload)
            else:
                with archive.open(member) as source_stream, open(target, "wb") as target_stream:
                    shutil.copyfileobj(source_stream, target_stream, length=1024 * 1024)
            extracted_files.append(normalized_name)
    return extracted_files


def validate_conversion_paths(convertible_paths, all_files, workspace=None):
    """Reject any conversion that cannot preserve the path and basename exactly."""
    workspace = Path(workspace).resolve() if workspace else None

    def display_path(path):
        path = Path(path).resolve()
        if workspace and os.path.commonpath([workspace, path]) == str(workspace):
            return path.relative_to(workspace).as_posix()
        return path.name

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
            existing = occupied[key]
            raise PageConversionError(
                f"Cannot preserve filename: {source.name} must become {output.name}, but that path already exists",
                entry_path=display_path(source),
                related_entry_path=display_path(existing),
                kind="filename_collision",
                stage="preparing",
                detail=f"Both {display_path(source)} and {display_path(existing)} require the same archive path",
            )
        if key in planned:
            other = planned[key]
            raise PageConversionError(
                f"Cannot preserve filenames: both {other.name} and {source.name} would become {output.name}",
                entry_path=display_path(source),
                related_entry_path=display_path(other),
                kind="filename_collision",
                stage="preparing",
                detail=f"Both entries would become {display_path(output)}",
            )
        planned[key] = source


def verify_filename_invariants(original_files, final_files, completed_pages):
    """Ensure processing changed only selected image extensions."""
    original = set(original_files)
    expected = set(original)
    for source, page in completed_pages.items():
        output = page.get("output", "")
        source_path = PurePosixPath(source)
        operation = page.get("operation") or "jxl_conversion"
        if operation == "extension_corrected":
            detected_extension = str(page.get("detected_extension") or "").lower()
            if detected_extension not in {
                ".jpg",
                ".png",
                ".tiff",
                ".bmp",
                ".webp",
                ".gif",
                ".avif",
            }:
                raise VerificationError(
                    f"Filename invariant has an unsupported detected extension: {detected_extension}"
                )
            required_output = source_path.with_suffix(detected_extension).as_posix()
        else:
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

    def prepare(
        self,
        max_uncompressed_bytes,
        token,
        *,
        recover_crc_entry=None,
        recover_crc_entries=None,
        recover_size_mismatch=False,
        salvage_archive=False,
        salvage_partial=False,
        defer_entries=None,
    ):
        resumed = False
        if self.manifest_path.is_file() and self.files.is_dir():
            try:
                self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
                resumed = (
                    self.manifest.get("version") == 5
                    and self.manifest.get("archive_path") == self.archive_path
                    and self.manifest.get("fingerprint") == self.fingerprint
                    and self.manifest.get("options") == self.option_key
                )
            except (OSError, json.JSONDecodeError):
                resumed = False
        if resumed:
            for page in self.manifest.get("completed_pages", {}).values():
                output = self.files / page.get("output", "")
                operation = page.get("operation") or "jxl_conversion"
                if not output.is_file() or (
                    operation != "extension_corrected"
                    and not jxl_has_valid_signature(output)
                ):
                    resumed = False
                    break
        if not resumed:
            if self.root.exists():
                shutil.rmtree(self.root)
            self.files.mkdir(parents=True)
            if salvage_archive:
                original_files = safe_extract_salvage_archive(
                    self.archive_path,
                    self.files,
                    max_uncompressed_bytes,
                    token,
                    allow_partial=salvage_partial,
                )
            else:
                original_files = safe_extract_archive(
                    self.archive_path,
                    self.files,
                    max_uncompressed_bytes,
                    token,
                    recover_crc_entry=recover_crc_entry,
                    recover_crc_entries=recover_crc_entries,
                    recover_size_mismatch=recover_size_mismatch,
                    defer_entries=defer_entries,
                )
            self.manifest = {
                "version": 5,
                "archive_path": self.archive_path,
                "fingerprint": self.fingerprint,
                "options": self.option_key,
                "stage": "extracted",
                "original_files": original_files,
                "completed_pages": {},
                "page_decisions": {},
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

    def record_decision(self, source_relative, **decision):
        with self._lock:
            self.manifest.setdefault("page_decisions", {})[source_relative] = {
                "entry_path": source_relative,
                **decision,
            }
            self._write_manifest()

    def discard_conversion_decisions(self, reason):
        """Describe pages whose candidates were discarded with the archive."""
        with self._lock:
            for decision in self.manifest.get("page_decisions", {}).values():
                if decision.get("outcome") != "converted":
                    continue
                decision["outcome"] = "retained"
                decision["final_size"] = decision.get("source_size")
                decision["reason"] = f"{decision.get('reason', 'Candidate verified')}; {reason}"
            self._write_manifest()

    def record_page(
        self,
        source_relative,
        output_relative,
        output_size,
        mode="Lossless",
        *,
        operation="jxl_conversion",
        detected_extension=None,
    ):
        with self._lock:
            self.manifest.setdefault("completed_pages", {})[source_relative] = {
                "output": output_relative,
                "bytes": output_size,
                "mode": mode,
                "operation": operation,
            }
            if detected_extension:
                self.manifest["completed_pages"][source_relative][
                    "detected_extension"
                ] = detected_extension
            self._write_manifest()

    def _entry_target(self, entry_path):
        normalized = PurePosixPath(str(entry_path or ""))
        if not entry_path or normalized.is_absolute() or ".." in normalized.parts:
            raise ValueError("Repair entry must be a safe relative archive path")
        canonical = normalized.as_posix()
        if canonical not in self.manifest.get("original_files", []):
            raise ValueError(f"Repair entry is not present in the original archive: {canonical}")
        target = (self.files / Path(*normalized.parts)).resolve()
        if os.path.commonpath([self.files.resolve(), target]) != str(self.files.resolve()):
            raise ValueError("Repair entry escapes the conversion workspace")
        return canonical, target

    def replace_entry(self, entry_path, replacement_path):
        canonical, target = self._entry_target(entry_path)
        replacement = Path(replacement_path or "").resolve()
        if not replacement.is_file():
            raise FileNotFoundError(f"Replacement image is unavailable: {replacement}")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.replacement")
        shutil.copy2(replacement, temporary)
        os.replace(temporary, target)
        self.manifest.setdefault("completed_pages", {}).pop(canonical, None)
        self.manifest["repair"] = {"action": "replace", "entry": canonical}
        self._write_manifest()

    def delete_entry(self, entry_path):
        canonical, target = self._entry_target(entry_path)
        target.unlink(missing_ok=True)
        self.manifest["original_files"] = [
            path for path in self.manifest.get("original_files", []) if path != canonical
        ]
        self.manifest.setdefault("completed_pages", {}).pop(canonical, None)
        self.manifest["repair"] = {"action": "delete", "entry": canonical}
        self._write_manifest()

    def mark_repair(self, action, entry_path=None, entries=None):
        canonical = None
        if entry_path:
            canonical, _target = self._entry_target(entry_path)
        canonical_entries = []
        for path in entries or []:
            normalized, _target = self._entry_target(path)
            canonical_entries.append(normalized)
        repair = {
            "action": action,
            "entry": canonical,
        }
        if canonical_entries or action in {"salvage", "salvage_partial"}:
            repair["entries"] = sorted(set(canonical_entries))
        self.manifest["repair"] = repair
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
    required_decode_entries=None,
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
    required_names = {
        PurePosixPath(path).as_posix()
        for path in (required_decode_entries or [])
    }
    required_paths = {
        path
        for path in jxl_paths
        if path.relative_to(workspace).as_posix() in required_names
    }
    sample_paths = set()
    if jxl_paths and deep_samples:
        positions = sorted({0, len(jxl_paths) // 2, len(jxl_paths) - 1})[:deep_samples]
        sample_paths.update(jxl_paths[index] for index in positions)
    decode_paths = sorted(required_paths | sample_paths)
    for path in decode_paths:
        token.check()
        result = run_cancellable(
            ["djxl", str(path), "--disable_output", "--num_threads=1", "--quiet"],
            token,
            heartbeat=heartbeat,
            discard_stdout=True,
        )
        if result.returncode != 0:
            raise VerificationError(f"JPEG XL decode check failed for {path.name}: {result.stderr.strip()}")
    return {
        "entries": len(expected_files),
        "jxl_entries": len(jxl_paths),
        "decoded_samples": len(decode_paths),
    }


def plan_parallelism(cpu_budget, page_count, encoder_threads=None):
    budget = max(1, int(cpu_budget or 1))
    workers = max(1, min(int(page_count or 1), budget, 4))
    threads = max(1, int(encoder_threads or budget // workers))
    while workers * threads > budget and workers > 1:
        workers -= 1
    return workers, threads
