import os
import shutil
import sqlite3
import tempfile
import zipfile
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
from enum import Enum, auto

# Default Constants (can be overwritten by args)
DEFAULT_JXL_EFFORT = 10
DEFAULT_THREADS = 10
SCRIPT_VERSION = "1.4"  # Versioning for tracking script changes

# Fixed Constants
DB_FILE = "converted_archives.db"
FAILED_DB_FILE = "failed_archives.db"
LOG_FILE = "cbz_jxl_conversion.log"
SUBPROCESS_TIMEOUT = 600 # Timeout — Will kill healthy conversions, so set high enough based on longest conversion duration on your hardware

# Global variables - will be set in main()
console = Console()
VERBOSE = True
SUPPRESS_SKIPPED = False
DRY_RUN = False
BACKUP_ENABLED = False
RECHECK_ALL = False
DELETE_EMPTY_ARCHIVES = False

# These will be populated from args or defaults
JXL_EFFORT = DEFAULT_JXL_EFFORT
THREADS = DEFAULT_THREADS

def get_cjxl_version():
    """Get the version of the cjxl encoder for tracking."""
    try:
        # Use --version flag to get encoder version
        result = subprocess.run(["cjxl", "--version"], capture_output=True, text=True, check=True, encoding='utf-8')
        # On some systems, version info is on stderr
        output = result.stdout.strip() if result.stdout else result.stderr.strip()
        return output.splitlines()[0]  # Return the first line of the version info
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"

JXL_VERSION = get_cjxl_version()


class ConversionStatus(Enum):
    PROCESSED_SAVED_SPACE = auto()
    PROCESSED_NO_SPACE_SAVED = auto()
    ALREADY_JXL_NO_CONVERTIBLES = auto()
    NO_JPG_PNG_FOUND = auto()
    NO_IMAGES_RECOGNIZED = auto()
    CONTAIN_OTHER_FORMATS = auto()


def log(msg, level="info", msg_type="general"):
    """Log messages to console and log file, with optional [DRY RUN] prefix"""
    log_prefix = "[DRY RUN] " if DRY_RUN else ""
    full_msg = f"{log_prefix}{msg}"

    if msg_type == "skipped" and SUPPRESS_SKIPPED and not DRY_RUN:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(full_msg + "\n")
        return

    if VERBOSE or level == "error" or DRY_RUN:
        console.print(full_msg)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(full_msg + "\n")


def init_db(db_path):
    """Initialize the SQLite database with the enhanced schema."""
    if DRY_RUN and not Path(db_path).exists():
        log(f"Would initialize database at {db_path}")
        return None
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS converted_archives (
                path TEXT PRIMARY KEY,
                original_size INTEGER,
                final_size INTEGER,
                bytes_saved INTEGER,
                percent_saved REAL,
                converted_at TEXT,
                status TEXT DEFAULT 'processed',
                dominant_type TEXT,
                jxl_effort INTEGER,
                processing_duration_seconds REAL,
                image_count INTEGER,
                jpg_count INTEGER,
                png_count INTEGER,
                script_version TEXT,
                jxl_version TEXT,
                error_message TEXT
            )
        """)
    return conn


def mark_processed(conn, path_str, original_size, final_size, saved_bytes, dominant_type, jxl_effort_level, duration, img_count, num_jpg, num_png):
    """Mark an archive as processed with enhanced metadata."""
    if DRY_RUN or conn is None:
        log(f"Would mark as processed: {path_str} (Duration: {duration:.2f}s, Images: {img_count})")
        return

    percent_saved = (saved_bytes / original_size) * 100 if original_size > 0 else 0
    converted_at_iso = datetime.now().isoformat()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO converted_archives
            (path, original_size, final_size, bytes_saved, percent_saved, converted_at, status, dominant_type, jxl_effort,
             processing_duration_seconds, image_count, jpg_count, png_count, script_version, jxl_version)
            VALUES (?, ?, ?, ?, ?, ?, 'processed', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path_str, original_size, final_size, saved_bytes, percent_saved, converted_at_iso, dominant_type, jxl_effort_level,
              duration, img_count, num_jpg, num_png, SCRIPT_VERSION, JXL_VERSION))
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as processed in DB: {path_str} — {e}", level="error")


def mark_failed(conn, path_str, duration, error_msg):
    """Mark an archive as failed with duration and a specific error message."""
    if DRY_RUN or conn is None:
        log(f"Would mark as FAILED: {path_str} (Reason: {error_msg})")
        return
    try:
        conn.execute("""
            INSERT OR REPLACE INTO converted_archives
            (path, status, converted_at, script_version, jxl_version, processing_duration_seconds, error_message)
            VALUES (?, 'failed', ?, ?, ?, ?, ?)
        """, (path_str, datetime.now().isoformat(), SCRIPT_VERSION, JXL_VERSION, duration, error_msg))
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as failed in DB: {path_str} — {e}", level="error")


def get_size(file_path):
    """Get the size of a file"""
    if not Path(file_path).exists():
        return 0
    return os.path.getsize(file_path)

def run_magick_command(cmd_list, dry_run_log_msg):
    if DRY_RUN:
        log(dry_run_log_msg)
        return True
    try:
        result = subprocess.run(cmd_list, check=False, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
        if result.returncode != 0:
            log(f"[red]Magick command failed: {' '.join(cmd_list)}", level="error")
            log(f"[red]Magick stderr: {result.stderr.strip()}", level="error")
            return False
        return True
    except FileNotFoundError:
        log(f"[red]Magick command failed: {cmd_list[0]} not found. Is ImageMagick installed and in PATH?", level="error")
        return False
    except subprocess.TimeoutExpired:
        log(f"[red]Magick command timed out after {SUBPROCESS_TIMEOUT}s: {' '.join(cmd_list)}", level="error")
        return False

def fix_grayscale_icc(img_path_str):
    run_magick_command(
        ["magick", "mogrify", "-strip", img_path_str],
        f"   Would strip ICC profile (grayscale fix): {img_path_str}"
    )

def convert_cmyk_to_rgb(img_path_str):
    run_magick_command(
        ["magick", img_path_str, "-colorspace", "sRGB", img_path_str],
        f"   Would convert CMYK to sRGB: {img_path_str}"
    )

def get_mime_type(path_obj):
    try:
        result = subprocess.run(["file", "--mime-type", "-b", str(path_obj)],
                                capture_output=True, text=True, check=True, timeout=30)
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log(f"[yellow]⚠️ Timeout getting MIME type for {path_obj.name}", level="error")
        return "application/octet-stream"
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        log(f"[red]❌ Error getting MIME type for {path_obj.name}: {e}", level="error")
        return "application/octet-stream"


def correct_extension(img_path, mime):
    ext_map = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/avif": ".avif",
    }
    correct_ext = ext_map.get(mime)
    if correct_ext and img_path.suffix.lower() != correct_ext:
        new_path = img_path.with_suffix(correct_ext)
        log(f"[blue]   🔧 Correcting extension: {img_path.name} → {new_path.name}")
        if not DRY_RUN:
            try:
                img_path.rename(new_path)
                return new_path
            except OSError as e:
                log(f"[red]❌ Failed to rename {img_path} to {new_path}: {e}", level="error")
                return img_path
        return new_path
    return img_path


def convert_single_image(img_path_obj):
    img_path_str = str(img_path_obj)
    mime = get_mime_type(img_path_obj)

    if mime not in ("image/jpeg", "image/png"):
        return 0

    img_path_obj = correct_extension(img_path_obj, mime)
    img_path_str = str(img_path_obj)

    jxl_path = img_path_obj.with_suffix(".jxl")
    orig_size = get_size(img_path_obj)

    if mime == "image/png":
        fix_grayscale_icc(img_path_str)
    elif mime == "image/jpeg":
        try:
            identify_result = subprocess.run(
                ["identify", "-format", "%[colorspace]", img_path_str],
                capture_output=True, text=True, check=True, timeout=30
            )
            if identify_result.stdout.strip() == "CMYK":
                convert_cmyk_to_rgb(img_path_str)
        except subprocess.TimeoutExpired:
            log(f"[yellow]⚠️ Timeout checking colorspace for {img_path_str}", level="error")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            log(f"[red]Could not identify colorspace for {img_path_str}: {e}", level="error")

    if DRY_RUN:
        log(f"   Would convert image to JXL: {img_path_obj.name} (Effort: {JXL_EFFORT})")
        return 0

    cjxl_cmd = ["cjxl", "-d", "0", f"--effort={JXL_EFFORT}", img_path_str, str(jxl_path)]
    result = None
    try:
        result = subprocess.run(cjxl_cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)

        if result.returncode != 0 and "JPEG bitstream reconstruction data could not be created" in result.stderr:
            log(f"[yellow]   ⚠️ Retrying {img_path_obj.name} with --allow_jpeg_reconstruction 0 due to error: {result.stderr.strip()}", level="error")
            if jxl_path.exists(): os.remove(jxl_path)
            
            cjxl_cmd_retry = ["cjxl", "-d", "0", f"--effort={JXL_EFFORT}", "--allow_jpeg_reconstruction", "0", img_path_str, str(jxl_path)]
            result = subprocess.run(cjxl_cmd_retry, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired:
        log(f"[red]❌ cjxl timed out after {SUBPROCESS_TIMEOUT}s on {img_path_obj.name}", level="error")
        if jxl_path.exists(): os.remove(jxl_path)
        return 0
    except Exception as e:
        log(f"[red]❌ An unexpected error occurred during cjxl execution: {e}", level="error")
        return 0


    try:
        if result and result.returncode == 0 and jxl_path.exists() and get_size(jxl_path) > 0:
            saved = orig_size - get_size(jxl_path)
            os.remove(img_path_obj)
            if VERBOSE:
                log(f"   🖼️  Converted image: {img_path_obj.name} (MIME: {mime}, Original Size: {orig_size / 1024:.2f} KB, JXL Size: {get_size(jxl_path) / 1024:.2f} KB)")
            return saved
        else:
            log(f"[red]❌ Failed to convert with cjxl: {img_path_obj.name}", level="error")
            if result and result.stdout: log(f"[red]   cjxl stdout: {result.stdout.strip()}", level="error")
            if result and result.stderr: log(f"[red]   cjxl stderr: {result.stderr.strip()}", level="error")
            if jxl_path.exists() and get_size(jxl_path) == 0: os.remove(jxl_path)
            return 0
    except FileNotFoundError:
        log("[red]Error: 'cjxl' command not found.", level="error")
        return 0
    except Exception as e:
        log(f"[red]❌ Unexpected error converting {img_path_obj.name}: {e}", level="error")
        return 0


def convert_images(temp_dir_path):
    total_saved = 0
    
    all_files_in_temp = list(temp_dir_path.rglob("*.*"))

    convertible_paths = []
    jxl_paths = []
    other_image_paths = []
    
    jpg_count = 0
    png_count = 0
    dominant_type = "N/A"

    known_jpg_exts = ('.jpg', '.jpeg')
    known_png_exts = ('.png',)
    known_jxl_exts = ('.jxl',)
    known_other_image_exts = ('.webp', '.avif', '.gif', '.tiff', '.bmp')

    for p in all_files_in_temp:
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext in known_jpg_exts:
            convertible_paths.append(p)
            jpg_count += 1
        elif ext in known_png_exts:
            convertible_paths.append(p)
            png_count += 1
        elif ext in known_jxl_exts:
            jxl_paths.append(p)
        elif ext in known_other_image_exts:
            other_image_paths.append(p)
    
    image_count = jpg_count + png_count

    if jpg_count > png_count:
        dominant_type = "JPG"
    elif png_count > jpg_count:
        dominant_type = "PNG"
    elif jpg_count > 0:
        dominant_type = "Mixed"

    base_return = (0, "", dominant_type, image_count, jpg_count, png_count)

    if not convertible_paths:
        if jxl_paths:
            return ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES, *base_return
        elif other_image_paths:
            ext_counts = {p.suffix.lower(): 0 for p in other_image_paths}
            for p in other_image_paths: ext_counts[p.suffix.lower()] += 1
            most_frequent_ext = max(ext_counts, key=ext_counts.get) if ext_counts else ""
            return ConversionStatus.CONTAIN_OTHER_FORMATS, 0, most_frequent_ext[1:].upper(), dominant_type, image_count, jpg_count, png_count
        else:
            return ConversionStatus.NO_IMAGES_RECOGNIZED, *base_return

    if VERBOSE or DRY_RUN:
        log(f"   Found {len(convertible_paths)} JPEG/PNG images for potential conversion...")
    
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(convert_single_image, path_obj): path_obj for path_obj in convertible_paths}
        for future in as_completed(futures):
            try:
                total_saved += future.result()
            except Exception as e:
                log(f"[red]❌ Error processing image {futures[future].name} in thread: {e}", level="error")
    
    final_return = (total_saved, "", dominant_type, image_count, jpg_count, png_count)
    if total_saved > 0:
        return ConversionStatus.PROCESSED_SAVED_SPACE, *final_return
    else:
        return ConversionStatus.PROCESSED_NO_SPACE_SAVED, *final_return


def flatten_cbz_archive(cbz_path_for_log, temp_dir_path):
    log(f"   🔄 Flattening {Path(cbz_path_for_log).name}. All nested files will be brought to the top level.")
    action_taken = False
    files_moved_count = 0
    files_to_move = [item for item in temp_dir_path.rglob('*') if item.is_file() and item.parent != temp_dir_path]

    if not files_to_move:
        if VERBOSE or DRY_RUN: log("     No nested files found to move for flattening.")
        return False

    for src_path in files_to_move:
        dest_path = temp_dir_path / src_path.name
        counter = 1
        while dest_path.exists() and (DRY_RUN or not src_path.samefile(dest_path)):
            new_name = f"{dest_path.stem}_{counter}{dest_path.suffix}"
            if DRY_RUN: log(f"     [DRY RUN] Name collision for {dest_path.name}. Would rename to {new_name}.")
            else: log(f"[yellow]   ⚠️ Name collision: {dest_path.name} exists. Renaming to {new_name}.")
            dest_path = temp_dir_path / new_name
            counter += 1
        
        if VERBOSE or DRY_RUN: log(f"     Moving {src_path.relative_to(temp_dir_path)} to {dest_path.name}")
        if not DRY_RUN:
            try:
                shutil.move(str(src_path), str(dest_path))
                action_taken = True
            except Exception as e:
                log(f"[red]     ❌ Error moving file during flatten: {src_path} to {dest_path} - {e}", level="error")
        else: action_taken = True
        files_moved_count +=1
    
    if action_taken and not DRY_RUN:
        for item in list(temp_dir_path.iterdir()):
            if item.is_dir():
                try:
                    if not any(f.name != '.DS_Store' for f in item.rglob('*')):
                        if VERBOSE: log(f"     Removing now empty/effectively empty directory: {item.name}")
                        shutil.rmtree(item)
                except OSError as e:
                    log(f"[red]     ❌ Error removing directory during flatten: {item} - {e}", level="error")
    elif action_taken and DRY_RUN:
        if VERBOSE: log("     [DRY RUN] Would attempt to remove empty subdirectories.")

    if action_taken: log(f"   ✅ Flattening complete. Moved {files_moved_count} file(s).")
    return action_taken


def process_cbz(cbz_path_obj, conn_main, conn_fail, cli_args):
    """Processes a single CBZ archive."""
    start_time = time.monotonic()
    duration = 0
    rel_path_str = os.path.relpath(cbz_path_obj, Path(cli_args.input_dir).resolve())
    log(f"\n[bold]📦 Processing: {rel_path_str}[/bold]")

    if BACKUP_ENABLED:
        backup_path = cbz_path_obj.with_suffix(cbz_path_obj.suffix + ".bak")
        log_msg = f"   💾 Backing up original to {backup_path.name}"
        if DRY_RUN: log(f"[DRY RUN] {log_msg}")
        else:
            log(log_msg)
            try:
                shutil.copy2(cbz_path_obj, backup_path)
            except Exception as e:
                duration = time.monotonic() - start_time
                log(f"[red]❌ Failed to backup {rel_path_str}: {e}", level="error")
                mark_failed(conn_fail, rel_path_str, duration, str(e))
                return 0, False, False

    temp_dir_obj = None
    original_cbz_size = get_size(cbz_path_obj)
    
    try:
        temp_dir_obj = Path(tempfile.mkdtemp())

        if not DRY_RUN:
            try:
                with zipfile.ZipFile(cbz_path_obj, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir_obj)
            except Exception as e:
                duration = time.monotonic() - start_time
                log(f"[red]❌ Failed to extract {rel_path_str}: {e}", level="error")
                mark_failed(conn_fail, rel_path_str, duration, str(e))
                return 0, False, False

        # --- Conversion Step ---
        conversion_saved_bytes = 0
        status, saved_bytes, dominant_other, dominant_type, img_count, num_jpg, num_png = (ConversionStatus.NO_IMAGES_RECOGNIZED, 0, "", "N/A", 0, 0, 0)
        
        if not cli_args.no_convert:
            status, saved_bytes, dominant_other, dominant_type, img_count, num_jpg, num_png = convert_images(temp_dir_obj)
            conversion_saved_bytes = saved_bytes
            if status == ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES: log(f"   ℹ️  Already JXL. No conversion needed.")
            elif status == ConversionStatus.CONTAIN_OTHER_FORMATS: log(f"   ℹ️  Contains {dominant_other} images. No JPEG/PNG found.")
            elif status == ConversionStatus.NO_JPG_PNG_FOUND: log(f"   ⚠️ No JPEG/PNG images found for conversion.")
            elif status == ConversionStatus.NO_IMAGES_RECOGNIZED: log(f"   ⚠️ No processable images found in the archive.")
        else:
            log("   ⏩ Conversion skipped by user command.")
        
        action_taken = status in (ConversionStatus.PROCESSED_SAVED_SPACE, ConversionStatus.PROCESSED_NO_SPACE_SAVED)
        
        # --- Delete Empty Archives Feature ---
        if DELETE_EMPTY_ARCHIVES and status == ConversionStatus.NO_IMAGES_RECOGNIZED:
            duration = time.monotonic() - start_time
            log(f"[yellow]   🗑️  Archive '{rel_path_str}' contains no recognized images. Deleting...[/yellow]")
            if not DRY_RUN:
                try:
                    os.remove(cbz_path_obj)
                    log(f"[green]   ✅ Deleted empty archive: {rel_path_str}[/green]")
                    mark_processed(conn_main, rel_path_str, original_cbz_size, 0, original_cbz_size, "DELETED", JXL_EFFORT, duration, 0, 0, 0)
                    return 0, False, True
                except Exception as e:
                    log(f"[red]❌ Failed to delete empty archive {rel_path_str}: {e}", level="error")
                    mark_failed(conn_fail, rel_path_str, duration, str(e))
                    return 0, False, False
            else:
                log(f"[DRY RUN]   Would delete empty archive: {rel_path_str}")
                mark_processed(conn_main, rel_path_str, original_cbz_size, 0, original_cbz_size, "WOULD_DELETE", JXL_EFFORT, duration, 0, 0, 0)
                return 0, False, True

        # --- Flattening Step ---
        flattened_this_archive = False
        if not cli_args.no_flatten and any(item.is_dir() for item in temp_dir_obj.iterdir()):
            if flatten_cbz_archive(cbz_path_obj.name, temp_dir_obj):
                flattened_this_archive = True
                action_taken = True
        elif cli_args.no_flatten:
            log("   ⏩ Flattening skipped by user command.")
        
        # --- Repacking and Finalizing Step ---
        duration = time.monotonic() - start_time
        if action_taken:
            final_size = original_cbz_size - conversion_saved_bytes # Dry run estimate
            if not DRY_RUN:
                new_cbz_path_str = tempfile.mktemp(suffix=".cbz", dir=cbz_path_obj.parent)
                with zipfile.ZipFile(new_cbz_path_str, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                    for file_path in temp_dir_obj.rglob("*"):
                        if file_path.is_file():
                            zip_out.write(file_path, file_path.relative_to(temp_dir_obj).as_posix())
                shutil.move(new_cbz_path_str, cbz_path_obj)
                final_size = get_size(cbz_path_obj)

            actual_saved_bytes = original_cbz_size - final_size
            reduction_percentage = (actual_saved_bytes / original_cbz_size) * 100 if original_cbz_size > 0 else 0
            log_msg = f"Repacked. Final Size: {final_size / (1024*1024):.2f} MB. Change: {actual_saved_bytes / (1024*1024):.2f} MB ({reduction_percentage:.2f}%)"
            log(f"   ✅ {log_msg}")
            mark_processed(conn_main, rel_path_str, original_cbz_size, final_size, actual_saved_bytes, dominant_type, JXL_EFFORT, duration, img_count, num_jpg, num_png)
            return actual_saved_bytes, flattened_this_archive, False
        else:
            log("   No actions performed that require repacking.")
            mark_processed(conn_main, rel_path_str, original_cbz_size, original_cbz_size, 0, dominant_type, JXL_EFFORT, duration, img_count, num_jpg, num_png)
            return 0, flattened_this_archive, False

    except Exception as e:
        duration = time.monotonic() - start_time
        log(f"[red]❌❌ UNHANDLED EXCEPTION while processing {rel_path_str}: {e}", level="error")
        import traceback
        log(f"[red]Traceback: {traceback.format_exc()}", level="error")
        if conn_fail: mark_failed(conn_fail, rel_path_str, duration, str(e))
        return 0, False, False
    finally:
        if temp_dir_obj and temp_dir_obj.exists() and not (DRY_RUN and os.environ.get("CBZJXL_KEEP_DRY_RUN_TEMP")):
            shutil.rmtree(temp_dir_obj)


def main():
    global VERBOSE, SUPPRESS_SKIPPED, DRY_RUN, BACKUP_ENABLED, JXL_EFFORT, THREADS, RECHECK_ALL, DELETE_EMPTY_ARCHIVES

    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL and flatten structure.")
    parser.add_argument("input_dir", nargs="?", default=".", help="Directory to scan for CBZ files (default: current directory)")
    
    action_group = parser.add_argument_group('Action Control')
    action_group.add_argument("--no-convert", action="store_true", help="Do not convert images to JXL.")
    action_group.add_argument("--no-flatten", action="store_true", help="Do not flatten archive directory structure.")
    action_group.add_argument("--backup", action="store_true", help="Backup original CBZ files (as .cbz.bak).")
    action_group.add_argument("--dry-run", action="store_true", help="Simulate processing without modifying files.")
    action_group.add_argument("--delete-empty-archives", action="store_true", help="Delete CBZ archives with no recognized images.")
    
    tuning_group = parser.add_argument_group('Conversion Tuning')
    tuning_group.add_argument("--effort", type=int, default=DEFAULT_JXL_EFFORT, choices=range(0,11), metavar="[0-10]", help=f"JXL encoding effort (default: {DEFAULT_JXL_EFFORT})")
    tuning_group.add_argument("--threads", type=int, default=DEFAULT_THREADS, help=f"Number of threads for image conversion (default: {DEFAULT_THREADS})")

    output_group = parser.add_argument_group('Output Control')
    output_group.add_argument("--quiet", "-q", action="store_true", help="Suppress all console output except critical errors.")
    output_group.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging to console.")
    output_group.add_argument("--suppress-skipped", action="store_true", help="Suppress 'Skipping' messages from console.")
    
    db_group = parser.add_argument_group('Database Utilities')
    db_group.add_argument("--stats", action="store_true", help="Show conversion stats from the database and exit.")
    db_group.add_argument("--reprocess-failed", action="store_true", help="Run processing only on files listed in the failed DB.")
    db_group.add_argument("--reset-db", action="store_true", help="Delete both databases to reprocess everything.")
    db_group.add_argument("--recheck-all", action="store_true", help="Process all CBZ files, overwriting existing DB entries.")
    
    args = parser.parse_args()
    
    DRY_RUN = args.dry_run
    VERBOSE = (args.verbose or DRY_RUN) and not args.quiet
    SUPPRESS_SKIPPED = args.suppress_skipped or args.quiet
    BACKUP_ENABLED = args.backup
    JXL_EFFORT = args.effort
    THREADS = args.threads
    RECHECK_ALL = args.recheck_all
    DELETE_EMPTY_ARCHIVES = args.delete_empty_archives

    if DRY_RUN: console.print("[bold yellow] DRY RUN MODE ENABLED [/bold yellow] - No actual changes will be made.")

    if args.stats:
        # (Stats and other utility functions remain largely the same, but could be updated to show new stats)
        # ... existing stats logic ...
        return 0

    if args.reset_db:
        # ... existing reset_db logic ...
        return 0
    
    if args.reprocess_failed:
        # ... existing reprocess_failed logic ...
        return 0

    for tool in ["cjxl", "magick", "identify", "file"]:
        if not shutil.which(tool):
            log(f"[red]❌ CRITICAL: '{tool}' command not found. Please ensure it is installed and in your system's PATH.", level="error")
            return 1

    conn = init_db(DB_FILE)
    fail_conn = init_db(FAILED_DB_FILE)
    
    processed_paths = set()
    if conn and not RECHECK_ALL:
        try:
            cursor = conn.execute("SELECT path FROM converted_archives WHERE status = 'processed'")
            processed_paths.update(row[0] for row in cursor)
            if VERBOSE and processed_paths: log(f"Found {len(processed_paths)} successfully processed archives in the database to be skipped.")
        except sqlite3.Error as e:
            log(f"[red]Could not pre-load paths from DB: {e}", level="error")

    resolved_input_dir = Path(args.input_dir).resolve()
    cbz_files_path_obj_list = list(resolved_input_dir.rglob('*.cbz'))
    total_files = len(cbz_files_path_obj_list)

    if total_files == 0:
        log(f"No CBZ files found in '{resolved_input_dir}'. Exiting.")
        return 0

    log(f"🛠️ Starting CBZ processing for {total_files} file(s) in '{resolved_input_dir}'...")
    log(f"   Script Version: {SCRIPT_VERSION}, JXL Version: {JXL_VERSION}")
    log(f"   JXL Effort: {JXL_EFFORT}, Conversion Threads: {THREADS}")
    if BACKUP_ENABLED: log("   Backup: ENABLED")
    if RECHECK_ALL: log("   Mode: RECHECK ALL ARCHIVES (Overwriting DB entries)")
    if DELETE_EMPTY_ARCHIVES: log("   Action: DELETE EMPTY ARCHIVES ENABLED")

    # ... (rest of the main processing loop is largely unchanged, as the new logic is in process_cbz) ...
    total_bytes_saved_overall, processed_count, skipped_count, flattened_archives_count, deleted_empty_archives_count = 0, 0, 0, 0, 0

    with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}[/cyan]"), BarColumn(), TextColumn("{task.completed}/{task.total} archives"), TimeRemainingColumn(), console=console, disable=args.quiet and not DRY_RUN) as progress_bar:
        task_id = progress_bar.add_task("Processing CBZs...", total=total_files)

        for cbz_path in cbz_files_path_obj_list:
            rel_path_for_db = os.path.relpath(cbz_path, resolved_input_dir)
            if rel_path_for_db in processed_paths:
                skipped_count += 1
                log(f"[yellow]⚠️ Skipping already processed archive: {rel_path_for_db}", msg_type="skipped")
                progress_bar.update(task_id, advance=1)
                continue
            
            bytes_saved, was_flattened, was_deleted = process_cbz(cbz_path, conn, fail_conn, args)
            
            if was_deleted: deleted_empty_archives_count += 1
            else:
                processed_count += 1
                total_bytes_saved_overall += bytes_saved
                if was_flattened: flattened_archives_count += 1
            progress_bar.update(task_id, advance=1)

    failed_to_process_count = 0
    if os.path.exists(FAILED_DB_FILE):
        with sqlite3.connect(FAILED_DB_FILE) as temp_fail_conn:
            try: failed_to_process_count = temp_fail_conn.execute("SELECT COUNT(*) FROM converted_archives").fetchone()[0]
            except sqlite3.Error: pass
    
    if conn: conn.close()
    if fail_conn: fail_conn.close()

    log("\n🎉 [bold green]Conversion process finished![/bold green]")
    log(f"   Total archives found:     {total_files}")
    log(f"   Archives processed:       {processed_count}")
    log(f"   Archives flattened:       {flattened_archives_count}")
    log(f"   Skipped (already done):   {skipped_count}")
    log(f"   Archives deleted (no images): {deleted_empty_archives_count}")
    log(f"   Failed to process:        {failed_to_process_count}")
    
    if total_bytes_saved_overall >= 0:
        log(f"   Total space saved:        {total_bytes_saved_overall / (1024 ** 3):.3f} GB ({total_bytes_saved_overall / (1024 ** 2):.2f} MB)")
    else:
        log(f"   Total space increased by: {-total_bytes_saved_overall / (1024 ** 3):.3f} GB ({-total_bytes_saved_overall / (1024 ** 2):.2f} MB)")

    log(f"Log file written to: {Path(LOG_FILE).resolve()}")
    if DRY_RUN: console.print("[bold yellow]DRY RUN COMPLETE[/bold yellow] - No actual changes were made to files or databases.")
    return 0

if __name__ == "__main__":
    main()
