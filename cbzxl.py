import os
import shutil
import sqlite3
import tempfile
import zipfile
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
from enum import Enum, auto

# Default Constants (can be overwritten by args)
DEFAULT_JXL_EFFORT = 10
DEFAULT_THREADS = 10

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

# These will be populated from args or defaults
JXL_EFFORT = DEFAULT_JXL_EFFORT
THREADS = DEFAULT_THREADS


class ConversionStatus(Enum):
    PROCESSED_SAVED_SPACE = auto()
    PROCESSED_NO_SPACE_SAVED = auto()
    ALREADY_JXL_NO_CONVERTIBLES = auto() # Only JXL, or JXL + other non-JPG/PNG
    NO_JPG_PNG_FOUND = auto()          # Other image types (webp, etc.) found, but no JPG/PNG
    NO_IMAGES_RECOGNIZED = auto()      # No files recognized as images the script cares about
    CONTAIN_OTHER_FORMATS = auto()     # Added for specific logging


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
    """Initialize the SQLite database"""
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
                status TEXT DEFAULT 'processed'
            )
        """)
    return conn


def mark_processed(conn, path_str, original_size, final_size, saved_bytes):
    """Mark an archive as processed with metadata"""
    if DRY_RUN or conn is None:
        log(f"Would mark as processed: {path_str} (Original: {original_size}, Final: {final_size}, Saved: {saved_bytes})")
        return

    percent_saved = (saved_bytes / original_size) * 100 if original_size > 0 else 0
    converted_at_iso = datetime.now().isoformat()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO converted_archives
            (path, original_size, final_size, bytes_saved, percent_saved, converted_at, status)
            VALUES (?, ?, ?, ?, ?, ?, 'processed')
        """, (path_str, original_size, final_size, saved_bytes, percent_saved, converted_at_iso))
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as processed in DB: {path_str} — {e}", level="error")


def mark_failed(conn, path_str):
    """Mark an archive as failed in the provided database connection."""
    if DRY_RUN or conn is None:
        log(f"Would mark as FAILED: {path_str}")
        return
    try:
        conn.execute("""
            INSERT OR REPLACE INTO converted_archives (path, status, converted_at) VALUES (?, 'failed', ?)
        """, (path_str, datetime.now().isoformat()))
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

    # First attempt at conversion
    cjxl_cmd = ["cjxl", "-d", "0", f"--effort={JXL_EFFORT}", img_path_str, str(jxl_path)]
    result = None
    try:
        result = subprocess.run(cjxl_cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)

        # Check for specific error and re-attempt with --allow_jpeg_reconstruction 0
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


def convert_images(temp_dir_path): # temp_dir_path is Path object
    total_saved = 0
    
    all_files_in_temp = list(temp_dir_path.rglob("*.*"))

    convertible_paths = [] # jpg, png
    jxl_paths = []
    other_image_paths = [] # webp, avif, gif, tiff, etc.

    known_convertible_exts = ('.jpg', '.jpeg', '.png')
    known_jxl_exts = ('.jxl',)
    known_other_image_exts = ('.webp', '.avif', '.gif', '.tiff', '.bmp')

    for p in all_files_in_temp:
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext in known_convertible_exts:
            convertible_paths.append(p)
        elif ext in known_jxl_exts:
            jxl_paths.append(p)
        elif ext in known_other_image_exts:
            other_image_paths.append(p)

    if not convertible_paths:
        if jxl_paths:
            return ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES, 0, ""
        elif other_image_paths:
            ext_counts = {}
            for p in other_image_paths:
                ext = p.suffix.lower()
                ext_counts[ext] = ext_counts.get(ext, 0) + 1
            
            if ext_counts:
                most_frequent_ext = max(ext_counts, key=ext_counts.get)
                return ConversionStatus.CONTAIN_OTHER_FORMATS, 0, most_frequent_ext[1:].upper()
            else:
                return ConversionStatus.NO_JPG_PNG_FOUND, 0, ""
        else:
            return ConversionStatus.NO_IMAGES_RECOGNIZED, 0, ""

    if VERBOSE or DRY_RUN:
        log(f"   Found {len(convertible_paths)} JPEG/PNG images for potential conversion...")
    
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(convert_single_image, path_obj): path_obj for path_obj in convertible_paths}
        for future in as_completed(futures):
            try:
                saved_on_this_image = future.result()
                total_saved += saved_on_this_image
            except Exception as e:
                path_obj = futures[future]
                log(f"[red]❌ Error processing image {path_obj.name} in thread: {e}", level="error")

    if total_saved > 0:
        return ConversionStatus.PROCESSED_SAVED_SPACE, total_saved, ""
    else: 
        return ConversionStatus.PROCESSED_NO_SPACE_SAVED, total_saved, ""


def flatten_cbz_archive(cbz_path_for_log, temp_dir_path):
    log(f"   🔄 Flattening {Path(cbz_path_for_log).name}. All nested files will be brought to the top level.")
    action_taken = False
    files_moved_count = 0
    files_to_move = []
    for item in temp_dir_path.rglob('*'):
        if item.is_file() and item.parent != temp_dir_path:
            files_to_move.append(item)

    if not files_to_move:
        if VERBOSE or DRY_RUN:
            log("     No nested files found to move for flattening.")
        return False

    for src_path in files_to_move:
        dest_path = temp_dir_path / src_path.name
        original_dest_name = dest_path.name
        counter = 1
        while dest_path.exists() and (DRY_RUN or not src_path.samefile(dest_path)):
            if DRY_RUN and dest_path.exists():
                 if VERBOSE: log(f"     [DRY RUN] Name collision for {original_dest_name}. Would rename.")
                 dest_path = temp_dir_path / f"{dest_path.stem}_{counter}{dest_path.suffix}"
                 counter += 1
                 continue
            if not DRY_RUN and dest_path.exists() and not src_path.samefile(dest_path):
                log(f"[yellow]   ⚠️ Name collision: {dest_path.name} exists. Renaming moved file.")
                dest_path = temp_dir_path / f"{dest_path.stem}_{counter}{dest_path.suffix}"
                counter += 1
            else:
                break
        
        if VERBOSE or DRY_RUN:
            log(f"     Moving {src_path.relative_to(temp_dir_path)} to {dest_path.name}")
        if not DRY_RUN:
            try:
                shutil.move(str(src_path), str(dest_path))
                files_moved_count += 1
                action_taken = True
            except Exception as e:
                log(f"[red]     ❌ Error moving file during flatten: {src_path} to {dest_path} - {e}", level="error")
        else:
            files_moved_count += 1
            action_taken = True

    if not DRY_RUN and action_taken:
        for item in list(temp_dir_path.iterdir()):
            if item.is_dir():
                try:
                    is_effectively_empty = True
                    for sub_item in item.rglob('*'):
                        if sub_item.name != '.DS_Store':
                            is_effectively_empty = False
                            break
                    if is_effectively_empty:
                        if VERBOSE: log(f"     Removing now empty/effectively empty directory: {item.name}")
                        shutil.rmtree(item)
                except OSError as e:
                    log(f"[red]     ❌ Error removing directory during flatten: {item} - {e}", level="error")
    elif DRY_RUN and action_taken:
        if VERBOSE: log("     [DRY RUN] Would attempt to remove empty subdirectories.")

    if action_taken and (VERBOSE or DRY_RUN):
        log(f"   ✅ Flattening complete. Moved {files_moved_count} file(s).")
    return action_taken


def process_cbz(cbz_path_obj, conn_main, conn_fail, cli_args):
    """Processes a single CBZ archive."""
    rel_path_str = os.path.relpath(cbz_path_obj, Path(cli_args.input_dir).resolve())
    log(f"\n[bold]📦 Processing: {rel_path_str}[/bold]")

    if BACKUP_ENABLED and not DRY_RUN:
        backup_path = cbz_path_obj.with_suffix(cbz_path_obj.suffix + ".bak")
        log(f"   💾 Backing up original to {backup_path.name}")
        try:
            shutil.copy2(cbz_path_obj, backup_path)
        except Exception as e:
            log(f"[red]❌ Failed to backup {rel_path_str}: {e}", level="error")
            mark_failed(conn_fail, rel_path_str)
            return 0, False
    elif BACKUP_ENABLED and DRY_RUN:
        log(f"   [DRY RUN] Would backup original to {cbz_path_obj.name}.bak")

    temp_dir_obj = None
    try:
        temp_dir_obj = Path(tempfile.mkdtemp())

        if not DRY_RUN:
            try:
                with zipfile.ZipFile(cbz_path_obj, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir_obj)
            except zipfile.BadZipFile:
                log(f"[red]❌ Corrupt CBZ archive: {rel_path_str}", level="error")
                mark_failed(conn_fail, rel_path_str)
                return 0, False
            except Exception as e:
                log(f"[red]❌ Failed to extract {rel_path_str}: {e}", level="error")
                mark_failed(conn_fail, rel_path_str)
                return 0, False
        
        if not DRY_RUN:
            # Feature: Clean up any leftover .converted placeholder files
            for leftover in temp_dir_obj.rglob("*.converted"):
                leftover.unlink()

        action_taken = False
        images_converted = False
        conversion_saved_bytes = 0
        
        # --- Conversion Step ---
        if not cli_args.no_convert:
            status, saved_bytes, dominant_other_format = convert_images(temp_dir_obj)
            conversion_saved_bytes = saved_bytes
            if status in (ConversionStatus.PROCESSED_SAVED_SPACE, ConversionStatus.PROCESSED_NO_SPACE_SAVED):
                action_taken = True
                images_converted = True
            elif status == ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES:
                log(f"   ℹ️  Already JXL. No conversion needed.")
            elif status == ConversionStatus.CONTAIN_OTHER_FORMATS:
                log(f"   ℹ️  Contains {dominant_other_format} images. No JPEG/PNG found for conversion.")
            elif status == ConversionStatus.NO_JPG_PNG_FOUND:
                log(f"   ⚠️ No JPEG/PNG images found for conversion.")
            elif status == ConversionStatus.NO_IMAGES_RECOGNIZED:
                log(f"   ⚠️ No processable images found in the archive.")
        else:
            log("   ⏩ Conversion skipped by user command.")

        # --- Flattening Step ---
        original_cbz_size = get_size(cbz_path_obj)
        flattened_this_archive = False
        if not cli_args.no_flatten:
            needs_flattening = any(item.is_dir() for item in temp_dir_obj.iterdir())
            if needs_flattening:
                flattened_this_archive = flatten_cbz_archive(cbz_path_obj.name, temp_dir_obj)
                if flattened_this_archive:
                    action_taken = True
        else:
            log("   ⏩ Flattening skipped by user command.")


        # --- Repacking and Finalizing Step ---
        if action_taken:
            if not DRY_RUN:
                new_cbz_path_str = tempfile.mktemp(suffix=".cbz", dir=cbz_path_obj.parent)
                with zipfile.ZipFile(new_cbz_path_str, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                    for file_path in temp_dir_obj.rglob("*"):
                        if file_path.is_file():
                            arcname = file_path.relative_to(temp_dir_obj).as_posix()
                            zip_out.write(file_path, arcname)
                shutil.move(new_cbz_path_str, cbz_path_obj)
                final_size = get_size(cbz_path_obj)
                actual_saved_bytes = original_cbz_size - final_size
                reduction_percentage = (actual_saved_bytes / original_cbz_size) * 100 if original_cbz_size > 0 else 0
                log(f"   ✅ Repacked. Final Size: {final_size / (1024*1024):.2f} MB. Change: {actual_saved_bytes / (1024*1024):.2f} MB ({reduction_percentage:.2f}%)")
                mark_processed(conn_main, rel_path_str, original_cbz_size, final_size, actual_saved_bytes)
                return actual_saved_bytes, flattened_this_archive
            else: # Dry Run
                log(f"   [DRY RUN] Would repack archive.")
                mark_processed(conn_main, rel_path_str, original_cbz_size, original_cbz_size - conversion_saved_bytes, conversion_saved_bytes)
                return conversion_saved_bytes, flattened_this_archive
        else:
            log("   No actions performed that require repacking.")
            mark_processed(conn_main, rel_path_str, original_cbz_size, original_cbz_size, 0)
            return 0, flattened_this_archive

    except Exception as e:
        log(f"[red]❌❌ UNHANDLED EXCEPTION while processing {rel_path_str}: {e}", level="error")
        import traceback
        log(f"[red]Traceback: {traceback.format_exc()}", level="error")
        if conn_fail:
            mark_failed(conn_fail, rel_path_str)
        return 0, False
    finally:
        if temp_dir_obj and temp_dir_obj.exists():
            if not DRY_RUN :
                 shutil.rmtree(temp_dir_obj)
            elif DRY_RUN and (VERBOSE or os.environ.get("CBZJXL_KEEP_DRY_RUN_TEMP")):
                 if not os.environ.get("CBZJXL_KEEP_DRY_RUN_TEMP"):
                    shutil.rmtree(temp_dir_obj)
                 else:
                    log(f"   [DRY RUN] Kept temporary directory for inspection: {temp_dir_obj}")
            else:
                 shutil.rmtree(temp_dir_obj)


def main():
    global VERBOSE, SUPPRESS_SKIPPED, DRY_RUN, BACKUP_ENABLED, JXL_EFFORT, THREADS

    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL and flatten structure.")
    parser.add_argument("input_dir", nargs="?", default=".",
                        help="Directory to scan for CBZ files (default: current directory)")
    
    # --- Action Control ---
    action_group = parser.add_argument_group('Action Control')
    action_group.add_argument("--no-convert", action="store_true", help="Do not convert images to JXL.")
    action_group.add_argument("--no-flatten", action="store_true", help="Do not flatten archive directory structure.")
    action_group.add_argument("--backup", action="store_true", help="Backup original CBZ files (as .cbz.bak) before processing.")
    action_group.add_argument("--dry-run", action="store_true", help="Simulate processing: show what would be done without modifying files.")

    # --- Conversion Tuning ---
    tuning_group = parser.add_argument_group('Conversion Tuning')
    tuning_group.add_argument("--effort", type=int, default=DEFAULT_JXL_EFFORT, choices=range(0,11), metavar="[0-10]",
                        help=f"JPEG XL encoding effort (0-10, default: {DEFAULT_JXL_EFFORT})")
    tuning_group.add_argument("--threads", type=int, default=DEFAULT_THREADS,
                        help=f"Number of threads for image conversion (default: {DEFAULT_THREADS})")

    # --- Output Control ---
    output_group = parser.add_argument_group('Output Control')
    output_group.add_argument("--quiet", "-q", action="store_true", help="Suppress ALL console output except critical errors.")
    output_group.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging to console.")
    output_group.add_argument("--suppress-skipped", action="store_true",
                        help="Suppress 'Skipping already processed' messages from console.")
    
    # --- Database Utilities ---
    db_group = parser.add_argument_group('Database Utilities')
    db_group.add_argument("--stats", action="store_true", help="Show conversion stats from the database and exit.")
    db_group.add_argument("--reprocess-failed", action="store_true", help="Delete the failed DB to allow reprocessing of failed items.")
    db_group.add_argument("--reset-db", action="store_true", help="Delete both databases to reprocess everything.")


    args = parser.parse_args()

    # --- Handle Utility Modes First ---
    if args.stats:
        conn = init_db(DB_FILE)
        fail_conn = init_db(FAILED_DB_FILE)
        console.print("\n[bold cyan]📊 Database Statistics[/bold cyan]")
        try:
            if conn:
                processed_archives = conn.execute("SELECT COUNT(*) FROM converted_archives").fetchone()[0]
                total_saved = conn.execute("SELECT SUM(bytes_saved) FROM converted_archives").fetchone()[0]
                console.print(f"   Successfully Processed: {processed_archives}")
                if total_saved is not None:
                    console.print(f"   Total Space Saved:      {total_saved / (1024**3):.3f} GB")
            else:
                console.print("   No successfully processed archives found.")

            if fail_conn:
                failed_archives = fail_conn.execute("SELECT COUNT(*) FROM converted_archives").fetchone()[0]
                console.print(f"   Marked as Failed:       {failed_archives}")
            else:
                console.print("   No failed archives found.")
        except sqlite3.Error as e:
            console.print(f"[red]Could not retrieve stats from database: {e}[/red]")
        finally:
            if conn: conn.close()
            if fail_conn: fail_conn.close()
        return 0

    if args.reset_db:
        console.print(f"[bold yellow]WARNING:[/bold yellow] This will delete '{DB_FILE}' and '{FAILED_DB_FILE}'.")
        if input("Are you sure you want to continue? (y/n): ").lower() == 'y':
            try:
                for db_file in [DB_FILE, FAILED_DB_FILE]:
                    if os.path.exists(db_file):
                        os.remove(db_file)
                        console.print(f"[green]'{db_file}' has been deleted.[/green]")
                    else:
                        console.print(f"'{db_file}' not found. Nothing to do.")
            except OSError as e:
                console.print(f"[red]Error during reset: {e}[/red]")
        else:
            console.print("Reset cancelled.")
        return 0

    if args.reprocess_failed:
        console.print(f"[bold yellow]This will delete the failed archives database ('{FAILED_DB_FILE}') to allow them to be reprocessed.[/bold yellow]")
        try:
            if os.path.exists(FAILED_DB_FILE):
                os.remove(FAILED_DB_FILE)
                console.print(f"[green]'{FAILED_DB_FILE}' has been deleted. Failed items will be reprocessed on the next run.[/green]")
            else:
                console.print(f"'{FAILED_DB_FILE}' not found. Nothing to do.")
        except OSError as e:
            console.print(f"[red]Error deleting failed database: {e}[/red]")
        return 0


    # --- Main Processing Logic ---
    DRY_RUN = args.dry_run
    if args.quiet:
        VERBOSE = False
        SUPPRESS_SKIPPED = True
    else:
        VERBOSE = args.verbose or DRY_RUN
        SUPPRESS_SKIPPED = args.suppress_skipped

    BACKUP_ENABLED = args.backup
    JXL_EFFORT = args.effort
    THREADS = args.threads

    if DRY_RUN:
        console.print("[bold yellow] DRY RUN MODE ENABLED [/bold yellow] - No actual changes will be made.")
        VERBOSE = True

    for tool in ["cjxl", "magick", "identify", "file"]:
        if not shutil.which(tool):
            log(f"[red]❌ CRITICAL: '{tool}' command not found. Please ensure it is installed and in your system's PATH.", level="error")
            return 1

    conn = init_db(DB_FILE)
    fail_conn = init_db(FAILED_DB_FILE)
    
    # Performance: Pre-load paths from the main DB to skip successfully processed files.
    processed_paths = set()
    if conn:
        try:
            cursor = conn.execute("SELECT path FROM converted_archives")
            processed_paths.update(row[0] for row in cursor)
            if VERBOSE and processed_paths:
                log(f"Found {len(processed_paths)} successfully processed archives in the database to be skipped.")
        except sqlite3.Error as e:
            log(f"[red]Could not pre-load paths from DB: {e}", level="error")


    resolved_input_dir = Path(args.input_dir).resolve()
    cbz_files_path_obj_list = list(resolved_input_dir.rglob('*.cbz'))
    total_files = len(cbz_files_path_obj_list)

    if total_files == 0:
        log(f"No CBZ files found in '{resolved_input_dir}'. Exiting.")
        return 0

    log(f"🛠️ Starting CBZ processing for {total_files} file(s) in '{resolved_input_dir}'...")
    log(f"   JXL Effort: {JXL_EFFORT}, Conversion Threads: {THREADS}")
    if BACKUP_ENABLED: log("   Backup: ENABLED")
    if DRY_RUN: log("   Mode: DRY RUN")

    total_bytes_saved_overall = 0
    processed_count = 0
    skipped_count = 0
    flattened_archives_count = 0

    with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}[/cyan]"), BarColumn(), TextColumn("{task.completed}/{task.total} archives"), TimeRemainingColumn(), console=console, disable=args.quiet and not DRY_RUN) as progress_bar:
        task_id = progress_bar.add_task("Processing CBZs...", total=total_files)

        for cbz_path in cbz_files_path_obj_list:
            rel_path_for_db = os.path.relpath(cbz_path, resolved_input_dir)

            if rel_path_for_db in processed_paths:
                skipped_count += 1
                log(f"[yellow]⚠️ Skipping already processed archive: {rel_path_for_db}", msg_type="skipped")
                progress_bar.update(task_id, advance=1)
                continue
            
            bytes_saved_this_cbz, was_flattened = process_cbz(cbz_path, conn, fail_conn, args)
            processed_count += 1
            total_bytes_saved_overall += bytes_saved_this_cbz

            if was_flattened:
                flattened_archives_count +=1

            progress_bar.update(task_id, advance=1)

    # Get final count of failed archives for the summary report
    failed_to_process_count = 0
    if os.path.exists(FAILED_DB_FILE):
        temp_fail_conn = sqlite3.connect(FAILED_DB_FILE)
        try:
            failed_to_process_count = temp_fail_conn.execute("SELECT COUNT(*) FROM converted_archives").fetchone()[0]
        except sqlite3.Error:
            pass # Fail silently if DB is corrupt
        finally:
            temp_fail_conn.close()

    if conn: conn.close()
    if fail_conn: fail_conn.close()

    log("\n🎉 [bold green]Conversion process finished![/bold green]")
    log(f"   Total archives found:     {total_files}")
    log(f"   Archives processed:       {processed_count}")
    log(f"   Archives flattened:       {flattened_archives_count}")
    log(f"   Skipped (already done):   {skipped_count}")
    log(f"   Failed to process:        {failed_to_process_count}")
    
    if total_bytes_saved_overall >= 0:
        log(f"   Total space saved:        {total_bytes_saved_overall / (1024 ** 3):.3f} GB ({total_bytes_saved_overall / (1024 ** 2):.2f} MB)")
    else:
        log(f"   Total space increased by: {-total_bytes_saved_overall / (1024 ** 3):.3f} GB ({-total_bytes_saved_overall / (1024 ** 2):.2f} MB)")

    log(f"Log file written to: {Path(LOG_FILE).resolve()}")
    if DRY_RUN:
        console.print("[bold yellow]DRY RUN COMPLETE[/bold yellow] - No actual changes were made to files or databases.")
    return 0

if __name__ == "__main__":
    main_exit_code = main()
    exit(main_exit_code)
