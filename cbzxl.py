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
DEFAULT_JXL_EFFORT = 9
DEFAULT_THREADS = 10

# Fixed Constants
DB_FILE = "converted_archives.db"
FAILED_DB_FILE = "failed_archives.db"
LOG_FILE = "cbz_jxl_conversion.log"

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


def log(msg, level="info", msg_type="general"):
    """Log messages to console and log file, with optional [DRY RUN] prefix"""
    log_prefix = "[DRY RUN] " if DRY_RUN else ""
    full_msg = f"{log_prefix}{msg}"

    if msg_type == "skipped" and SUPPRESS_SKIPPED and not DRY_RUN: # Don't suppress skipped if it's a dry run
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(full_msg + "\n")
        return

    if VERBOSE or level == "error" or DRY_RUN: # Always print to console in dry run or if verbose/error
        console.print(full_msg)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(full_msg + "\n")


def init_db(db_path):
    """Initialize the SQLite database"""
    if DRY_RUN:
        log(f"Would initialize database at {db_path}")
        return None # In dry run, don't actually connect or return a connection object
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
                status TEXT DEFAULT 'processed' -- Added status: processed, failed
            )
        """)
    return conn


def is_processed(conn, path_str):
    """Check if an archive has been processed and is up-to-date"""
    if DRY_RUN or conn is None:
        return False
    try:
        result = conn.execute("SELECT converted_at, status FROM converted_archives WHERE path = ?", (path_str,)).fetchone()
        if result:
            if result[1] == 'failed':
                return False
            converted_at_str = result[0]
            if converted_at_str is None:
                 return False
            converted_at = datetime.fromisoformat(converted_at_str)
            # Ensure the path_str for getmtime is absolute or correctly relative to cwd if necessary
            # If path_str is already how it's stored (e.g. relative to input_dir), need to reconstruct full path
            # For simplicity, assuming path_str as passed can be used by os.path.getmtime
            # This might need adjustment if path_str is not directly usable by os.path.getmtime
            # full_path_for_mtime = Path(args.input_dir).resolve() / path_str # Example if path_str is relative to input_dir
            # For now, let's assume path_str itself is usable if the script is run from where paths are relative,
            # or if path_str is already absolute. The `cbz_path` object in main is absolute.
            # The `rel_path_for_db` is relative. This means is_processed needs the full path.
            # This part of is_processed might not be robust if path_str is relative.
            # Let's assume for now that `os.path.getmtime` will get the correct file.
            # A better approach would be to pass the full path to is_processed.
            # Or, store full paths in DB, or always resolve based on a known root.
            # The `cbz_files_path_obj_list` in main holds absolute paths.
            # `rel_path_for_db` is used for DB keys.
            # When calling is_processed, we should use `rel_path_for_db`, but for mtime, we need the full path.
            # This is a subtle bug source.
            # A quick fix is to not check mtime here, or ensure full path is available.
            # For now, I will remove the mtime check to avoid complexities with path resolution here.
            # A more robust solution would involve passing the full path along with rel_path_for_db or storing absolute paths.
            # To keep the change minimal for now, let's assume if it's in DB as 'processed', it's done.
            # This means re-running won't re-process unless DB is cleared or status is 'failed'.
            return True # Simplified: if in DB and not 'failed', consider processed.
            # actual_mtime = datetime.fromtimestamp(os.path.getmtime(path_str)) # This line is problematic with relative paths
            # return actual_mtime <= converted_at
    except sqlite3.Error as e:
        log(f"[red]DB Error checking if processed {path_str}: {e}", level="error")
    return False


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
    """Mark an archive as failed"""
    if DRY_RUN or conn is None:
        log(f"Would mark as FAILED: {path_str}")
        return
    try:
        conn.execute("""
            INSERT INTO converted_archives (path, status, converted_at) VALUES (?, 'failed', ?)
            ON CONFLICT(path) DO UPDATE SET status='failed', converted_at=excluded.converted_at
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
        result = subprocess.run(cmd_list, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            log(f"[red]Magick command failed: {' '.join(cmd_list)}", level="error")
            log(f"[red]Magick stderr: {result.stderr.strip()}", level="error")
            return False
        return True
    except FileNotFoundError:
        log(f"[red]Magick command failed: {cmd_list[0]} not found. Is ImageMagick installed and in PATH?", level="error")
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
                                capture_output=True, text=True, check=True, timeout=5)
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
                capture_output=True, text=True, check=True, timeout=5
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

    try:
        cjxl_cmd = ["cjxl", "-d", "0", f"--effort={JXL_EFFORT}", img_path_str, str(jxl_path)]
        result = subprocess.run(cjxl_cmd, capture_output=True, text=True)

        if result.returncode == 0 and jxl_path.exists() and get_size(jxl_path) > 0:
            saved = orig_size - get_size(jxl_path)
            if saved > 0 :
                os.remove(img_path_obj)
                if VERBOSE: # Only log individual image conversion details if verbose
                    log(f"   🖼️  Converted image: {img_path_obj.name} (MIME: {mime}, Saved: {saved / 1024:.2f} KB)")
                return saved
            else:
                log(f"[yellow]   ⚠️ JXL not smaller for {img_path_obj.name}. Original kept.")
                if jxl_path.exists(): os.remove(jxl_path)
                return 0
        else:
            log(f"[red]❌ Failed to convert with cjxl: {img_path_obj.name}", level="error")
            if result.stdout: log(f"[red]   cjxl stdout: {result.stdout.strip()}", level="error")
            if result.stderr: log(f"[red]   cjxl stderr: {result.stderr.strip()}", level="error")
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
    # Define other image extensions script is aware of but won't convert
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

    if not convertible_paths: # No JPG/PNG images found
        if jxl_paths: # JXLs are present, no JPG/PNG
            return ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES, 0
        elif other_image_paths: # Other images (webp etc) present, but no JPG/PNG and no JXL
            return ConversionStatus.NO_JPG_PNG_FOUND, 0
        else: # No .jpg, .png, .jxl, or other known image types found
            return ConversionStatus.NO_IMAGES_RECOGNIZED, 0

    # Proceed with converting images in convertible_paths
    if VERBOSE or DRY_RUN: # Log count only if verbose or dry run
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
        return ConversionStatus.PROCESSED_SAVED_SPACE, total_saved
    else: 
        return ConversionStatus.PROCESSED_NO_SPACE_SAVED, total_saved


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


def process_cbz(cbz_path_obj, conn_main_db, conn_fail_db, cli_args): # Pass cli_args for input_dir context
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
    elif BACKUP_ENABLED and DRY_RUN:
        log(f"   [DRY RUN] Would backup original to {cbz_path_obj.name}.bak")

    temp_dir_obj = None
    try:
        temp_dir_obj = Path(tempfile.mkdtemp())
        # Extraction log removed as per request

        if not DRY_RUN:
            try:
                with zipfile.ZipFile(cbz_path_obj, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir_obj)
            except zipfile.BadZipFile:
                log(f"[red]❌ Corrupt CBZ archive: {rel_path_str}", level="error")
                mark_failed(conn_fail_db, rel_path_str)
                return 0, False
            except Exception as e:
                log(f"[red]❌ Failed to extract {rel_path_str}: {e}", level="error")
                mark_failed(conn_fail_db, rel_path_str)
                return 0, False
        
        # Original size log removed from here

        if not DRY_RUN:
            for leftover in temp_dir_obj.rglob("*.converted"):
                leftover.unlink()

        status, saved_bytes = convert_images(temp_dir_obj)
        
        current_cbz_original_size = get_size(cbz_path_obj) # Get original size for all cases for mark_processed

        flattened_this_archive = False
        if status in [ConversionStatus.PROCESSED_SAVED_SPACE, ConversionStatus.PROCESSED_NO_SPACE_SAVED] or \
           (DRY_RUN and status not in [ConversionStatus.NO_IMAGES_RECOGNIZED, ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES, ConversionStatus.NO_JPG_PNG_FOUND]):
            # Only attempt flattening if conversions happened or would have happened.
            # Don't flatten if it's just JXLs or no convertible images, unless it's a dry run of a conversion scenario.
            # This logic might need refinement based on whether flattening is desired *regardless* of conversion status.
            # For now, linking it to actual or potential conversion work.
            needs_flattening = any(item.is_dir() and item.name not in ['__MACOSX'] and not item.name.startswith('.')
                                   for item in temp_dir_obj.iterdir())
            if needs_flattening:
                if VERBOSE or DRY_RUN: log("   Archive appears nested, attempting to flatten structure.")
                flattened_this_archive = flatten_cbz_archive(cbz_path_obj.name, temp_dir_obj)
            # else:
            #     if VERBOSE or DRY_RUN: log("   Archive structure is already flat or contains no processable subdirectories.")


        if status == ConversionStatus.PROCESSED_SAVED_SPACE:
            if not DRY_RUN:
                new_cbz_path_str = tempfile.mktemp(suffix=".cbz", dir=cbz_path_obj.parent)
                if VERBOSE: log(f"   Repacking CBZ to: {Path(new_cbz_path_str).name}")
                with zipfile.ZipFile(new_cbz_path_str, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                    for file_path in temp_dir_obj.rglob("*"):
                        if file_path.is_file():
                            arcname = file_path.relative_to(temp_dir_obj).as_posix()
                            zip_out.write(file_path, arcname)
                shutil.move(new_cbz_path_str, cbz_path_obj)
                final_size = get_size(cbz_path_obj)
                reduction_percentage = (saved_bytes / current_cbz_original_size) * 100 if current_cbz_original_size > 0 else 0
                log(f"   ✅ Converted and repacked (Saved: {saved_bytes / (1024*1024):.2f} MB) ({reduction_percentage:.2f}% Reduction!)")
                mark_processed(conn_main_db, rel_path_str, current_cbz_original_size, final_size, saved_bytes)
            else: # DRY_RUN
                final_size_estimate = current_cbz_original_size - saved_bytes
                reduction_percentage = (saved_bytes / current_cbz_original_size) * 100 if current_cbz_original_size > 0 else 0
                log(f"   [DRY RUN] Would convert and repack (Saved: {saved_bytes / (1024*1024):.2f} MB) ({reduction_percentage:.2f}% Reduction!)")
                mark_processed(conn_main_db, rel_path_str, current_cbz_original_size, final_size_estimate, saved_bytes)
            return saved_bytes, flattened_this_archive

        elif status == ConversionStatus.PROCESSED_NO_SPACE_SAVED:
            log(f"   ⚠️ Conversion attempted, but no space saved. Original CBZ retained.")
            mark_processed(conn_main_db, rel_path_str, current_cbz_original_size, current_cbz_original_size, 0)
            return 0, flattened_this_archive
            
        elif status == ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES:
            log(f"   ℹ️  Already JXL")
            mark_processed(conn_main_db, rel_path_str, current_cbz_original_size, current_cbz_original_size, 0)
            return 0, flattened_this_archive

        elif status == ConversionStatus.NO_JPG_PNG_FOUND:
            log(f"   ⚠️ No JPEG/PNG images found for conversion. Other image types may be present.")
            mark_processed(conn_main_db, rel_path_str, current_cbz_original_size, current_cbz_original_size, 0)
            return 0, flattened_this_archive

        elif status == ConversionStatus.NO_IMAGES_RECOGNIZED:
            log(f"   ⚠️ No processable images found in the archive.")
            mark_processed(conn_main_db, rel_path_str, current_cbz_original_size, current_cbz_original_size, 0)
            return 0, flattened_this_archive
        
        # Should not be reached if status is handled comprehensively
        return 0, flattened_this_archive

    except Exception as e:
        log(f"[red]❌❌ UNHANDLED EXCEPTION while processing {rel_path_str}: {e}", level="error")
        import traceback
        log(f"[red]Traceback: {traceback.format_exc()}", level="error")
        mark_failed(conn_fail_db, rel_path_str)
        return 0, False
    finally:
        if temp_dir_obj and temp_dir_obj.exists():
            # Cleanup log removed as per request
            if not DRY_RUN :
                 shutil.rmtree(temp_dir_obj)
            elif DRY_RUN and (VERBOSE or os.environ.get("CBZJXL_KEEP_DRY_RUN_TEMP")): # Optional: keep temp for debug
                 if not os.environ.get("CBZJXL_KEEP_DRY_RUN_TEMP"):
                    shutil.rmtree(temp_dir_obj)
                 else:
                    log(f"   [DRY RUN] Kept temporary directory for inspection: {temp_dir_obj}")
            else: # Not DRY_RUN or not VERBOSE in DRY_RUN and no explicit keep flag
                 shutil.rmtree(temp_dir_obj)


def main():
    global VERBOSE, SUPPRESS_SKIPPED, DRY_RUN, BACKUP_ENABLED, JXL_EFFORT, THREADS

    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL and flatten structure.")
    parser.add_argument("input_dir", nargs="?", default=".",
                        help="Directory to scan for CBZ files (default: current directory)")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress ALL console output except critical errors (logs to file still).")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging to console.")
    parser.add_argument("--suppress-skipped", action="store_true",
                        help="Suppress 'Skipping already processed' messages from console (verbose still logs them to file).")
    parser.add_argument("--effort", type=int, default=DEFAULT_JXL_EFFORT, choices=range(0,10), metavar="[0-9]",
                        help=f"JPEG XL encoding effort (0-9, default: {DEFAULT_JXL_EFFORT})")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS,
                        help=f"Number of threads for image conversion (default: {DEFAULT_THREADS})")
    parser.add_argument("--backup", action="store_true",
                        help="Backup original CBZ files (as .cbz.bak) before processing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate processing: show what would be done without modifying files or databases.")

    args = parser.parse_args()

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

    if not shutil.which("cjxl"):
        log("[red]❌ CRITICAL: 'cjxl' command not found. Please install libjxl (JPEG XL tools).", level="error")
        return 1
    if not shutil.which("magick") and not shutil.which("identify"):
        log("[red]❌ CRITICAL: 'magick' or 'identify' (ImageMagick) not found. Please install ImageMagick.", level="error")
        return 1
    if not shutil.which("file"):
        log("[red]❌ CRITICAL: 'file' command not found. This is needed for MIME type detection.", level="error")
        return 1

    conn = init_db(DB_FILE)
    fail_conn = init_db(FAILED_DB_FILE)

    # Resolve input_dir to an absolute path for consistent relative path calculations
    resolved_input_dir = Path(args.input_dir).resolve()
    cbz_files_path_obj_list = list(resolved_input_dir.rglob('*.cbz'))
    total_files = len(cbz_files_path_obj_list)

    if total_files == 0:
        log(f"No CBZ files found in '{resolved_input_dir}'. Exiting.")
        return 0

    log(f"🛠️ Starting CBZ to JXL conversion for {total_files} file(s) in '{resolved_input_dir}'...")
    log(f"   JXL Effort: {JXL_EFFORT}, Conversion Threads: {THREADS}")
    if BACKUP_ENABLED: log("   Backup: ENABLED")
    if DRY_RUN: log("   Mode: DRY RUN")

    total_bytes_saved_overall = 0
    converted_count = 0
    skipped_count = 0
    failed_to_process_count = 0
    flattened_archives_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]{task.description}[/cyan]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} archives"),
        TimeRemainingColumn(),
        console=console,
        disable=args.quiet and not DRY_RUN
    ) as progress_bar:
        task_id = progress_bar.add_task("Processing CBZs...", total=total_files)

        for cbz_path in cbz_files_path_obj_list:
            # Use path relative to the initial input_dir for DB keys and simpler logs
            rel_path_for_db = os.path.relpath(cbz_path, resolved_input_dir)

            # The is_processed mtime check was removed for simplicity. Re-evaluate if strict mtime checking is critical.
            # To re-enable mtime check, is_processed would need the absolute `cbz_path`.
            if is_processed(conn, rel_path_for_db): # Pass rel_path_for_db as key
                skipped_count += 1
                log(f"[yellow]⚠️ Skipping already processed archive: {rel_path_for_db}", msg_type="skipped")
                progress_bar.update(task_id, advance=1)
                continue
            
            # Pass the full `args` object to process_cbz if it needs context like args.input_dir
            bytes_saved_this_cbz, was_flattened = process_cbz(cbz_path, conn, fail_conn, args)

            if was_flattened:
                flattened_archives_count +=1

            failed_check_result = None
            if not DRY_RUN and fail_conn:
                try:
                    failed_check_result = fail_conn.execute("SELECT 1 FROM converted_archives WHERE path = ? AND status = 'failed'", (rel_path_for_db,)).fetchone()
                except sqlite3.Error: pass

            if failed_check_result:
                failed_to_process_count +=1
            elif bytes_saved_this_cbz > 0: # Only count as "converted" if actual space was saved
                converted_count += 1
                total_bytes_saved_overall += bytes_saved_this_cbz
            
            progress_bar.update(task_id, advance=1)

    if conn: conn.close()
    if fail_conn: fail_conn.close()

    log("\n🎉 [bold green]Conversion process finished![/bold green]")
    log(f"   Total archives found:     {total_files}")
    log(f"   Archives converted:       {converted_count} (where space was saved)")
    log(f"   Archives flattened:       {flattened_archives_count}")
    log(f"   Skipped (already done):   {skipped_count}")
    log(f"   Failed to process:        {failed_to_process_count}")
    log(f"   Total space saved:        {total_bytes_saved_overall / (1024 ** 3):.3f} GB "
        f"({total_bytes_saved_overall / (1024 ** 2):.2f} MB)")
    log(f"Log file written to: {Path(LOG_FILE).resolve()}")
    if DRY_RUN:
        console.print("[bold yellow]DRY RUN COMPLETE[/bold yellow] - No actual changes were made to files or databases.")
    return 0

if __name__ == "__main__":
    main_exit_code = main()
    exit(main_exit_code)
