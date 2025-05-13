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

# Constants
JXL_EFFORT = 8
DB_FILE = "converted_archives.db"
FAILED_DB_FILE = "failed_archives.db"
LOG_FILE = "cbz_jxl_conversion.log"
THREADS = 10

# Global variables
console = Console()
VERBOSE = True

def log(msg, level="info"):
    """Log messages to console and log file"""
    if VERBOSE or level == "error":
        console.print(msg)
    with open(LOG_FILE, 'a') as f:
        f.write(msg + "\n")

def init_db(path):
    """Initialize the SQLite database"""
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS converted_archives (
                path TEXT PRIMARY KEY,
                original_size INTEGER,
                final_size INTEGER,
                bytes_saved INTEGER,
                percent_saved REAL,
                converted_at TEXT
            )
        """)
    return conn

def is_processed(conn, path):
    """Check if an archive has been processed and is up-to-date"""
    result = conn.execute("SELECT converted_at FROM converted_archives WHERE path = ?", (path,)).fetchone()
    if result:
        converted_at = datetime.fromisoformat(result[0])
        actual_mtime = datetime.fromtimestamp(os.path.getmtime(path))
        return actual_mtime <= converted_at
    return False

def mark_processed(conn, path, original_size, final_size, saved_bytes):
    """Mark an archive as processed with metadata"""
    percent_saved = (saved_bytes / original_size) * 100 if original_size else 0
    converted_at = datetime.now().isoformat()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO converted_archives
            (path, original_size, final_size, bytes_saved, percent_saved, converted_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (path, original_size, final_size, saved_bytes, percent_saved, converted_at))
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as processed: {path} — {e}", level="error")

def mark_failed(conn, path):
    """Mark an archive as failed"""
    try:
        conn.execute("INSERT OR IGNORE INTO converted_archives (path) VALUES (?)", (path,))
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as failed: {path} — {e}", level="error")

def get_size(path):
    """Get the size of a file"""
    return os.path.getsize(path)

def fix_grayscale_icc(path):
    subprocess.run(["magick", "mogrify", "-strip", path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def convert_cmyk_to_rgb(path):
    subprocess.run(["magick", path, "-colorspace", "sRGB", path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def get_mime_type(path):
    return subprocess.getoutput(f"file --mime-type -b \"{path}\"").strip()

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
        img_path.rename(new_path)
        log(f"[blue]   🔧 Corrected extension: {img_path.name} → {new_path.name}")
        return new_path
    return img_path

def convert_single_image(img_path):
    img_path = img_path.resolve()
    mime = get_mime_type(img_path)
    if mime not in ("image/jpeg", "image/png"):
        return 0
    img_path = correct_extension(img_path, mime)
    jxl_path = img_path.with_suffix(".jxl")
    orig_size = get_size(img_path)
    if mime == "image/png":
        fix_grayscale_icc(str(img_path))
    elif mime == "image/jpeg":
        if subprocess.getoutput(f"identify -format '%[colorspace]' \"{img_path}\"").strip() == "CMYK":
            convert_cmyk_to_rgb(str(img_path))
    result = subprocess.run([
        "cjxl", "-d", "0", f"--effort={JXL_EFFORT}", str(img_path), str(jxl_path)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode == 0 and jxl_path.exists():
        os.remove(img_path)
        if VERBOSE:
            log(f"   🖼️  Converted image: {img_path.name} (MIME: {mime})")
        return orig_size - get_size(jxl_path)
    else:
        log(f"[red]❌ Failed to convert: {img_path}", level="error")
        return 0

def convert_images(temp_dir):
    total_saved = 0
    mime_cache = {}
    paths = [p for ext in ('*.jpg', '*.jpeg', '*.png', '*.webp', '*.avif') for p in Path(temp_dir).rglob(ext)]
    
    if not paths:
        return False, total_saved

    # Check if any images are already converted to JXL format
    jxl_paths = [p for p in Path(temp_dir).rglob('*.jxl')]
    if jxl_paths:
        log("   ⚠️ Images are already in JXL format, skipping conversion.")
        return False, total_saved

    # Proceed with converting images
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for path in paths:
            mime = mime_cache.setdefault(str(path), get_mime_type(path))
            if mime in ("image/jpeg", "image/png"):
                futures.append(executor.submit(convert_single_image, path))
        for future in as_completed(futures):
            total_saved += future.result()

    return True, total_saved

def flatten_cbz_archive(cbz_path, temp_dir):
    """Flatten the archive by moving all files to the top level"""
    log(f"   🔄 Flattening {cbz_path}. All nested files will be brought to the top level.")
    with zipfile.ZipFile(cbz_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    original_files = {zi.filename for zi in zip_ref.infolist()}
    for root, dirs, files in os.walk(temp_dir):
        for dir_name in dirs:
            if root != temp_dir:
                for file_name in os.listdir(os.path.join(root, dir_name)):
                    file_path = Path(root) / dir_name / file_name
                    shutil.move(str(file_path), str(temp_dir / file_name))
                shutil.rmtree(os.path.join(root, dir_name))
    return original_files

def process_cbz(cbz_path, conn, fail_conn):
    rel_path = os.path.relpath(cbz_path)
    log(f"\n[bold]📦 {rel_path}[/bold]")
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(cbz_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        for leftover in Path(temp_dir).rglob("*.converted"):
            leftover.unlink()
        original_size = get_size(cbz_path)
        converted, saved_bytes = convert_images(temp_dir)
        if converted:
            if any(file.is_dir() for file in Path(temp_dir).rglob("*")):
                flatten_cbz_archive(cbz_path, temp_dir)
            new_cbz = tempfile.mktemp(suffix=".cbz")
            with zipfile.ZipFile(cbz_path, 'r') as orig_zip:
                original_files = {zi.filename for zi in orig_zip.infolist()}
            with zipfile.ZipFile(new_cbz, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                for file_path in Path(temp_dir).rglob("*"):
                    arcname = file_path.relative_to(temp_dir).as_posix()
                    if arcname in original_files or file_path.suffix == ".jxl":
                        zip_out.write(file_path, arcname)
            shutil.move(new_cbz, cbz_path)
            final_size = get_size(cbz_path)
            reduction_percentage = (saved_bytes / original_size) * 100
            log(f"   ✅ Converted and repacked (Saved: {saved_bytes / (1024 ** 2):.2f} MB) ({reduction_percentage:.2f}% Reduction!)")
            mark_processed(conn, rel_path, original_size, final_size, saved_bytes)
        else:
            log(f"   ⚠️ No convertible images.")
            mark_processed(conn, rel_path, original_size, original_size, 0)
        return saved_bytes
    except Exception as e:
        mark_failed(fail_conn, rel_path)
        log(f"[red]❌ Failed to process archive: {rel_path} — {e}", level="error")
        return 0
    finally:
        shutil.rmtree(temp_dir)

def main():
    global VERBOSE
    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL")
    parser.add_argument("--quiet", action="store_true", help="Suppress console output (only log to file)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    VERBOSE = args.verbose or not args.quiet

    cbz_files = list(Path('.').rglob('*.cbz'))
    total = len(cbz_files)
    total_saved = 0
    converted_count = 0
    skipped_count = 0
    flattened_count = 0
    conn = init_db(DB_FILE)
    fail_conn = init_db(FAILED_DB_FILE)

    log("🛠️ Starting CBZ to JXL conversion...")

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task("[cyan]Processing...", total=total)
        for cbz in cbz_files:
            rel = os.path.relpath(cbz)
            if is_processed(conn, rel):
                skipped_count += 1
                log(f"[yellow]⚠️ Skipping already processed archive: {rel}")
                progress.advance(task)
                continue
            saved = process_cbz(cbz, conn, fail_conn)
            if saved:
                converted_count += 1
                total_saved += saved
            progress.advance(task)

    log("\n🎉 [bold green]Done![/bold green]")
    log(f"   Total archives processed: {total}")
    log(f"   Archives converted:       {converted_count}")
    log(f"   Already processed:        {skipped_count}")
    log(f"   Archives flattened:       {flattened_count}")
    log(f"   Total space saved:        {total_saved / (1024 ** 3):.2f} GB")

if __name__ == "__main__":
    main()
