import os
import shutil
import sqlite3
import tempfile
import zipfile
import subprocess
import argparse
from pathlib import Path
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
        conn.execute("CREATE TABLE IF NOT EXISTS converted_archives (path TEXT PRIMARY KEY)")
    return conn

def is_processed(conn, path):
    """Check if an archive has been processed"""
    return conn.execute("SELECT 1 FROM converted_archives WHERE path = ?", (path,)).fetchone() is not None

def mark_processed(conn, path):
    """Mark an archive as processed"""
    try:
        conn.execute("INSERT OR IGNORE INTO converted_archives (path) VALUES (?)", (path,))
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
    """Fix grayscale ICC profile in PNG images"""
    subprocess.run(["magick", "mogrify", "-strip", path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def convert_cmyk_to_rgb(path):
    """Convert CMYK images to RGB using ImageMagick"""
    subprocess.run(["magick", path, "-colorspace", "sRGB", path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def get_mime_type(path):
    """Get the MIME type of a file"""
    return subprocess.getoutput(f"file --mime-type -b \"{path}\"").strip()

def correct_extension(img_path, mime):
    """Correct the file extension based on MIME type"""
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
        log(f"[blue]🔧 Corrected extension: {img_path.name} → {new_path.name}")
        return new_path
    return img_path

def convert_single_image(img_path):
    """Convert a single image to JPEG XL"""
    img_path = img_path.resolve()
    mime = get_mime_type(img_path)
    
    # Only process jpeg and png images
    if mime not in ("image/jpeg", "image/png"):
        return 0

    # Correct the file extension based on MIME type
    img_path = correct_extension(img_path, mime)
    jxl_path = img_path.with_suffix(".jxl")
    orig_size = get_size(img_path)

    # Handle color profiles (grayscale and CMYK)
    if mime == "image/png":
        fix_grayscale_icc(str(img_path))
    elif mime == "image/jpeg":
        if subprocess.getoutput(f"identify -format '%[colorspace]' \"{img_path}\"").strip() == "CMYK":
            convert_cmyk_to_rgb(str(img_path))

    # Convert to JPEG XL
    result = subprocess.run([
        "cjxl", "-d", "0", f"--effort={JXL_EFFORT}", str(img_path), str(jxl_path)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Check if the conversion was successful
    if result.returncode == 0 and jxl_path.exists():
        os.remove(img_path)
        if VERBOSE:
            log(f"   🖼️  Converted image: {img_path.name} (MIME: {mime})")
        return orig_size - get_size(jxl_path)
    else:
        log(f"[red]❌ Failed to convert: {img_path}", level="error")
        return 0

def convert_images(temp_dir):
    """Convert all images in a temporary directory"""
    total_saved = 0
    mime_cache = {}
    paths = [p for ext in ('*.jpg', '*.jpeg', '*.png', '*.webp', '*.avif') for p in Path(temp_dir).rglob(ext)]

    # If no image files found
    if not paths:
        log("   ⚠️  No image files found to convert")
        return False, total_saved

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for path in paths:
            mime = mime_cache.setdefault(str(path), get_mime_type(path))
            if mime in ("image/jpeg", "image/png"):
                futures.append(executor.submit(convert_single_image, path))

        for future in as_completed(futures):
            total_saved += future.result()

    return total_saved > 0, total_saved

def process_cbz(cbz_path, conn, fail_conn):
    """Process a single CBZ archive"""
    rel_path = os.path.relpath(cbz_path)

    # Skip already processed archives
    if is_processed(conn, rel_path):
        log(f"[yellow]⚠️ Skipping already processed archive: {rel_path}")
        return 0

    log(f"\n[bold]📦 {rel_path}[/bold]")
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(cbz_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Clean up leftover files
        for leftover in Path(temp_dir).rglob("*.converted"):
            leftover.unlink()

        # Convert images and calculate savings
        converted, saved_bytes = convert_images(temp_dir)

        if converted:
            # Create new CBZ file with converted images
            new_cbz = tempfile.mktemp(suffix=".cbz")
            with zipfile.ZipFile(cbz_path, 'r') as orig_zip:
                original_files = {zi.filename for zi in orig_zip.infolist()}

            with zipfile.ZipFile(new_cbz, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                for file_path in Path(temp_dir).rglob("*"):
                    arcname = file_path.relative_to(temp_dir).as_posix()
                    if arcname in original_files or file_path.suffix == ".jxl":
                        zip_out.write(file_path, arcname)

            shutil.move(new_cbz, cbz_path)
            reduction_percentage = (saved_bytes / get_size(cbz_path)) * 100
            log(f"   ✅ Converted and repacked (Saved: {saved_bytes / (1024 ** 2):.2f} MB) ({reduction_percentage:.2f}% Reduction!)")
        else:
            log("   ⚠️  No convertible images found")

        mark_processed(conn, rel_path)
        return saved_bytes

    except Exception as e:
        mark_failed(fail_conn, rel_path)
        log(f"[red]❌ Failed to process archive: {rel_path} — {e}", level="error")
        return 0

    finally:
        shutil.rmtree(temp_dir)

def main():
    """Main function to start the CBZ to JXL conversion process"""
    global VERBOSE
    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL")
    parser.add_argument("--quiet", action="store_true", help="Suppress console output (only log to file)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    VERBOSE = args.verbose

    cbz_files = list(Path('.').rglob('*.cbz'))
    total = len(cbz_files)
    total_saved = 0
    converted_count = 0
    skipped_count = 0
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
    log(f"   Total space saved:        {total_saved / (1024 ** 3):.2f} GB")

if __name__ == "__main__":
    main()
