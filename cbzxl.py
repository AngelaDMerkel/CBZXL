import os
import shutil
import sqlite3
import tempfile
import zipfile
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn

JXL_EFFORT = 8
DB_FILE = "converted_archives.db"
LOG_FILE = "cbz_jxl_conversion.log"
THREADS = 10

console = Console()

def log(msg):
    with open(LOG_FILE, 'a') as f:
        f.write(msg + "\n")
    console.print(msg)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS converted_archives (
                path TEXT PRIMARY KEY
            )
        """)
    return conn

def is_processed(conn, path):
    return conn.execute("SELECT 1 FROM converted_archives WHERE path = ?", (path,)).fetchone() is not None

def mark_processed(conn, path):
    conn.execute("INSERT OR IGNORE INTO converted_archives (path) VALUES (?)", (path,))
    conn.commit()

def get_size(path):
    return os.path.getsize(path)

def fix_grayscale_icc(path):
    subprocess.run(["magick", "mogrify", "-strip", path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def convert_cmyk_to_rgb(path):
    subprocess.run(["magick", path, "-colorspace", "sRGB", path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

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
        log(f"[blue]🔧 Corrected extension: {img_path.name} → {new_path.name}")
        return new_path
    return img_path

def convert_single_image(img_path):
    img_path = img_path.resolve()
    mime = subprocess.getoutput(f"file --mime-type -b \"{img_path}\"").strip()
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
        jxl_size = get_size(jxl_path)
        os.remove(img_path)
        return orig_size - jxl_size
    else:
        log(f"[red]❌ Failed to convert: {img_path}")
        return 0

def convert_images(temp_dir):
    total_saved = 0
    paths = [p for ext in ('*.jpg', '*.jpeg', '*.png', '*.webp', '*.avif') for p in Path(temp_dir).rglob(ext)]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(convert_single_image, path) for path in paths]
        for future in as_completed(futures):
            total_saved += future.result()

    return total_saved > 0, total_saved

def process_cbz(cbz_path, conn):
    rel_path = os.path.relpath(cbz_path)

    if is_processed(conn, rel_path):
        log(f"[yellow]⚠️ Skipping already processed archive: {rel_path}")
        console.print(f"[yellow]⚠️ Skipped: {rel_path} (already processed)")
        return 0

    log(f"\n[bold]📦 {rel_path}[/bold]")
    temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(cbz_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    console.print("   ⏳ Extracted...")

    for leftover in Path(temp_dir).rglob("*.converted"):
        leftover.unlink()

    converted, saved_bytes = convert_images(temp_dir)

    if converted:
        new_cbz = tempfile.mktemp(suffix=".cbz")
        with zipfile.ZipFile(new_cbz, 'w', zipfile.ZIP_DEFLATED) as zip_out:
            for file_path in Path(temp_dir).rglob("*"):
                arcname = file_path.relative_to(temp_dir)
                zip_out.write(file_path, arcname)
        shutil.move(new_cbz, cbz_path)
        console.print(f"   ✅ Converted and repacked (Saved: {saved_bytes / (1024 ** 2):.2f} MB)")
    else:
        console.print("   ⚠️  No convertible images found")

    mark_processed(conn, rel_path)
    shutil.rmtree(temp_dir)
    return saved_bytes

def main():
    cbz_files = list(Path('.').rglob('*.cbz'))
    total = len(cbz_files)
    total_saved = 0
    converted_count = 0
    skipped_count = 0
    conn = init_db()

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
                console.print(f"[yellow]⚠️ Skipped: {rel} (already processed)")
                progress.advance(task)
                continue
            saved = process_cbz(cbz, conn)
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
