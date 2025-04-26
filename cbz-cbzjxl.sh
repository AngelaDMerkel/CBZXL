#!/usr/bin/env bash
set -euo pipefail

# Config
LOG_FILE="cbz_jxl_conversion.log"
JXL_THREADS=10
JXL_EFFORT=8
TEMP_DIR_ROOT=$(mktemp -d /tmp/cbzjxl.XXXXXX)
DB_FILE="converted_archives.db"  # SQLite DB for tracking processed archives

# Global counters
total_cbz_files=$(find . -type f -name "*.cbz" | wc -l)
processed_cbz_count=0
total_original=0
total_final=0
total_saved_bytes=0

# SQLite helper function to escape paths
sqlite3_escape() {
    echo "$1" | sed "s/'/''/g"
}

# Initialize SQLite database and create an index
init_db() {
    if [ ! -f "$DB_FILE" ]; then
        sqlite3 "$DB_FILE" <<EOF
CREATE TABLE converted_archives (
    path TEXT PRIMARY KEY
);

-- Add an index to speed up searches
CREATE INDEX idx_path ON converted_archives (path);
EOF
    fi
}

# Check if a CBZ has already been processed (using SQLite)
is_processed() {
    result=$(sqlite3 "$DB_FILE" "SELECT 1 FROM converted_archives WHERE path = '$(sqlite3_escape "$1")';")
    [ "$result" = "1" ]
}

# Mark a CBZ as processed
mark_processed() {
    sqlite3 "$DB_FILE" "INSERT INTO converted_archives (path) VALUES ('$(sqlite3_escape "$1")');"
}

# Remove a CBZ entry from the database if the file doesn't exist
remove_deleted_from_db() {
    sqlite3 "$DB_FILE" "DELETE FROM converted_archives WHERE path = '$(sqlite3_escape "$1")';"
}

# Get the size of a file
get_size() {
    stat -f%z "$1"
}

# Logging function
log_msg() { 
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

# Update progress bar in terminal
update_progress() {
  local progress="$((processed_cbz_count * 100 / total_cbz_files))"
  echo -ne "\rProgress: ${progress}% (${processed_cbz_count}/${total_cbz_files}) - Saved: $(echo "scale=2; $total_saved_bytes / (1024^3)" | bc) GB"
}

# Clean up any leftover .converted files in the temp directory
clean_converted_markers() {
  find "$1" -type f -name "*.converted" -exec rm {} \; && log_msg "🧹 Cleaned up leftover .converted files in $1"
}

# Initialize temporary directory and SQLite DB
log_msg "🛠️ Starting CBZ to JXL conversion..."
init_db

# Periodically remove any deleted archives from the database
cleanup_db() {
    find . -type f -name "*.cbz" | while IFS= read -r cbz; do
        rel_cbz="${cbz#./}"
        if ! [ -e "$cbz" ]; then
            log_msg "🧹 Removing deleted archive from DB: $rel_cbz"
            remove_deleted_from_db "$rel_cbz"
        fi
    done
}

# Periodic cleanup every 500 files
find . -type f -name "*.cbz" | while IFS= read -r cbz; do
  processed_cbz_count=$((processed_cbz_count + 1))
  rel_cbz="${cbz#./}"
  update_progress

  # Periodic cleanup after every 500 files
  if (( processed_cbz_count % 500 == 0 )); then
    log_msg "🧹 Periodic database cleanup at $processed_cbz_count files"
    cleanup_db
  fi

  if is_processed "$rel_cbz"; then
    log_msg "✅ Previously processed: $rel_cbz"
    continue
  fi

  log_msg "📦 Processing: $rel_cbz"
  original_size=$(get_size "$cbz")
  TEMP_DIR=$(mktemp -d -p "$TEMP_DIR_ROOT")
  unzip -q "$cbz" -d "$TEMP_DIR"

  # Clean up any leftover .converted files immediately
  clean_converted_markers "$TEMP_DIR"
    
  # Fix extensions based on MIME
  find "$TEMP_DIR" -type f \( -iname "*.jpg" -o -iname "*.jpeg" -o -iname "*.png" \) | while IFS= read -r img; do
    case "$(file --mime-type -b "$img")" in
      image/webp)
        new="${img%.*}.webp"
        [ "$img" != "$new" ] && mv "$img" "$new" && log_msg "🔁 Renamed: $img → $new"
        ;;
      image/jpeg|image/png) : ;;
      *) log_msg "⚠️ Unknown type: $img" ;;
    esac
  done

  # Export vars for parallel jobs
  export JXL_EFFORT LOG_FILE

  # Create a file to track if anything was converted
  touch "$TEMP_DIR/.converted_flag"

  find "$TEMP_DIR" -type f \( -iname "*.jpg" -o -iname "*.jpeg" -o -iname "*.png" \) \
    ! -iname "*.gif" ! -iname "*.apng" ! -iname "*.avif" ! -iname "*.webp" ! -iname "*.jxl" -print0 |
    xargs -0 -n1 -P "$JXL_THREADS" -I{} bash -c '
      img="$1"
      jxl="${img%.*}.jxl"
      original_size=$(stat -f%z "$img" 2>/dev/null || echo 0)
      if cjxl -d 0 --effort=$JXL_EFFORT "$img" "$jxl" >/dev/null 2>&1; then
        rm "$img"
        printf "%s 🖼️ Converted: %s → %s\n" "$(date "+%Y-%m-%d %H:%M:%S")" "$img" "$jxl" >> "$LOG_FILE"
        touch "$img.converted"
        echo "$original_size"
      else
        printf "%s ❌ Failed: %s\n" "$(date "+%Y-%m-%d %H:%M:%S")" "$img" >> "$LOG_FILE"
        echo 0
      fi
    ' _ {} | while IFS= read -r bytes_saved; do
      total_saved_bytes=$((total_saved_bytes + bytes_saved))
    done

  # Clean up any leftover .converted files before repacking
  clean_converted_markers "$TEMP_DIR"

  # Check if any conversions happened and repack
  if find "$TEMP_DIR" -type f -name "*.converted" | grep -q .; then
    new_cbz=$(mktemp -p "$TEMP_DIR_ROOT" tmp.XXXXXX).cbz
    (cd "$TEMP_DIR" && zip -qr "$new_cbz" . -x "*.converted") && mv "$new_cbz" "$cbz"
    new_size=$(get_size "$cbz")
    total_original=$((total_original + original_size))
    total_final=$((total_final + new_size))
    mark_processed "$rel_cbz"
    log_msg "🖼️ Converted and repacked: $rel_cbz"
  else
    log_msg "ℹ️ Skipped (no conversions): $rel_cbz"
    mark_processed "$rel_cbz"
  fi

  rm -rf "$TEMP_DIR"
done

# Final cleanup and message
log_msg "🎉 Done! Total space saved: $(echo "scale=2; $total_saved_bytes / (1024^3)" | bc) GB"
echo "" # Add a newline at the end for cleaner terminal output
