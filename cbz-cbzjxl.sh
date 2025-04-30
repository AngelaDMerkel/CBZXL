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
CREATE INDEX idx_path ON converted_archives (path);
EOF
    fi
}

is_processed() {
    result=$(sqlite3 "$DB_FILE" "SELECT 1 FROM converted_archives WHERE path = '$(sqlite3_escape "$1")';")
    [ "$result" = "1" ]
}

mark_processed() {
    sqlite3 "$DB_FILE" "INSERT INTO converted_archives (path) VALUES ('$(sqlite3_escape "$1")');"
}

remove_deleted_from_db() {
    sqlite3 "$DB_FILE" "DELETE FROM converted_archives WHERE path = '$(sqlite3_escape "$1")';"
}

get_size() {
    stat -f%z "$1"
}

log_msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

update_progress() {
    local progress="$((processed_cbz_count * 100 / total_cbz_files))"
    echo -ne "\rProgress: ${progress}% (${processed_cbz_count}/${total_cbz_files}) - Saved: $(echo "scale=2; $total_saved_bytes / (1024^3)" | bc) GB"
}

init_db
log_msg "🛠️ Starting CBZ to JXL conversion..."

cleanup_db() {
    find . -type f -name "*.cbz" | while IFS= read -r cbz; do
        rel_cbz="${cbz#./}"
        if ! [ -e "$cbz" ]; then
            log_msg "🧹 Removing deleted archive from DB: $rel_cbz"
            remove_deleted_from_db "$rel_cbz"
        fi
    done
}

count=0
find . -type f -name "*.cbz" | while IFS= read -r cbz; do
    processed_cbz_count=$((processed_cbz_count + 1))
    count=$((count + 1))
    rel_cbz="${cbz#./}"
    update_progress

    if is_processed "$rel_cbz"; then
        log_msg "✅ Previously processed: $rel_cbz"
        continue
    fi

    if (( count % 500 == 0 )); then
        cleanup_db
    fi

    log_msg "📦 Processing: $rel_cbz"
    original_size=$(get_size "$cbz")
    TEMP_DIR=$(mktemp -d -p "$TEMP_DIR_ROOT")
    unzip -q "$cbz" -d "$TEMP_DIR"

    # Remove any pre-existing .converted files
    find "$TEMP_DIR" -type f -name "*.converted" -delete

    # Correct extensions and detect file types
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

    export JXL_EFFORT LOG_FILE
    converted_flag="$TEMP_DIR/.converted"

    find "$TEMP_DIR" -type f \( -iname "*.jpg" -o -iname "*.jpeg" -o -iname "*.png" \) \
        ! -iname "*.gif" ! -iname "*.apng" ! -iname "*.avif" ! -iname "*.webp" ! -iname "*.jxl" -print0 |
    while IFS= read -r -d '' img; do
        jxl="${img%.*}.jxl"

        # Strip ICC profiles or metadata that may confuse the JXL encoder
        if [[ "$img" =~ \.png$ ]]; then
            magick "$img" -strip "$img" 2>/dev/null || true
        fi

        original_img_size=$(stat -f%z "$img" 2>/dev/null || echo 0)
        if cjxl -d 0 --effort=$JXL_EFFORT "$img" "$jxl" >/dev/null 2>&1; then
            rm "$img"
            touch "$converted_flag"
            total_saved_bytes=$((total_saved_bytes + original_img_size))
            log_msg "🖼️ Converted: $img → $jxl"
        else
            log_msg "❌ Failed: $img"
        fi
    done

    if [ -f "$converted_flag" ]; then
        new_cbz=$(mktemp -p "$TEMP_DIR_ROOT" tmp.XXXXXX).cbz
        (cd "$TEMP_DIR" && zip -qr "$new_cbz" .)
        mv "$new_cbz" "$cbz"
        new_size=$(get_size "$cbz")
        total_original=$((total_original + original_size))
        total_final=$((total_final + new_size))
        log_msg "📦 Repacked: $rel_cbz"
    else
        log_msg "ℹ️ Skipped (no conversions): $rel_cbz"
    fi

    mark_processed "$rel_cbz"
    rm -rf "$TEMP_DIR"
done

log_msg "🎉 Done! Total space saved: $(echo "scale=2; $total_saved_bytes / (1024^3)" | bc) GB"
echo ""
