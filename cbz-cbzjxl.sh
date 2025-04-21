#!/usr/bin/env bash

set -euo pipefail

# Parameters
LOG_FILE="cbz_jxl_conversion.log"
JXL_THREADS=10
TEMP_DIR_ROOT=$(mktemp -d)
JXL_EFFORT=8

# Logs
log_msg() {
    echo "$1"
    echo "$1" >> "$LOG_FILE"
}

# Skip previously processed
is_processed() {
    grep -Fxq "$1" "$LOG_FILE" 2>/dev/null
}

# Storage Savings
total_original_size=0
total_final_size=0

# Process .cbz
find . -type f -name "*.cbz" | while read -r cbz; do
    rel_cbz="${cbz#./}"
    if is_processed "$rel_cbz"; then
        log_msg "✅ Already processed: $rel_cbz"
        continue
    fi

    log_msg "📦 Processing: $rel_cbz"

    # Store original size for space saving calculation
    original_size=$(stat -f%z "$cbz")
    total_original_size=$((total_original_size + original_size))

    # Create temp dir and unzip
    TEMP_DIR=$(mktemp -d -p "$TEMP_DIR_ROOT")
    unzip -q "$cbz" -d "$TEMP_DIR"

    # Fix extensions based on actual MIME type
    find "$TEMP_DIR" -type f \( -iname "*.jpg" -o -iname "*.jpeg" -o -iname "*.png" \) | while read -r img; do
        actual_type=$(file --mime-type -b "$img")

        case "$actual_type" in
            image/webp)
                new_name="${img%.*}.webp"
                if [ "$img" != "$new_name" ]; then
                    mv "$img" "$new_name"
                    log_msg "🔁 Renamed WebP file with wrong extension: $img → $new_name"
                fi
                ;;
            image/jpeg|image/png)
                # Correct, no action needed
                ;;
            *)
                log_msg "⚠️ Unknown file type ($actual_type): $img"
                ;;
        esac
    done

    # Convert eligible images to JXL
    export JXL_EFFORT
    export TEMP_DIR
    find "$TEMP_DIR" -type f \( -iname "*.jpg" -o -iname "*.jpeg" -o -iname "*.png" \) ! -iname "*.gif" ! -iname "*.apng" ! -iname "*.avif" ! -iname "*.webp" ! -iname "*.jxl" | while read -r img; do
        jxl_path="${img%.*}.jxl"
        if cjxl --effort=$JXL_EFFORT "$img" "$jxl_path" >/dev/null 2>&1; then
            rm -f "$img"
            log_msg "🖼️ Converted: $img → $jxl_path"
        else
            log_msg "❌ Failed to convert: $img"
        fi
    done

    # Repack the CBZ
    NEW_CBZ="$(mktemp -p "$TEMP_DIR_ROOT" tmp.XXXXXX).cbz"
    if (cd "$TEMP_DIR" && zip -qr "$NEW_CBZ" .); then
        mv "$NEW_CBZ" "$cbz"
        log_msg "✅ Repacked: $rel_cbz"
    else
        log_msg "❌ Failed to rezip: $rel_cbz"
    fi

    # Store final size for space saving calculation
    new_cbz_size=$(stat -f%z "$cbz")
    total_final_size=$((total_final_size + new_cbz_size))

    # Clean up
    rm -rf "$TEMP_DIR"
    echo "$rel_cbz" >> "$LOG_FILE"
done

# Calculate and print saved space
saved_bytes=$((total_original_size - total_final_size))
saved_gb=$(echo "scale=2; $saved_bytes / (1024^3)" | bc)
log_msg "✅ Total space saved: $saved_gb GB"

# Final cleanup
rm -rf "$TEMP_DIR_ROOT"
log_msg "🎉 Done!"
