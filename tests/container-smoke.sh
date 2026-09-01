#!/bin/sh
set -eu

fixture_root="$(mktemp -d)"
container_name="cbzxl-runtime-smoke"
cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  rm -rf "$fixture_root"
}
trap cleanup EXIT INT TERM
mkdir -p "$fixture_root/data" "$fixture_root/config" "$fixture_root/runtime"

python3 - "$fixture_root/data" <<'PY'
import pathlib, struct, sys, zipfile

root = pathlib.Path(sys.argv[1])
width = height = 256
row = bytes((20, 90, 180)) * width
pixels = row * height
header = b"BM" + struct.pack("<IHHI", 54 + len(pixels), 0, 0, 54)
header += struct.pack("<IIIHHIIIIII", 40, width, height, 1, 24, 0, len(pixels), 2835, 2835, 0, 0)
with zipfile.ZipFile(root / "convert.cbz", "w", compression=zipfile.ZIP_STORED) as archive:
    archive.writestr("001.bmp", header + pixels)
with zipfile.ZipFile(root / "preserve-webp.cbz", "w", compression=zipfile.ZIP_STORED) as archive:
    archive.writestr("001.webp", b"RIFF\x10\x00\x00\x00WEBPVP8 ")
PY

docker run -d --rm --name "$container_name" \
  -p 127.0.0.1:55080:5000 \
  -v "$fixture_root/data:/data" \
  -v "$fixture_root/config:/config" \
  -v "$fixture_root/runtime:/runtime" \
  -e ARCHIVE_STABLE_SECONDS=0 \
  cbzxl:ci >/dev/null

attempt=0
until curl -fsS http://127.0.0.1:55080/ready >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 60 ] || { docker logs "$container_name"; exit 1; }
  sleep 1
done
docker exec "$container_name" cbzxl healthcheck

curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:55080/api/v2/jobs >/dev/null

attempt=0
while :; do
  status="$(curl -fsS 'http://127.0.0.1:55080/api/v2/jobs?limit=1' | python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["status"])')"
  case "$status" in
    succeeded|partial|failed|cancelled|interrupted) break ;;
  esac
  attempt=$((attempt + 1))
  [ "$attempt" -lt 120 ] || { docker logs "$container_name"; exit 1; }
  sleep 1
done
[ "$status" = succeeded ] || { docker logs "$container_name"; exit 1; }

python3 - "$fixture_root/data" <<'PY'
import pathlib, sys, zipfile

root = pathlib.Path(sys.argv[1])
with zipfile.ZipFile(root / "convert.cbz") as archive:
    names = archive.namelist()
    assert "001.jxl" in names and "001.bmp" not in names, names
with zipfile.ZipFile(root / "preserve-webp.cbz") as archive:
    assert archive.namelist() == ["001.webp"], archive.namelist()
PY

curl -fsS http://127.0.0.1:55080/health >/dev/null
