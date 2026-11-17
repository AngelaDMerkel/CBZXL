#!/bin/sh
set -eu

image="${1:-cbzxl:ci}"
runtime_uid="${SMOKE_UID:-0}"
fixture_root="$(mktemp -d)"
container_name="cbzxl-smoke-$(basename "$fixture_root" | tr '[:upper:]' '[:lower:]')"
token="container-smoke-test-token"
cleanup() {
  result=$?
  if [ "$result" -ne 0 ]; then
    docker logs "$container_name" >&2 || true
  fi
  docker rm -fv "$container_name" >/dev/null 2>&1 || true
  # The container may have created files owned by the runtime UID.
  docker run --rm --entrypoint sh -v "$fixture_root:/fixture" "$image" \
    -c 'rm -rf /fixture/config /fixture/runtime' >/dev/null 2>&1 || true
  rm -rf "$fixture_root"
}
trap cleanup EXIT
trap 'exit 1' INT TERM
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

# Only disposable test fixtures are made writable by the numeric test user.
chmod -R a+rwX "$fixture_root"

docker run --rm "$image" doctor
docker run --rm -e CBZXL_UID=65532 -e CBZXL_GID=65532 "$image" doctor

start_container() {
  docker run -d --name "$container_name" \
  -p 127.0.0.1::5000 \
  --shm-size=256m --security-opt no-new-privileges:true \
  --cap-drop ALL --cap-add CHOWN --cap-add SETUID --cap-add SETGID --cap-add DAC_OVERRIDE \
  -v "$fixture_root/data:/data" \
  -v "$fixture_root/config:/config" \
  -v "$fixture_root/runtime:/runtime" \
  -e ARCHIVE_STABLE_SECONDS=0 \
  -e CBZXL_API_TOKEN="$token" \
  -e CBZXL_UID="$runtime_uid" -e CBZXL_GID="$runtime_uid" \
  "$image" >/dev/null
  base_url="http://$(docker port "$container_name" 5000/tcp)"

  attempt=0
  until curl -fsS "$base_url/ready" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 90 ] || exit 1
    sleep 1
  done
  docker exec "$container_name" cbzxl healthcheck
  actual_uid="$(docker exec "$container_name" sh -c "awk '/^Uid:/ {print \$2}' /proc/1/status")"
  [ "$actual_uid" = "$runtime_uid" ]
}
start_container

# Exercise the bundled frontend and a real hashed JS asset, not just API readiness.
curl -fsS "$base_url/" > "$fixture_root/index.html"
asset="$(python3 - "$fixture_root/index.html" <<'PY'
import pathlib, re, sys
html = pathlib.Path(sys.argv[1]).read_text()
assert '<div id="root">' in html
print(re.search(r'src="(/assets/[^\"]+\.js)"', html).group(1))
PY
)"
curl -fsS "$base_url$asset" > /dev/null
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$base_url/api/v2/jobs")" = 401 ]

curl -fsS -X POST -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' -d '{}' "$base_url/api/v2/jobs" >/dev/null

attempt=0
while :; do
  status="$(curl -fsS -H "Authorization: Bearer $token" "$base_url/api/v2/jobs?limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["status"])')"
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
    assert archive.testzip() is None
    assert archive.read("001.jxl").startswith((b"\xff\x0a", b"\x00\x00\x00\x0cJXL \r\n\x87\n"))
with zipfile.ZipFile(root / "preserve-webp.cbz") as archive:
    assert archive.namelist() == ["001.webp"], archive.namelist()
    assert archive.read("001.webp") == b"RIFF\x10\x00\x00\x00WEBPVP8 "
PY

curl -fsS "$base_url/health" >/dev/null

# Recreate the container against the same persistent storage and check history.
docker rm -fv "$container_name" >/dev/null
start_container
curl -fsS -H "Authorization: Bearer $token" "$base_url/api/v2/jobs?limit=1" \
  | python3 -c 'import json,sys; rows=json.load(sys.stdin)["rows"]; assert len(rows) == 1 and rows[0]["status"] == "succeeded", rows'
printf 'Runtime smoke passed: %s (UID %s)\n' "$image" "$runtime_uid"
