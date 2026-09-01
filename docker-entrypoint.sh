#!/bin/sh
set -eu

runtime_uid="${CBZXL_UID:-0}"
runtime_gid="${CBZXL_GID:-0}"
if [ "$runtime_uid" != 0 ] || [ "$runtime_gid" != 0 ]; then
  mkdir -p /config /runtime /dev/shm/cbzxl
  chown -R "$runtime_uid:$runtime_gid" /config /runtime /dev/shm/cbzxl
  exec gosu "$runtime_uid:$runtime_gid" cbzxl "$@"
fi
exec cbzxl "$@"
