#!/bin/sh
set -eu

case "${CBZXL_PROCESS:-web}" in
  worker)
    exec python -m cbzxl_web.worker
    ;;
  web)
    exec uvicorn app:app --host 0.0.0.0 --port 5000
    ;;
  *)
    echo "CBZXL_PROCESS must be 'web' or 'worker'" >&2
    exit 2
    ;;
esac
