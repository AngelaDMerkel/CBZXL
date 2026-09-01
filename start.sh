#!/bin/sh
set -eu

mode="${CBZXL_PROCESS:-run}"
case "$mode" in
  run|serve|worker) exec cargo run --locked -- "$mode" ;;
  *) echo "CBZXL_PROCESS must be run, serve, or worker" >&2; exit 2 ;;
esac
