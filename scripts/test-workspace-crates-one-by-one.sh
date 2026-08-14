#!/usr/bin/env bash

set -euo pipefail

# Usage: ./test-workspace-crates-one-by-one.sh [--start-from crate]

start_from=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --start-from)
            start_from="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

crates=$(cargo metadata --no-deps --format-version=1 | jq -r '.packages[].name')

started=1
if [[ -n "$start_from" ]]; then
    started=0
fi

for crate in $crates; do
  if [[ $started -eq 0 ]]; then
    if [[ "$crate" == "$start_from" ]]; then
        started=1
    else
        continue
    fi
  fi

  echo
  echo "==> Testing crate: $crate"
  echo
  if ! cargo nextest run -p "$crate" --no-tests=warn; then
    echo
    echo "==> FAILED: $crate"
    echo

    exit 1
  fi
done

echo
echo "All crates passed."
