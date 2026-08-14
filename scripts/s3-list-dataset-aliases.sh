#!/usr/bin/env bash

set -euo pipefail

BUCKET="${1:-}"

if [ -z "$BUCKET" ]; then
  echo "Usage: $0 <bucket>"
  echo
  echo "Tip: to list buckets, please use:"
  echo "  aws s3 ls"
  exit 1
fi

alias_files=$(
  aws s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --query "Contents[?ends_with(Key, 'alias')].Key" \
    --output text \
    --no-cli-pager \
  | tr '\t' '\n'
)

for key in $alias_files; do
  # NOTE: It's slow, yeah, but works
  aws s3 cp "s3://$BUCKET/$key" -
  echo
done
