#!/usr/bin/env bash

set -euo pipefail

# Firstly, check the tag, because it may be missing
VERSION=$(git describe --tags --exact-match | grep -E "^v[0-9]+$")
FEATURE=$(git rev-parse --abbrev-ref HEAD | sed 's/\//-/g')

echo "${FEATURE}-${VERSION}"
