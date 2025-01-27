#!/usr/bin/env bash

set -euo pipefail

kamu init --multi-tenant --exists-ok
cp -f .kamuconfig .kamu/

(
    cd ./snapshots/alice

    kamu --account alice add --visibility public public-root-dataset.yaml
    kamu --account alice add --visibility private private-root-dataset.yaml
    kamu --account alice add --visibility public public-derivative-dataset.yaml

    kamu --account alice pull public-root-dataset
    kamu --account alice pull private-root-dataset
    kamu --account alice pull public-derivative-dataset
)

(
    cd ./snapshots/bob

    kamu --account bob add --visibility private private-derivative-dataset.yaml
)
