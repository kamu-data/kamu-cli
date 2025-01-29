#!/usr/bin/env bash

set -euo pipefail

kamu init --multi-tenant --exists-ok
cp -f .kamuconfig .kamu/

(
    cd ./snapshots/alice


    kamu --account alice add --visibility public 1pub2.yaml
    kamu --account alice add --visibility private 1priv2.yaml
    kamu --account alice add --visibility public 2pub.yaml
    kamu --account alice add --visibility public 1pub1.yaml
    kamu --account alice add --visibility private 1priv1.yaml
    kamu --account alice add --visibility private 2priv.yaml
    kamu --account alice add --visibility public 3pub.yaml
    kamu --account alice add --visibility private 4priv.yaml
    kamu --account alice add --visibility public 4pub.yaml
    kamu --account alice add --visibility private 5priv1.yaml
    kamu --account alice add --visibility private 5priv2.yaml
    kamu --account alice add --visibility public 5pub1.yaml
    kamu --account alice add --visibility public 5pub2.yaml

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
