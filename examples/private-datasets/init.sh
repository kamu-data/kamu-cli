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
)

(
    cd ./snapshots/bob

    kamu --account bob add --visibility public 6pub1.yaml
    kamu --account bob add --visibility public 6pub2.yaml
)
