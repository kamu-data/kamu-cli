#!/bin/sh
set -e

KAMU_NODE_URL="odf+https://node.demo.kamu.dev/"

kamu init --multi-tenant --exists-ok
cp -f .kamuconfig .kamu/

kamu --account acme.fishing.co pull "${KAMU_NODE_URL}acme.fishing.co/vessels.gps"
kamu --account acme.fishing.co pull "${KAMU_NODE_URL}acme.fishing.co/vessels.trawl"
kamu --account acme.fishing.co pull "${KAMU_NODE_URL}acme.fishing.co/vessels.fuel"
kamu --account acme.fishing.co pull "${KAMU_NODE_URL}acme.fishing.co/vessels.location-annotated"
kamu --account globalfishingwatch.org pull "${KAMU_NODE_URL}globalfishingwatch.org/protected-areas"