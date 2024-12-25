#!/bin/sh
set -e

KAMU_NODE_URL="odf+https://node.demo.kamu.dev/"

kamu init --multi-tenant --exists-ok
cp -f .kamuconfig .kamu/

kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/stations"
kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/measurements.boven-rijn"
kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/measurements.ijssel"
kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/measurements.lek"
kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/measurements.nederrijn"
kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/measurements.pannerdensch-kanaal"
kamu --account rijkswaterstaat.nl pull "${KAMU_NODE_URL}rijkswaterstaat.nl/measurements.waal"
kamu --account deltares.nl pull "${KAMU_NODE_URL}deltares.nl/rhine-basin.netherlands"
kamu --account deltares.nl pull "${KAMU_NODE_URL}deltares.nl/rhine-basin.netherlands.sim"
