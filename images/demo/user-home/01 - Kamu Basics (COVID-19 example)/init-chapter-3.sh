#!/bin/sh
set -e

rm -rf .kamu
kamu init

kamu repo add kamu-node "${KAMU_NODE_URL}"
kamu pull kamu-node/kamu/covid19.british-columbia.case-details --no-alias
kamu pull kamu-node/kamu/covid19.ontario.case-details
kamu add datasets/canada*.yaml
kamu pull --all
