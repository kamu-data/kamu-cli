#!/bin/sh
set -e

rm -rf .kamu
kamu init
kamu add datasets/british-columbia.case-details.yaml
kamu pull --all
kamu repo add kamu-node "${KAMU_NODE_URL}"