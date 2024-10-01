#!/usr/bin/env bash
# Downloads cached root datasets from ODF node for faster experimentation and testing
set -e

BASE_URL="odf+https://node.demo.kamu.dev/kamu/"

kamu init || true

kamu pull "${BASE_URL}covid19.alberta.case-details"
kamu pull "${BASE_URL}covid19.british-columbia.case-details"
kamu pull "${BASE_URL}covid19.ontario.case-details"
kamu pull "${BASE_URL}covid19.quebec.case-details"

kamu add -r .
