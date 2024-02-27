#!/bin/sh
set -e

rm -rf .kamu
kamu init

kamu repo add kamu-hub s3+http://minio/kamu-hub
kamu pull kamu-hub/covid19.british-columbia.case-details --no-alias
kamu pull kamu-hub/covid19.ontario.case-details
kamu pull kamu-hub/covid19.canada.case-details --no-alias
kamu pull kamu-hub/covid19.canada.daily-cases --no-alias
