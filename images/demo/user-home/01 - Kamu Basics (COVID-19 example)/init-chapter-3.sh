#!/bin/sh
set -e

rm -rf .kamu
kamu init

kamu add datasets/british-columbia.case-details.yaml
kamu repo add kamu-hub s3+http://minio/kamu-hub
kamu pull kamu-hub/covid19.ontario.case-details --as ontario.case-details
kamu add datasets/canada.case-details.yaml
kamu add datasets/canada.daily-cases.yaml
kamu pull --all
