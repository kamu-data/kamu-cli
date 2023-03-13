#!/bin/sh
#
# Inputs:
# - KAMU_S3_URL - S3 bucket path which will be scanned for datasets

set -e

datasets=`aws s3 ls $KAMU_S3_URL | awk '{print $2}' | awk -F '/' '/\// {print $1}'`

kamu init || true

for name in $datasets; do
    kamu pull --no-alias "${KAMU_S3_URL}${name}"
done
