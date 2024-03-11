#!/bin/sh
# Downloads cached root and derivative datasets from S3 repository for faster experimentation and testing
set -e

S3_BASE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/example/"

kamu init || true

# Root
kamu pull "${S3_BASE_URL}covid19.alberta.case-details"
kamu pull "${S3_BASE_URL}covid19.british-columbia.case-details"
kamu pull "${S3_BASE_URL}covid19.ontario.case-details"
kamu pull "${S3_BASE_URL}covid19.quebec.case-details"

# Derivative
kamu pull "${S3_BASE_URL}covid19.alberta.case-details.hm" --no-alias
kamu pull "${S3_BASE_URL}covid19.british-columbia.case-details.hm" --no-alias
kamu pull "${S3_BASE_URL}covid19.ontario.case-details.hm" --no-alias
kamu pull "${S3_BASE_URL}covid19.quebec.case-details.hm" --no-alias

kamu pull "${S3_BASE_URL}covid19.canada.case-details" --no-alias
kamu pull "${S3_BASE_URL}covid19.canada.daily-cases" --no-alias
