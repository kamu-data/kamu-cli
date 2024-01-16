#!/bin/sh
# Downloads cached root datasets from S3 repository for faster experimentation and testing
set -e

S3_BASE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v1/example/"

kamu init || true

kamu pull "${S3_BASE_URL}alberta.case-details"
kamu pull "${S3_BASE_URL}british-columbia.case-details"
kamu pull "${S3_BASE_URL}ontario.case-details"
kamu pull "${S3_BASE_URL}quebec.case-details"
