#!/bin/sh
set -e

S3_EXAMPLE_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/example/"

kamu init || true

kamu pull "${S3_EXAMPLE_URL}ca.vancouver.opendata.property.block-outlines"
kamu pull "${S3_EXAMPLE_URL}ca.vancouver.opendata.property.local-area-boundaries"
kamu pull "${S3_EXAMPLE_URL}ca.vancouver.opendata.property.parcel-polygons"
kamu pull "${S3_EXAMPLE_URL}ca.vancouver.opendata.property.tax-reports"
