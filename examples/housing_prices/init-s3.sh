#!/bin/sh
set -e

kamu init || true

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/example/ca.vancouver.opendata.property.block-outlines"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/example/ca.vancouver.opendata.property.local-area-boundaries"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/example/ca.vancouver.opendata.property.parcel-polygons"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/example/ca.vancouver.opendata.property.tax-reports"
