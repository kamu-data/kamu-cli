#!/bin/sh
# Downloads cached root datasets from S3 repository (for faster experimetation and testing)
set -e

kamu init || true

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/alberta.case-details"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/british-columbia.case-details"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/ontario.case-details"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/quebec.case-details"
