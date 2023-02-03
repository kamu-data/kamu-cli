#!/bin/sh
# Downloads cached root and derivative datasets from S3 repository (for faster experimetation and testing)
set -e

./init-s3.sh

kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/alberta.case-details.hm"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/british-columbia.case-details.hm"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/canada.case-details"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/canada.daily-cases"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/ontario.case-details.hm"
kamu pull "https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/quebec.case-details.hm"
