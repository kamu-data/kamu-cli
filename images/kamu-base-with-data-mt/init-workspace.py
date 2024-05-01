#!/usr/bin/env python

import os
import sys
import subprocess

###############################################################################

# TODO: FIXME: These dual URLs are needed because:
# - AWS CLI can only list the directory using S3 urls
# - Without AWS auth keys `kamu pull` will fail to init AWS SDK, so we switch to plain HTTP instead
S3_REPO_URL = "s3://datasets.kamu.dev/odf/v2/example-mt/"
HTTP_REPO_URL="https://s3.us-west-2.amazonaws.com/datasets.kamu.dev/odf/v2/example/"

###############################################################################

def s3_listdir(url):
    return [
        line.strip().split(' ')[1]
        for line in subprocess.run(
            f"aws --no-sign-request s3 ls {url}",
            shell=True, 
            text=True,
            check=True,
            capture_output=True,
        ).stdout.splitlines()
    ]

def s3_cat(url):
    return subprocess.run(
        f"aws --no-sign-request s3 cp {url} -",
        shell=True, 
        text=True,
        check=True,
        capture_output=True,
    ).stdout.strip()

###############################################################################

subprocess.run(
    "kamu init --multi-tenant --exists-ok", 
    shell=True,
    check=True,
)

for did in s3_listdir(S3_REPO_URL):
    alias = s3_cat(f"{S3_REPO_URL}{did}info/alias")
    url = HTTP_REPO_URL + did
    account, name = alias.split('/', 1)
    subprocess.run(
        f"kamu --account {account} pull --no-alias {url} --as {name}",
        shell=True,
        check=True,
    )
