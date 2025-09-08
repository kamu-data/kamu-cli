#!/usr/bin/env python

import shutil
import subprocess
from pathlib import Path

###############################################################################

CURRENT_PATH = Path(__file__).resolve().parent
S3_REPO_URL = "s3://datasets.kamu.dev/odf/v2/example-mt/"


###############################################################################

def s3_listdir(url):
    return [
        line.strip().split(' ')[1]
        for line in subprocess.run(
            f"aws s3 ls {url}",
            shell=True,
            text=True,
            check=True,
            capture_output=True,
        ).stdout.splitlines()
    ]


def s3_cat(url):
    return subprocess.run(
        f"aws s3 cp {url} -",
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

shutil.copy(CURRENT_PATH / "extra/.kamuconfig", ".kamuconfig")

for did in s3_listdir(S3_REPO_URL):
    url = S3_REPO_URL + did
    alias = s3_cat(f"{S3_REPO_URL}{did}info/alias")
    account, name = alias.split('/', 1)

    subprocess.run(
        f"kamu --account {account} pull --no-alias {url} --as {name} --visibility public",
        shell=True,
        check=True,
    )
