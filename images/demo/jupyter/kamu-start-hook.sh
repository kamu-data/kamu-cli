#!/bin/bash

set -eo pipefail

# Generate Kamu Node auth token if GitHub access token is provided
if [ -n "${GITHUB_TOKEN}" ] && [ -n "${GITHUB_LOGIN}" ] && [ -n "${KAMU_JWT_SECRET}" ] && [ -n "${KAMU_NODE_URL}" ]; then
    kamu_token=$(kamu system generate-token --gh-login ${GITHUB_LOGIN} --gh-access-token ${GITHUB_TOKEN})
    kamu login --user --access-token "${kamu_token}" "${KAMU_NODE_URL#odf+}"
fi

# Patch notebooks with node and frontend URLs
find . -type f -name '*.ipynb' -print0 | while read -d $'\0' file
do
    if [ -n "${GITHUB_LOGIN}" ]; then
        sed -i "s|\${GITHUB_LOGIN}|${GITHUB_LOGIN}|g" "$file"
    fi
    if [ -n "${KAMU_NODE_URL}" ]; then
        sed -i "s|\${KAMU_NODE_URL}|${KAMU_NODE_URL}|g" "$file"
    fi
    if [ -n "${KAMU_WEB_UI_URL}" ]; then
        sed -i "s|\${KAMU_WEB_UI_URL}|${KAMU_WEB_UI_URL}|g" "$file"
    fi
done
