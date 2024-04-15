#!/bin/bash
set -euo pipefail

kamu init --exists-ok
kamu add . -r

for file in `ls ./data/ | sort -g`; do
    echo "kamu ingest player-scores data/$file"
    kamu ingest player-scores data/$file

    echo "kamu pull leaderboard"
    kamu pull leaderboard
done
