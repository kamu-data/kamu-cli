#/bin/sh
set -eux

DATASETS_DIR=/tmp/datasets
#DATASETS_DIR=.


get_dataset_id() {
    local name=$1
    RETVAL="$(kamu list --wide --output-format json | jq -r ".[] | select(.Name == \"${name}\") | .ID")"
}

patch_yaml() {
    local name=$1
    local patch=$2
    cat "$name" | yq "$patch" > "${name}.tmp"
    mv "${name}.tmp" "$name"
}


# Init example datasets
$DATASETS_DIR/examples/covid/init-s3.sh
kamu add -r $DATASETS_DIR/examples/covid/

$DATASETS_DIR/examples/reth-vs-snp500/init-s3.sh
kamu add -r $DATASETS_DIR/examples/reth-vs-snp500/

kamu pull --all


# Setup test datasets
get_dataset_id "com.cryptocompare.ohlcv.eth-usd"
dataset_id="$RETVAL"

file="${DATASETS_DIR}/testing/testing.set-transform-input-by-id.yaml"
cp "$file.tpl" "$file"
patch_yaml "$file" ".content.metadata[0].inputs[0].id = \"${dataset_id}\""

kamu pull testing.set-transform-input-by-id
