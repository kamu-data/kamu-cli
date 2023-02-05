#/bin/sh
set -eux


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
examples/covid/init-s3.sh
kamu add -r examples/covid/

examples/reth-vs-snp500/init-s3.sh
kamu add -r examples/reth-vs-snp500/

kamu -v pull --all


# Setup test datasets
get_dataset_id "com.cryptocompare.ohlcv.eth-usd"
dataset_id="$RETVAL"

file="testing/testing.set-transform-input-by-id.yaml"
cp "$file.tpl" "$file"
patch_yaml "$file" ".content.metadata[0].inputs[0].id = \"${dataset_id}\""

kamu add -r testing/
kamu -v pull testing.set-transform-input-by-id
