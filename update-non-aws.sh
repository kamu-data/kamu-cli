echo "Extracting non-AWS dependencies from Cargo.lock..."
NON_AWS_CRATES=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.source != null and (.name | test("^aws-") | not)) | "\(.name)@\(.version)"')

if [ -z "$NON_AWS_CRATES" ]; then
    echo "No non-AWS dependencies found. Exiting."
    exit 0
fi

echo "Found non-AWS crates: $NON_AWS_CRATES"

# Convert to `-p package@version` format
UPDATE_ARGS=$(echo "$NON_AWS_CRATES" | sed 's/^/-p /' | tr '\n' ' ')

echo "Updating non-AWS dependencies..."
cargo update $UPDATE_ARGS

echo "Update complete!"