# datafusion-cli

This crate is heavily based on `datafusion-cli` in the https://github.com/apache/arrow-datafusion/ repository and therefore is licensed under the same `Apache 2.0` license.

We decided to copy the code instead of using the existing `datafusion-cli` crate because:
- It was hard to align the dependency versions between our and `arrow-datafusion` repo
- The crate comes with more dependencies than we actually need / want
- Maintaining optional features in the upstream repo would introduce too much overhead
- Maintaining a fork was problematic due to `datafusion-cli` sharing the repo with all other `datafusion` crates, not only due to slow clone speed, but also due to `cargo` also switching to use `datafusion` crates from that repo instead of using published artifacts from `crates.io`.


## Upgrade procedure
1. Clone or sync the upstream repo:
  
  `git clone https://github.com/apache/datafusion.git`

2. Checkout the appropriate release tag:
    
  `git checkout MAJOR.MINOR.PATCH`

3. Replace source files (except `main.rs`):

  ```
  rm -rf src/utils/datafusion-cli/src
  cp -r ../datafusion/datafusion-cli/src src/utils/datafusion-cli/
  rm src/utils/datafusion-cli/src/main.rs
  ```

4. Compare `Cargo.toml` files:

  `diff ../datafusion/datafusion-cli/Cargo.toml src/utils/datafusion-cli/Cargo.toml`
