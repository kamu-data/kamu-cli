# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.96.0] - 2022-07-23
### Added
- Support for ingesting Parquet format, a new kind of ReadStep in ODF protocol

## [0.95.0] - 2022-07-15
### Added
- New `kamu reset` command that reverts the dataset back to the specified state
- New `kamu push --force` switch to overwrite diverged remote dataset with a local version
- New `kamu pull --force` switch to overwrite diverged local dataset with a remote version
### Fixed
- Improved the `kamu log` command's performance on datasets with high block counts and introruced a `--limit` parameter

## [0.94.0] - 2022-06-22
### Added
- `kamu list --wide` now shows the number of blocks and the current watermarks
- Iterating on Web3 demo

## [0.93.1] - 2022-06-17
### Fixed
- Fixed `completions` command that paniced after we upgraded to new `clap` version

## [0.93.0] - 2022-06-16
### Added
- By default we will resolve IPNS DNSLink URLs (e.g. `ipns://dataset.example.org`) using DNS query instead of delegating to the gateway. This is helpful when some gateway does not support IPNS (e.g. Infura) and in general should be a little faster and provides more information for possible debugging

## [0.92.0] - 2022-06-08
### Added
- `kamu system ipfs add` command that adds dataset to IPFS and returns the CID - it can be used to skip slow and unreliable IPNS publishing
### Fixed
- Engine runtime error when it was incorrectly using a `.crc` file instead of a parquet data file

## [0.91.0] - 2022-06-05
### Changed
- Clean up cached data files upon successful commit to save disk space
- Push `cache` directory of datasets to remote repositories
  - This is a temporary measure to allow resuming heavy-weight ingest actions from checkpoints

## [0.90.0] - 2022-06-04
### Added
- Experimental support for templating of ingest URLs with environment variables:
```yaml
fetch:
  kind: url
  url: "https://example.org/api/?apikey=${{ env.EXAMPLE_ORG_API_KEY }}"
```
- Experimental support for containerized fetch steps:
```yaml
fetch:
  kind: container
  image: "docker.io/kamudata/example:0.1.0"
  env:
    - name: SOME_API_KEY
```
### Changed
- Flag `kamu pull --force-uncacehable` was renamed to `--fetch-uncacheable`

## [0.89.0] - 2022-05-22
### Added
- When pushing to IPNS execute `ipfs name publish` even when data is up-to-date to extend the lifetime of the record.

## [0.88.0] - 2022-05-19
### Added
- Support pushing datasets to IPFS via IPNS URLs `kamu push <dataset> ipns://<key_id>`

## [0.87.0] - 2022-05-16
### Changed
- **Breaking:** We got rid of `.kamu.local` volume directory in favor of keeping all dataset data under `.kamu/datasets/<name>` folders. This unifies the directory structure of the local workspace with how datasets are stored in remote repositories, and makes it easier to sync datasets to and from.
### Added
- Support for `ipns://` URLs in `kamu pull`

## [0.86.0] - 2022-05-16
### Changed
- **Breaking:** Implements [ODF RFC-006](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/006-checkpoints-as-files.md) to store checkpoints as files and reference them using physical hashes
- **Breaking:** Data files are now named and stored accodring to their physical hashes, as per ODF spec
- **Breaking:** Above changes also affect the repository format
- Upgraded to `rustc-1.62.0` and latest dependencies
- Improved error handling and reporting
### Added
- Support for [ODF: Simple Transfer Protocol](https://github.com/open-data-fabric/open-data-fabric/blob/2e43c39a484309b2726a015dc3c10e4cfb9fd590/rfcs/007-simple-transfer-protocol.md) for syncing datasets to/from remotes
- Support for URL-based remote references, e.g. `kamu pull http://my.repo.org/some/dataset --as my.dataset`
  - URLs are also allowed for pull/push aliases
- Auto-deriving local names from remote reference, e.g. `kamu pull http://my.repo.org/some/dataset` will pull into `dataset`
- Support for IPFS source (via HTTP Gateway), e.g. `kamu pull ipfs://bafy...aabbcc/some/dataset`
  - IPFS gateway is configurable
  - To use your local IPFS daemon as gateway do `kamu config --user set protocol.ipfs.httpGateway "http://localhost:8080"`
### Fixed
- Truncation of `kamu.log` in visual mode

## [0.85.1] - 2022-04-09
### Fixed
- Don't panic in `kamu ui` when there is no default browser set

## [0.85.0] - 2022-04-09
### Changed
- Updated to latest ODF schemas
- Unhacked dataset descriptions in GQL
- Updated Web UI
- Extended GQL metadata block type with mock author information

## [0.84.1] - 2022-04-08
### Added
- Web UI embedding into MacOS build

## [0.84.0] - 2022-04-08
### Added
- Early Web UI prototype is now available via `kamu ui` command

## [0.83.0] - 2022-03-30
### Added
- Using code generation for GQL wrappers around ODF types
- Extended GQL API with root source metadata

## [0.82.0] - 2022-03-22
### Added
- Added more libraries into Jupyter image (including `xarray`, `netcdf4`, and `hvplot`).
- Improved examples and demo environment.

## [0.81.1] - 2022-03-16
### Fixed
- Upgrading Spark engine that fixes merge strategies producing unnecessary updates.
- Fixed `filesGlob` entering infinite loop when last file does not result in a commit (no new data to ingest).

## [0.81.0] - 2022-02-19
### Changed
- GQL `tail` query will now return `DataQueryResult` containing schema information instead of raw `DataSlice`.

## [0.80.0] - 2022-02-19
### Added
- GQL API now supports executing arbitrary SQL queries (using `datafusion` engine).
### Changed
- Removed use of default values from GQL schema.
- Upgraded to latest `arrow` and `datafusion`.

## [0.79.0] - 2022-02-08
### Added
- `kamu inspect schema` now supports JSON output.
### Changed
- Upgraded to `datafusion` version 6.

## [0.78.2] - 2022-01-27
### Added
- Added `currentPage` field to the GQL pagination info.

## [0.78.1] - 2022-01-24
### Changed
- Upgraded to latest version of `dill` and other dependencies.

## [0.78.0] - 2022-01-17
### Added
- Evolving the GraphQL API to support multiple `MetadataEvent` types.

## [0.77.1] - 2022-01-17
### Fixed
- CORS policy was too strict not allowing any headers.
- Small CLI parsing tweaks.

## [0.77.0] - 2022-01-16
### Added
- New crate `kamu-adapter-graphql` provides GraphQL interface for ODF repositories.
- New command `kamu system api-server` runs HTTP + GraphQL server over current Kamu workspace with built-in GQL Playground UI.
- New command `kamu system api-server gql-query` executes single GraphQL query and writes JSON output to stdout.
- New command `kamu system api-server gql-schema` dumps GraphQL API schema to stdout.

## [0.76.0] - 2022-01-14
### Changed
- Maintenance release - no user-facing changes.
- Migrated from explicit threading to `tokio` async.
- Better separation of architectural layers.
- Improved error handling.

## [0.75.1] - 2022-01-05
### Fixed
- Added more validation on events that appear in `DatasetSnapshot`s.
- Fix flag completion after `clap` crate upgrade.

## [0.75.0] - 2022-01-04
### Changed
- Implements [ODF RFC-004](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/004-metadata-extensibility.md).
- Breaking: Workspaces will need to be re-created. This is the last major metadata format change - we will be working on stabilizing metadata now.
- Breaking: Some manifest formats have changed and will need to be updated.
  - `DatasetSnapshot` needs to specify `kind` field (`root` or `derivative`)
  - `DatasetSnapshot.source` was replaced with `metadata` which is an array of metadata events

## [0.74.0] - 2021-12-28
### Changed
- Implements [ODF RFC-003](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/003-content-addressability.md).
- Breaking: Workspaces will need to be re-created. Sorry again!
- Breaking: Some manifest formats have changed and will need to be updated.
  - `Manifest.apiVersion` renamed to `version`
  - `DatasetSnapshot.id` renamed to `name`
  - `DatasetSourceDerivative.inputs` should now specify `id` (optional) and `name` (required)
  - `TemporalTable.id` renamed to `name`
- Datasets now have a globally unique identity.
  - IDs can be viewed using `kamu log -w`
- Metadata format switched to a much faster and compact `flatbuffers`.
  - You can still inspect it as YAML using `kamu log --output-format yaml`

## [0.73.0] - 2021-12-11
### Changed
- Implements [ODF RFC-002](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/002-logical-data-hashes.md).
- Breaking change - workspaces will need to be re-created. Sorry again!
- Engines are no longer responsible for data hashing - a stable hash algorithm is implemented in `kamu`
- Pending data part files and checkpoints will be stored in `cache` directory along with other ingest artifacts
### Added
- A fully working implementation of data integrity checks
- `kamu verify` command now accepts `--integrity` flag to only check data hashes without replaying transformations

## [0.72.0] - 2021-12-07
### Changed
- Breaking change - workspaces will need to be re-created. Sorry!
- Implements [ODF RFC-001](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/001-record-offsets.md) that adds record offset system column.

## [0.71.0] - 2021-11-29
### Changed
- Made engine timeouts configurable
- Better error logging
- Upgrade `rustc` and dependencies

## [0.70.0] - 2021-11-07
### Changed
- Upgraded to latest `rustc`
- Upgraded to latest dependencies

## [0.69.0] - 2021-10-25
### Added
- The `verify` command now accepts root datasets and in future will perform data integrity check
### Fixed
- The `up-to-date` status reporting when pulling derivative datasets

## [0.68.0] - 2021-10-16
### Fixed
- `tail` command will truncate cells that are too long and mask binary types for human-friendly output
- Fetch overrides will no longer interfere with regular caching

## [0.67.0] - 2021-10-02
### Added
- Engine provisioning now support concurrency limit (via `engine.maxConcurrency` config option)
### Fixed
- When running in host networking mode (e.g. in container) the engine concurrency will be set to `1` to prevent engine instances from interfering with one another

## [0.66.0] - 2021-10-01
### Changed
- Major logging system improvements (switched from `slog` to `tracing`)

## [0.65.2] - 2021-09-29
### Fixed
- SQL shell does not use temp directory for shell init script to avoid extra mounts

## [0.65.1] - 2021-09-29
### Fixed
- Ingest errors will be correctly reported under `podman` where exit code was taking precedence over error specified in the engine response file

## [0.65.0] - 2021-09-29
### Changed
- Coordinator now communicates with engines via `gRPC` protocol as specified by ODF spec
- Above lets us display readable errors to the user without them needing to dig through log files

## [0.64.0] - 2021-09-11
### Changed
- Switched to BSL code license. See [License FAQ](docs/license_faq.md) for more information about this change.

## [0.63.0] - 2021-09-03
### Fixed
- Data exchange with the engines not uses `.kamu/run` directory, so on systems that run Docker in VMs (Linux, Windows) it's no longer necessary to add extra mounts for the temp directories

## [0.62.2] - 2021-08-30
### Fixed
- Adding few workarounds around bugs in Arrow / DataFusion related to working with DECIMAL and TIMESTAMP types

## [0.62.1] - 2021-08-28
### Fixed
- Allowing Flink engine to run under host network namespace, for the "podman-in-docker" scenario

## [0.62.0] - 2021-08-26
### Changed
- Some internal improvements

## [0.61.1] - 2021-08-25
### Added
- Some internal convenience commands for image manipulation

## [0.61.0] - 2021-08-25
### Added
- New `kamu inspect schema` command that can display the DDL-style schema and the underlying physical Parquet schema of a dataset

## [0.60.0] - 2021-08-24
### Added
- New `kamu tail` command allows to inspect last few records in a dataset
- The `kamu sql` command now supports experimental [DataFusion](https://github.com/apache/arrow-datafusion) engine that can execute SQL queries extremely fast. It doesn't have a shell yet so can only be used in `--command` mode but we will be expanding its use in future.

## [0.59.0] - 2021-08-18
### Added
- Ability to delete datasets in remote repositories via `kamu delete` command

## [0.58.1] - 2021-08-17
### Fixed
- Upgraded Spark engine brings better error handling for invalid event times

## [0.58.0] - 2021-08-17
### Added
- New `kamu search` command allows listing and searching for datasets in remote repositories

## [0.57.0] - 2021-08-16
### Added
- `kamu inspect lineage` now provides HTML output and can show lineage graph in a browser

## [0.56.0] - 2021-08-16
### Changed
- Upgrade to latest Spark and Livy
- Fixes some issues with geometry types in notebooks and SQL shell

## [0.55.0] - 2021-08-15
### Added
- New `kamu inspect query` command for derivative transform audit
- `kamu log` now supports Yaml output and filtering

## [0.54.0] - 2021-08-15
### Added
- New `kamu inspect lineage` command that shows dependency tree of a dataset
### Changed
- The `kamu list depgraph` command was removed in favor of a specialized lineage output type: `kamu inspect lineage --all -o dot`

## [0.53.0] - 2021-08-13
### Fixed
- Upgrading Spark engine with a workaround for upstream bug in Shapefile import
- Engine image pulling status freezes

## [0.52.0] - 2021-08-13
### Added
- New `kamu verify` command that can attest that data in the dataset matches what's declared in metadata
### Fixed
- Spark engines will not produce an empty block when there was no changes to data

## [0.51.0] - 2021-08-08
### Added
- `kamu pull` now supports `--fetch` argument that lets you override where data is ingested from, essentially allowing to push data into a dataset in addition to pulling from the source

## [0.50.0] - 2021-08-05
### Changed
- Renamed `remote` to `repo` / `repository` to be consistent with ODF spec

## [0.49.0] - 2021-08-05
### Added
- Datasets can now be associated with remote repositories for ease of pulling and pushing
- `push` and `pull` commands now allow renaming the remote or local datasets
### Fixed
- Improved error reporting of invalid remote credentials
- Ingest will not create a new block if neither data now watermark had changed

## [0.48.2] - 2021-07-31
### Fixed
- Dependency error in `remote` commands
- Remove incorrect `.gitignore` file generation

## [0.48.1] - 2021-07-26
### Fixed
- `sql` command failing on OSX

## [0.48.0] - 2021-07-23
### Added
- Terminal pagination in `log` command

## [0.47.0] - 2021-07-12
### Fixed
- Upgrading to newer Jupyter and Sparkmagic (fixes some issues in notebooks)
- Better error handling
### Added
- Environment variable completions

## [0.46.0] - 2021-07-11
### Changed
- Multiple internal improvements
- Support for host networking with `podman`
- Support for running standalone Livy for using `kamu` under JupyterHub

## [0.45.0] - 2021-06-26
### Fixed
- Updated Spark engine version to fix networking issue under `podman`
- Updated `rustc` and dependencies

## [0.44.0] - 2021-06-11
### Fixed
- Support for Zip files that cannot be decoded in a streaming fashion.
- Dependency upgrades.

## [0.43.1] - 2021-06-06
### Fixed
- Increased socket check timeout to prevent `kamu sql` trying to connect before Spark server is fully up.

## [0.43.0] - 2021-06-06
### Added
- New `kamu config` command group provides `git`-like configuration.
- Experimental support for `podman` container runtime - a daemon-less and root-less alternative to `docker` that fixes the permission escalation problem. To give it a try run `kamu config set --user engine.runtime podman`.
### Fixed
- The `kamu notebook` command no longer fails if `kamu` network in `docker` was not cleaned up.

## [0.42.0] - 2021-06-05
### Changed
- Upgraded Flink engine to latest `v1.13.1`
- Improved empty data block handling in metadata
- Improved some CLI output format

## [0.41.0] - 2021-05-08
### Added
- `zsh` completions support (via `bashcompinit`)
### Fixed
- Improved error handling of pipe preprocessing commands
### Changed
- Replaced `io.exchangeratesapi.daily.usd-cad` dataset in examples with `ca.bankofcanada.exchange-rates.daily` as it became for-profit.

## [0.40.1] - 2021-05-02
### Fixed
- Improved error handling when `kamu sql` is ran in an empty workspace
### Changed
- Upgraded to latest dependencies

## [0.40.0] - 2021-04-29
### Added
- Implemented `sql server` command - now it's possible to run Thrift server on a specific address/port and connect to it remotely.

## [0.39.0] - 2021-04-25
### Added
- Added `--replace` flag to the `add` command allowing to delete and re-add a dataset in a single step.

## [0.38.3] - 2021-04-23
### Fixed
- An error in the `sql` command help message
- Release binaries will now be packaged as `kamu`, not `kamu-cli`

## [0.38.2] - 2021-04-18
### Added
- Detailed help and examples to all CLI commands
- Dependency bump

## [0.38.1] - 2021-04-08
### Changed
- Bumped Spark engine version

## [0.38.0] - 2021-03-28
### Changed
- Maintenance release
- Upgraded to latest rust toolchain and dependencies
- BREAKING: Updated `flatbuffers` version that includes support for optional fields - this changes binary layout making this new version incompatible with metadata generated by the previous ones
### Fixed
- Uncacheable message will no longer obscure he commit message

## [0.37.0] - 2020-12-30
### Changed
- Breaking: Metadata restructuring means you'll need to re-create your datasets
- Data files as well as checkpoints are now named with the hash of the block they are associated with
- Protocol between coordinator and engines was modified to pass data files (in the right order) as well as checkpoints explicitly
- Intermediate checkpoints are now preserved (for faster validation and resets)
- Vocabulary has been incorporated into the metadata block (ODF `0.17.0`)
- Lazily computing dataset summaries
- Upgraded dependencies
- Happy New Year!

## [0.36.0] - 2020-11-16
### Changed
- Engine errors will not list all relevant log files
- UI improvements

## [0.35.0] - 2020-11-14
### Added
- Initial support for `S3` as a remote backend

## [0.34.0] - 2020-11-09
### Added
- Initial version of `remote` management
- Initial version of `push` / `pull` commands working with `remote`
- Local file system `remote` backend implementation

## [0.33.0] - 2020-11-01
### Changed
- Upgraded the Spark engine to Spark 3.0

## [0.32.0] - 2020-10-31
### Added
- Support for `flatbuffers` metadata encoding.
- Using `flatbuffers` format for achieving true stable hashes of metadata.
- Aligned metadata with ODF `v0.16.0`.

## [0.31.0] - 2020-10-08
### Added
- `pull` command now supports `--force-uncacheable` flag for refreshing uncacheable datasets.

## [0.30.1] - 2020-09-07
### Fixed
- Add back `.gitignore` file when creating local volume dir.

## [0.30.0] - 2020-09-05
### Changed
- This version is a complete re-write of the application from **Scala** into
  **Rust**. It is mostly on par with the functionality of the previous version
  but has many improvements. Most notably, all interactions not involving
  engines (which are still heavy and run in `docker`) are blazing fast.

## [0.23.0] - 2020-08-22
### Changed
- Updated to latest ODF schema

## [0.22.0] - 2020-07-15
### Added
- `#26`: Follow redirects when fetching data from URL
### Changed
- Follow ODF spec on metadata `refs`

## [0.21.0] - 2020-07-12
### Fixed
- Encoding issue in `DatasetSummary` manifest
### Changed
- Upgraded to use ODF resources

## [0.20.0] - 2020-06-30
### Added
- Windows is somewhat supported now

## [0.19.0] - 2020-06-28
### Changed
- Improving Windows support by removing Hadoop FS dependencies

## [0.18.2] - 2020-06-26
### Changed
- Upgraded Flink engine to `0.3.3`

## [0.18.1] - 2020-06-25
### Changed
- Upgraded Flink engine to `0.3.0`

## [0.18.0] - 2020-06-23
### Added
- Watermarking support

## [0.17.0] - 2020-06-14
### Added
- Added support for [Apache Flink](https://github.com/kamu-data/kamu-engine-flink) engine!

## [0.16.0] - 2020-05-25
### Changed
- Improve sort order of glob-based file sources
- Spark engine will persist events ordered by event time

## [0.15.0] - 2020-05-09
### Added
- `purge` command now supports `--recursive` flag
- Internal improvements and refactoring

## [0.14.0] - 2020-05-03
### Changed
- Consolidating more logic into `engine.spark`

## [0.13.0] - 2020-05-02
### Added
- New `log` command
### Changed
- Use `SHA-256` for dataset and metadata hashing
- The concept of `volume` was converted into `remote`
- Metadata refactoring to isolate engine-specific query parameters
- Metadata refactoring of root/derivative sources
- Moved most ingest logic into coordinator
- Moved transform batching logic into coordinator

## [0.12.1] - 2020-03-22
### Fixed
- Snapshot merge strategy was completely broken

## [0.12.0] - 2020-03-21
### Changed
- Use dataset caching for faster hash computation

## [0.11.0] - 2020-03-08
### Added
- Human-readable formatting support (e.g. in `list` command)

## [0.10.1] - 2020-03-08
### Fixed
- Some issues with `list` and `add` commands

## [0.10.0] - 2020-03-08
### Added
- `list` command now shows data size, number of records, and last pulled time
- `add` command now accounts for dataset dependency order

## [0.9.0] - 2020-03-08
### Changed
- Using new metadata chain prototype!

## [0.8.0] - 2020-01-12
### Added
- Experimental support for remote S3 volumes

## [0.7.1] - 2019-12-29
### Changed
- Bumped ingest version

## [0.7.0] - 2019-12-29
### Changed
- Using snake_case dataset vocabulary

## [0.6.0] - 2019-12-15
### Added
- Richer set of CSV reader options
### Fixed
- Recursive pull concurrency issues

## [0.5.0] - 2019-12-09
### Added
- New `fetchFilesGlob` data source that can load multiple data files at once
- Event time is now a core part of the datasets
### Fixed
- Snapshot merge strategy de-duplicates input rows now

## [0.4.0] - 2019-11-24
### Added
- Basic SQL templating feature
### Fixed
- The `purge` command will no longer delete the dateset

## [0.3.0] - 2019-11-21
### Added
- Command to generate `bash` completions
- Keeping a CHANGELOG
