# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.152.0] - 2024-01-17
### Added
- Initial support for local predefined Admin users
- New GraphQL APIs extending flows system capabilities:
   - ability to list summary information about flows
   - flow execution history function
- New command `kamu system diagnose` to run checks for correct system work
- Additional info about workspace and container version for `kamu system info` command
### Fixed
- Fixed bug in getting available dataset actions for a user, in case of `kamu ui`
- `kamu add`: corrected a link in help examples

## [0.151.0] - 2024-01-09
### Added
- New GraphQL APIs to manually schedule and cancel dataset flows
- Cron expression implementation for dataset flows
### Changed
- Automatically cancelling scheduled tasks if parent flow is cancelled or aborted
### Fixed
- Fixed initialization of datasets without dependencies causing UI breakage

## [0.150.1] - 2023-12-29
### Added
- New flag `--get-token` for `kamu system api-server` cli command which additionaly prints
  JWT token in console
### Fixed
- Fixed async file flushing issues that could result in a race condition when using containerized ingest
- `kamu pull`: fixed containerized iterative ingestion

## [0.150.0] - 2023-12-27
### Added
- GraphQL API to configure automatic run of dataset flows:
  - a schedule for main flows, like ingest of root datasets
  - a batching condition for dependent flows, such as executing transforms
### Changed
- Changed logic in `SimpleTransferProtocol` now block data and checkpoint downloading/uploading
  in parallel. Default parallel tasks is 10, but it could be changed by changing 
  `SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS` environment variable

## [0.149.0] - 2023-12-23
### Added
- Added `KAMU_WORKSPACE` env var to handle custom workspace path if needed
- Added `event-bus` crate: an utility component based on Observer design pattern,
  which allows event producers and event consumers not to know about each other
- Applied `event-bus` component to inform consumers of dataset removal, dependency changes,
  task completions
- Added in-memory dataset dependency graph instead of continous rescanning of all datasets:
   - the initial dependencies are computed on demand on first request 
   - using `petgraph` project to represent dataset dependencies in the form of directed acyclic graph
   - further events like new/removed dependency or dataset removal update the graph
   - simplified GraphQL APIs and dataset removal check using new dependency graph
- Added prototype of flow management system:
   - flows are automatically launched activities, which are either dataset related or represent system process
   - flows can have a schedule configuration, using time delta or CRON expressions
   - flows system manages activation of flows according to the dynamically changing configuration
   - flows system manages triggering of dependent dataset flows, when their inputs have events
   - derived flows may have throttling settings
### Changed
- Integrated latest `dill=0.8` version, which removes a need in registering simple dependency binds
- Using new `dill=0.8` to organize bindings of structs to implemented traits via declarative attributes

## [0.148.0] - 2023-12-20
### Added
- GQL `currentPushSources` endpoint
### Changed
- GQL `currentSource` endpoint was deprecated in favor of new `currentPollingSource` endpoint

## [0.147.2] - 2023-12-19
### Fixed
- Cargo.lock file update to address [RUSTSEC-2023-0074](https://rustsec.org/advisories/RUSTSEC-2023-0074)

## [0.147.1] - 2023-12-15
### Fixed
- Legacy Spark ingest was failing to start a container when fetch source is pointing at a local FS file with a relative path

## [0.147.0] - 2023-12-13
### Changed
- Updates to support open-data-fabric/open-data-fabric#63
- Metadata chain will complain about `AddData` or `ExecuteQuery` events if there is no `SetDataSchema` event
- Push ingest no longer piggy-backs on `SetPollingSource` - it requires `AddPushSource` event to be present
- `DatasetWriter` will now validate that schema of the new data block matches the schema in `SetDataSchema` to prevent schema drift
### Added
- Name of the push source can optionally be specified in CLI command and REST API

## [0.146.1] - 2023-11-27
### Added
- New dataset permission for UI: `canSchedule`
- Added `kamu ui` feature flag to control availability of datasets scheduling

## [0.146.0] - 2023-11-24
### Added
- New `kamu ingest` command allows you to push data into a root dataset, examples:
  - `kamu ingest my.dataset data-2023-*.json` - to push from files
  - `echo '{"city": "Vancouver", "population": 675218}' | kamu ingest cities --stdin`
- New `/:dataset/ingest` REST endpoint also allows you to push data via API, example:
  - Run API server and get JWT token: `kamu ui --http-port 8080 --get-token`
  - Push data: `echo '[{...}]' | curl -v -X POST http://localhost:8080/freezer/ingest -H 'Authorization:  Bearer <token>'`
- The `kamu ui` command now supports `--get-token` flag to print out the access token upon server start that you can use to experiment with API
### Changed
- Upgraded to `arrow v48`, `datafusion v33`, and latest AWS SDK

## [0.145.6] - 2023-10-30
### Fixed
- Jupyter image now correctly parses dataset aliases in multi-tenant repositories

## [0.145.5] - 2023-10-26
### Changed
- FlightSQL interface tweaks to simplify integration into `api-server`

## [0.145.4] - 2023-10-24
### Fixed
- `kamu ui` should run without specifying any auth secrets

## [0.145.3] - 2023-10-18
### Changed
- Demanding required env vars to be set before API server / Web UI server startup
- Custom error messages for non-valid session in GraphQL queries depending on the reason
### Fixed
- If access token points on unsupported login method, it's a client error (4xx), not (5xx)

## [0.145.2] - 2023-10-16
### Changed
- GitHub API results (login, token resolution, account queries) now have a runtime cache
  to avoid excessive number of external calls (and related call rate bans)

## [0.145.1] - 2023-10-13
### Changed
- GitHub Client ID now delivered as a part of runtime configuration for UI
### Fixed
- Crashes upon iterating multi-tenant local FS datasets with short dataset alias

## [0.145.0] - 2023-10-11
### Added
- Ability to login to a remote ODF server via CLI commands:
   - `kamu login` command opens platform's frontend login form and collects access token on success
   - `kamu logout` command drops previously saved access token
   - tokens can be stored both in local workspace as well as in user home folder
   - access tokens are attached as Bearer Authentication header in simple/smart protocol client requests
- ODF server handlers for simple/smart protocols correctly implement authentication & authorization checks
- ODF server URLs vary depending on multi-tenancy vs single-tenancy of dataset repository
### Changed
- Signifficantly reworked smart transfer protocol tests, support of multi-tenancy, authentication, authorization


## [0.144.1] - 2023-10-02
### Fixed
- Flight SQL + JDBC connector showing empty result for `GROUP BY` statements

## [0.144.0] - 2023-09-25
### Added
- New protocol adapter for [Arrow Flight SQL](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
  - Using this adapter you can connect to `kamu` as JDBC data source from DBeaver, Tableau and other BI tools
  - To run Flight SQL server use `kamu sql server --flight-sql` command

## [0.143.0] - 2023-09-19
### Added
- Support for multi-tenant workspaces in Jupyter Notebook extension
- Support for GraphQL multi-tenant mode:
   - Added new endpoint for querying engine supported login methods
   - Added `AuthenticationService` and `AuthenticationProvider` concepts that implement login functionality
   - CLI authentication provider: login same as password with preconfigured accounts in `.kamucliconfig` files
   - Login issues and interpets Kamu-specific JWT tokens
   - GraphQL queries are expected to attach JWT tokens as Bearer authentication header
   - Modeling anonymous account sessions
   - Login guards in GraphQL write operations
   - Simple model for dataset permission queries
   - Login instructions and feature flags are sent as configuration in `kamu ui` mode
   - Implemented previously mocked account resolution API
### Fixed
- Failing transform operations in multi-tenant workspaces due to invalid propagation of dataset aliases
### Changed
- Updated WEB UI image to latest release 0.10.0
- Github OAuth functionality isolated in a separate component `kamu-adapter-oauth`
- GraphQL: filtering datasets based on logged account
- Unified and clarified namings in account-related data structures

## [0.142.1] - 2023-09-02
### Fixed
- `RecordsFormat` Utf8 issue when truncating strings
### Changed
- Updated `kamu-base:latest-with-data` image to use new DF-ingest-based datasets

## [0.142.0] - 2023-09-01
### Fixed
- Ignoring the downloads cache when `--fetch-uncacheable` flag is used
- Restored pre-sorting of events by `event_time` within one data slice in DataFusion-baesd ingest
- Performance degradation in local file copying due to async
- Race condition in Zip decompress step that caused file truncation

## [0.141.0] - 2023-08-28
### Fixed
- `datafusion` ingest will not crash on empty inputs when schema inference is enabled
### Added
- Added a warning when fetch cache is used to resume ingest: `Using cached data from X minutes ago (use kamu system gc to clear cache)`

## [0.140.0] - 2023-08-27
### Added
- **Experimental:** Data ingest using `DataFusion` engine
  - It's an entirely new implementation of data readers and merge strategies
  - It's often over **100x faster** than the `Spark`-based ingest as it has near-instant startup time (even avoids container overhead)
  - New merge strategies can work directly over S3 without downloading all data locally
  - It supports all existing data formats (Parquet, CSV, NdJson, GeoJson, Shapefile)
    - Some advanced CSV / Json reader options are not yet implemented, most notably `timestampFormat`
  - `Spark` is still used by default for compatibility. To start using `DataFusion` declare (a potentially no-op) `preprocess` step in your root dataset manifest ([see example](examples/currency_conversion/ca.bankofcanada.exchange-rates.daily.yaml))
  - `Spark`-based ingest will be remove in future versions with `DataFusion` becoming the default, however we are planning to support `Spark` and all other engines in the `preprocess` step, while `DataFusion` will still be handling the initial reading of data and merging of results
### Changed
- All examples where possible are now using `DataFusion` ingest

## [0.139.0] - 2023-08-17
### Added
- Prototyped authorization checks for CLI functionality based on OSO-framework:
   - for now the assumption is that all datasets are public
   - public datasets can be read by anyone, but written only by owner
   - authorization checks (Read, Write) integrated into every known CLI command or underlying service

## [0.138.0] - 2023-08-15
### Added
- GQL API now supports renaming and deleting datasets

## [0.137.0] - 2023-08-11
### Added
- CLI and GQL API now both support skipping N first records when querying data for the sake of pagination

## [0.136.0] - 2023-08-04
### Added
- Support multi-tenancy within S3-based dataset repository
### Changed
- Updated WEB UI image to latest release 0.8.0

## [0.135.0] - 2023-08-01
### Added
- New `kamu version` command that outputs detailed build information in JSON or YAML
  - `kamu --version` remains for compatibility and will be removed in future versions
- New `kamu system info` command that outputs detailed system information
  - Currently only contains build info but will be in future expanded with host environment info, docker/podman versions, and other things useful for diagnostics
- GQL API: New `updateReadme` mutation
### Changed
- GQL API: Moved dataset creation and event commit operations to `mutation`

## [0.134.0] - 2023-07-27
### Changed
- New engine I/O strategies allow ingest/transform to run over datasets in remote storage (e.g. S3) even when engine does not support remote inputs
- Improved credental reuse in S3-based dataset repository
- Simplified S3 tests

## [0.133.0] - 2023-07-17
### Changed
- Lots of internal improvements in how data is being passed to engines
- All engine inputs are now mounted as individual files and as read-only to tighten up security
- Using an updated Spark engine image

## [0.132.1] - 2023-07-13
### Fixed
- S3 ObjectStore is now properly initialized from standard AWS environment variables

## [0.132.0] - 2023-07-12
### Added
- Initial support for CLI-only local multi-tenant workspaces and resolving multi-tenant dataset references
### Changed
- Improved `--trace` feature to output detailed async task breakdown
### Fixed
- Data hashing will no longer stall the event main loop thread

## [0.131.1] - 2023-06-27
### Fixed
- Resolved wrong cache directory setting for ingest tasks

## [0.131.0] - 2023-06-23
### Changed
- Internal: isolated infra layer from direct access to WorkspaceLayout / DatasetLayout to support different layouts or non-local stores in future
- Workspace version #2: storing remote aliases configuration as a part of dataset info-repo

## [0.130.1] - 2023-06-19
### Added
- Improved tracing and error handling in the GQL API

## [0.130.0] - 2023-06-16
### Added
- New `knownEngines` GQL API for displaying a list of recommended engines in Web UI

## [0.129.0] - 2023-06-15
### Added
- Event-sourced implementation of the basic but functional task system
### Changed
- Upgraded to latest `datafusion`

## [0.128.5] - 2023-06-13
### Added
- Task system prototyping (in-memory backend + GQL API)
### Changed
- More tracing instrumentation for Query service, and ObjectStore builders
### Fixed
- Caught root cause of platform's S3 data querying issues

## [0.128.0] - 2023-05-31
### Added
- New `kamu --trace` flag will record the execution of the program and open [Perfetto UI](https://perfetto.dev/) in a browser, allowing to easily analyze async code execution and task performance. Perfetto support is still in early stages and needs more work to correctly display the concurrent tasks.
- Added a few common command aliases: `ls -> list`, `rm -> delete`, `mv -> rename`

## [0.127.1] - 2023-05-29 (**BREAKING**)
### Fixed
- Handle "Interrupted system call" error when waiting for container to spawn.

## [0.127.0] - 2023-05-29 (**BREAKING**)
### Changed
-  Upgraded to new `spark` engine image that better follows the ODF spec regarding the timestamp formats. This may cause schema harmonization issues when querying the history of old datasets.
### Added
- Experimental support for new [`datafusion` engine](https://github.com/kamu-data/kamu-engine-datafusion) based on [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion). Although its a batch-oriented engine it can provide a massive performance boost for simple filter/map operations. See the [updated documentation](https://docs.kamu.dev/cli/supported-engines/) for details and current limitations.

## [0.126.2] - 2023-05-26
### Fixed
- AWS S3 object store is getting proper credentials from already resolved AWS SDK cache

## [0.126.1] - 2023-05-26
### Fixed
- Silent reaction on missing AWS environment variables when constructing S3 object store

## [0.126.0] - 2023-05-26
### Changed
- Refactoring of query service to support S3-based dataset repositories

## [0.125.1] - 2023-05-22
### Fixed
- Container will be considered ready only when reaching `running` status, not `created`

## [0.125.0] - 2023-05-22
### Changed
- Refactoring of container process handling to unify termination and cleanup procedure
### Added
- Containerized ingest will display fetch progress
### Fixed
- Spinners displayed by `pull`/`push` commands sometimes were not animating or refreshing too frequently

## [0.124.1] - 2023-05-17
### Changed
- Smart transfer protocol: improved resilience to web-socket idle timeouts during long push flows

## [0.124.0] - 2023-05-14
### Changed
- Limiting use of `curl` dependency to FTP
- FTP client is now an optional feature. It's still enabled in release but off by default in dev to speed up builds.

## [0.123.1] - 2023-05-14
### Changed
- Build improvements
- Excluding most of `openssl` from the build
- Removed `--pull-test-images` flag from `kamu init` command in favor of pulling images directly during tests

## [0.123.0] - 2023-05-12
### Fixed
- Verification of data through reproducibility should ignore differences in data and checkpoint sizes

## [0.122.0] - 2023-05-11
### Fixed
- `kamu add` command no longer fails to deduplicate existing datasets
- Tracing spans in logs no longer interfere with one another in concurrent code
### Changed
- Improved dataset creation logic to make it more resilient to various failures
- Improved `kamu add` command error handling

## [0.121.1] - 2023-05-06
### Fixed
- Preserving previous watermark upon fetch step returning no data

## [0.121.0] - 2023-05-05 (**BREAKING**)
### Changed
- Deprecated `.kamu/datasets/<dataset>/cache` directory - a workspace upgrade will be required (see below)
- Support ingest source state per [ODF RFC-009](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/009-ingest-source-state.md)
- Introduced workspace-wide cache in `.kamu/cache`
- Introduced workspace versioning and upgrade procedure via `kamu system upgrade-workspace` command
- Upgraded to latest `datafusion` and `arrow`
### Added
- New `kamu system gc` command to run garbage collector (currently only cleans up cache)

## [0.120.1] - 2023-05-01
### Fixed
- Improved recovery after aborting smart protocol push/pull operations

## [0.120.0] - 2023-04-20
### Added
- Smart Transfer Protocol now supports pushing into local filesystem workspace repository.
  It can be activated via:
   `kamu push <dataset_name> --to odf+http://<server-address>:<port>/<dataset_name>`.
  I.e., run API server in one Kamu workspace directory, where you intend to push `my-dataset`to:
   `kamu system api-server --http-port=35433`
  and push from the Kamu workspace directory, where `my-dataset` is stored:
   `kamu push my-dataset --to odf+http://localhost::35433/my-dataset`  
### Changed
- Upgraded to latest `datafusion` and `arrow`
- Updated to multi-tenant references and aliases in accordance with ODF spec
- New datasets no longer use staging area before `DatasetBuilder` finishes them

## [0.119.0] - 2023-04-10
### Added
- Smart Transfer Protocol: implemneted Push flow for S3-based dataset repository only.
  Does not work with the local workspace yet.

## [0.118.0] - 2023-04-06
### Fixed
- Updated IPFS gateway status code handling after upstream fixes to correctly report "not found" status (#108)

## [0.117.0] - 2023-04-03
### Changed
- Revised workspace dependencies management:
  - Sharing single definition of common dependencies between modules
  - Using `cargo-deny` utility for dependencies linting
- Migrated to official S3 SDK (got rid of untamintained `Rusoto` package)
- Moved developer's guide to this repository

## [0.116.0] - 2023-03-28
### Added
- Smart Transfer Protocol: implemented Pull flow based on web-sockets. It can be activated via:
   `kamu pull odf+http://<server-address>:<port>/<dataset_name>`.
  I.e., run API server in one Kamu workspace directory, where a `my-dataset` dataset is present:
   `kamu system api-server --http-port=35433`
  and pull from another Kamu workspace directory:
   `kamu pull odf+http://localhost::35433/my-dataset`
- S3-based dataset repository implementation
### Changed
- Improved status codes and error handling in API server's routes for Simple Transfer Protocol

## [0.115.0] - 2023-03-19
### Changed
- Updated to new `rustc`
- Updated to latest `datafusion` and `arrow`
- Improved release procedure

## [0.114.3] - 2023-03-18
### Changed
- Migrated engine images from Docker Hub to `ghcr.io`

## [0.114.2] - 2023-03-13
### Fixed
- Fixed JSON serialization of some datasets by enabling `chrono-tz` feature in `arrow` that became optional

## [0.114.1] - 2023-03-12
### Fixed
- `kamu pull` will not panic on root datasets that don't define a polling source and will consider them up-to-date

## [0.114.0] - 2023-03-12
### Added
- `kamu add` command now supports reading from STDIN

## [0.113.0] - 2023-03-11
### Changed
- Upgraded major dependencies

## [0.112.0] - 2023-02-19
### Added
- New GraphQL API for dataset creation and committing new blocks

## [0.111.0] - 2023-02-16
### Added
- GraphQL API exposes current vocabulary as a part of dataset's metadata

## [0.110.1] - 2023-02-10
### Fixed
- Fixed GraphQL API issues with TransformInput structures with the aliфs name that is different from dataset name

## [0.110.0] - 2023-02-09
### Changed
- Improved reaction of `kamu inspect schema` and `kamu tail` on datasets without schema yet
- GraphQL schema and corresponding schema/tail responses now assume there might not be a dataset schema yet
- Web-platform version upgraded after GraphQL API changes

## [0.109.0] - 2023-02-05
### Fixed
- Lineage and transform now respect names of datasets in the `SetTransform` metadata events, that may be different from names in a workspace

## [0.108.0] - 2023-01-30
### Fixed
- Upgrade to Flink engine prevents it from crashing on checkpoints over 5 MiB large

## [0.107.0] - 2023-01-25 (**BREAKING**)
### Changed
- Major upgrade of Apache Flink version
- No updates to exisitng datasets needed, but the verifiability of some datasets may be broken (since we don't yet implement engine versioning as per ODF spec)
### Added
- Flink now supports a much nicer temporal table join syntax:
  ```sql
  SELECT
    t.event_time,
    t.symbol,
    p.volume as volume,
    t.price as current_price,
    p.volume * t.price as current_value
  FROM tickers as t
  JOIN portfolio FOR SYSTEM_TIME AS OF t.event_time AS p
  WHERE t.symbol = p.symbol
  ```
- We recommend using `FOR SYSTEM_TIME AS OF` join syntax as replacement for old `LATERAL TABLE` joins 
- Determinism of Flink computation should be improved

## [0.106.0] - 2023-01-19
### Changed
- Upgrade to stable version of `arrow-datafusion`

## [0.105.0] - 2023-01-13
### Fixed
- Upgraded `sparkmagic` dependency and removed hacks to make it work with latest `pandas`
- Returning `[]` instead of empty string for no data in GQL
### Changed
- Improved testing utilities
- Refactored commit procedure

## [0.104.0] - 2022-12-28
### Added
- Installer script that can be used via `curl -s "https://get.kamu.dev" | sh`
- Unified table output allowsing commands like `kamu list` to output in `json` and other formats

## [0.103.0] - 2022-12-23
### Changed
- Major upgrade to latest versions of `flatbuffers`, `arrow`, `datafusion`, and many more dependencies

## [0.102.2] - 2022-12-08
### Change
- Fixed Web UI server code to match `axum-0.6.1`

## [0.102.1] - 2022-12-08
### Changed
- Dependencies upgrade
- Demo environment synchronized

## [0.102.0] - 2022-11-18
### Changed
- Upgraded embedded Web UI to the latest version

## [0.101.0] - 2022-11-17
### Added
- Made core images configurable for customization and ease of experimentation
### Changed
- Updated to latest Jupyter image and all other dependencies

## [0.100.2] - 2022-11-17
### Fixed
- CLI parser crash on `kamu repo alias add/delete` subcommand

## [0.100.1] - 2022-11-17
### Fixed
- CLI parser crash on `kamu sql server --livy` subcommand

## [0.100.0] - 2022-11-14
### Added
- More elaborate SQL error messages propagated from DataFusion via GraphQL API
### Changed
- Upgraded rusttoolchain to fix `thiserror` issues. Backtrace feature is now considered stable
- Fixed issues with `ringbuf` library updates
- Updates related to breaking changes in `clap`
- Utilized value parsing capabilities from `clap` to simplify argument conversions
### Fixed
- Field 'total_count' in pagination views of GraphQL API should be mandatory

## [0.99.0] - 2022-10-01
### Added
- Support custom headers when fetching data from a URL (e.g. HTTP `Authorization` header)

## [0.98.0] - 2022-09-05
### Added
- Progress indication when syncing datasets to and from remote repositories
### Changed
- Upgraded to new `rust` toolchain and latest dependencies

## [0.97.1] - 2022-08-19
### Fixed
- Output truncation in `kamu config *` commands

## [0.97.0] - 2022-08-05 (**BREAKING**)
### Added
- Metadata blocks now contain sequence number (breaking format change!)
- Optimized sync operations for diverged datasets using sequence number in blocks
### Fixed
- Panic when pulling a non-existing dataset

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
  image: "ghcr.io/kamu-data/example:0.1.0"
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

## [0.87.0] - 2022-05-16 (**BREAKING**)
### Changed
- We got rid of `.kamu.local` volume directory in favor of keeping all dataset data under `.kamu/datasets/<name>` folders. This unifies the directory structure of the local workspace with how datasets are stored in remote repositories, and makes it easier to sync datasets to and from.
### Added
- Support for `ipns://` URLs in `kamu pull`

## [0.86.0] - 2022-05-16 (**BREAKING**)
### Changed
- Implements [ODF RFC-006](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/006-checkpoints-as-files.md) to store checkpoints as files and reference them using physical hashes
- Data files are now named and stored accodring to their physical hashes, as per ODF spec
- Above changes also affect the repository format
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

## [0.75.0] - 2022-01-04 (**BREAKING**)
### Changed
- Implements [ODF RFC-004](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/004-metadata-extensibility.md).
- Workspaces will need to be re-created. This is the last major metadata format change - we will be working on stabilizing metadata now.
- Some manifest formats have changed and will need to be updated.
  - `DatasetSnapshot` needs to specify `kind` field (`root` or `derivative`)
  - `DatasetSnapshot.source` was replaced with `metadata` which is an array of metadata events

## [0.74.0] - 2021-12-28 (**BREAKING**)
### Changed
- Implements [ODF RFC-003](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/003-content-addressability.md).
- Workspaces will need to be re-created. Sorry again!
- Some manifest formats have changed and will need to be updated.
  - `Manifest.apiVersion` renamed to `version`
  - `DatasetSnapshot.id` renamed to `name`
  - `DatasetSourceDerivative.inputs` should now specify `id` (optional) and `name` (required)
  - `TemporalTable.id` renamed to `name`
- Datasets now have a globally unique identity.
  - IDs can be viewed using `kamu log -w`
- Metadata format switched to a much faster and compact `flatbuffers`.
  - You can still inspect it as YAML using `kamu log --output-format yaml`

## [0.73.0] - 2021-12-11 (**BREAKING**)
### Changed
- Implements [ODF RFC-002](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/002-logical-data-hashes.md).
- Engines are no longer responsible for data hashing - a stable hash algorithm is implemented in `kamu`
- Pending data part files and checkpoints will be stored in `cache` directory along with other ingest artifacts
### Added
- A fully working implementation of data integrity checks
- `kamu verify` command now accepts `--integrity` flag to only check data hashes without replaying transformations

## [0.72.0] - 2021-12-07 (**BREAKING**)
### Changed
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

## [0.38.0] - 2021-03-28 (**BREAKING**)
### Changed
- Maintenance release
- Upgraded to latest rust toolchain and dependencies
- Updated `flatbuffers` version that includes support for optional fields - this changes binary layout making this new version incompatible with metadata generated by the previous ones
### Fixed
- Uncacheable message will no longer obscure he commit message

## [0.37.0] - 2020-12-30 (**BREAKING**)
### Changed
- Metadata restructuring means you'll need to re-create your datasets
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
