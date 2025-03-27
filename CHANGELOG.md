# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!--
Recommendation: for ease of reading, use the following order:
- Added
- Changed
- Fixed
-->

## [Unreleased]
### Changed
- `DatasetSummary` files replaced with `DatasetStatistics` stored in the database
     and updated synchronously with the HEAD reference updates
- Statistics is automatically pre-computed for all existing datasets on first use
- `DatasetHandle` and `DatasetEntry` now contain dataset kind marker
- Provenance service and pull request planner fetch dependencies from the graph
- Implemented caching layer for `DatasetEntry` within the currently open transaction

## [0.230.0] - 2025-03-25
### Added
- Private Datasets, access sharing:
  - ReBAC: there can be only one role between a subject (account) and an object (dataset)
  - ReBAC: Added `RebacDatasetRegistryFacade` to simplify work with authorization validation
  - GQL: CRUD Collaboration API
  - GQL: Implemented API for searching by account name
  - OSO: Added `maintain` & `own` permissions in addition to `read` & `write`
### Fixed
- Flow APIs: correctly returns response for dataset with cleaned blocks
- Private Datasets:
  - GQL: Correct processing of access permissions when viewing flows of another account

## [0.229.0] - 2025-03-24
### Added
- Experimental support for `ChangelogStream` and `UpsertStream` ODF merge strategies.

## [0.228.1] - 2025-03-23
### Added
- Semantic search:
  - More configuration options for indexing, allowing to skip datasets without descriptions or no data.
  - Overfetch is now configurable
  - Service will make repeated queries to the vector store to fill the requested results page size.
### Changed
- Flow: Updated the `BatchingRule` trigger to accept 0 for both properties(`min_records_to_await` and `max_batching_interval`), enabling dependency flow execution even when no data is added to the root dataset.
### Fixed
- HTTP & GQL API: Fixed internal error when query contains an unknown column

## [0.228.0] - 2025-03-19
### Added
- DB: utilities for working with bind parameter placeholders
- DB-backed dataset references: they are now stored in the database, supporting transactional updates.  
- Ensured short transaction length in ingest & transform updates and compaction tasks.  
- Dataset Reference indexing to build the initial state of the dataset references.  
- Implemented in-memory caching for dataset references that works within the current transaction
### Changed
- Replaced default GraphQL playground with better maintained `graphiql` (old playground is still available)
- Improved API server web console looks
- Upgraded to `datafusion v45` (#1146)
- CI: terminate tests after 5 minutes of execution
- GQL: cleaning up unnecessary `dummy` fields where it can be avoided
- GQL: improve performance by adding shared query state (for query and mutation requests)
- Cleaning up unnecessary `.map_err()` constructs
- DB: cleanup of unnecessary allocations during parameter bindings
- Various small refactorings extracting common methods (e.g. `PaginationOpts::from_page()`)
- Dependency graph updates are improved for transactional correctness.  
- Setting dataset references is now the responsibility of computation service callers. 
- Extracted ODF dataset builders for LFS and S3 to allow for custom implementations.  
- Improved tests for dataset use cases.  
### Fixed
- REST API: `GET /datasets/{id}` returns account data as it should 
- If dataset creation is interrupted before a dataset entry is written, 
   such a dataset is ignored and may be overwritten

## [0.227.1] - 2025-03-14
### Fixed
- Trigger activation during flow throttling correctly save next activation time

## [0.227.0] - 2025-03-13
### Added
- `kamu search` now supports `--local` flag which will use natural language search on datasets in the local workspace (#1136)
  - To use this feature you'll need to configure the OpenAI key in kamu config or set it via `OPENAI_API_KEY` env var
  - By default, uses [Qdrant](https://qdrant.tech/) vector database spawned per command in a container
### Fixed
- `kamu sql server` now works again inside containers (e.g. jupyter)

## [0.226.5] - 2025-03-10
### Fixed
- Prometheus metrics, S3: fixed `storage_url` label values

## [0.226.4] - 2025-03-07
### Changed
- Simple Transfer Protocol & Smart Transfer Protocol use `AppendDatasetMetadataBatchUseCase`
- SQLite: protection against database locking, in case of parallel execution of `kamu` commands.
  - Based on `journal_mode=WAL`
- GQL: speeding up work by reducing allocations
### Fixed
- API server correctly logs unknown routes
- GQL: `Search::query()`: fix regression resulting in unstable ordering of search results
- Fix JSON encoding of `BinaryView` and `Utf8View` data (#1127)


## [0.226.3] - 2025-02-27
### Changed
- `kamu login`: only one argument `--user` (root) is left, other arguments (from subcommands) are removed
### Fixed
- Demo Jupyter start-up failure

## [0.226.2] - 2025-02-26
### Added
- New `FlowSystemConfig` structure in `CLIConfig` which allows 
    to configure `flow_agent` and `task_agent` services 
    with next options `awaiting_step_secs` and `mandatory_throttling_period_secs`
### Fixed
- Single-tenant mode:
  - `kamu add`: public default visibility, unless otherwise specified
  - `kamu pull`: public new default visibility, unless otherwise specified
- Simple Transfer Protocol:
  - Respect the visibility option
  - Updating the dependency graph during block processing

## [0.226.1] - 2025-02-25
### Changed
- New Jupyter image 0.7.1, which can handle v6 workspace layout

## [0.226.0] - 2025-02-24
### Added
- Externally configurable Argon2 hashing mode (minimal settings to speedup e2e tests)
### Changed
- Unified dataset repository format:
  - both LFS and S3, regardless of tenancy config, now organize storage folders solely by dataset ID
  - workspace migration to new repository format (v6) is fully automatic
  - the role of "info/alias" file was reduced only for recovery purposes, 
      the dataset alias resolutions now happen in database only
  - ODF storage unit implementations (LFS, S3) only deal with dataset identifiers:
    - no more dependency on accounts or tenancy configuration
    - S3 cache now considers identifiers of stored datasets only
    - moved them to ODF crate
    - reading HEAD is a must for storage iteration
    - removing HEAD is a first step of dataset deletion
  - `kamu-datasets` domain now has own error and result structures layer, separate from ODF basics
  - Rename operation does not touch a storage unit, became solely a database operation
  - workspace version checking now takes startup job dependencies into account
### Fixed
- Less linear search in in-mem entry repository

## [0.225.3] - 2025-02-24
### Fixed
- E2E: `repo-tests` crate again contains the `kamu-cli` dependency, but as an optional one
  - This way we can correctly reuse the tests in the `kamu-node` repository without affecting the build time
  - It also fixes `make sqlx-prepate` developer command

## [0.225.2] - 2025-02-24
### Added
- Added prometheus metrics for AWS SDK S3 calls
### Changed
- E2E: make `repo-tests` crate independent from `kamu-cli` crate

## [0.225.1] - 2025-02-23
### Fixed
- Fixed arrow `BinaryView` incorrectly being treated as incompatible with `Binary` fields during dataset schema compatibility checks (#1096)

## [0.225.0] - 2025-02-20
### Added
- Added [common-macros](src/utils/common-macros) crate containing macros of general use
  - `kamu list`: display dataset visibility in multi-tenant
  - `kamu pull`: added `--visibility private|public` argument to specify the created dataset visibility
### Changed
- Improved/added trace for repositories & GQL to contain not only the method name but also the structure name
### Fixed
- Restoring OData API tolerance to trailing slashes
- OData API: fixed crash when accessing private dataset

## [0.224.0] - 2025-02-18
### Added
- The usage of local database (SQLite) is activated by default for all single tenant workspaces
### Fixed
- Improved error message for SQL parsing method for queries which includes invalid or reserved keywords
- Fixed false-positive panic ("There cannot be predefined users in a single-tenant workspace") 
    if a `kamu` subcommand that doesn't require workspace found a multiuser `.kamuconfig`
  - For example, before attempting to initialize a workspace or attempting to invoke autocomplete in shell 

## [0.223.0] - 2025-02-13
### Added
- Increased test coverage of the code responsible for access checks
### Changed
- Restructured responsibilities between core and dataset domains upon key dataset CRUD use cases:
  - 8 use cases moved from `core` to `kamu-datasets` domain
  - no longer using outbox for "Renamed" and "DependenciesUpdated" events in datasets, 
      these became internal aspect inside `kamu-datasets` domain
  - simplified creation and commit result structures as they no longer transport new dependencies
  - revised many integration tests:
    - flow system no longer uses real datasets
    - HTTP and GQL use real accounts and dataset entries
- Moved several account-related routines from `AuthenticationService` to `AccountService`, 
  the authentication services has focus only on JWT token and login flows
- Upgraded to `datafusion v45` (#1063)
### Fixed
- GQL metadata query now correctly returns dataset aliases for `SetTransform` event in multi-tenant mode
- Handle panic errors in `kamu inspect lineage -- browse` command
- Improved result messages for `kamu system diagnose` command

## [0.222.0] - 2025-02-06
### Added
- New `AccessTokenLifecycleMessage` outbox message which produced during access token creation
### Changed
- `kamu pull` command now can be called with passing `<remote_repo>/<dataset_name>` arg
  and pull url will be combined automatically 

## [0.221.1] - 2025-01-31
### Fixed
- Private Datasets:
  - Validation `SetTransform` event (input datasets): IDs are used for accessibility checks. 
      Aliases are used only to generate an error message.
  - `OsoDatasetAuthorizer`: readiness to handle duplicates when filtering

## [0.221.0] - 2025-01-29
### Added
- GQL support to query and update email on the currently logged account
- Account registration sends `AccountLifecycleEvent` to `Outbox`
### Changed
- Emails are mandatory for Kamu accounts now:
  - predefined users need to specify an email in config
  - predefined users are auto-synced at startup in case they existed before
  - GitHub users are queried for primary verified email, even if it is not public
  - migration code for the database existing users

## [0.220.0] - 2025-01-27
### Changed
- Private Datasets:
  - GQL, `DatasetFlowRunsMut::trigger_flow()`: added check of input datasets for accessibility for `ExecuteTransform`
  - GQL, `TransformInput::input_dataset()`: dataset may not be accessible
  - GQL, `MetadataChainMut::commit_event`: added check of dataset inputs for availability for `SetTransform`
  - HTTP: added access check for the dataset router (`DatasetAuthorizationMiddleware`), affected:
    - `POST /{dataset}/ingest`
    - `GET /{dataset}/metadata`
    - `GET /{dataset}/tail`
  - HTTP, `GET /datasets/{id}`: access check corrected
  - HTTP: replaced access errors with not found errors
  - CLI, `kamu pull`: replaced access errors with not found errors
- Continued work on use cases extracting:
  - `ViewDatasetUseCase`
  - `EditDatasetUseCase`
  - `GetDatasetDownstreamDependenciesUseCaseImpl`
  - `GetDatasetUpstreamDependenciesUseCaseImpl`

## [0.219.2] - 2025-01-24
### Added
- Reusable static database migrators 

## [0.219.1] - 2029-01-18
### Fixed
- Reverted unstable AWS API dependencies

## [0.219.0] - 2029-01-17
### Changed
- Massive crates restructuring around Open Data Fabric code:
  - `src/odf` concentrates large number of related crates now, preparing for future separation in different Git repo
  - old `opendatafabric` crate became `odf-metadata`
  - low-level repository implementations became `odf-storage[-...]` crates
  - specific storage technologies are gated via features (`lfs`, `s3`, `http`)
  - `DatasetFactory`, `Dataset`, `DatasetSummary`, `DatasetLayout`, `BlockRef`, 
      `MetadataChain` and visiting logic now are residents of `odf-dataset`
  - `kamu-data-utils` became `odf-data-utils`
  - multiple utility libraries introduced, shared both via ODF and main Kamu code 
    (`async-utils`, `file-utils`, `s3-utils`, `test-utils`)
  - `DatasetRepository` still stays in Kamu core, renamed to `DatasetStorageUnit` to better match it's current purpose
  - import statements pointing at ODF code have been cleaned all over the code base (use `odf` meta-crate only outside `src/odf`)


## [0.218.0] - 2029-01-17
### Changed
- Private Datasets:
  - OSO: using user actors / dateset resources that come from the database
    - Thus, any access check relies on real entities
  - GQL, added `Dataset.visibility()` to get the current visibility value
  - GQL, added `DatasetMut.setVisibility()` to be able to change the dataset visibility after it has been created
  - Deletion of previously created (and unused) ReBAC-properties and reindexing
  - OSO: updating the schema to use identifiers instead of names
  - OSO: added resource storage for access speed
  - E2E: Using the correct account in multi-tenant mode
    - And also the possibility of set it up
  - `DatasetOwnershipService`: moved to the `kamu-dataset` crate area & implemented via `DatasetEntryServiceImpl`
  - GQL, `DatasetMetadata.currentUpstreamDependencies`: indication if datasets not found/not accessed
  - GQL, `DatasetMetadata.currentDownstreamDependencies`: exclude datasets that cannot be accessed
  - E2E: added the ability to create an account using CLI

## [0.217.3] - 2025-01-14
### Fixed
- Fix crash on resolving dataset by non-existing account
- Minor improvements in event sourcing aggregation

## [0.217.2] - 2025-01-10
### Changed
- Updated to latest `datafusion` and `alloy` dependencies
- Performance improvements with batch loading of event sourcing aggregates

## [0.217.1] - 2025-01-09
### Changed
- Extended database config options with next fields: `maxConnections`, `maxLifeTimeSecs` and `acquireTimeoutSecs`

## [0.217.0] - 2025-01-08
### Changed
- GraphQL: flows are listed ordered by status and last event time
- Merged two methods(`saveEnvVariable` and `modifyEnvVariable`) from `DatasetEnvVarsMut` info one `upsertEnvVariable`
### Fixed
- GQL api flows queries now fetch dataset polling source only once per dataset(and only if Ingest flow type is here)
- Flow trigger status now become disable on flow fail

## [0.216.0] - 2024-12-30
### Changed
- Flight SQL protocol now supports anonymous and bearer token authentication
- The `kamu notebook` command now defaults to `DataFusion` engine for speed, but you can switch to Spark with `--engine spark` argument
- The `kamu notebook` command uses new image based on latest Jupyter and new [`kamu-client-python`](https://github.com/kamu-data/kamu-client-python) library
- The `kamu sql server` command now defaults to `DataFusion` engine with interface changed to use `--engine datafusion/spark`, removing the `--flight-sql` flag
- Examples in `examples/flightsql/python` were updated to new auth and showcasing `kamu` Python library
- Most notebooks in `examples/` directory are using `kamu` Python library with `DataFusion` engine, with Spark still in use for GIS extensions

## [0.215.1] - 2024-12-30
### Fixed
- GraphQL: in a multi-tenant workspace, `datasets.createEmpty` and `datasets.createFromSnapshot` mutations now return dataset aliases prefixed with account name.
- Fix DB transaction error in `/verify` REST endpoint (cherry-picked from `0.214.1`)

## [0.215.0] - 2024-12-27
### Added
- New entity `FlowTrigger` which is now responsible for flow activation and schedules
### Changed
- `DatasetFlowConfigsMut` now has only one method `setConfig` for all types of configurations

## [0.214.0] - 2024-12-23
### Added
- New `kamu system decode` command that can decode an arbitrary block file for debugging
- `export` command for bulk data exporting
### Changed
- `sql` command now allows to export query command results to file(s)
- FlightSQL session state management improvements

## [0.213.1] - 2024-12-18
### Fixed
- Removed all occurrences of `DataWriterMetadataState` from telemetry spans (too much pressure)

## [0.213.0] - 2024-12-18
### Added
- kamu-adapter-graphql: added macros (`from_catalog_n!()` & `unsafe_from_catalog_n!()`)
   that simplify the extraction of components from the DI catalog
- database-common: the logic for pagination of data processing is generalized in `EntityPageStreamer`
### Changed
- Speed up project build time by removing unused dependencies which were not detected by automated tools
- Extracted "planner" and "executor" for compacting, reset, set watermark, push ingest, partially polling ingest.
- Renamed long-running "executors" to "agents".
- Introduced `MetadataQueryService` to absorb simple queries that do not have to be defined at the level of metadata chian from the interface point of view.
### Fixed
- `DatasetEnvVar` entity now deletes during deleting `DatasetEntry` entity

## [0.212.0] - 2024-12-11
### Changed
- Upgraded to `datafusion v43`
### Fixed
- Ingest was sometimes producing Parquet files with non-sequential `offset` column which violated the ODF spec

## [0.211.0] - 2024-12-02
### Changed
- Dataset dependency graph is now backed with a database, removing need in dependency scanning at startup.

## [0.210.0] - 2024-11-28
### Added
- Console warning when deleting datasets which are out of sync with their push remotes
### Changed
- Separated Web UI runtime and UI configuration flags. UI configuration is now provided by API server too.
### Fixed
- Typo in feature flags (enableDatasetEnvVarsManagement)
                                                  ^

## [0.209.0] - 2024-11-25
### Changed
- Improved OpenAPI integration
- Replaced Swagger with Scalar for presenting OpenAPI spec
### Fixed
- `EXECUTE_TRANSFORM` flows now respect last success run time during config enabling and api-server restarting
- `kamu login`: add repo with `odf+` schema protocol

## [0.208.1] - 2024-11-22
### Fixed
- `kamu-base-with-data-mt` image building

## [0.208.0] - 2024-11-21
### Added
Introduced `DatasetRegistry` abstraction, encapsulating listing and resolution of datasets:
- Registry is backed by database-stored dataset entries, which are automatically maintained
- Scope for `DatasetRepository` is now limited to support `DatasetRegistry` and in-memory dataset dependency graph
- New concept of `ResolvedDataset`: a wrapper around `Arc<dyn Dataset>`, aware of dataset identity
- `DatasetRegistryRepoBridge` utility connects both abstractions in a simple way for testing needs
- Query and Dataset Search functions now consider only the datasets accessible for current user
- Core services now explicitly separate planning (transactional) and execution (non-transactional) processing phases
- Similar decomposition introduced in task system execution logic
- Revised implementation of core commands and services: `pull`, `push`, `reset`, `verify`, `compact`, setting watermark
- More parallelism from `pull` command, allowing to mix ingest/sync/transform operations of the same depth level
- Optimized `pull` flow, when a single non-recursive dataset is sent for processing
- Batched form for dataset authorization checks
- Ensuring correct transactionality for dataset lookup and authorization checks all over the code base
- Passing multi/single tenancy as an enum configuration instead of boolean
- Renamed outbox "durability" term to "delivery mechanism" to clarify the design intent
- Greatly reduced complexity and code duplication of many use case and service tests with `oop` macro for inheritance of harnesses

## [0.207.3] - 2024-11-21
### Changed
- Add version for `OutboxMessage` structure to prevent startup failures after breaking changes

## [0.207.2] - 2024-11-15
### Fixed
- E2E: revision of st/mt tests:
  - In cases where temporary workspaces are created,
     test variants for both single-tenant and multi-tenant have been added
  - New combinations activated
  - Certain duplicate tests have been removed
  - Some of the tests related to `kamu pull` only have been moved to the appropriate module
  - Activated missing tests for databases
- `kamu push`: crash in multi-tenant mode

## [0.207.1] - 2024-11-14
### Fixed
- `kamu pull`: crash in multi-tenant mode

## [0.207.0] - 2024-11-11
### Added
- E2E: reinforce test coverage
  - Covered all flow scenarios
  - Covered hot REST API endpoints
  - Reconfiguring test groups for a small speedup (10%)
  - Directory structure grooming
  - `KamuApiServerClientExt`: method grouping
- Dataset definition: added possibility to set defaults in templates:
  ```yaml
  fetch:
    kind: Container
    image: "ghcr.io/kamu-data/fetch-com.defillama:0.1.5"
    args:
      - --request-interval
      - '${{ env.request_interval || 2 }}'
  ```
### Changed
- HTTP API errors will now come in JSON format instead of plain text, for example:
  ```json
  { "message": "Incompatible client version" }
  ```
- GQL: The `DataQueryResultSuccess` type is extended to the optional `datasets` field,
   which contains information about the datasets participating in the query. Affected API:
  - `GQL DataQueries`: the field will be filled
  - `GQL DatasetData`: field will be empty because we already know which dataset is involved
### Fixed
- `kamu add` correctly handles snapshots with circular dependencies
- `kamu push` shows a human-readable error when trying to push to the non-existing repository
- Jupyter repository block documentation misleading

## [0.206.5] - 2024-10-29
### Changed
- Allow anonymous access to the content of recently uploaded files
- Updated to `arrow 53.2`, `datafusion 42.1`, `tower 0.6`, `opentelemetry 27` + minor updates

## [0.206.4] - 2024-10-28
### Fixed
- `kamu push` correctly handle `odf+` format repositories

## [0.206.3] - 2024-10-28
### Fixed
- Improved telemetry for dataset entry indexing process
- Corrected recent migration related to outbox consumptions of old dataset events

## [0.206.2] - 2024-10-26
### Changed
- GraphQL: Removed deprecated `JSON_LD` in favor of `ND_JSON` in `DataBatchFormat`
- GraphQL: In `DataBatchFormat` introduced `JSON_AOS` format to replace the now deprecated `JSON` in effort to harmonize format names with REST API
### Fixed
- GraphQL: Fixed invalid JSON encoding in `PARQUET_JSON` schema format when column names contain special characters (#746)

## [0.206.1] - 2024-10-24
### Changed
- `kamu repo list`: supports all types of output
- Tests: `sqlx + nextest` combination has been stabilized
- `DatasetEntryIndexer`: guarantee startup after `OutboxExecutor` for a more predictable initialization
  - Add `DatasetEntry`'is re-indexing migration
### Fixed
- `kamu push`: show correct error if server failed to store data

## [0.206.0] - 2024-10-22
### Added
- Introduced OpenAPI spec generation
  - `/openapi.json` endpoint now returns the generated spec
  - `/swagger` endpoint serves an embedded Swagger UI for viewing the spec directly in the running server
  - OpenAPI schema is available in the repo `resources/openapi.json` beside its multi-tenant version
- Added and expanded many E2E tests, improved test stability
- Added endpoint to read a recently uploaded file (`GET /platform/file/upload/{upload_token}`)
### Changed
- Removed support for deprecated V1 `/query` endpoint format
- The `/tail` endpoint was updated to better match V2 `/query` endpoint
### Fixed
- `kamu add`: fixed behavior when using `--stdin` and `--name` arguments

## [0.205.0] - 2024-10-15
### Changed
- `kamu push <dataset>` command now can be called without `--to` reference and Alias or Remote dataset repository will be used as destination
- `kamu login` command now will store repository to Repository registry. Name can be provided with `--repo-name` flag and to skip creating repo can be used `--skip-add-repo` flag

## [0.204.5] - 2024-10-08
### Added
- Postgres implementation for dataset entry and account Re-BAC repositories
### Changed
- `kamu repo alias list`: added JSON output alongside with other formats mentioned in the command's help
- Private Datasets, `DatasetEntry` integration that will allow us to build dataset indexing
  - Added `DatasetEntryService` for message processing
  - Added `DatasetEntryIndexer` for one-shot indexing
  - Extend `DatasetLifecycleMessageCreated` with `dataset_name` field
  - Introducing `DatasetLifecycleMessageRenamed`
- Simplified error handling code in repositories
- Hidden part of the test code behind the feature gate
- Updated our crate dependencies so they can be built in isolation
### Fixed
- `--yes / -y` flag: fixed when working from a TTY
- CI: Fixes `kamu-base-with-data-mt` image builds

## [0.204.4] - 2024-09-30
### Changed
- CLI command tweaks:
  - Make `--yes / -y` flag global
  - Add confirmation step to `system compact` command
  - Add support for patterns to `system compact` to process multiple datasets at once
  - Fixed argument parsing error in `kamu system compact` command
- Simplified organization of startup initialization code over different components
### Fixed
- Broken catalog issue for server and transactional modes
  - Added several E2E tests (happy paths) covering the Flows tab in the UI
- Corrected behavior of `MySqlAccountRepository::get_accounts_by_ids()`, for the case of empty IDs collection

## [0.204.3] - 2024-09-26
### Fixed
- Dataset creation with unique alias but with existing id for FS dataset storage mode
- `kamu init`: fixed regression in case of using `exists_ok` flag... finally

## [0.204.2] - 2024-09-26
### Fixed
- `kamu init`: fixed regression in case of using `exists_ok` flag

## [0.204.1] - 2024-09-25
### Fixed
- Fixed build regression, in case `web-ui` feature flag is used

## [0.204.0] - 2024-09-25
### Changed
- If not explicitly configured, a SQLite database is used for a multi-tenant workspace
- If a SQLite database is used, built-in migrations are automatically applied
- Start processing added Outbox messages after successful command execution
- DI: `ServerCatalog` added, to split dependencies

## [0.203.1] - 2024-09-24
### Added
- Added database migration & scripting to create an application user with restricted permissions
- `kamu delete` command will respect dependency graph ordering allowing to delete multiple datasets without encountering dangling reference

## [0.203.0] - 2024-09-22
### Added
- Support `List` and `Struct` arrow types in `json` and `json-aoa` encodings

## [0.202.1] - 2024-09-20
### Fixed
- Open Telemetry integration fixes

## [0.202.0] - 2024-09-20
### Changed
- Major dependency upgrades:
  - DataFusion 42
  - HTTP stack v.1
  - Axum 0.7
  - latest AWS SDK
  - latest versions of all remaining libs we depend on
- Outbox refactoring towards true parallelism via Tokio spaned tasks instead of futures
### Fixed
- Failed flows should still propagate `finishedAt` time
- Eliminate `span.enter`, replaced with instrument everywhere

## [0.201.0] - 2024-09-18
### Added
- REST API: New `/verify` endpoint allows verification of query commitment as per [documentation](https://docs.kamu.dev/node/commitments/#dispute-resolution) (#831)
### Changed
- Outbox main loop was revised to minimize the number of transactions:
    - split outbox into planner and consumption jobs components
    - planner analyzes current state and loads a bunch of unprocessed messages within a 1 transaction only
    - consumption jobs invoke consumers and detect their failures
- Detecting concurrent modifications in flow and task event stores
- Improved and cleaned handling of flow abortions at different stages of processing
- Revised implementation of flow scheduling to avoid in-memory time wheel:
    - recording `FlowEventScheduledForActivation` event (previously, placement moment into the time wheel)
    - replaced binary heap based time wheel operations with event store queries
    - Postgres/SQLite event stores additionally track activation time for the waiting flows
    - in-memory event store keeps prepared map-based lookup structures for activation time

## [0.200.0] - 2024-09-13
### Added
- Added first integration of Prometheus metrics starting with Outbox
- Added `--metrics` CLI flag that will dump metrics into a file after command execution
### Changed
- Telemetry improvements:
   - Improved data collected around transactional code
   - Revised associating span objects with large JSON structures such as messages
   - Suppressed several noisy, but not very useful events
- Improved Outbox stability when message consumers fail
- Similarly, Task Executor keeps executing next tasks in case running a task results in an internal error

## [0.199.3] - 2024-09-11
### Fixed
- Associating correct input dataset that was hard compacted with the error during transformation of derived dataset

## [0.199.2] - 2024-09-09
### Added
- REST API: The `/query` endpoint now supports response proofs via reproducibility and signing (#816)
- REST API: New `/{dataset}/metadata` endpoint for retrieving schema, description, attachments etc. (#816)
### Fixed
- Fixed unguaranteed ordering of events when restoring event sourcing aggregates
- Enqueuing and cancelling future flows should be done with transactions taken into account (via Outbox)

## [0.199.1] - 2024-09-06
### Fixed
- Fixed crash when a derived dataset is manually forced to update while an existing flow
  for this dataset is already waiting for a batching condition

## [0.199.0] - 2024-09-06
### Added
- Persistency has been enabled for Task and Flow domains.
  Both `TaskExecutor` and `FlowExecutor` now fully support transactional processing mode,
  and save state in Postgres or Sqlite database.
- Tasks now support attaching metadata properties. Storing task->flow association as this type of metadata.
- Flows and Tasks now properly recover the unfinished requests after server restart
### Changed
- Simplified database schema for flow configurations and minimized number of migrations
   (breaking change of the database schema)
- Introduced `pre_run()` phase in flow executor, task executor & outbox processor to avoid startup races
- Explicit in-memory task queue has been eliminated and replaced with event store queries
- Get Data Panel: use SmTP for pull & push links
- GQL api method `setConfigCompaction` allows to set `metadataOnly` configuration for both root and derived datasets
- GQL api `triggerFlow` allows to trigger `HARD_COMPACTION` flow in `metadataOnly` mode for both root and derived datasets

## [0.198.2] - 2024-08-30
### Added
- Container sources allow string interpolation in env vars and command
- Private Datasets, changes related to Smart Transfer Protocol:
  - `kamu push`: added `--visibility private|public` argument to specify the created dataset visibility
  - Send the visibility attribute in the initial request of the push flow
### Changed
- Schema propagation improvements:
  - Dataset schema will be defined upon first ingest, even if no records were returned by the source
  - Schema will also be defined for derivative datasets even if no records produced by the transformation
  - Above ensures that datasets that for a long time don't produce any data will not block data pipelines
- Smart Transfer Protocol:
  - Use `CreateDatasetUseCase` in case of creation at the time of the dataset pulling
  - Now requires the `x-odf-smtp-version` header, which is used to compare client and server versions to prevent issues with outdated clients

## [0.198.1] - 2024-08-28
### Added
- Private Datasets, ReBAC integration:
  - ReBAC properties update based on `DatasetLifecycleMessage`'s:
  - `kamu add`: added hidden `--visibility private|public` argument, assumed to be used in multi-tenant case
  - GQL: `DatasetsMut`:
    - `createEmpty()`: added optional `datasetVisibility` argument
    - `createFromSnapshot()`: added optional `datasetVisibility` argument

## [0.198.0] - 2024-08-27
### Changed
- If a polling/push source does not declare a `read` schema or a `preprocess` step (which is the case when ingesting data from a file upload) we apply the following new inference rules:
  - If `event_time` column is present - we will try to coerce it into a timestamp:
    - strings will be parsed as RFC3339 date-times
    - integers will be treated as UNIX timestamps in seconds
  - Columns with names that conflict with system columns will get renamed
- All tests related to databases use the `database_transactional_test` macro
- Some skipped tests will now also be run
- Access token with duplicate names can be created if such name exists but was revoked (now for MySQL as well)
- Updated `sqlx` crate to address [RUSTSEC-2024-0363](https://rustsec.org/advisories/RUSTSEC-2024-0363)
### Fixed
- Derivative transform crash when input datasets have `AddData` events but don't have any Parquet files yet

## [0.197.0] - 2024-08-22
### Changed
- **Breaking:** Using DataFusion's [`enable_ident_normalization = false`](https://datafusion.apache.org/user-guide/configs.html) setting to work with upper case identifiers without needing to put quotes everywhere. This may impact your root and derivative datasets.
- Datafusion transform engine was updated to latest version and includes JSON extensions
- **Breaking:** Push ingest from `csv` format will default to `header: true` in case schema was not explicitly provided
- Access token with duplicate names can be created if such name exists but was revoked
- Many examples were simplified due to ident normalization changes
### Fixed
- Crash in `kamu login` command on 5XX server responses
- The push smart protocol now delivers internal errors to the client
### Added
- HTTP sources now include `User-Agent` header that defaults to `kamu-cli/{major}.{minor}.{patch}`
- Externalized configuration of HTTP source parameters like timeouts and redirects
- CI: build `sqlx-cli` image if it is missing

## [0.196.0] - 2024-08-19
### Added
- The `/ingest` endpoint will try to infer the media type of file by extension if not specified explicitly during upload.
   This resolves the problem with `415 Unsupported Media Type` errors when uploading `.ndjson` files from the Web UI.
- Private Datasets, preparation work:
  - Added SQLite-specific implementation of ReBAC repository
  - Added SQLite-specific implementation of `DatasetEntryRepository`
- `internal-error` crate:
  - Added `InternalError::reason()` to get the cause of an error
  - Added methods to `ResultIntoInternal`:
    - `map_int_err()` - shortcut for `result.int_err().map_err(...)` combination
    - `context_int_err()` - ability to add a context message to an error
- Added macro `database_transactional_test!()` to minimize boilerplate code
### Changed
- Upgraded `sqlx` crate to v0.8
- Renamed `setConfigSchedule` GQL api to `setConfigIngest`. Also extended
  `setConfigIngest` with new field `fetchUncacheable` which indicates to ignore cache
  during ingest step

## [0.195.1] - 2024-08-16
### Fixed
- Add `reset` ENUM variant to `dataset_flow_type` in postgres migration

## [0.195.0] - 2024-08-16
### Added
- Reliable transaction-based internal cross-domain message passing component (`MessageOutbox`), replacing `EventBus`
  - Metadata-driven producer/consumer annotations
  - Immediate and transaction-backed message delivery
  - Background transactional message processor, respecting client idempotence
- Persistent storage for flow configuration events
### Changed
- Upgraded to `datafusion v41` (#713)
- Introduced use case layer, encapsulating authorization checks and action validations, for first 6 basic dataset scenarios
   (creating, creating from snapshot, deleting, renaming, committing an event, syncing a batch of events),
- Separated `DatasetRepository` on read-only and read-write parts
- Isolated `time-source` library
### Fixed
- E2E: added additional force off colors to exclude sometimes occurring ANSI color sequences
- E2E: modify a workaround for MySQL tests

## [0.194.1] - 2024-08-14
### Fixed
- Add `recursive` field to `Reset` flow configurations in GQL Api which triggers `HardCompaction` in `KeepMetadataOnly` mode flow for each owned downstream dependency

## [0.194.0] - 2024-08-13
### Changed
- Change `mode` argument for `DatasetEnvVarsConfig` to `enabled: Option<bool>`
### Added
- New `Reset` flow in GQL Api which can be triggered manually for `Root` and `Derivative` datasets
- Private Datasets, preparation work:
  - Added in-mem implementation of ReBAC repository
  - Added in-mem implementation of `DatasetEntryRepository`

## [0.193.1] - 2024-08-09
### Fixed
- Panic for `EXECUTE_TRANSFORM` flow without dataset env vars enabled feature

## [0.193.0] - 2024-08-07
### Added
- `kamu add` command accepts optional `--name` argument to add a snapshot under a different name

## [0.192.0] - 2024-08-07
### Added
- `kamu --no-color` to disable color output in the terminal.
### Changed
- New recursive flag for `CompactionConditionFull` input to trigger
  Hard compaction with keep metadata only mode for each derived dataset
- E2E: Reorganized work with tests that call `kamu-cli`:
  - Added `kamu-cli-puppet` crate to allow `kamu-cli` to be run as a separate process from tests
  - Removed past `kamu-cli` wrapper that ran in-process.
  - Some of `kamu-cli` tests that are inherently E2E are moved and adapted to E2E scope (in-mem area)
  - For convenience, the test run macros are now procedural
  - Various Windows-related tweaks & fixes
### Fixed
- Return `RootDatasetCompacted` error for manual triggered `EXECUTE_TRANSFROM` flows
- Using new Spark image that fixes serialization errors when working with large GIS datasets
- Fixed container runtime for systems using SELinux

## [0.191.5] - 2024-07-30
### Fixed
- Ingest flow panic with database api mode

## [0.191.4] - 2024-07-22
### Fixed
- Parsing passwords includes specific symbols in database common crate

## [0.191.3] - 2024-07-22
### Fixed
- Script for api server database migration issue with passwords includes
  specific symbols

## [0.191.2] - 2024-07-18
### Fixed
- Panic for `DatasetEnvVars` API with wrong configuration
- Moved `DATASET_ENV_VAR_ENCRYPTION_KEY`from env to config

## [0.191.1] - 2024-07-16
### Fixed
- `opendatafabric` crate was not compiling without `sqlx` feature

## [0.191.0] - 2024-07-16
### Fixed
- Obscure error during push duplicate dataset with different aliases
- Get Data Panel: fixed `base_url_rest` calculation  in case the API server is started on a random port
- Minor corrections to references in generated code documentation
### Changed
- Upgraded `rustc` version and some dependencies
- E2E: unblocked parallel run for tests

## [0.190.1] - 2024-07-10
### Fixed
- `DatasetEnvVars` inmem deleting

## [0.190.0] - 2024-07-09
### Added
- New repository `DatasetEnvVars` to work with dataset secrets
- Now is possible to set new `DatasetEnvVars` configuration to `storage`
  and manage it via GQL api
- New Gql APIs to manage dataset env vars
  - `listEnvVariables` to fetch list of env vars by dataset
  - `exposedValue` to get secret value of env var by id
  - `saveEnvVariable` to store new dataset env var
  - `deleteEnvVariable` to delete dataset env var
  - `modifyEnvVariable` to modify dataset env var

## [0.189.7] - 2024-07-04
### Added
- Added Kamu access token E2E test
- SmTP: added E2E test group to cover the pushing and pulling of datasets
### Changed
- Wrapping only necessary, not all HTTP requests in a transaction
- Respect the `--quiet` option for:
  - `kamu add`
  - `kamu ingest`
  - `kamu push`
### Fixed
- Fixed SmTP working together with transactions

## [0.189.6] - 2024-07-03
### Fixed
- GQL API regression where unparsable SQL was ending up in internal error
- REST API `/query` endpoint will return `400 Bad Request` in case of unparsable SQL
- Bug fixed in database IAM token authentication method (redundant session token request)

## [0.189.4] - 2024-07-02
### Fixed
- GQL access token list pagination

## [0.189.3] - 2024-07-02
### Fixed
- SQLX images now include scripts and programs necessary to fetch database credentials from AWS secrets manager

## [0.189.2] - 2024-07-01
### Fixed
- AWS secret stores both user name and password, so database username should be a secret too.

## [0.189.1] - 2024-06-28
### Fixed
- Modify revoke access token GQL response according to design

## [0.189.0] - 2024-06-28
### Added
- Support multiple methods to access database:
  - via raw password
  - via password stored as AWS secret
  - via generated AWS IAM authentication tokens
- Support periodic database password rotation with configurable period

## [0.188.6] - 2024-06-27
### Fixed
- Added missed field to create access token result API

## [0.188.5] - 2024-06-26
### Added
- `kamu system api-server`: Added the option of overriding the base URL in the API with `--external-address`.
  Can be handy when launching inside a container

## [0.188.4] - 2024-06-25
### Changed
- Renamed all places compacting -> compacting

## [0.188.3] - 2024-06-24
### Fixed
- Fixed support of  ingestion via file upload in `kamu ui` mode
  and `kamu system api-server` mode with custom HTTP port
- Fixed launching `kamu ui` mode (related to transactions management)

## [0.188.2] - 2024-06-20
### Added
- Added an E2E test group for REST API
### Changed
- MySQL E2E tests are turned off
- The `<owner/dataset>/tail` REST API endpoint will:
  - return `404 Not Found` on not found datasets
  - return `202 No Content` on empty datasets
### Fixed
- Stabilized query handling in case of database usage; affected:
  - The `<owner/dataset>/tail` REST API
  - The `/query` REST API
  - `kamu system tail` command
  - `kamu sql` command
- Fixed memory leak when working with DataFusion

## [0.188.1] - 2024-06-17
### Changed
- The `/query` REST API endpoint will:
  - return `404 Not Found` on not found datasets
  - return `400 Bad Request` on invalid SQL
  - return `422 Unprocessable Content` on unrecognized request body fields

## [0.188.0] - 2024-06-14
### Added
- New repository `AccessTokenRepository` to work with new access tokens
- Middleware now accept new token format `Bearer ka_*`
- New Gql APIs to manage new access tokens
  - `listAccessTokens` to fetch access tokens by account
  - `createAccessToken` to create new access token for account
  - `revokeAccessToken` to revoke existing access token

## [0.187.0] - 2024-06-14
### Added
- The `/query` REST API endpoint now supports:
  - POST requests with all parameters being passed via body
  - Specifying schema format
  - Specifying `aliases` to associate table names with specific dataset IDs
  - Returning and providing state information to achieve full reproducibility of queries

## [0.186.0] - 2024-06-13
### Added
- New `EthereumLogs` polling source allows to stream and decode log data directly from any ETH-compatible blockchain node
  - See the updated `examples/reth-vs-snp500` example
  - See the new [`datafusion-ethers`](https://github.com/kamu-data/datafusion-ethers) crate for implementation details
- Added E2E test infrastructure
  - Added necessary components for managed run -- for startup, operations, and shutdown
### Changed
- Upgraded to `arrow 52` and `datafusion 39`
- Improved binary data formatting in CLI table output - instead of the `<binary>` placeholder it will display an abbreviated hex values e.g. `c47cf6…7e3755`
- JSON and CSV formatters can now output binary data - it will be `hex`-encoded by default
- Hidden arguments and options are excluded from [the CLI reference](resources/cli-reference.md)
### Fixed
- JSON formatter now properly supports `Decimal` types
- Stabilized startup using connection to databases
  - Added HTTP middleware that wraps each request into a separate transaction
  - Also added wrapping for some commands, in particular `kamu system generate-token`
  - The structure of services that required lazy access to databases was reorganized:
     - Extracted `PredefinedAccountsRegistrator` & `DatasetOwnershipServiceInMemoryStateInitializer`
- Fixed potential crash when attempting to rollback a transaction if the connection fails to establish

## [0.185.1] - 2024-06-07
### Fixed
- Fixed support of `--force` mode for pull/push actions using Smart Transfer Protocol

## [0.185.0] - 2024-06-06
### Added
- New `--reset-derivatives-on-diverged-input` flag to `kamu pull` command, which will trigger
  compaction for derived dataset if transformation fails due to root dataset compaction and retry transformation
- Initial support for ingestion via file uploads, with local FS and S3-based storage for temporary files
### Changed
- `AddPushSource` event may omit specifying a schema. In this case, the very first push ingestion invocation
  would try to make a best-effort auto-inference of the data schema.
### Fixed
- Fixed issue with smart protocol transfer operations upon empty datasets

## [0.184.0] - 2024-05-28
### Changed
- `InitiatorFilterInput` now accept `[AccountID]` instead of `AccountName`
  `AccountFlowFilters` now filter by `DatasetId` instead of `DatasetName`.
- Upgraded to `datafusion v38`
### Added
- Added a public image with [sqlx-cli](/images/sqlx-cli)
- Added a [configuration](/images/persistent-storage) of running a `kamu` API server along with a database,
  for persistent storage of data
- New `listFlowInitiators` api to fetch all initiators of flows
- New `allPaused` method in `AccountFlowConfigs` API

## [0.183.0] - 2024-05-22
### Added
- New `keep_metadata_only` flag to `HardCompaction` flow. Also extended `FlowState` with `ConfigSnapshot`
  and possibility to pass configuration during triggering a flow
### Fixed
- Added support of `--all` flag to the `kamu delete` command
- Made recursive deletion dataset with provided `%` pattern
### Changed
- `HardCompaction` configuration now is one of `Full` or `KeepMetadataOnly` variants. In case of
  `KeepMetadataOnly` variant required to provide `recursive` value which will trigger downstream
  dependencies compaction

## [0.182.0] - 2024-05-20
### Added
- Loading database components relying on CLI config
### Fixed
- Panic when creating a workspace with an existing config

## [0.181.2] - 2024-05-20
### Fixed
- `kamu login --check` supports searching both by frontend and backend URL

## [0.181.1] - 2024-05-20
### Fixed
- Panic when resolving datasets in GraphQL API for unregistered accounts

## [0.181.0] - 2024-05-14
### Added
- Introduced MQTT protocol support (see [FetchStepMqtt](https://docs.kamu.dev/odf/reference/#fetchstepmqtt) and the new [`mqtt` example](/examples/mqtt))
- The `kamu system compact` command now accepts the `--keep-metadata-only` flag, which performs hard
  compaction of dataset(root or derived) without retaining `AddData` or `ExecuteTransform` blocks
### Fixed
- Panic while performing data ingesting in the derived datasets

## [0.180.0] - 2024-05-08
### Added
- GraphQL account flows endpoints:
  - List of flows by account(including filters `byDatasetName`, `byFlowType`, `byStatus`, `byInitiator`)
  - Pause all flows by account
  - Resume all flows by account
### Changed
- Correct JWT config creation for `kamu-node`
### Fixed
- `kamu repo alias rm` command regression crash. Changed accepted type and covered by integration test

## [0.179.1] - 2024-05-07
### Changed
- Adding derived traits needed for `kamu-node`

## [0.179.0] - 2024-05-06
### Changed
- Refactoring of authorization configuration data: separation into configuration models and logic for loading them from environment variables

## [0.178.0] - 2024-05-04
### Added
- Flow system: implemented persistent repositories (PostgreSQL, SQLite) for flow configuration events
- Support for persistent accounts:
  - supports Postgres, MySQL/MariaDB, SQLite database targets
  - accounts have a fully functional DID-identifier:
    - based on account name for CLI mode
    - auto-registered randomly on first GitHub login
  - account ID resolutions are no longer mocked
- REST API to login remotely using password and GitHub methods
### Fixed
- Compaction datasets stored in an S3 bucket

## [0.177.0] - 2024-04-25
### Added
- REST data APIs and CLI commands now support different variations of JSON representation, including:
  - `Array-of-Structures` e.g. `[{"c1": 1, "c2": 2}, {"c1": 3, "c2": 4}]` (default)
  - `Structure-of-Arrays` e.g. `{"c1": [1, 3], "c2": [2, 4]}`
  - `Array-of-Arrays` e.g. `[[1, 2], [3, 4]]`
- REST data APIs now also return schema of the result (pass `?schema=false` to switch it off)
### Changed
- Upgraded to `datafusion` `v37.1.0`
- Split the Flow system crates
### Fixed
- `kamu system diagnose` command crashes when executed outside the workspace directory

## [0.176.3] - 2024-04-18
### Changed
- Updated KAMU_WEB_UI image to latest release 0.18.1

## [0.176.2] - 2024-04-16
### Fixed
- Fix the instant run of `ExecuteTransform` flow after the successful finish of `HardCompaction` flow
- Extend `FlowFailed` error type to indicate when `ExecuteTransform` failed due to `HardCompaction` of root dataset

## [0.176.1] - 2024-04-16
### Fixed
- Split result for different(`setFlowConfigResult`, `setFlowBatchingConfigResult`, `setFlowCompactionConfigResult`) flow configuration mutations

## [0.176.0] - 2024-04-15
- New engine based on RisingWave streaming database ([repo](https://github.com/kamu-data/kamu-engine-risingwave)) that provides mature streaming alternative to Flink. See:
  - Updated [supported engines](https://docs.kamu.dev/cli/supported-engines/) documentation
  - New [top-n](https://docs.kamu.dev/cli/get-started/examples/leaderboard/) dataset example highlighting retractions
  - Updated `examples/covid` dataset where RisingWave replaced Flink in tumbling window aggregation

## [0.175.0] - 2024-04-15
### Added
- The `kamu ingest` command can now accept `--event-time` hint which is useful for snapshot-style data that doesn't have an event time column
- The `/ingest` REST API endpoint also supports event time hints via `odf-event-time` header
- New `--system-time` root parameter allows overriding time for all CLI commands
### Fixed
- CLI shows errors not only under TTY
- Removed `paused` from `setConfigCompaction` mutation
- Extended GraphQL `FlowDescriptionDatasetHardCompaction` empty result with a resulting message
- GraphQL Dataset Endpoints object: fixed the query endpoint

## [0.174.1] - 2024-04-12
### Fixed
- Set correct ODF push/pull websocket protocol

## [0.174.0] - 2024-04-12
### Added
- `HardCompaction` to flow system

## [0.173.0] - 2024-04-09
### Added
- OData API now supports querying by collection ID/key (e.g. `account/covid.cases(123)`)
### Fixed
- Handle broken pipe panic when process piping data into `kamu` exits with an error
- GraphQL Dataset Endpoints object: tenant-insensitive paths & updated REST API push endpoint

## [0.172.1] - 2024-04-08
### Fixed
- Add precondition flow checks
- Fix URLs for Get Data panel

## [0.172.0] - 2024-04-08
### Added
- Added persistence infrastructure prototype based on `sqlx` engine:
   - supports Postgres, MySQL/MariaDB, SQLite database targets
   - sketched simplistic Accounts domain (not-integrated yet)
   - converted Task System domain to use persistent repositories
   - added test infrastructure for database-specific features
   - automated and documented development flow procedures in database offline/online modes

## [0.171.0] - 2024-04-05
### Added
- Support `ArrowJson` schema output format in QGL API and CLI commands
- New `kamu system compact <dataset>` command that compacts dataslices for the given dataset
### Changed
- Case-insensitive comparisons of `dataset`s, `account`s and `repo`s

## [0.170.0] - 2024-03-29
### Added
- Added GraphQL Dataset Endpoints object
### Changed
- REST API: `/ingest` endpoint will return HTTP 400 error when data cannot be read correctly
- Improved API token generation command

## [0.169.0] - 2024-03-25
### Changed
- Updated embedded Web UI to `v0.17.0`
### Fixed
- S3 Repo: Ignore dataset entries without a valid alias and leave them to be cleaned up by GC
- Caching object repo: Ensure directory exists before writing objects

## [0.168.0] - 2024-03-23
### Changed
- FlightSQL: For expensive queries `GetFlightInfo` we will only prepare schemas and not compute results - this avoids doing double the work just to return `total_records` and `total_bytes` in `FlightInfo` before result is fetched via `DoGet`
- Optimized implementation of Datafusion catalog, scheme, and table providers that includes caching and maximally delays the metadata scanning

## [0.167.2] - 2024-03-23
### Fixed
- FlightSQL: Improved Python connectivity examples (ADBC, Sqlalchemy, DBAPI2, JDBC)
- FlightSQL: Fix invalid `location` info in `FlightInfo` that was causing errors in some client libraries

## [0.167.1] - 2024-03-20
### Fixed
- Bug when handle created during dataset creation had empty account name in a multi-tenant repo.

## [0.167.0] - 2024-03-19
### Added
- Implementation of `ObjectRepository` that can cache small objects on local file system (e.g. to avoid too many calls to S3 repo)
- Optional `S3RegistryCache` component that can cache the list of datasets under an S3 repo to avoid very expensive bucket prefix listing calls

## [0.166.1] - 2024-03-14
### Fixed
- Allow OData adapter to skip fields with unsupported data types instead of chasing

## [0.166.0] - 2024-03-14
### Added
- Experimental support for [OData](https://www.odata.org/) protocol
### Fixed
- Pulling datasets by account in multi-tenant workspace

## [0.165.0] - 2024-03-12
### Updated
- Extended flow history:
   - start condition update event now holds a snapshot of the start condition
   - input dataset trigger now returns a queryable Dataset object instead of simply identifier
   - dependent dataset flows should not be launched after Up-To-Date input flow
### Fixed
- Zero value handling for `per_page` value in GraphQL
- Flow history: more corrections to show natural time of flow events and keep flow system testable
- Fixed metadata chain scanning regression

## [0.164.2] - 2024-03-07
### Changed
- Using `cargo udeps` to detect unused dependencies during linting
### Fixed
- Flow history no longer produces duplicate flow abortion events
- Flow history no longer shows initiation time that is earlier than the next event's time

## [0.164.1] - 2024-03-06
### Changed:
- Flow status `Cancelled` was finally replaced with `Aborted`, unifying cancellation types for simplicity
### Fixed
- Flow system now pauses flow configuration, even when cancelling an already completed flow

## [0.164.0] - 2024-03-06
### Added
- Added support of wildcard patterns for `kamu pull` and `kamu push` commands
- Added `{dataset}/tail` and `/query` REST API endpoints
### Changed
- Optimization of passes through metadata chain with API using Visitors

## [0.163.1] - 2024-03-01
### Fixed
- Fixed `mapboxgl` dependency issue in Jupyter image

## [0.163.0] - 2024-03-01
### Changed
- Simplified flow statuses within Flow System (no more Queued or Scheduled status)
- Extended flow start conditions with more debug information for UI needs
- Simplified flow cancellation API:
    - Cancelling in Waiting/Running states is accepted, and aborts the flow, and it's associated tasks
    - Cancelling in Waiting/Running states also automatically pauses flow configuration

## [0.162.1] - 2024-02-28
### Added
- `kamu system check-token` command for token debugging
### Fixed
- `kamu system api-server` startup failure

## [0.162.0] - 2024-02-28
### Added
- Flow system now fully supports batching conditions for derived datasets:
   - not launching excessive flows unless minimal number of input records is accumulated
   - not waiting on the batching condition over a limit of 24h, if at least something accumulated
### Fixed
- `kamu login` no longer requires workspace in `--user` scope (#525)
- Sync will correctly select smart ODF protocol when pushing/pulling via repository alias (#521)
- Fixed `kamu verify` crash under verbose logging (#524)
- Increased default number of results returned by `kamu search` command

## [0.161.0] - 2024-02-26
### Added
- `kamu search` command now works with ODF repositories

## [0.160.0] - 2024-02-26
### Changed
- Upgraded to latest `datafusion` `v36.0.0`
- Upgraded to latest `jupyter`
### Added
- New `kamu system generate-token` command useful for debugging node interactions
- New `--check` flag for `kamu login` command to validate token with the remote and exit

## [0.159.0] - 2024-02-16
### Added
- New `--exists-ok` flag for `kamu init` command
### Fixed
- Ignoring the trailing slash during inference of a dataset name from pull URL

## [0.158.0] - 2024-02-13
### Added
- Flows API now reports number of ingested/transformed blocks & records to improve UI informativity
- Support of `--recursive` flag for `kamu delete` and `kamu verify` commands
### Changed
- The state when all flows of the given dataset are paused should be queryable via GraphQL API
- Added caching of metadata chains to improve performance within transactions

## [0.157.0] - 2024-02-12
### Added
- Complete support for `arm64` architecture (including M-series Apple Silicon)
  - `kamu-cli` now depends on multi-platform Datafusion, Spark, Flink, and Jupyter images allowing you to run data processing at native CPU speeds
### Changed
- Spark engine is upgraded to latest version of Spark 3.5
- Spark engine is using [ANSI mode](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html) by default which produces helpful errors instead of silently returning `null` in many built-in functions
### Fixed
- Modeling mistake in Smart Transfer Protocol that creates unexpected push/pull transfer path for metadata blocks

## [0.156.3] - 2024-02-09
### Added
- Native support for `arm64` architecture (including M-series Apple Silicon) in `kamu-cli` and `kamu-engine-datafusion`
  - Note: Flink and Spark engine images still don't provide `arm64` architecture and continue to require QEMU
### Changed
- Flow system scheduling rules improved to respect system-wide throttling setting and take last successful run into account when rescheduling a flow or after a restart

## [0.156.2] - 2024-02-07
### Changed
- Using unique container and network names to prevent collisions when running concurrent `kamu` processes
- Improved error handling for all subprocesses

## [0.156.1] - 2024-02-05
### Fixed
- Pausing dataset flow configuration via the dedicated API should have impact on the currently running flow

## [0.156.0] - 2024-02-03
### Added
- New type `DatasetRefPattern` which allows CLI command to accept global pattern
- New GraphQL APIs for quick pausing/resuming of dataset flow configs preserving the scheduling rules
- New GraphQL APIs for server-side filtering of flow listings (by type, by status, and by initiator)
### Changed
- `kamu deleted` and `kamu verify` now accepts global pattern expression
- Error handling for pipe subprocesses with file output
- Pagination implementation made more efficient for flows and tasks event stores

## [0.155.0] - 2024-01-25
### Added
- Datafusion-based interactive SQL shell
### Changed
- Datafusion is now the default engine for `kamu sql` command

## [0.154.2] - 2024-01-25
### Changed
- Use artificial control over time in `FlowService` tests to stabilize their behavior
- CRON expressions API should allow only classic 5-component syntax

## [0.154.1] - 2024-01-24
### Fixed
- Eliminated issue when launching transform flows for derived datasets after upstream dataset is updated.

## [0.154.0] - 2024-01-24
### Added
- `kamu logout` command accepts `--all` switch to drop all current server sessions
### Changed
- `kamu login` and `kamu logout` commands accept server URL as a positional argument, not as `-s` switch
- Improved output reporting of `kamu logout` command
### Fixed
- `kamu login` and `kamu logout` commands properly handle platform URLs without an explicit schema (`https://` attached by default)
- Flow configuration API no longer crashes on specific input combinations when interval expressed in smaller time units
   exceed minimal boundary that forms a bigger time unit
- Fixed runtime crashes related to background execution of automatically scheduled tasks

## [0.153.0] - 2024-01-17 (**BREAKING**)
### Changed
- This release contains major breaking changes and will require you to re-create your workspace (sorry!)
- Changes primarily reflect the **major updates in ODF spec**:
  - schema harmonization and scanning performance (see https://github.com/open-data-fabric/open-data-fabric/pull/71)
  - and unified changelog schema to support **retractions and corrections** (see https://github.com/open-data-fabric/open-data-fabric/pull/72)
- Metadata
  - DIDs and hashes now use `base16` encoding (see [RFC-012](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/012-recommend-base16-encoding.md))
  - Enum representation in YAML manifests now favors `PascalCase` (see [RFC-013](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/013-yaml-enum-representation.md))
  - When defining transformation queries in `SetPollingSource`, `AddPushSource`, and `SetTransform` events, the output query is now considered to be the one without an alias
  - the `inputs` in `SetTransform` now use only two fields `datasetRef` (for reference or ID of a dataset) and `alias` for referring to the input in SQL queries
  - `Csv` reader format has been reduced to essential properties only
- Data
  - You will notice a new `op` column in all dataset which is used to signify **retractions and corrections** (see [RFC-015](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/015-unified-changelog-stream-schema.md))
  - `Snapshot` merge strategy will no longer produce `obsv` column but instead use the new unified retraction/correction mechanism via `op` column
- `tail` command now sorts events by `offset` in descending order
- `multiformats` were extracted into a separate crate
### Removed
- Pure Spark ingest has been removed
  - Datafusion ingest is now default option for polling and push sources
  - Spark and other engines can still be used for `preprocess` step to perform transformations which Datafusion does not yet support (e.g. GIS projection conversion)
- Dropped support for deprecated `JsonLines` format
### Added
- Engine protocol was extended with `execute_raw_query` operation
- Metadata chain added a lot more strict validation rules
- `setWatermark` mutation in GQL API

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
- New flag `--get-token` for `kamu system api-server` cli command which additionally prints
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
- Added `event-bus` crate: a utility component based on Observer design pattern,
  which allows event producers and event consumers not to know about each other
- Applied `event-bus` component to inform consumers of dataset removal, dependency changes,
  task completions
- Added in-memory dataset dependency graph instead of continuous rescanning of all datasets:
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
- Metadata chain will complain about `AddData` or `ExecuteTransform` events if there is no `SetDataSchema` event
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
- New `/{dataset}/ingest` REST endpoint also allows you to push data via API, example:
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
- Significantly reworked smart transfer protocol tests, support of multi-tenancy, authentication, authorization


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
   - Login issues and interprets Kamu-specific JWT tokens
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
- GitHub OAuth functionality isolated in a separate component `kamu-adapter-oauth`
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
- Restored pre-sorting of events by `event_time` within one data slice in DataFusion-based ingest
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
  - `Spark`-based ingest will be removed in future versions with `DataFusion` becoming the default, however we are planning to support `Spark` and all other engines in the `preprocess` step, while `DataFusion` will still be handling the initial reading of data and merging of results
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
- Improved credential reuse in S3-based dataset repository
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
- Experimental support for new [`datafusion` engine](https://github.com/kamu-data/kamu-engine-datafusion) based on [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion). Although it's a batch-oriented engine it can provide a massive performance boost for simple filter/map operations. See the [updated documentation](https://docs.kamu.dev/cli/supported-engines/) for details and current limitations.

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
- Smart Transfer Protocol: implemented Push flow for S3-based dataset repository only.
  Does not work with the local workspace yet.

## [0.118.0] - 2023-04-06
### Fixed
- Updated IPFS gateway status code handling after upstream fixes to correctly report "not found" status (#108)

## [0.117.0] - 2023-04-03
### Changed
- Revised workspace dependencies management:
  - Sharing single definition of common dependencies between modules
  - Using `cargo-deny` utility for dependencies linting
- Migrated to official S3 SDK (got rid of unmaintained `Rusoto` package)
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
- Fixed GraphQL API issues with TransformInput structures with the alias name that is different from dataset name

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
- No updates to existing datasets needed, but the verifiability of some datasets may be broken (since we don't yet implement engine versioning as per ODF spec)
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
- Unified table output allowing commands like `kamu list` to output in `json` and other formats

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
- Upgraded rust toolchain to fix `thiserror` issues. Backtrace feature is now considered stable
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
- Improved the `kamu log` command's performance on datasets with high block counts and introduced a `--limit` parameter

## [0.94.0] - 2022-06-22
### Added
- `kamu list --wide` now shows the number of blocks and the current watermarks
- Iterating on Web3 demo

## [0.93.1] - 2022-06-17
### Fixed
- Fixed `completions` command that panicked after we upgraded to new `clap` version

## [0.93.0] - 2022-06-16
### Added
- By default, we will resolve IPNS DNSLink URLs (e.g. `ipns://dataset.example.org`) using DNS query instead of delegating to the gateway. This is helpful when some gateway does not support IPNS (e.g. Infura) and in general should be a little faster and provides more information for possible debugging

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
- Data files are now named and stored according to their physical hashes, as per ODF spec
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
- Unpacked dataset descriptions in GQL
- Updated Web UI
- Extended GQL metadata block type with mock author information

## [0.84.1] - 2022-04-08
### Added
- Web UI embedding into macOS build

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
- The `kamu sql` command now supports experimental [DataFusion](https://github.com/apache/arrow-datafusion) engine that can execute SQL queries extremely fast. It doesn't have a shell yet so can only be used in `--command` mode, but we will be expanding its use in the future.

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
- Spark engines will not produce an empty block when there were no changes to data

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
- Improved error handling when `kamu sql` is run in an empty workspace
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
- Uncacheable message will no longer obscure the commit message

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
