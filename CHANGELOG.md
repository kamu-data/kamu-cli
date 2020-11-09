# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
