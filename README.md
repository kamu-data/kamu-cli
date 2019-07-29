# Kamu CLI

# MVP
- V define dataset manifest formats and schemas
- V setup verbose logging
- V implement root poll command with passing a manifest in
- V ftp support
- V integrate transform
- V Multi-file support
- V Get rid of `spark-warehouse` folder
- V Replace cache json with yaml
- V Use docker to run spark
- V Put GeoSpark into image, not dependencies
- V Create PySpark + Livy image
- V Create Jupyter + SparkMagic + Kamu image
- V Create `notebook` command to launch Jupyter
- V Permission fix-up for `notebook` command
- V Rethink repository structure and how datasets are linked
- V Implement `pull` command to refresh dataset
- V Implement `list` command showing datasets and their metadata
- V DOT graph output
- V Referential integrity
- V `delete` command
- V `purge` command
- V Root dataset creation wizard (generate based on schema)
- Add `sql` shell
- Control output verbosity
- Implement `describe` command showing dataset metadata
- Implement file `import` and `export`
- Simplify historical vocab
- Distribution `arch`
- Distribution `ubuntu`
- Distribution `mac`
- Implement `status` command showing which data is out-of-date
- Implement recursive mode for `pull` command
- Handle dependencies during `purge`
- Write version metainfo for later upgrades
- Force `pull` to update uncacheable datasets

# Post-MVP
- V Suppress HDFS warning on startup
- Add bash completions
- Upgrade to latest Spark
- Fix notebook warnings
- Avoid permission issues by propagating UID/GID into containers
- Cleanup poll data
