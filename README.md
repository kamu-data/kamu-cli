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
- Handle dependencies between datasets
- DOT graph output
- Implement recursive mode for `pull` command
- Implement `status` command showing which data is out-of-date
- Cleanup poll data
- Implement file `import` and `export`
- Root dataset creation wizard (generate based on schema)
- Simplify historical vocab
- Control output verbosity
- Add `sql` shell
- Include usage GIF in the readme

# Post-MVP
- V Suppress HDFS warning on startup
- Upgrade to latest Spark
- Fix notebook warnings
- Avoid permission issues by propagating UID/GID into containers
