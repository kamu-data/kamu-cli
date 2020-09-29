# Kamu CLI

[![build](https://github.com/kamu-data/kamu-cli/workflows/build/badge.svg)](https://github.com/kamu-data/kamu-cli/actions)
[![Release](https://github.com/kamu-data/kamu-cli/workflows/release/badge.svg)](https://github.com/kamu-data/kamu-cli/actions)

Kamu is a new-generation data management and exploration tool which aims to enable effective collaboration of people around data. To put it simply, it tries to accomplish what `git` and other version control systems did for software - provide a reliable, transparent, and trustworthy way of iteratively improving something as a community - but does it in a way that is most suitable to data.

Kamu is a reference implementation of the [Open Data Fabric](https://github.com/kamu-data/open-data-fabric) data exchange and transformation protocol. Please see the protocol specification for more technical details.

Website: https://kamu.dev

## Documentation
- [Installation](docs/install.md)
- [First Steps](docs/first_steps.md)
- Examples
  - [Stock Market Trading Data Analysis](docs/examples/trading.md)
  - [Overdue Order Shipments Detection](docs/examples/overdue_shipments.md)
- Ingesting Data
  - Supported Formats
  - Merge Strategies
- Transforming Data
  - Streaming Aggregations
  - Temporal Table Joins
  - Stream-to-Stream Joins
  - Watermarks
  - Geo-Spatial Data
- Exploring Data
- Sharing data
- Reference
  - [Metadata Schemas](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema)
  - Supported Engines
- Contributing
  - Contribution Guidelines
  - [Developer Guide](docs/developer_guide.md)

## Features

- Easily ingest any data set from the web
- Keep track of any updates to the data source in the future

![Pull Data](docs/readme_files/pull-multi.gif)

- All data is stored in the efficient structured format (Parquet)
- Explore data and run adhoc SQL queries (backed by the power of Apache Spark)

![SQL Shell](docs/first_steps_files/sql.gif)

- Launch a Jupyter notebook with one command
- Join, filter, and shape your data using Spark dataframe API or Spark SQL
- Fetch the result of any query as a Pandas dataframe
- Visualize it in any way you prefer using any Python library

![Jupyter](docs/first_steps_files/notebook-003.png)
