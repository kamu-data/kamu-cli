# Transforming Data <!-- omit in toc -->

- [Supported Engines](#supported-engines)
  - [Schema Support](#schema-support)
  - [Operation Types](#operation-types)

## Supported Engines

| Name         |                        Technology                        |                                                  Query Dialect                                                   |                     Code Repository                      |                            Image                             | Notes                                                                                                                                                   |
| ------------ | :------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------: | :----------------------------------------------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark`      |        [Apache Spark](https://spark.apache.org/)         |                     [Spark Streaming SQL](https://spark.apache.org/docs/latest/sql-ref.html)                     | [GitHub](https://github.com/kamu-data/kamu-engine-spark) | [Docker Hub](https://hub.docker.com/r/kamudata/engine-spark) | Also used in SQL shell. Currently the only engine that supports GIS data.                                                                               |
| `flink`      |        [Apache Flink](https://flink.apache.org/)         | [Flink Streaming SQL](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/table/sql/gettingstarted/) | [GitHub](https://github.com/kamu-data/kamu-engine-flink) | [Docker Hub](https://hub.docker.com/r/kamudata/engine-flink) | Has best support for stream-to-table joins                                                                                                              |
| `datafusion` | [DataFusion](https://github.com/apache/arrow-datafusion) |                        [DataFusion Batch SQL](https://github.com/apache/arrow-datafusion)                        |   [GitHub](https://github.com/apache/arrow-datafusion)   |                              -                               | Experimental engine used in `tail` command and as an alternative to Spark in SQL shell. It's batch-oriented so unlikely to be used for transformations. |

### Schema Support

| Feature      | kamu  | Spark | Flink | DataFusion |
| ------------ | :---: | :---: | :---: | :--------: |
| Basic types  |   ✔️   |   ✔️   |   ✔️   |     ✔️      |
| Decimal type |   ✔️   |   ✔️   |  ✔️**  |    ❌***    |
| Nested types |  ✔️*   |   ✔️   |   ❌   |     ❌      |
| GIS types    |  ✔️*   |   ✔️   |   ❌   |     ❌      |

> `*` There is currently no way to express nested and GIS data types when declaring root dataset schemas, but you still can use them through pre-processing queries
>
> `**` Apache Flink has known issues with Decimal type and currently relies on our patches that have not been upstreamed yet, so stability is not guaranteed [FLINK-17804](https://issues.apache.org/jira/browse/FLINK-17804).
>
> `***` Arrow is lacking Decimal compute support [ARROW-RS-272](https://github.com/apache/arrow-rs/issues/272)

### Operation Types

| Feature                           | Spark | Flink |
| --------------------------------- | :---: | :---: |
| Filter                            |   ✔️   |   ✔️   |
| Map                               |   ✔️   |   ✔️   |
| Aggregations                      |  ❌*   |   ✔️   |
| Stream-to-Stream Joins            |  ❌*   |   ✔️   |
| Projection / Temporal Table Joins |  ❌*   |   ✔️   |
| GIS extensions                    |   ✔️   |   ❌   |

> `*` Spark Engine is capable of stream processing but temporarily we have to use it in the batch processing mode, so only row-level operations like map and filter are currently usable, as those do not require corrent stream processing and watermarking semantics.