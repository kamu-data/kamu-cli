# Status

## Open Data Fabric

| Feature           |  ODF  | Kamu  |
| ----------------- | :---: | :---: |
| Source evolution  |   V   |   X   |
| Schema evolution  |   V   |   X   |
| Query migrations  |   V   |   X   |
| Engine versioning |   V   |   X   |
| Engine migrations |   V   |   X   |

## Schema

| Feature      | Coordinator | Spark | Flink | DataFusion |
| ------------ | :---------: | :---: | :---: | :--------: |
| Basic types  |      V      |   V   |   V   |     V      |
| Decimal type |      V      |   V   |  V*   |    X**     |
| Nested types |      X      |   V   |   X   |     X      |
| GIS types    |      X      |   V   |   X   |     X      |

> * Apache Flink has known issues with Decimal type and currently relies on our patches that have not been upstreamed yet, so stability is not guaranteed [FLINK-17804](https://issues.apache.org/jira/browse/FLINK-17804).

> ** Arrow is lacking Decimal compute support [ARROW-RS-272](https://github.com/apache/arrow-rs/issues/272)

## Transforms

| Feature                           | Spark | Flink |
| --------------------------------- | :---: | :---: |
| Filter                            |   V   |   V   |
| Map                               |   V   |   V   |
| Aggregations                      |  X*   |   V   |
| Stream-to-Stream Joins            |  X*   |   V   |
| Projection / Temporal Table Joins |  X*   |   V   |
| GIS extensions                    |   V   |   X   |

> * Spark Engine is capable of stream processing but temporarily we have to use it in the batch processing mode, so only row-level operations like map and filter are currently usable which do not require corrent stream processing and watermarking semantics.
