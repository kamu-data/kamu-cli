# Merge Strategies <!-- omit in toc -->

- [Purpose](#purpose)
- [Types](#types)
  - [Ledger](#ledger)
  - [Snapshot](#snapshot)
  - [Append](#append)

# Purpose

Open Data Fabric [by design](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#nature-of-data) stores all data in the append-only event log format, always preserving the entire history. Unfortunately, a lot of data in the world is not stored or exposed this way. Some organizations may expose their data in the form of periodic database dumps, while some choose to provide it as a log of changes between current and the previous export.

When ingesting data from external sources, the [Root Datasets](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#root-dataset) can choose between different merge strategies that define how to combine the newly-ingested data with the existing one.

# Types

## Ledger
This strategy should be used for data sources containing append-only event streams. New data exports can have new rows added, but once data already made it into one export it should never change or disappear. A user-specified primary key is used to identify which events were already seen, not to duplicate them.

For more details see [Schema Reference](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema)

<!-- TODO: Describe what happens if historical records were modified by the publisher --->

### Example <!-- omit in toc -->

Imagine a data publisher that exposes dataset with **historical** populations of major cities around the world, for example:

```csv
Year,Country,City,Population
2019,CA,Vancouver,2581000
2019,US,Seattle,3433000
```

This dataset is temporal (has a `year` column), so it's a good sign that we're dealing with a `ledger` data.

> Note that having a time column doesn't always mean that the dataset is a ledger - to be a true ledger all previous records should be immutable and never change.

So we use the following root dataset manifest to ingest it:

```yaml
apiVersion: 1
kind: DatasetSnapshot
content:
  id: cities-population
  source:
    kind: root
    ...
    merge:
      kind: ledger
      primaryKey:
      - country
      - city
  vocab:
    eventTimeColumn: year
```

Notice that we specify `ledger` merge strategy with composite primary key `(country, city)`, and also set `eventTimeColumn` to use `year` as the source of event times.

The resulting dataset when ingested will look like this:

| system_time | event_time | country |   city    | population |
| :---------: | :--------: | :-----: | :-------: | :--------: |
|     s1      |    2019    |   CA    | Vancouver |  2581000   |
|     s1      |    2019    |   US    |  Seattle  |  3433000   |

Now let's say after a census in Vancouver our source data changes to:

```csv
Country,City,Population
2019,CA,Vancouver,2581000
2019,US,Seattle,3433000
2020,CA,Vancouver,2606000
```

So far this is a valid ledger data - history is preserved and changes are append-only.

Pulling the dataset will now result in the following history:

| system_time | event_time | country |   city    | population |
| :---------: | :--------: | :-----: | :-------: | :--------: |
|     s1      |    2019    |   CA    | Vancouver |  2581000   |
|     s1      |    2019    |   US    |  Seattle  |  3433000   |
|     s1      |    2020    |   CA    | Vancouver |  2606000   |

Note that the old events from 2019 were recognized as ones we already seen and were not added again.

## Snapshot
This strategy can be used for data exports that are taken periodically and contain only the latest state snapshot of the observed entity or system. Over time such exports can have new rows added, and old rows either removed or modified.

It's important to understand that publishers that use such formats routinely lose information. When a record in the database is updated, or one DB dump is replaced with another we not only lose the history of previous values, but you also lose the context of why those changes happened. This is a really bad default!

The `snapshot` strategy transforms such data sources into a history of changes by performing the [change data capture](https://en.wikipedia.org/wiki/Change_data_capture). It relies on a user-specified primary key to correlate the rows between the two snapshots.

A new event is added into the output stream whenever:

- A row with a certain primary key appears for the first time
- A row with a certain key disappears from the snapshot
- A row data associated with a certain key has changed

Each event will have an additional column that signifies the kind of observation that was encountered.

The Snapshot strategy also requires special treatment in regards to the event time. Since snapshot-style data exports represent the state of some system at a certain time - it is important to know what that time was. This time is usually captured in some form of metadata (e.g. in the name of the snapshot file, in the URL, or the HTTP caching headers. It should be possible to extract and propagate this time into a data column.

<!-- TODO: Describe event time sources --->

For more details see [Schema Reference](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema)

### Example <!-- omit in toc -->

Imagine a data publisher that exposes dataset with **current** populations of major cities around the world, for example:

```csv
Country,City,Population
CA,Vancouver,2581000
US,Seattle,3433000
```

This dataset is non-temporal (doesn't have any date/time columns), so it's a clear sign we're dealing with a `snapshot` data.

So we use the following root dataset manifest to ingest it:

```yaml
apiVersion: 1
kind: DatasetSnapshot
content:
  id: cities-population
  source:
    kind: root
    fetch:
      kind: url
      url: https://...
      eventTime:
        kind: fromMetadata
    ...
    merge:
      kind: snapshot
      primaryKey:
      - country
      - city
```

Notice that we specify `snapshot` merge strategy with composite primary key `(country, city)`. We also specify the `eventTime` of kind `fromMetadata`, instructing the ingest to use time from the caching headers as the event time of new records.

The resulting dataset when ingested will look like this:

| system_time | event_time | observed | country |   city    | population |
| :---------: | :--------: | :------: | :-----: | :-------: | :--------: |
|     s1      |     e1     |    I     |   CA    | Vancouver |  2581000   |
|     s1      |     e1     |    I     |   US    |  Seattle  |  3433000   |

Notice that since it's the first time we ingested data both records have `observed == 'I'` values, i.e. "insert".

Now let's say after a census in Vancouver our source data changes to:

```csv
Country,City,Population
CA,Vancouver,2606000
US,Seattle,3433000
```

Pulling the dataset will now result in the following history:

| system_time | event_time | observed | country |   city    | population |
| :---------: | :--------: | :------: | :-----: | :-------: | :--------: |
|     s1      |     e1     |    I     |   CA    | Vancouver |  2581000   |
|     s1      |     e1     |    I     |   US    |  Seattle  |  3433000   |
|     s2      |     e2     |    U     |   CA    | Vancouver |  2606000   |


## Append
Under this strategy, the new data will be appended to the Dataset in its original form without any modifications.

This strategy is rarely used in real datasets and mostly present for testing.
