# Data Ingestion Examples  <!-- omit in toc -->

> This document is under construction, contributions are welcome.

- [Compressed Data & Archives](#compressed-data--archives)
- [CSV and Variants](#csv-and-variants)
- [JSON Document](#json-document)
- [JSON Lines](#json-lines)
- [Esri Shapefile](#esri-shapefile)

# Compressed Data & Archives

Use `decompress` preparation step to extract data from `gzip`, `zip` archives.

```yaml
prepare:
- kind: decompress
  format: gzip
```

In case of a multi-file archive:

```yaml
prepare:
- kind: decompress
  format: zip
  subPath: specific-file-*.csv  # Note: can contain glob patterns
```

See also: [PrepStep::Decompress](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstepdecompress-schema)

# CSV and Variants

Tab-separated file:

```yaml
read:
  kind: csv
  separator: "\t"
  quote: '"'
```

See also: [ReadStep::Csv](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepcsv-schema)

# JSON Document

A JSON document such as the following:

```json
{
    "values": [
        {"id": 1, "key": "value"},
        {"id": 2, "key": "value"},
    ]
}
```

Can be "flattened" into a columnar form and read using:

```yaml
prepare:
- kind: pipe
  command:
  - 'jq'
  - '-r'
  - '.values[] | [.id, .key] | @csv'
read:
  kind: csv
  schema:
  - id BIGINT
  - key STRING
```

# JSON Lines

JSONL, aka newline-delimited JSON file such as:

```json
{"id": 1, "key": "value"}
{"id": 2, "key": "value"}
```

Can be read using:

```yaml
read:
  kind: jsonLines
  schema:
  - id BIGINT
  - key STRING
```

See also: [ReadStep::JsonLines](https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepjsonlines-schema)

# Esri Shapefile

```yaml
read:
  kind: esriShapefile
  subPath: specific_data.*
# Use preprocess to optionally convert between different projections
preprocess:
  kind: sql
  engine: spark
  query: >
    SELECT
      ST_Transform(geometry, "epsg:3157", "epsg:4326") as geometry,
      ...
    FROM input
```
