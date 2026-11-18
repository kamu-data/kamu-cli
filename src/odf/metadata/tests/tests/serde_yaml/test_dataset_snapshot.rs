// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use indoc::indoc;
use opendatafabric_metadata::datasets::*;
use opendatafabric_metadata::legacy::*;
use opendatafabric_metadata::serde::{yaml as serde, *};
use opendatafabric_metadata::sources::*;
use opendatafabric_metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_dataset_snapshot_root() {
    let data = indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: kamu.test
          kind: Root
          metadata:
          - kind: SetPollingSource
            fetch:
              kind: Url
              url: ftp://kamu.dev/test.zip
              cache:
                kind: Forever
            prepare:
            - kind: Decompress
              format: Zip
              subPath: data_*.csv
            read:
              kind: Csv
              header: true
            preprocess:
              kind: Sql
              engine: spark
              query: SELECT * FROM input
            merge:
              kind: Snapshot
              primaryKey:
              - id
        "#
    );

    let expected = DatasetSnapshot {
        name: DatasetAlias::try_from("kamu.test").unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![MetadataEvent::SetPollingSource(SetPollingSource {
            fetch: FetchStep::Url(FetchStepUrl {
                url: "ftp://kamu.dev/test.zip".to_owned(),
                event_time: None,
                cache: Some(SourceCaching::Forever(SourceCachingForever {})),
                headers: None,
            }),
            prepare: Some(vec![PrepStep::Decompress(PrepStepDecompress {
                format: CompressionFormat::Zip,
                sub_path: Some("data_*.csv".to_owned()),
            })]),
            read: ReadStep::Csv(ReadStepCsv {
                schema: None,
                ddl_schema: None,
                separator: None,
                encoding: None,
                quote: None,
                escape: None,
                header: Some(true),
                infer_schema: None,
                null_value: None,
                date_format: None,
                timestamp_format: None,
            }),
            preprocess: Some(Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SELECT * FROM input".to_owned()),
                queries: None,
                temporal_tables: None,
            })),
            merge: MergeStrategy::Snapshot(MergeStrategySnapshot {
                primary_key: vec!["id".to_owned()],
                compare_columns: None,
            }),
        })],
    };

    let actual = serde::YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = serde::YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_dataset_snapshot_derivative() {
    let data = indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: com.naturalearthdata.admin0
          kind: Derivative
          metadata:
          - kind: SetTransform
            inputs:
            - datasetRef: com.naturalearthdata.10m.admin0
            - datasetRef: com.naturalearthdata.50m.admin0
            transform:
              kind: Sql
              engine: spark
              query: SOME_SQL
        "#
    );

    let expected = DatasetSnapshot {
        name: DatasetAlias::try_from("com.naturalearthdata.admin0").unwrap(),
        kind: DatasetKind::Derivative,
        metadata: vec![MetadataEvent::SetTransform(SetTransform {
            inputs: vec![
                TransformInput {
                    dataset_ref: DatasetName::try_from("com.naturalearthdata.10m.admin0")
                        .unwrap()
                        .into(),
                    alias: None,
                },
                TransformInput {
                    dataset_ref: DatasetName::try_from("com.naturalearthdata.50m.admin0")
                        .unwrap()
                        .into(),
                    alias: None,
                },
            ],
            transform: Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SOME_SQL".to_owned()),
                queries: None,
                temporal_tables: None,
            }),
        })],
    };

    let actual = serde::YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = serde::YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_dataset_snapshot_derivative_with_multi_tenant_ref() {
    let data = indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: a/com.naturalearthdata.admin0
          kind: Derivative
          metadata:
          - kind: SetTransform
            inputs:
            - datasetRef: b/com.naturalearthdata.10m.admin0
            - datasetRef: c/com.naturalearthdata.50m.admin0
            transform:
              kind: Sql
              engine: spark
              query: SOME_SQL
        "#
    );

    let expected = DatasetSnapshot {
        name: DatasetAlias::try_from("a/com.naturalearthdata.admin0").unwrap(),
        kind: DatasetKind::Derivative,
        metadata: vec![MetadataEvent::SetTransform(SetTransform {
            inputs: vec![
                TransformInput {
                    dataset_ref: datasets::legacy::DatasetRef::try_from(
                        "b/com.naturalearthdata.10m.admin0",
                    )
                    .unwrap(),
                    alias: None,
                },
                TransformInput {
                    dataset_ref: datasets::legacy::DatasetRef::try_from(
                        "c/com.naturalearthdata.50m.admin0",
                    )
                    .unwrap(),
                    alias: None,
                },
            ],
            transform: Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SOME_SQL".to_owned()),
                queries: None,
                temporal_tables: None,
            }),
        })],
    };

    let actual = serde::YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = serde::YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
