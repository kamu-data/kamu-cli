// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use ::serde::{Deserialize, Serialize};
use chrono::prelude::*;
use indoc::indoc;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

#[cfg(feature = "arrow")]
#[test]
fn serde_set_data_schema() {
    use arrow::datatypes::*;

    let expected_schema = SchemaRef::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Boolean, false),
    ]));

    let event: MetadataEvent = SetDataSchema::new(&expected_schema).into();

    let expected_block = MetadataBlock {
        sequence_number: 123,
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event,
    };

    let actual_data = YamlMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let expected_data = indoc!(
        r#"
        kind: MetadataBlock
        version: 2
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 123
          event:
            kind: setDataSchema
            schema: DAAAAAgACAAAAAQACAAAAAQAAAACAAAAQAAAAAQAAADY////GAAAAAwAAAAAAAAGEAAAAAAAAAAEAAQABAAAAAEAAABiAAAAEAAUABAAAAAPAAQAAAAIABAAAAAYAAAAIAAAAAAAAAIcAAAACAAMAAQACwAIAAAAQAAAAAAAAAEAAAAAAQAAAGEAAAA=
        "#
    );

    assert_eq!(expected_data, std::str::from_utf8(&actual_data).unwrap());

    let actual_block = YamlMetadataBlockDeserializer
        .read_manifest(&actual_data)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema_as_arrow()
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[test]
fn serde_dataset_snapshot_root() {
    let data = indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: kamu.test
          kind: root
          metadata:
          - kind: setPollingSource
            fetch:
              kind: url
              url: ftp://kamu.dev/test.zip
              cache:
                kind: forever
            prepare:
            - kind: decompress
              format: zip
              subPath: data_*.csv
            read:
              kind: csv
              header: true
            preprocess:
              kind: sql
              engine: spark
              query: SELECT * FROM input
            merge:
              kind: snapshot
              primaryKey:
              - id
        "#
    );

    let expected = DatasetSnapshot {
        name: DatasetName::try_from("kamu.test").unwrap(),
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
                separator: None,
                encoding: None,
                quote: None,
                escape: None,
                comment: None,
                header: Some(true),
                enforce_schema: None,
                infer_schema: None,
                ignore_leading_white_space: None,
                ignore_trailing_white_space: None,
                null_value: None,
                empty_value: None,
                nan_value: None,
                positive_inf: None,
                negative_inf: None,
                date_format: None,
                timestamp_format: None,
                multi_line: None,
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
                observation_column: None,
                obsv_added: None,
                obsv_changed: None,
                obsv_removed: None,
            }),
        })],
    };

    let actual = YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_dataset_snapshot_derivative() {
    let data = indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: com.naturalearthdata.admin0
          kind: derivative
          metadata:
          - kind: setTransform
            inputs:
            - name: com.naturalearthdata.10m.admin0
            - name: com.naturalearthdata.50m.admin0
            transform:
              kind: sql
              engine: spark
              query: SOME_SQL
        "#
    );

    let expected = DatasetSnapshot {
        name: DatasetName::try_from("com.naturalearthdata.admin0").unwrap(),
        kind: DatasetKind::Derivative,
        metadata: vec![MetadataEvent::SetTransform(SetTransform {
            inputs: vec![
                TransformInput {
                    id: None,
                    name: DatasetName::try_from("com.naturalearthdata.10m.admin0").unwrap(),
                    dataset_ref: None,
                },
                TransformInput {
                    id: None,
                    name: DatasetName::try_from("com.naturalearthdata.50m.admin0").unwrap(),
                    dataset_ref: None,
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

    let actual = YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_dataset_snapshot_derivative_with_multi_tenant_ref() {
    let data = indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: com.naturalearthdata.admin0
          kind: derivative
          metadata:
          - kind: setTransform
            inputs:
            - name: com.naturalearthdata.10m.admin0
              datasetRef: kamu/com.naturalearthdata.10m.admin0
            - name: com.naturalearthdata.50m.admin0
              datasetRef: remote-repo/kamu/com.naturalearthdata.50m.admin0
            transform:
              kind: sql
              engine: spark
              query: SOME_SQL
        "#
    );

    let expected = DatasetSnapshot {
        name: DatasetName::try_from("com.naturalearthdata.admin0").unwrap(),
        kind: DatasetKind::Derivative,
        metadata: vec![MetadataEvent::SetTransform(SetTransform {
            inputs: vec![
                TransformInput {
                    id: None,
                    name: DatasetName::try_from("com.naturalearthdata.10m.admin0").unwrap(),
                    dataset_ref: Some(
                        DatasetRefAny::try_from("kamu/com.naturalearthdata.10m.admin0").unwrap(),
                    ),
                },
                TransformInput {
                    id: None,
                    name: DatasetName::try_from("com.naturalearthdata.50m.admin0").unwrap(),
                    dataset_ref: Some(
                        DatasetRefAny::try_from("remote-repo/kamu/com.naturalearthdata.50m.admin0")
                            .unwrap(),
                    ),
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

    let actual = YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_metadata_block() {
    let data = indoc!(
        "
        kind: MetadataBlock
        version: 2
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 127
          event:
            kind: executeQuery
            inputSlices:
            - datasetID: \
         did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
              blockInterval:
                start: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
                end: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
              dataInterval:
                start: 10
                end: 20
            - datasetID: \
         did:odf:fed01826ef0a9abea3a3a826ef0a9abea3a3a90e6065747270300c1d3b30a2cd9d724
              blockInterval:
                start: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
                end: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
            outputData:
              logicalHash: f162076d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01
              physicalHash: f1620cceefd7e0545bcf8b6d19f3b5750c8a3ee8350418877bc6fb12e32de28137355
              interval:
                start: 10
                end: 20
              size: 10
            outputWatermark: 2020-01-01T12:00:00Z
        "
    );

    let expected = MetadataBlock {
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event: MetadataEvent::ExecuteQuery(ExecuteQuery {
            input_slices: vec![
                InputSlice {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input1"),
                    block_interval: Some(BlockInterval {
                        start: Multihash::from_digest_sha3_256(b"a"),
                        end: Multihash::from_digest_sha3_256(b"b"),
                    }),
                    data_interval: Some(OffsetInterval { start: 10, end: 20 }),
                },
                InputSlice {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input2"),
                    block_interval: Some(BlockInterval {
                        start: Multihash::from_digest_sha3_256(b"a"),
                        end: Multihash::from_digest_sha3_256(b"b"),
                    }),
                    data_interval: None,
                },
            ],
            input_checkpoint: None,
            output_data: Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                interval: OffsetInterval { start: 10, end: 20 },
                size: 10,
            }),
            output_checkpoint: None,
            output_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
        }),
        sequence_number: 127,
    };

    let actual = YamlMetadataBlockDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlMetadataBlockSerializer.write_manifest(&actual).unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_metadata_block_obsolete_version() {
    let data = indoc!(
        "
    kind: MetadataBlock
    version: 1
    content:
      systemTime: 2020-01-01T12:00:00Z
      prevBlockHash: zW1k8aWxnH37Xc62cSJGQASfCTHAtpEH3HdaGB1gv6NSj7P
      event:
        kind: executeQuery
        inputSlices:
        - datasetID: did:odf:z4k88e8oT6CUiFQSbmHPViLQGHoX8x5Fquj9WvvPdSCvzTRWGfJ
          blockInterval:
            start: zW1i4mki3rvFyZZ3DyKnT8WbqwykmSNj2adNfjZtGKrodD4
            end: zW1mJtUjH235JZ4BBpJBousTNHaDXer4r4QzSdsqTfKENrr
          dataInterval:
            start: 10
            end: 20
        - datasetID: did:odf:z4k88e8kjvUAfcpgRSvrTL7XmEmrQfvHaYqo11wtT1JewT16nSc
          blockInterval:
            start: zW1i4mki3rvFyZZ3DyKnT8WbqwykmSNj2adNfjZtGKrodD4
            end: zW1mJtUjH235JZ4BBpJBousTNHaDXer4r4QzSdsqTfKENrr
        outputData:
          logicalHash: zW1hSqbjSkaj1wY6EEWY7h1M1rRMo5uCLPSc5EHD4rjFxcg
          physicalHash: zW1oExmNvSZ5wSiv7q4LmiRFDNe9U7WerQsbP5EUvyKmypG
          interval:
            start: 10
            end: 20
          size: 10
        outputWatermark: 2020-01-01T12:00:00Z
        "
    );

    let expected_error = Error::UnsupportedVersion(UnsupportedVersionError {
        manifest_version: 1,
        supported_version_range: METADATA_BLOCK_SUPPORTED_VERSION_RANGE,
    });

    let actual_error = YamlMetadataBlockDeserializer
        .read_manifest(data.as_bytes())
        .unwrap_err();

    assert_eq!(expected_error.to_string(), actual_error.to_string());
}

#[test]
fn serde_fetch_step_files_glob() {
    let data = indoc!(
        "
        kind: filesGlob
        path: /opt/x/*.txt
        cache:
          kind: forever
        order: byName"
    );

    #[derive(Serialize, Deserialize)]
    struct Helper(#[serde(with = "FetchStepDef")] FetchStep);
    let hlp: Helper = serde_yaml::from_str(data).unwrap();
    let actual = hlp.0;

    let expected = FetchStep::FilesGlob(FetchStepFilesGlob {
        path: "/opt/x/*.txt".to_owned(),
        event_time: None,
        cache: Some(SourceCaching::Forever(SourceCachingForever {})),
        order: Some(SourceOrdering::ByName),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&Helper(actual)).unwrap(),
        indoc!(
            "
            kind: filesGlob
            path: /opt/x/*.txt
            cache:
              kind: forever
            order: byName
            "
        )
    );
}

#[test]
fn serde_transform() {
    let data = indoc!(
        "
        kind: sql
        engine: flink
        temporalTables:
        - name: foo
          primaryKey:
          - id
        queries:
        - alias: bar
          query: >
            SELECT * FROM foo"
    );

    #[derive(Serialize, Deserialize)]
    struct Helper(#[serde(with = "TransformDef")] Transform);
    let hlp: Helper = serde_yaml::from_str(data).unwrap();
    let actual = hlp.0;

    let expected = Transform::Sql(TransformSql {
        engine: "flink".to_owned(),
        version: None,
        query: None,
        temporal_tables: Some(vec![TemporalTable {
            name: "foo".to_owned(),
            primary_key: vec!["id".to_owned()],
        }]),
        queries: Some(vec![SqlQueryStep {
            alias: Some("bar".to_owned()),
            query: "SELECT * FROM foo".to_owned(),
        }]),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&Helper(actual)).unwrap(),
        indoc!(
            "
            kind: sql
            engine: flink
            queries:
            - alias: bar
              query: SELECT * FROM foo
            temporalTables:
            - name: foo
              primaryKey:
              - id
            "
        )
    );
}
