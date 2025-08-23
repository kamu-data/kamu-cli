// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::convert::TryFrom;

use ::serde::{Deserialize, Serialize};
use chrono::prelude::*;
use indoc::indoc;
use opendatafabric_metadata::ext::*;
use opendatafabric_metadata::serde::yaml::*;
use opendatafabric_metadata::*;

#[test]
fn serde_enum_tagging() {
    #[derive(Deserialize, Serialize, Debug)]
    struct Wrapper(#[serde(with = "ReadStepDef")] ReadStep);

    let value = ReadStep::NdJson(ReadStepNdJson {
        encoding: Some("utf-8".to_string()),
        ..Default::default()
    });

    // Check produces PascalCase tags on serialize
    assert_eq!(
        serde_yaml::to_string(&Wrapper(value.clone())).unwrap(),
        indoc::indoc!(
            "
            kind: NdJson
            encoding: utf-8
            "
        )
    );

    // Check deserialize with exact tag match
    let de_value = serde_yaml::from_str::<Wrapper>(indoc::indoc!(
        "
        kind: NdJson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.0, value);

    // Check deserialize with old camelCase tag
    let de_value = serde_yaml::from_str::<Wrapper>(indoc::indoc!(
        "
        kind: ndJson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.0, value);

    // Check deserialize with lowercase tag
    let de_value = serde_yaml::from_str::<Wrapper>(indoc::indoc!(
        "
        kind: ndjson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.0, value);

    // Check rejects other case permutations
    assert_matches!(
        serde_yaml::from_str::<Wrapper>(indoc::indoc!(
            "
            kind: nDjson
            encoding: utf-8
            "
        )),
        Err(_)
    );
}

#[test]
fn serde_string_enum_names() {
    #[derive(Deserialize, Serialize, Debug)]
    struct Wrapper {
        #[serde(with = "SourceOrderingDef")]
        kind: SourceOrdering,
    }

    let value = SourceOrdering::ByEventTime;

    // Check produces PascalCase values on serialize as in spec schemas
    assert_eq!(
        serde_yaml::to_string(&Wrapper { kind: value }).unwrap(),
        indoc::indoc!(
            "
            kind: ByEventTime
            "
        )
    );

    // Can deserialize from camelCase
    let de_value = serde_yaml::from_str::<Wrapper>(indoc::indoc!(
        "
        kind: byEventTime
        "
    ))
    .unwrap();
    assert_eq!(de_value.kind, value);

    // Can deserialize from lowercase
    let de_value = serde_yaml::from_str::<Wrapper>(indoc::indoc!(
        "
        kind: byeventtime
        "
    ))
    .unwrap();
    assert_eq!(de_value.kind, value);

    // Check rejects other case permutations
    assert_matches!(
        serde_yaml::from_str::<Wrapper>(indoc::indoc!(
            "
            kind: bYeventtime
            "
        )),
        Err(_)
    );
}

#[test]
fn serde_set_data_schema() {
    let expected_schema = DataSchema::new(vec![
        DataField::string("city").encoding(ArrowBufferEncoding::View {
            offset_bit_width: Some(32),
        }),
        DataField::u64("population"),
        DataField::string("census")
            .optional()
            .extra(&AttrType::new(DataTypeExt::object_link(
                DataTypeExt::multihash(),
            ))),
        DataField::list("links", DataType::string()),
    ])
    .extra(&AttrArchetype::new(DatasetArchetype::Collection));

    let event: MetadataEvent = SetDataSchema::new(expected_schema.clone()).into();

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
            kind: SetDataSchema
            schema:
              fields:
              - name: city
                type:
                  kind: String
                extra:
                  arrow.apache.org/bufferEncoding:
                    kind: View
                    offsetBitWidth: 32
              - name: population
                type:
                  kind: UInt64
              - name: census
                type:
                  kind: Option
                  inner:
                    kind: String
                extra:
                  opendatafabric.org/type:
                    kind: ObjectLink
                    linkType:
                      kind: Multihash
              - name: links
                type:
                  kind: List
                  itemType:
                    kind: String
              extra:
                kamu.dev/archetype: Collection
        "#
    );

    pretty_assertions::assert_eq!(expected_data, std::str::from_utf8(&actual_data).unwrap());

    let actual_block = YamlMetadataBlockDeserializer
        .read_manifest(&actual_data)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema
        .clone()
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[cfg(feature = "arrow")]
#[test]
#[expect(deprecated)]
fn serde_set_data_schema_legacy() {
    use arrow::datatypes::*;

    let expected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Boolean, false),
    ]);

    let event: MetadataEvent = SetDataSchema::new_legacy_raw_arrow(&expected_schema).into();

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
            kind: SetDataSchema
            rawArrowSchema: DAAAAAgACAAAAAQACAAAAAQAAAACAAAAQAAAAAQAAADY////GAAAAAwAAAAAAAAGEAAAAAAAAAAEAAQABAAAAAEAAABiAAAAEAAUABAAAAAPAAQAAAAIABAAAAAYAAAAIAAAAAAAAAIcAAAACAAMAAQACwAIAAAAQAAAAAAAAAEAAAAAAQAAAGEAAAA=
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
        .schema_as_arrow(&ToArrowSettings::default())
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[ignore = "Requires improving validation. See https://github.com/open-data-fabric/open-data-fabric/issues/112"]
#[test]
fn serde_set_data_schema_validation() {
    // Valid
    assert_matches!(
        YamlMetadataEventDeserializer.read_manifest(indoc!(
            br#"
            kind: MetadataEvent
            version: 1
            content:
              kind: SetDataSchema
              schema:
                fields:
                - name: city
                  type:
                    kind: String
            "#
        )),
        Ok(_)
    );

    // Invalid bit width
    assert_matches!(
        YamlMetadataEventDeserializer.read_manifest(indoc!(
            br#"
            kind: MetadataEvent
            version: 1
            content:
              kind: SetDataSchema
              schema:
                fields:
                - name: population
                  type:
                    kind: Int
                    bitWidth: 46
                    signed: false
            "#
        )),
        Ok(_)
    );

    // Invalid arrow encoding type
    assert_matches!(
        YamlMetadataEventDeserializer.read_manifest(indoc!(
            br#"
            kind: MetadataEvent
            version: 1
            content:
              kind: SetDataSchema
              schema:
                fields:
                - name: city
                  type:
                    kind: String
                  extra:
                    arrow.apache.org/encoding: blerb
                    arrow.apache.org/offsetBitWidth: 32
            "#
        )),
        Err(_)
    );
}

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
                    dataset_ref: DatasetRef::try_from("b/com.naturalearthdata.10m.admin0").unwrap(),
                    alias: None,
                },
                TransformInput {
                    dataset_ref: DatasetRef::try_from("c/com.naturalearthdata.50m.admin0").unwrap(),
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
    let block = MetadataBlock {
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event: MetadataEvent::ExecuteTransform(ExecuteTransform {
            query_inputs: vec![
                ExecuteTransformInput {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input1"),
                    prev_block_hash: Some(Multihash::from_digest_sha3_256(b"a")),
                    new_block_hash: Some(Multihash::from_digest_sha3_256(b"b")),
                    prev_offset: Some(9),
                    new_offset: Some(20),
                },
                ExecuteTransformInput {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input2"),
                    prev_block_hash: Some(Multihash::from_digest_sha3_256(b"a")),
                    new_block_hash: Some(Multihash::from_digest_sha3_256(b"b")),
                    prev_offset: None,
                    new_offset: None,
                },
            ],
            prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"checkpoint")),
            prev_offset: Some(9),
            new_data: Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                offset_interval: OffsetInterval { start: 10, end: 19 },
                size: 10,
            }),
            new_checkpoint: None,
            new_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
        }),
        sequence_number: 127,
    };

    let expected = indoc!(
        "
        kind: MetadataBlock
        version: 2
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 127
          event:
            kind: ExecuteTransform
            queryInputs:
            - datasetId: \
         did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
              prevOffset: 9
              newOffset: 20
            - datasetId: \
         did:odf:fed01826ef0a9abea3a3a826ef0a9abea3a3a90e6065747270300c1d3b30a2cd9d724
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
            prevCheckpoint: f1620894b355f91b194f31c1116dcec200b68d5cfd90a3df19032829fa643bc711fcb
            prevOffset: 9
            newData:
              logicalHash: f162076d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01
              physicalHash: f1620cceefd7e0545bcf8b6d19f3b5750c8a3ee8350418877bc6fb12e32de28137355
              offsetInterval:
                start: 10
                end: 19
              size: 10
            newWatermark: 2020-01-01T12:00:00Z
        "
    );

    let actual = YamlMetadataBlockSerializer.write_manifest(&block).unwrap();

    assert_eq!(expected, std::str::from_utf8(&actual).unwrap());

    let de_block = YamlMetadataBlockDeserializer
        .read_manifest(expected.as_bytes())
        .unwrap();

    assert_eq!(block, de_block);
}

#[test]
fn serde_metadata_block_obsolete_version() {
    let data = indoc!(
        "
        kind: MetadataBlock
        version: 1
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 127
          event:
            kind: ExecuteTransform
            queryInputs:
            - datasetId: \
         did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
              prevOffset: 9
              newOffset: 20
            - datasetId: \
         did:odf:fed01826ef0a9abea3a3a826ef0a9abea3a3a90e6065747270300c1d3b30a2cd9d724
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
            prevCheckpoint: f1620894b355f91b194f31c1116dcec200b68d5cfd90a3df19032829fa643bc711fcb
            prevOffset: 9
            newData:
              logicalHash: f162076d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01
              physicalHash: f1620cceefd7e0545bcf8b6d19f3b5750c8a3ee8350418877bc6fb12e32de28137355
              offsetInterval:
                start: 10
                end: 19
              size: 10
            newWatermark: 2020-01-01T12:00:00Z
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
        kind: FilesGlob
        path: /opt/x/*.txt
        cache:
          kind: Forever
        order: ByName
        "
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

    assert_eq!(serde_yaml::to_string(&Helper(actual)).unwrap(), data);
}

#[test]
fn serde_transform() {
    let data = indoc!(
        "
        kind: Sql
        engine: flink
        queries:
        - alias: bar
          query: SELECT * FROM foo
        temporalTables:
        - name: foo
          primaryKey:
          - id
        "
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

    assert_eq!(serde_yaml::to_string(&Helper(actual)).unwrap(), data);
}
