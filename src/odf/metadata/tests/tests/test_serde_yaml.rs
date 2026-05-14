// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::BTreeMap;
use std::convert::TryFrom;

use chrono::prelude::*;
use indoc::indoc;
use opendatafabric_metadata::auth::*;
use opendatafabric_metadata::config::*;
use opendatafabric_metadata::data::ext::*;
use opendatafabric_metadata::data::*;
use opendatafabric_metadata::dataset::*;
use opendatafabric_metadata::errors::ValidationError;
use opendatafabric_metadata::legacy::*;
use opendatafabric_metadata::resource::*;
use opendatafabric_metadata::serde::yaml::IntoDto;
use opendatafabric_metadata::serde::{yaml as serde, *};
use opendatafabric_metadata::source::*;
use opendatafabric_metadata::*;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_enum_tagging() {
    let value = ReadStep::NdJson(ReadStepNdJson {
        encoding: Some("utf-8".to_string()),
        ..Default::default()
    });

    // Check produces PascalCase tags on serialize
    assert_eq!(
        serde_yaml::to_string(&serde::source::ReadStep::from(value.clone())).unwrap(),
        indoc::indoc!(
            "
            kind: NdJson
            encoding: utf-8
            "
        )
    );

    // Check deserialize with exact tag match
    let de_value: serde::source::ReadStep = serde_yaml::from_str(indoc::indoc!(
        "
        kind: NdJson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check deserialize with old camelCase tag
    let de_value: serde::source::ReadStep = serde_yaml::from_str(indoc::indoc!(
        "
        kind: ndJson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check deserialize with lowercase tag
    let de_value: serde::source::ReadStep = serde_yaml::from_str(indoc::indoc!(
        "
        kind: ndjson
        encoding: utf-8
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check rejects other case permutations
    serde_yaml::from_str::<serde::source::ReadStep>(indoc::indoc!(
        "
        kind: nDjson
        encoding: utf-8
        "
    ))
    .map(serde::IntoDto::into_dto)
    .expect_err("Should fail");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_string_enum_names() {
    let value = SourceOrdering::ByEventTime;

    // Check produces PascalCase values on serialize as in spec schemas
    assert_eq!(
        serde_yaml::to_string(&serde::source::SourceOrdering::from(value)).unwrap(),
        indoc::indoc!(
            "
            ByEventTime
            "
        )
    );

    // Can deserialize from camelCase
    let de_value: serde::source::SourceOrdering = serde_yaml::from_str(indoc::indoc!(
        "
        byEventTime
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Can deserialize from lowercase
    let de_value: serde::source::SourceOrdering = serde_yaml::from_str(indoc::indoc!(
        "
        byeventtime
        "
    ))
    .unwrap();
    assert_eq!(de_value.into_dto().unwrap(), value);

    // Check rejects other case permutations
    serde_yaml::from_str::<serde::source::SourceOrdering>(indoc::indoc!(
        "
        bYeventtime
        "
    ))
    .map(serde::IntoDto::into_dto)
    .expect_err("Should fail");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_maps() {
    // String -> Struct
    let data = indoc!(
        r#"
        password:
          value: swordfish
        tls:
          value: aabbcc
          contentEncoding: base64
        username:
          value: 'true'
        "#
    );

    let val = serde_yaml::from_str::<serde::config::Secrets>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Secrets {
            entries: BTreeMap::from_iter([
                (
                    "password".to_string(),
                    Secret {
                        value: "swordfish".into(),
                        content_encoding: None
                    }
                ),
                (
                    "username".to_string(),
                    Secret {
                        value: "true".into(),
                        content_encoding: None
                    }
                ),
                (
                    "tls".to_string(),
                    Secret {
                        value: "aabbcc".into(),
                        content_encoding: Some("base64".into())
                    }
                )
            ])
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::config::Secrets::from(val)).unwrap(),
        data
    );

    // String -> Struct + FromString
    let data = indoc!(
        r#"
        password: swordfish
        "#
    );

    let val = serde_yaml::from_str::<serde::config::Secrets>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Secrets {
            entries: BTreeMap::from_iter([(
                "password".to_string(),
                Secret {
                    value: "swordfish".into(),
                    content_encoding: None
                }
            )])
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::config::Secrets::from(val)).unwrap(),
        indoc!(
            r#"
            password:
              value: swordfish
            "#
        )
    );

    // String -> AnyJson
    let data = indoc!(
        r#"
        arrow.apache.org/offsetBitWidth: 32
        opendatafabric.org/description: foobar
        opendatafabric.org/type:
          kind: ObjectLink
          linkType:
            kind: Multihash
        "#
    );

    let val = serde_yaml::from_str::<serde::data::ExtraAttributes>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        ExtraAttributes {
            entries: BTreeMap::from_iter([
                ("arrow.apache.org/offsetBitWidth".to_string(), json!(32)),
                (
                    "opendatafabric.org/description".to_string(),
                    json!("foobar")
                ),
                (
                    "opendatafabric.org/type".to_string(),
                    json!({
                        "kind": "ObjectLink",
                        "linkType": { "kind": "Multihash" }
                    })
                )
            ])
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::data::ExtraAttributes::from(val)).unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_set_data_schema() {
    let expected_schema = DataSchema::builder()
        .extend(vec![
            DataField::string("city").encoding(ArrowBufferEncoding::View {
                offset_bit_width: Some(32),
            }),
            DataField::u64("population"),
            DataField::string("census")
                .optional()
                .extra(DataTypeExt::object_link(DataTypeExt::multihash())),
            DataField::list("links", DataType::string()),
        ])
        .extra(DatasetArchetype::Collection)
        .build()
        .unwrap();

    let event: MetadataEvent = SetDataSchema::new(expected_schema.clone()).into();

    let expected_block = MetadataBlock {
        sequence_number: 123,
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event,
    };

    let actual_data = serde::YamlMetadataBlockSerializer
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

    let actual_block = serde::YamlMetadataBlockDeserializer
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let actual_data = serde::YamlMetadataBlockSerializer
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

    let actual_block = serde::YamlMetadataBlockDeserializer
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Requires improving validation. See https://github.com/open-data-fabric/open-data-fabric/issues/112"]
#[test]
fn serde_set_data_schema_validation() {
    // Valid
    assert_matches!(
        serde::YamlMetadataEventDeserializer.read_manifest(indoc!(
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
        serde::YamlMetadataEventDeserializer.read_manifest(indoc!(
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
        serde::YamlMetadataEventDeserializer.read_manifest(indoc!(
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

    let actual = serde::YamlMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(expected, std::str::from_utf8(&actual).unwrap());

    let de_block = serde::YamlMetadataBlockDeserializer
        .read_manifest(expected.as_bytes())
        .unwrap();

    assert_eq!(block, de_block);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let actual_error = serde::YamlMetadataBlockDeserializer
        .read_manifest(data.as_bytes())
        .unwrap_err();

    assert_eq!(expected_error.to_string(), actual_error.to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let actual = serde_yaml::from_str::<serde::legacy::FetchStep>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    let expected = FetchStep::FilesGlob(FetchStepFilesGlob {
        path: "/opt/x/*.txt".to_owned(),
        event_time: None,
        cache: Some(SourceCaching::Forever(SourceCachingForever {})),
        order: Some(SourceOrdering::ByName),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&serde::legacy::FetchStep::from(actual)).unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let actual = serde_yaml::from_str::<serde::dataset::Transform>(data)
        .unwrap()
        .into_dto()
        .unwrap();

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
        serde_yaml::to_string(&serde::dataset::Transform::from(actual)).unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_enum_short_form() {
    let data = indoc!(
        "
        fields:
        - name: long
          type:
            kind: Timestamp
        - name: short
          type: Timestamp
        - name: full
          type:
            kind: Timestamp
            unit: Nanosecond
            timezone: UTC
        "
    );

    // Deserializes fine
    let actual = serde_yaml::from_str::<serde::data::DataSchema>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    let expected = DataSchema::new(vec![
        DataField::new(
            "long",
            DataTypeTimestamp {
                unit: None,
                timezone: None,
            },
        ),
        DataField::new(
            "short",
            DataTypeTimestamp {
                unit: None,
                timezone: None,
            },
        ),
        DataField::new(
            "full",
            DataTypeTimestamp {
                unit: Some(TimeUnit::Nanosecond),
                timezone: Some("UTC".to_owned()),
            },
        ),
    ]);

    pretty_assertions::assert_eq!(expected, actual);

    // Serialization expands to long form, but does not include defaults
    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::data::DataSchema::from(actual)).unwrap(),
        indoc!(
            "
            fields:
            - name: long
              type:
                kind: Timestamp
            - name: short
              type:
                kind: Timestamp
            - name: full
              type:
                kind: Timestamp
                unit: Nanosecond
                timezone: UTC
            "
        )
    );

    // Short form correctly handles invalid types
    let data = indoc!(
        "
        fields:
        - name: short
          type: Invalid
        "
    );

    let res = serde_yaml::from_str::<serde::data::DataSchema>(data).map(serde::IntoDto::into_dto);
    assert_matches!(res, Err(e) if e.to_string().contains("unknown variant `Invalid`"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_generics() {
    use ::serde::de::IgnoredAny as Ignore;

    let data = indoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
        headers:
          name: my-secret
        spec:
          secrets:
            tls:
              value: <cert>
        "#
    );

    // Read only header ignoring the rest
    let val = serde_yaml::from_str::<serde::resource::Resource<Ignore>>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: SecretSet::schema().clone(),
            headers: ResourceHeaders {
                id: None,
                name: "my-secret".parse().unwrap(),
                account: None,
                labels: None,
                annotations: None,
                generation: None,
                created_at: None,
                updated_at: None,
                deleted_at: None,
            },
            spec: Ignore,
            status: None,
        }
    );

    // Read & write typed spec
    let val = serde_yaml::from_str::<serde::resource::Resource<serde::config::SecretSetSpec>>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: SecretSet::schema().clone(),
            headers: ResourceHeaders {
                id: None,
                name: "my-secret".parse().unwrap(),
                account: None,
                labels: None,
                annotations: None,
                generation: None,
                created_at: None,
                updated_at: None,
                deleted_at: None,
            },
            spec: SecretSetSpec {
                secrets: Secrets {
                    entries: BTreeMap::from_iter([(
                        "tls".to_string(),
                        Secret {
                            value: "<cert>".into(),
                            content_encoding: None,
                        }
                    )]),
                }
            },
            status: None,
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(
            &serde::resource::Resource::<serde::config::SecretSetSpec>::from(val)
        )
        .unwrap(),
        data
    );

    // Read & write generic spec
    let val = serde_yaml::from_str::<serde::resource::Resource<serde_json::Value>>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: SecretSet::schema().clone(),
            headers: ResourceHeaders {
                id: None,
                name: "my-secret".parse().unwrap(),
                account: None,
                labels: None,
                annotations: None,
                generation: None,
                created_at: None,
                updated_at: None,
                deleted_at: None,
            },
            spec: json!({
                "secrets": {
                    "tls": {
                        "value": "<cert>",
                    }
                }
            }),
            status: None,
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::resource::Resource::<serde_json::Value>::from(val)).unwrap(),
        data
    );

    // Read & write typed spec - all fields
    let data = indoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
        headers:
          id: aa-12345
          name: my-secret
          account:
            name: sergiimk
          labels:
            nested:
              a: x
              b: y
            string: foo
          annotations:
            nested:
              a: x
              b: y
            string: foo
          generation: 1
          createdAt: 2026-01-01T00:00:00Z
          updatedAt: 2026-01-01T00:00:00Z
          deletedAt: 2026-01-01T00:00:00Z
        spec:
          secrets:
            tls:
              value: <cert>
        status:
          phase: Ready
          observedGeneration: 1
          reconciledAt: 2026-01-01T00:00:00Z
          conditions:
            https://opendatafabric.org/core/v1/Bool:
              value: true
            https://opendatafabric.org/core/v1/Nested:
              value:
                a: x
                b: y
            https://opendatafabric.org/core/v1/Str:
              lastTransitionTime: 2026-01-01T00:00:00Z
              observedGeneration: 1
              value: foo
        "#
    );

    let val = serde_yaml::from_str::<serde::resource::Resource<serde::config::SecretSetSpec>>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: SecretSet::schema().clone(),
            headers: ResourceHeaders {
                id: Some("aa-12345".parse().unwrap()),
                name: "my-secret".parse().unwrap(),
                account: Some("sergiimk".parse().unwrap()),
                labels: Some(ResourceLabels {
                    entries: BTreeMap::from_iter([
                        ("string".parse().unwrap(), json!("foo")),
                        ("nested".parse().unwrap(), json!({"a": "x", "b": "y"}))
                    ])
                }),
                annotations: Some(ResourceAnnotations {
                    entries: BTreeMap::from_iter([
                        ("string".parse().unwrap(), json!("foo")),
                        ("nested".parse().unwrap(), json!({"a": "x", "b": "y"}))
                    ])
                }),
                generation: Some(1),
                created_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                updated_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                deleted_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
            },
            spec: SecretSetSpec {
                secrets: Secrets {
                    entries: BTreeMap::from_iter([(
                        "tls".to_string(),
                        Secret {
                            value: "<cert>".into(),
                            content_encoding: None,
                        }
                    )]),
                }
            },
            status: Some(ResourceStatus {
                phase: ResourcePhase::Ready,
                observed_generation: Some(1),
                reconciled_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                conditions: Some(ResourceConditions {
                    entries: BTreeMap::from_iter([
                        (
                            "https://opendatafabric.org/core/v1/Str".parse().unwrap(),
                            json!({
                                "value": "foo",
                                "observedGeneration": 1,
                                "lastTransitionTime": "2026-01-01T00:00:00Z",
                            })
                        ),
                        (
                            "https://opendatafabric.org/core/v1/Bool".parse().unwrap(),
                            json!({"value": true})
                        ),
                        (
                            "https://opendatafabric.org/core/v1/Nested".parse().unwrap(),
                            json!({"value": {"a": "x", "b": "y"}})
                        )
                    ])
                }),
            }),
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(
            &serde::resource::Resource::<serde::config::SecretSetSpec>::from(val)
        )
        .unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_short_forms() {
    #[serde_with::serde_as]
    #[derive(Debug, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
    struct MySpec {
        #[serde_as(as = "serde::config::Secrets")]
        secrets: Secrets,
        #[serde_as(as = "serde::config::Variables")]
        variables: Variables,
    }

    impl IntoDto for MySpec {
        type Dto = Self;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            Ok(self)
        }
    }

    let data = indoc!(
        r#"
        $schema: https://kamu.dev/schemas/core/v1/MyResource
        headers:
          name: my-thing
          account: sergiimk
        spec:
          secrets:
            tls: <cert>
          variables:
            poolSize: '10'
        "#
    );

    let val = serde_yaml::from_str::<serde::resource::Resource<MySpec>>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: "https://kamu.dev/schemas/core/v1/MyResource"
                .parse()
                .unwrap(),
            headers: ResourceHeaders {
                id: None,
                name: "my-thing".parse().unwrap(),
                account: Some("sergiimk".parse().unwrap()),
                labels: None,
                annotations: None,
                generation: None,
                created_at: None,
                updated_at: None,
                deleted_at: None,
            },
            spec: MySpec {
                secrets: Secrets {
                    entries: BTreeMap::from_iter([(
                        "tls".to_string(),
                        Secret {
                            value: "<cert>".into(),
                            content_encoding: None,
                        }
                    )]),
                },
                variables: Variables {
                    entries: BTreeMap::from_iter([(
                        "poolSize".to_string(),
                        Variable { value: "10".into() }
                    )]),
                }
            },
            status: None,
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::resource::Resource::<MySpec>::from(val)).unwrap(),
        indoc!(
            r#"
            $schema: https://kamu.dev/schemas/core/v1/MyResource
            headers:
              name: my-thing
              account:
                name: sergiimk
            spec:
              secrets:
                tls:
                  value: <cert>
              variables:
                poolSize:
                  value: '10'
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_ref() {
    let data = indoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relations
        headers:
          name: alice-bob
        spec:
          relations:
            - subject:
                type: Account
                name: alice
              relation: role
              value: maintainer
              object:
                type: Dataset
                account: bob
                name: bobs-dataset
          attributes:
            - object: Dataset:bob/bobs-dataset # Short form of `{ type: Dataset, account: bob, name: bobs-dataset }`
              name: allowPublicRead
              value: true
        "#
    );

    let val = serde_yaml::from_str::<serde::resource::Resource<serde::auth::RelationsSpec>>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: Relations::schema().clone(),
            headers: ResourceHeaders {
                id: None,
                name: "alice-bob".parse().unwrap(),
                account: None,
                labels: None,
                annotations: None,
                generation: None,
                created_at: None,
                updated_at: None,
                deleted_at: None,
            },
            spec: RelationsSpec {
                relations: Some(vec![Relation {
                    subject: ResourceRef::Name {
                        account: None,
                        typ: TypeRef::Name("Account".parse().unwrap()),
                        name: "alice".parse().unwrap(),
                    },
                    relation: "role".to_string(),
                    value: Some(json!("maintainer")),
                    object: ResourceRef::Name {
                        account: Some(AccountRef::Name("bob".parse().unwrap())),
                        typ: TypeRef::Name("Dataset".parse().unwrap()),
                        name: "bobs-dataset".parse().unwrap(),
                    },
                }]),
                attributes: Some(vec![Attribute {
                    object: ResourceRef::Name {
                        account: Some(AccountRef::Name("bob".parse().unwrap())),
                        typ: TypeRef::Name("Dataset".parse().unwrap()),
                        name: "bobs-dataset".parse().unwrap(),
                    },
                    name: "allowPublicRead".to_string(),
                    value: json!(true),
                },],),
            },
            status: None,
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(&serde::resource::Resource::<serde::auth::RelationsSpec>::from(val))
            .unwrap(),
        indoc!(
            r#"
            $schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relations
            headers:
              name: alice-bob
            spec:
              relations:
              - subject:
                  type: Account
                  name: alice
                relation: role
                value: maintainer
                object:
                  account:
                    name: bob
                  type: Dataset
                  name: bobs-dataset
              attributes:
              - object:
                  account:
                    name: bob
                  type: Dataset
                  name: bobs-dataset
                name: allowPublicRead
                value: true
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
