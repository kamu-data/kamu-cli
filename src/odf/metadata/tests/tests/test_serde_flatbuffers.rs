// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use digest::Digest;
use opendatafabric_metadata::serde::flatbuffers::*;
use opendatafabric_metadata::serde::*;
use opendatafabric_metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_test_events() -> [(MetadataEvent, &'static str); 8] {
    [
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: None,
                prev_offset: None,
                new_data: None,
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: None,
            }),
            "73e2977b8beae4aef53670067c1173b9d68e69721908d080b84286ab366d1e14",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"prev")),
                prev_offset: Some(9),
                new_data: None,
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: None,
            }),
            "32516689d582e02e77778af75a71b6d131a640d0bebed99b99d96b385cee915f",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"prev")),
                prev_offset: Some(9),
                new_data: None,
                new_checkpoint: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
                new_source_state: None,
            }),
            "1ad91cabca81e1f1771f28070980bfdcf409e5f8a877b3976b611401d2998189",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"prev")),
                prev_offset: Some(9),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    offset_interval: OffsetInterval { start: 10, end: 99 },
                    size: 100,
                }),
                new_checkpoint: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
                new_source_state: None,
            }),
            "ec1d3f6aa39bb256fcae16d79a95705f614e26d9a86d393b6f9e6b19a60d0df6",
        ),
        (
            MetadataEvent::SetPollingSource(SetPollingSource {
                fetch: FetchStep::FilesGlob(FetchStepFilesGlob {
                    path: "./*.csv".to_owned(),
                    event_time: Some(EventTimeSource::FromMetadata(
                        EventTimeSourceFromMetadata {},
                    )),
                    cache: Some(SourceCaching::Forever(SourceCachingForever {})),
                    order: Some(SourceOrdering::ByName),
                }),
                prepare: Some(vec![PrepStep::Decompress(PrepStepDecompress {
                    format: CompressionFormat::Gzip,
                    sub_path: None,
                })]),
                read: ReadStep::GeoJson(ReadStepGeoJson {
                    schema: Some(vec!["a: INT".to_owned(), "b: INT".to_owned()]),
                }),
                preprocess: Some(Transform::Sql(TransformSql {
                    engine: "spark".to_owned(),
                    version: Some("1.0.0".to_owned()),
                    query: Some("SELECT * FROM input".to_owned()),
                    queries: None,
                    temporal_tables: None,
                })),
                merge: MergeStrategy::Ledger(MergeStrategyLedger {
                    primary_key: vec!["a".to_owned()],
                }),
            }),
            "18cc1680b3d36f63358b59d469d76dcbf71ddac3ea66a693ce4158cfc5dfb28d",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"checkpoint")),
                prev_offset: Some(9),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    offset_interval: OffsetInterval { start: 10, end: 99 },
                    size: 100,
                }),
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: Some(SourceState {
                    source_name: "push-source-1".to_owned(),
                    kind: "kamu/kafka-offset".to_owned(),
                    value: "SOME_OFFSET".to_owned(),
                }),
            }),
            "db5044313bbd596fb54bef9387ad61a18206d1e93e833db4d1159d20cd3cac58",
        ),
        (
            MetadataEvent::SetTransform(SetTransform {
                inputs: vec![
                    TransformInput {
                        dataset_ref: DatasetID::new_seeded_ed25519(b"input1").into(),
                        alias: Some("input1".to_string()),
                    },
                    TransformInput {
                        dataset_ref: DatasetName::try_from("input2").unwrap().into(),
                        alias: Some("input2".to_string()),
                    },
                ],
                transform: Transform::Sql(TransformSql {
                    engine: "spark".to_owned(),
                    version: None,
                    query: Some("SELECT * FROM input1 UNION ALL SELECT * FROM input2".to_owned()),
                    queries: None,
                    temporal_tables: None,
                }),
            }),
            "dc2ab3737792021b6794d3969315bb521701780c91548104f149906d98c8da70",
        ),
        (
            MetadataEvent::ExecuteTransform(ExecuteTransform {
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
            "a955281dca303056a88e39291d17e045d88ecdafca3ef9e75b09b83169633b52",
        ),
    ]
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_SEQUENCE_NUMBER: u64 = 117;

fn wrap_into_block(event: MetadataEvent) -> MetadataBlock {
    MetadataBlock {
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        sequence_number: TEST_SEQUENCE_NUMBER,
        event,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_metadata_block() {
    for (event, _) in get_test_events() {
        let expected = wrap_into_block(event);

        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(&expected)
            .unwrap();

        let actual = FlatbuffersMetadataBlockDeserializer
            .read_manifest(&buffer)
            .unwrap();

        assert_eq!(expected, actual);
    }
}

#[test]
fn test_serializer_stability() {
    for (i, (event, hash_expected)) in get_test_events().into_iter().enumerate() {
        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(&wrap_into_block(event))
            .unwrap();

        let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));

        assert_eq!(hash_actual, hash_expected, "Case {i}");
    }
}

#[cfg(feature = "arrow")]
#[test]
fn serde_set_data_schema() {
    use serde_json::json;

    let expected_schema = DataSchema::new(vec![
        DataField::string("city").encoding(ArrowBufferEncoding::View {
            offset_bit_width: Some(32),
        }),
        DataField::u64("population"),
        DataField::string("census").optional().extra(json!({
            "kamu.dev/logicalType": "multihash",
            "kamu.dev/referenceType": "embedded",
        })),
        DataField::list("links", DataType::string()),
    ])
    .extra(json!({
        "kamu.dev/archetype": "table",
        "kamu.dev/nested": {
                "x": "a",
                "y": "b",
            },
        }
    ));

    let event: MetadataEvent = SetDataSchema::new(expected_schema.clone()).into();

    let expected_block = wrap_into_block(event);

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let actual_block = FlatbuffersMetadataBlockDeserializer
        .read_manifest(&buffer)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));
    let hash_expected = "b78ea8622a86e8452c03d7652a48568fa377b22e432dd3c124725037e87cca27";

    assert_eq!(hash_actual, hash_expected);

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

    let expected_block = wrap_into_block(event);

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let actual_block = FlatbuffersMetadataBlockDeserializer
        .read_manifest(&buffer)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));
    let hash_expected = "44d64d764fb207713407b9a761566409404c907f765993105608299cd6f5bebf";

    assert_eq!(hash_actual, hash_expected);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema_as_arrow(&ToArrowSettings::default())
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[test]
fn serde_execute_transform_response() {
    let examples = [
        TransformResponse::Success(TransformResponseSuccess {
            new_offset_interval: Some(OffsetInterval { start: 0, end: 10 }),
            new_watermark: Some(Utc::now()),
        }),
        TransformResponse::InvalidQuery(TransformResponseInvalidQuery {
            message: "boop".to_owned(),
        }),
        TransformResponse::InternalError(TransformResponseInternalError {
            message: "boop".to_owned(),
            backtrace: Some("woop".to_owned()),
        }),
        TransformResponse::Progress(TransformResponseProgress {}),
    ];

    for expected in examples {
        let buf = FlatbuffersEngineProtocol
            .write_transform_response(&expected)
            .unwrap();
        let actual = FlatbuffersEngineProtocol
            .read_transform_response(&buf)
            .unwrap();
        assert_eq!(actual, expected);
    }
}
