// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use chrono::prelude::*;
use digest::Digest;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::serde::*;
use opendatafabric::*;

///////////////////////////////////////////////////////////////////////////////

fn get_test_events() -> [(MetadataEvent, &'static str); 6] {
    [
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
            "b7b9d53f84abee88393a92bde7f615c3a3cd2d33a7d9df1409c8a8ffee8aaf2c",
        ),
        (
            MetadataEvent::AddData(AddData {
                input_checkpoint: None,
                output_data: None,
                output_checkpoint: None,
                output_watermark: None,
                source_state: None,
            }),
            "eb745e4f975c66d8794afce2f5792a888ecf3cb367e2c90bf108753a34a0690e",
        ),
        (
            MetadataEvent::AddData(AddData {
                input_checkpoint: None,
                output_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    interval: OffsetInterval { start: 0, end: 100 },
                    size: 100,
                }),
                output_checkpoint: None,
                output_watermark: None,
                source_state: None,
            }),
            "5f5736202e41d0da69ec9835bb0d15efc23e844dd98243da585135c9f772812d",
        ),
        (
            MetadataEvent::AddData(AddData {
                input_checkpoint: None,
                output_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    interval: OffsetInterval { start: 0, end: 100 },
                    size: 100,
                }),
                output_checkpoint: None,
                output_watermark: None,
                source_state: Some(SourceState {
                    kind: "odf/etag".to_owned(),
                    source: "odf/polling-source".to_owned(),
                    value: "SOME_ETAG".to_owned(),
                }),
            }),
            "bb459566b5913507f6627a07e13ab60bbd16948ee4d8fb640e77fc4c3df57998",
        ),
        (
            MetadataEvent::SetTransform(SetTransform {
                inputs: vec![
                    TransformInput {
                        id: Some(DatasetID::new_seeded_ed25519(b"input1")),
                        name: DatasetName::try_from("input1").unwrap(),
                        dataset_ref: None,
                    },
                    TransformInput {
                        id: Some(DatasetID::new_seeded_ed25519(b"input2")),
                        name: DatasetName::try_from("input2").unwrap(),
                        dataset_ref: None,
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
            "e634a72f9a9a314a64afb26fd0d34012a08c7d47402dfbc78afa78edac978777",
        ),
        (
            MetadataEvent::ExecuteQuery(ExecuteQuery {
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
            "6b9715492945e2c69a335a1cb292ccad9d3f856e6c202ca0e8335ca9e45dd9ca",
        ),
    ]
}

///////////////////////////////////////////////////////////////////////////////

const TEST_SEQUENCE_NUMBER: i32 = 117;

fn wrap_into_block(event: MetadataEvent) -> MetadataBlock {
    MetadataBlock {
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        sequence_number: TEST_SEQUENCE_NUMBER,
        event,
    }
}

///////////////////////////////////////////////////////////////////////////////

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
    for (event, hash_expected) in get_test_events() {
        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(&wrap_into_block(event))
            .unwrap();

        let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));

        assert_eq!(hash_actual, hash_expected);
    }
}

#[cfg(feature = "arrow")]
#[test]
fn serde_set_data_schema() {
    use arrow::datatypes::*;

    let expected_schema = SchemaRef::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Boolean, false),
    ]));

    let event: MetadataEvent = SetDataSchema::new(&expected_schema).into();

    let expected_block = wrap_into_block(event);

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let actual_block = FlatbuffersMetadataBlockDeserializer
        .read_manifest(&buffer)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));
    let hash_expected = "d2f009192573b1ce449c04a8b1a3866ad0f86bb15808f447481f735cd90d95f0";

    assert_eq!(hash_actual, hash_expected);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema_as_arrow()
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[test]
fn serde_execute_query_response() {
    let examples = [
        ExecuteQueryResponse::Success(ExecuteQueryResponseSuccess {
            data_interval: Some(OffsetInterval { start: 0, end: 10 }),
            output_watermark: Some(Utc::now()),
        }),
        ExecuteQueryResponse::InvalidQuery(ExecuteQueryResponseInvalidQuery {
            message: "boop".to_owned(),
        }),
        ExecuteQueryResponse::InternalError(ExecuteQueryResponseInternalError {
            message: "boop".to_owned(),
            backtrace: Some("woop".to_owned()),
        }),
        ExecuteQueryResponse::Progress(ExecuteQueryResponseProgress {}),
    ];

    for expected in examples {
        let buf = FlatbuffersEngineProtocol
            .write_execute_query_response(&expected)
            .unwrap();
        let actual = FlatbuffersEngineProtocol
            .read_execute_query_response(&buf)
            .unwrap();
        assert_eq!(actual, expected);
    }
}
