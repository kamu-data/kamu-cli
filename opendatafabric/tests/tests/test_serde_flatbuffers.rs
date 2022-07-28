// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::serde::{flatbuffers::*, EngineProtocolDeserializer, EngineProtocolSerializer};
use opendatafabric::*;

use chrono::prelude::*;
use digest::Digest;
use std::convert::TryFrom;

///////////////////////////////////////////////////////////////////////////////

const TEST_SEQUENCE_NUMBER: i32 = 117;

fn get_block_root() -> MetadataBlock {
    MetadataBlock {
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
        event: MetadataEvent::SetPollingSource(SetPollingSource {
            fetch: FetchStep::FilesGlob(FetchStepFilesGlob {
                path: "./*.csv".to_owned(),
                event_time: Some(EventTimeSource::FromMetadata),
                cache: Some(SourceCaching::Forever),
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
        sequence_number: TEST_SEQUENCE_NUMBER,
    }
}

fn get_block_deriv() -> Vec<MetadataBlock> {
    vec![
        MetadataBlock {
            prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
            system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
            event: MetadataEvent::SetTransform(SetTransform {
                inputs: vec![
                    TransformInput {
                        id: Some(DatasetID::from_pub_key_ed25519(b"input1")),
                        name: DatasetName::try_from("input1").unwrap(),
                    },
                    TransformInput {
                        id: Some(DatasetID::from_pub_key_ed25519(b"input2")),
                        name: DatasetName::try_from("input2").unwrap(),
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
            sequence_number: TEST_SEQUENCE_NUMBER,
        },
        MetadataBlock {
            prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
            system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
            event: MetadataEvent::ExecuteQuery(ExecuteQuery {
                input_slices: vec![
                    InputSlice {
                        dataset_id: DatasetID::from_pub_key_ed25519(b"input1"),
                        block_interval: Some(BlockInterval {
                            start: Multihash::from_digest_sha3_256(b"a"),
                            end: Multihash::from_digest_sha3_256(b"b"),
                        }),
                        data_interval: Some(OffsetInterval { start: 10, end: 20 }),
                    },
                    InputSlice {
                        dataset_id: DatasetID::from_pub_key_ed25519(b"input2"),
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
                output_watermark: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            }),
            sequence_number: TEST_SEQUENCE_NUMBER,
        },
    ]
}

///////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_metadata_block_root() {
    let expected = get_block_root();

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&expected)
        .unwrap();
    let actual = FlatbuffersMetadataBlockDeserializer
        .read_manifest(&buffer)
        .unwrap();
    assert_eq!(expected, actual);

    // Ensure produces same binary result
    let buffer2 = FlatbuffersMetadataBlockSerializer
        .write_manifest(&actual)
        .unwrap();
    assert_eq!(buffer.inner(), buffer2.inner());
}

#[test]
fn serde_metadata_block_deriv() {
    for expected in get_block_deriv() {
        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(&expected)
            .unwrap();
        let actual = FlatbuffersMetadataBlockDeserializer
            .read_manifest(&buffer)
            .unwrap();
        assert_eq!(expected, actual);

        // Ensure produces same binary result
        let buffer2 = FlatbuffersMetadataBlockSerializer
            .write_manifest(&actual)
            .unwrap();
        assert_eq!(buffer.inner(), buffer2.inner());
    }
}

#[test]
fn serializer_hashes_are_stable_root() {
    let block = get_block_root();

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(
        format!("{:x}", sha3::Sha3_256::digest(&buffer)),
        "8afb3148121ced75b2762f238a21d399b1d453cfebad21afca1c37afff6eb8f4"
    );
}

#[test]
fn serializer_hashes_are_stable_deriv() {
    let expected_hashes = vec![
        "1c09c5b547a77217d50039d395e2db096128ff362ba2ca36625ba894414504c6",
        "66de102280d9aa4485a6013354d4dddcbe4229c6696002f44b8de081f1b7d564",
    ];

    for (block, expected_hash) in get_block_deriv().iter().zip(expected_hashes) {
        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .unwrap();

        assert_eq!(
            format!("{:x}", sha3::Sha3_256::digest(&buffer)),
            expected_hash
        );
    }
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
        ExecuteQueryResponse::Progress,
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
