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
use std::convert::TryFrom;

///////////////////////////////////////////////////////////////////////////////

fn get_block_root() -> MetadataBlock {
    MetadataBlock {
        block_hash: Sha3_256::zero(),
        prev_block_hash: Some(Sha3_256::new([0x0b; 32])),
        system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
        input_slices: None,
        output_slice: None,
        output_watermark: None,
        source: Some(DatasetSource::Root(DatasetSourceRoot {
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
        })),
        vocab: Some(DatasetVocabulary {
            event_time_column: Some("date".to_owned()),
            ..Default::default()
        }),
    }
}

fn get_block_deriv() -> MetadataBlock {
    MetadataBlock {
        block_hash: Sha3_256::zero(),
        prev_block_hash: Some(Sha3_256::new([0x0b; 32])),
        system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
        source: Some(DatasetSource::Derivative(DatasetSourceDerivative {
            inputs: vec![
                DatasetIDBuf::try_from("input1").unwrap(),
                DatasetIDBuf::try_from("input2").unwrap(),
            ],
            transform: Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SELECT * FROM input1 UNION ALL SELECT * FROM input2".to_owned()),
                queries: None,
                temporal_tables: None,
            }),
        })),
        vocab: Some(DatasetVocabulary {
            event_time_column: Some("date".to_owned()),
            ..Default::default()
        }),
        output_slice: Some(OutputSlice {
            data_logical_hash: Sha3_256::new([0x0a; 32]),
            data_interval: OffsetInterval { start: 10, end: 20 },
        }),
        output_watermark: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
        input_slices: Some(vec![
            InputSlice {
                dataset_id: DatasetIDBuf::try_from("input1").unwrap(),
                block_interval: Some(BlockInterval {
                    start: Sha3_256::new([0xB; 32]),
                    end: Sha3_256::new([0xC; 32]),
                }),
                data_interval: Some(OffsetInterval { start: 10, end: 20 }),
            },
            InputSlice {
                dataset_id: DatasetIDBuf::try_from("input2").unwrap(),
                block_interval: Some(BlockInterval {
                    start: Sha3_256::new([0xB; 32]),
                    end: Sha3_256::new([0xC; 32]),
                }),
                data_interval: None,
            },
        ]),
    }
}

///////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_metadata_block_root() {
    let expected = get_block_root();

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&expected)
        .unwrap();
    let actual = FlatbuffersMetadataBlockDeserializer
        .read_manifest_unchecked(&buffer)
        .unwrap();
    assert_eq!(expected, actual);

    // Ensure produces same binary result
    let buffer2 = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&actual)
        .unwrap();
    assert_eq!(buffer.inner(), buffer2.inner());
}

#[test]
fn serde_metadata_block_deriv() {
    let expected = get_block_deriv();

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&expected)
        .unwrap();
    let actual = FlatbuffersMetadataBlockDeserializer
        .read_manifest_unchecked(&buffer)
        .unwrap();
    assert_eq!(expected, actual);

    // Ensure produces same binary result
    let buffer2 = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&actual)
        .unwrap();
    assert_eq!(buffer.inner(), buffer2.inner());
}

#[test]
fn serializer_hashes_are_stable_root() {
    let block = get_block_root();

    let (block_hash, _) = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(
        block_hash,
        Sha3_256::try_from("d8be7cde15b4507b98226444a8e75c3459be33de5aa2abf59974ed8f9a71faea")
            .unwrap()
    );
}

#[test]
fn serializer_hashes_are_stable_deriv() {
    let block = get_block_deriv();

    let (block_hash, _) = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(
        block_hash,
        Sha3_256::try_from("f4c565176cbebc34f551e3bb86a891eca006d92fcdf6b87c66ff20ebe6e3c0f9")
            .unwrap()
    );
}

#[test]
fn serializer_rejects_incorrect_hashes() {
    let invalid = MetadataBlock {
        block_hash: Sha3_256::new([0xab; 32]),
        ..get_block_root()
    };

    assert!(matches!(
        FlatbuffersMetadataBlockSerializer.write_manifest(&invalid),
        Err(opendatafabric::serde::Error::InvalidHash { .. })
    ));
}

#[test]
fn deserializer_rejects_incorrect_hashes() {
    let invalid = MetadataBlock {
        block_hash: Sha3_256::new([0xab; 32]),
        ..get_block_root()
    };

    let buf = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&invalid)
        .unwrap();

    assert!(matches!(
        FlatbuffersMetadataBlockDeserializer.validate_manifest(&buf),
        Err(opendatafabric::serde::Error::InvalidHash { .. })
    ));

    assert!(matches!(
        FlatbuffersMetadataBlockDeserializer.read_manifest(&buf),
        Err(opendatafabric::serde::Error::InvalidHash { .. })
    ));
}

#[test]
fn serde_execute_query_response() {
    let examples = [
        ExecuteQueryResponse::Success(ExecuteQueryResponseSuccess {
            metadata_block: MetadataBlock { ..get_block_root() },
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
