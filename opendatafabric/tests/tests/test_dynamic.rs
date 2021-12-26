// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use opendatafabric::*;
use std::convert::TryFrom;

fn load() -> MetadataBlock {
    MetadataBlock {
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
        seed: Some(DatasetID::from_pub_key_ed25519(b"pk")),
        source: Some(DatasetSource::Derivative(DatasetSourceDerivative {
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
        })),
        vocab: Some(DatasetVocabulary {
            event_time_column: Some("date".to_owned()),
            ..Default::default()
        }),
        output_slice: Some(OutputSlice {
            data_logical_hash: Multihash::from_digest_sha3_256(b"foo"),
            data_physical_hash: Multihash::from_digest_sha3_256(b"bar"),
            data_interval: OffsetInterval { start: 10, end: 20 },
        }),
        output_watermark: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
        input_slices: Some(vec![
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
        ]),
    }
}

fn load_dynamic() -> Box<dyn dynamic::MetadataBlock> {
    Box::new(load())
}

#[test]
fn test_accessors() {
    let block = load_dynamic();
    assert_eq!(
        *block.prev_block_hash().unwrap(),
        Multihash::from_digest_sha3_256(b"prev")
    );
    let source = match block.source().unwrap() {
        dynamic::DatasetSource::Derivative(source) => source,
        _ => panic!(),
    };
    let inputs: Vec<TransformInput> = source.inputs().map(|i| i.into()).collect();
    assert_eq!(
        inputs,
        vec![
            TransformInput {
                id: Some(DatasetID::from_pub_key_ed25519(b"input1")),
                name: DatasetName::try_from("input1").unwrap(),
            },
            TransformInput {
                id: Some(DatasetID::from_pub_key_ed25519(b"input2")),
                name: DatasetName::try_from("input2").unwrap(),
            },
        ]
    );
}

#[test]
fn test_conversion() {
    let block_dyn = load_dynamic();
    let block_dto: MetadataBlock = block_dyn.as_ref().into();
    assert_eq!(block_dto, load());
}
