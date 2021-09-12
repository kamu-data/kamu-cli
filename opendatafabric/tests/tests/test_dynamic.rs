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
        block_hash: Sha3_256::new([0xAA; 32]),
        prev_block_hash: Some(Sha3_256::new([0xBB; 32])),
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
            system_time_column: None,
            event_time_column: Some("date".to_owned()),
        }),
        output_slice: Some(DataSlice {
            hash: Sha3_256::new([0xA; 32]),
            interval: TimeInterval::singleton(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            num_records: 10,
        }),
        output_watermark: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
        input_slices: Some(vec![
            DataSlice {
                hash: Sha3_256::new([0xB; 32]),
                interval: TimeInterval::unbounded_closed_right(
                    Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
                ),
                num_records: 10,
            },
            DataSlice {
                hash: Sha3_256::new([0xC; 32]),
                interval: TimeInterval::empty(),
                num_records: 0,
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
    assert_eq!(block.block_hash(), &Sha3_256::new([0xAA; 32]));
    assert_eq!(block.prev_block_hash().unwrap(), &Sha3_256::new([0xBB; 32]));
    let source = match block.source().unwrap() {
        dynamic::DatasetSource::Derivative(source) => source,
        _ => panic!(),
    };
    let inputs: Vec<DatasetIDBuf> = source.inputs().map(|id| id.to_owned()).collect();
    assert_eq!(
        inputs,
        vec![
            DatasetIDBuf::try_from("input1").unwrap(),
            DatasetIDBuf::try_from("input2").unwrap(),
        ]
    );
}

#[test]
fn test_conversion() {
    let block_dyn = load_dynamic();
    let block_dto: MetadataBlock = block_dyn.as_ref().into();
    assert_eq!(block_dto, load());
}
