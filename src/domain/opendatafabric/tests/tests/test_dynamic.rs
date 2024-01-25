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
use opendatafabric::*;

const TEST_SEQUENCE_NUMBER: u64 = 132;

fn load() -> MetadataBlock {
    MetadataBlock {
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event: MetadataEvent::SetTransform(SetTransform {
            inputs: vec![
                TransformInput {
                    dataset_ref: DatasetID::new_seeded_ed25519(b"input1").into(),
                    alias: Some("input1".to_string()),
                },
                TransformInput {
                    dataset_ref: DatasetRef::try_from("kamu/input2").unwrap(),
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
        sequence_number: TEST_SEQUENCE_NUMBER,
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
    assert_eq!(block.sequence_number(), TEST_SEQUENCE_NUMBER);
    let dynamic::MetadataEvent::SetTransform(transform) = block.event() else {
        panic!()
    };
    let inputs: Vec<TransformInput> = transform.inputs().map(|i| i.into()).collect();
    assert_eq!(
        inputs,
        vec![
            TransformInput {
                dataset_ref: DatasetID::new_seeded_ed25519(b"input1").into(),
                alias: Some("input1".to_string()),
            },
            TransformInput {
                dataset_ref: DatasetRef::try_from("kamu/input2").unwrap(),
                alias: Some("input2".to_string()),
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
