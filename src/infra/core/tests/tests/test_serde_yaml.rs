// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use indoc::indoc;
use odf::metadata::serde::yaml::Manifest;

#[test]
fn serde_dataset_summary() {
    let data = indoc!(
        "
        kind: DatasetSummary
        version: 1
        content:
          id: did:odf:fed01626f6f21b8373800626f6f21b837380020f6f606070000008d6edc2eb877e8cc
          kind: Root
          lastBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
          dependencies:
          - did:odf:fed01666f6fb3b7370000666f6fb3b737000060f6f60600000000895cddbcb7f7b8cc
          - did:odf:fed01626172b130390000626172b1303900002016260700000000508ebebd3079f00e
          lastPulled: 2020-01-01T12:00:00Z
          numRecords: 100
          dataSize: 1024
          checkpointsSize: 64
        "
    );

    let actual: Manifest<odf::DatasetSummary> = serde_yaml::from_str(data).unwrap();

    let expected = Manifest {
        kind: "DatasetSummary".to_owned(),
        version: 1,
        content: odf::DatasetSummary {
            id: odf::DatasetID::new_seeded_ed25519(b"boop"),
            kind: odf::DatasetKind::Root,
            last_block_hash: odf::Multihash::from_multibase(
                "zW1mJtUjH235JZ4BBpJBousTNHaDXer4r4QzSdsqTfKENrr",
            )
            .unwrap(),
            dependencies: vec![
                odf::DatasetID::new_seeded_ed25519(b"foo"),
                odf::DatasetID::new_seeded_ed25519(b"bar"),
            ],
            last_pulled: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
            num_records: 100,
            data_size: 1024,
            checkpoints_size: 64,
        },
    };

    assert_eq!(expected.content, actual.content);
    assert_eq!(serde_yaml::to_string(&actual).unwrap(), data);
}
