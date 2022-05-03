// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::DatasetSummary;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use chrono::prelude::*;
use indoc::indoc;
use std::convert::TryFrom;

#[test]
fn serde_dataset_summary() {
    let data = indoc!(
        "
    ---
    kind: DatasetSummary
    version: 1
    content:
      id: \"did:odf:z4k88e8rX1oHBg1rS4kJb3KKj7xxBQRcCxRChnDA8KsXywfSBdh\"
      kind: root
      lastBlockHash: zW1mJtUjH235JZ4BBpJBousTNHaDXer4r4QzSdsqTfKENrr
      dependencies:
        - name: foo
        - name: bar
      lastPulled: \"2020-01-01T12:00:00Z\"
      numRecords: 100
      dataSize: 1024
      checkpointsSize: 64\n"
    );

    let actual: Manifest<DatasetSummary> = serde_yaml::from_str(data).unwrap();

    let expected = Manifest {
        kind: "DatasetSummary".to_owned(),
        version: 1,
        content: DatasetSummary {
            id: DatasetID::from_pub_key_ed25519(b"boop"),
            kind: DatasetKind::Root,
            last_block_hash: Multihash::from_multibase_str(
                "zW1mJtUjH235JZ4BBpJBousTNHaDXer4r4QzSdsqTfKENrr",
            )
            .unwrap(),
            dependencies: vec![
                TransformInput {
                    id: None,
                    name: DatasetName::try_from("foo").unwrap(),
                },
                TransformInput {
                    id: None,
                    name: DatasetName::try_from("bar").unwrap(),
                },
            ],
            last_pulled: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            num_records: 100,
            data_size: 1024,
            checkpoints_size: 64,
        },
    };

    assert_eq!(expected.content, actual.content);
    assert_eq!(serde_yaml::to_string(&actual).unwrap(), data);
}
