// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::parquet::record::RowAccessor;
use indoc::indoc;
use itertools::Itertools;
use opendatafabric::*;

use crate::utils::Kamu;

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_push_ingest_from_file() {
    let kamu = Kamu::new_workspace_tmp().await;

    kamu.add_dataset(DatasetSnapshot {
        name: "population".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![AddPushSource {
            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "city STRING".to_owned(),
                    "population BIGINT".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: MergeStrategy::Ledger(MergeStrategyLedger {
                primary_key: vec!["event_time".to_owned(), "city".to_owned()],
            }),
        }
        .into()],
    })
    .await
    .unwrap();

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(
        &data_path,
        indoc!(
            "
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    kamu.execute([
        "ingest",
        "population",
        "--input-format",
        "csv",
        path(&data_path),
    ])
    .await
    .unwrap();

    let parquet = kamu
        .get_last_data_slice(&DatasetName::new_unchecked("population"))
        .await;
    assert_eq!(
        parquet.get_column_names(),
        ["offset", "system_time", "event_time", "city", "population"]
    );
    assert_eq!(
        parquet
            .get_row_iter()
            .map(|r| r.unwrap())
            .map(|r| (r.get_string(3).unwrap().clone(), r.get_long(4).unwrap()))
            .sorted()
            .collect::<Vec<_>>(),
        [
            ("A".to_owned(), 1000),
            ("B".to_owned(), 2000),
            ("C".to_owned(), 3000)
        ]
    );
}

fn path(p: &Path) -> &str {
    p.as_os_str().to_str().unwrap()
}
