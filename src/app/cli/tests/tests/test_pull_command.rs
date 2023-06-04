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
use url::Url;

use crate::utils::Kamu;

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_pull_ingest_from_file() {
    let kamu = Kamu::new_workspace_tmp().await;

    kamu.add_dataset(DatasetSnapshot {
        name: "population".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![MetadataEvent::SetPollingSource(SetPollingSource {
            fetch: FetchStep::Url(FetchStepUrl {
                url: Url::from_file_path(&kamu.workspace_path().join("data.csv"))
                    .unwrap()
                    .to_string(),
                event_time: None,
                cache: None,
                headers: None,
            }),
            prepare: None,
            read: ReadStep::Csv(ReadStepCsv {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "city STRING".to_owned(),
                    "population BIGINT".to_owned(),
                ]),
                ..ReadStepCsv::default()
            }),
            preprocess: None,
            merge: MergeStrategy::Ledger(MergeStrategyLedger {
                primary_key: vec!["event_time".to_owned(), "city".to_owned()],
            }),
        })],
    })
    .await
    .unwrap();

    {
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

        kamu.execute(["pull", "population"]).await.unwrap();

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

    {
        let data_path = kamu.workspace_path().join("data2.csv");
        std::fs::write(
            &data_path,
            indoc!(
                "
            2021-01-01,A,1100
            2021-01-01,B,2100
            2021-01-01,C,3100
            "
            ),
        )
        .unwrap();

        kamu.execute(["pull", "population", "--fetch", path(&data_path)])
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
                .map(|r| (r.get_string(3).unwrap().clone(), r.get_long(4).unwrap()))
                .sorted()
                .collect::<Vec<_>>(),
            [
                ("A".to_owned(), 1100),
                ("B".to_owned(), 2100),
                ("C".to_owned(), 3100)
            ]
        );
    }
}

fn path(p: &Path) -> &str {
    p.as_os_str().to_str().unwrap()
}
