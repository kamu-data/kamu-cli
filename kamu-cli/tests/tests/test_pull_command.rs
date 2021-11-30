// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{convert::TryFrom, path::Path};

use indoc::indoc;
use opendatafabric::*;
use parquet::record::RowAccessor;
use url::Url;

use crate::utils::Kamu;

#[test_log::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_pull_ingest_from_file() {
    let kamu = Kamu::new_workspace_tmp();

    kamu.add_dataset(DatasetSnapshot {
        id: DatasetIDBuf::try_from("population").unwrap(),
        source: DatasetSource::Root(DatasetSourceRoot {
            fetch: FetchStep::Url(FetchStepUrl {
                url: Url::from_file_path(&kamu.workspace_path().join("data.csv"))
                    .unwrap()
                    .to_string(),
                event_time: None,
                cache: None,
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
        }),
        vocab: None,
    })
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

        kamu.execute(["pull", "population"]).unwrap();

        let parquet = kamu.get_last_data_slice(DatasetID::new_unchecked("population"));
        assert_eq!(
            parquet.column_names(),
            ["system_time", "event_time", "city", "population"]
        );
        assert_eq!(
            parquet.records(|r| (r.get_string(2).unwrap().clone(), r.get_long(3).unwrap())),
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
            .unwrap();

        let parquet = kamu.get_last_data_slice(DatasetID::new_unchecked("population"));
        assert_eq!(
            parquet.column_names(),
            ["system_time", "event_time", "city", "population"]
        );
        assert_eq!(
            parquet.records(|r| (r.get_string(2).unwrap().clone(), r.get_long(3).unwrap())),
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
