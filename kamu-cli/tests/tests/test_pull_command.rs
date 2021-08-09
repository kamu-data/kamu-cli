use std::{convert::TryFrom, path::Path};

use indoc::indoc;
use opendatafabric::*;

use crate::utils::Kamu;

#[test]
fn test_pull_ingest_from_file() {
    let kamu = Kamu::new_workspace_tmp();

    kamu.add_dataset(DatasetSnapshot {
        id: DatasetIDBuf::try_from("population").unwrap(),
        source: DatasetSource::Root(DatasetSourceRoot {
            fetch: FetchStep::Url(FetchStepUrl {
                url: "/nowhere".to_owned(),
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

    kamu.execute(["pull", "population", "--fetch", path(&data_path)])
        .unwrap();
}

fn path(p: &Path) -> &str {
    p.as_os_str().to_str().unwrap()
}
