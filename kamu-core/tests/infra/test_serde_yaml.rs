use kamu::infra::{DatasetKind, DatasetSummary, Manifest};
use opendatafabric::*;

use chrono::prelude::*;
use indoc::indoc;
use std::convert::TryFrom;

#[test]
fn serde_dataset_summary() {
  let data = indoc!(
    "
    ---
    apiVersion: 1
    kind: DatasetSummary
    content:
      id: foo.bar
      kind: root
      lastBlockHash: 8b310549db6da0e1fedadda588a0e3c809f99454879f9953b06dbbb8af6a91ca
      dependencies:
        - foo
        - bar
      lastPulled: \"2020-01-01T12:00:00Z\"
      numRecords: 100
      dataSize: 1024"
  );

  let actual: Manifest<DatasetSummary> = serde_yaml::from_str(data).unwrap();

  let expected = Manifest {
    api_version: 1,
    kind: "DatasetSummary".to_owned(),
    content: DatasetSummary {
      id: DatasetIDBuf::try_from("foo.bar").unwrap(),
      kind: DatasetKind::Root,
      last_block_hash: Sha3_256::from_str(
        "8b310549db6da0e1fedadda588a0e3c809f99454879f9953b06dbbb8af6a91ca",
      )
      .unwrap(),
      dependencies: vec![
        DatasetIDBuf::try_from("foo").unwrap(),
        DatasetIDBuf::try_from("bar").unwrap(),
      ],
      last_pulled: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
      data_size: 1024,
      num_records: 100,
    },
  };

  assert_eq!(expected, actual);
  assert_eq!(serde_yaml::to_string(&actual).unwrap(), data);
}
