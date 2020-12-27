use chrono::{DateTime, Utc};
use opendatafabric::serde::yaml::formats::datetime_rfc3339_opt;
use opendatafabric::DatasetIDBuf;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKind {
    Root,
    Derivative,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSummary {
    pub id: DatasetIDBuf,
    pub kind: DatasetKind,
    pub dependencies: Vec<DatasetIDBuf>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub last_pulled: Option<DateTime<Utc>>,
    pub num_records: u64,
    pub data_size: u64,
}
