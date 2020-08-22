use super::dtos_odf::DatasetVocabulary;
use super::formats::datetime_rfc3339_opt;
use crate::domain::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest<T> {
    pub api_version: i32,
    pub kind: String,
    pub content: T,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKind {
    Root,
    Derivative,
    Remote,
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
    pub vocab: DatasetVocabulary,
}

impl Default for DatasetVocabulary {
    fn default() -> Self {
        Self {
            system_time_column: None,
            event_time_column: None,
        }
    }
}
