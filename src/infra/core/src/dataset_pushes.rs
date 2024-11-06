use std::collections::HashMap;

use chrono::{DateTime, Utc};
use opendatafabric::{DatasetRefRemote, Multihash};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetPush {
    pub target: DatasetRefRemote,
    pub pushed_at: DateTime<Utc>,
    pub head: Multihash,
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetPushes {
    pub pushes: HashMap<DatasetRefRemote, DatasetPush>,
}

impl DatasetPushes {
    pub(crate) fn default() -> DatasetPushes {
        DatasetPushes {
            pushes: HashMap::new(),
        }
    }
}
