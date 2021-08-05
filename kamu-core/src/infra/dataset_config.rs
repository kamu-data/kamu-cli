use opendatafabric::DatasetRefBuf;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetConfig {
    pub pull_aliases: Vec<DatasetRefBuf>,
    pub push_aliases: Vec<DatasetRefBuf>,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            pull_aliases: Vec::new(),
            push_aliases: Vec::new(),
        }
    }
}
