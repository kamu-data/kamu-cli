use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

pub type RemoteID = str;
pub type RemoteIDBuf = String;

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Remote {
    pub url: Url,
}
