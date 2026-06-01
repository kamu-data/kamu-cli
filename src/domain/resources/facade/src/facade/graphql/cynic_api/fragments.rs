use chrono::{DateTime, Utc};

use super::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct Resource {
    pub api_version: String,
    pub kind: ResourceKind,
    pub metadata: ResourceMetadata,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceKind {
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceMetadata {
    pub id: kamu_resources::ResourceUID,
    pub account_id: odf::AccountID,
    pub name: String,
    pub description: Option<String>,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub generation: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceIdentity {
    pub id: kamu_resources::ResourceUID,
    pub api_version: String,
    pub kind: ResourceKind,
    pub canonical_kind_name: String,
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceRenderManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::Enum, Debug, Clone, Copy)]
#[cynic(graphql_type = "ResourceManifestFormat")]
pub(crate) enum ResourceManifestFormat {
    Json,
    Yaml,
}

impl From<crate::ResourceManifestFormat> for ResourceManifestFormat {
    fn from(value: crate::ResourceManifestFormat) -> Self {
        match value {
            crate::ResourceManifestFormat::Json => Self::Json,
            crate::ResourceManifestFormat::Yaml => Self::Yaml,
        }
    }
}

impl From<ResourceManifestFormat> for crate::ResourceManifestFormat {
    fn from(value: ResourceManifestFormat) -> Self {
        match value {
            ResourceManifestFormat::Json => Self::Json,
            ResourceManifestFormat::Yaml => Self::Yaml,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceProblem {
    pub request_index: i32,
    pub code: BatchResourceProblemCode,
    pub message: String,
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum BatchResourceProblemCode {
    UidNotFound,
    NameNotFound,
    ApiVersionMismatch,
    KindMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
