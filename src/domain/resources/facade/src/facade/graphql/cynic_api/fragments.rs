use chrono::{DateTime, Utc};

use super::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceSummary {
    pub id: kamu_resources::ResourceUID,
    pub api_version: String,
    pub kind: ResourceKind,
    pub name: String,
    pub description: Option<String>,
    pub generation: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummary>,
    pub list_values: Vec<ResourceListColumnValueView>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceStatusSummary {
    pub phase: Option<String>,
    pub observed_generation: Option<i32>,
    pub ready: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceListColumnValueView {
    pub key: String,
    pub string_value: Option<String>,
    pub uint64_value: Option<i32>,
    pub bool_value: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceConnection {
    pub nodes: Vec<ResourceSummary>,
    pub total_count: i32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceIdentityConnection {
    pub nodes: Vec<ResourceIdentity>,
    pub total_count: i32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourcesSummary {
    pub resource_counts: Vec<ResourceTypeCountSummary>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceTypeCountSummary {
    pub kind: String,
    pub name: String,
    pub api_version: String,
    pub total_count: i32,
    pub phase_counts: ResourcePhaseCounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourcePhaseCounts {
    pub pending: i32,
    pub reconciling: i32,
    pub ready: i32,
    pub degraded: i32,
    pub failed: i32,
}

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
