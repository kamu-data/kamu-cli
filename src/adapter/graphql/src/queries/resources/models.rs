// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_configuration::{SecretSetResource, VariableSetResource};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Type aliases for cleaner From implementations

type BatchGetResourcesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceView,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchGetResourceIdentitiesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceIdentityView,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchRenderResourceManifestsResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources_facade::RenderResourceManifestResult,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchGetResourceProblem =
    kamu_resources_facade::BatchResourceProblem<kamu_resources_facade::ResourceLookupProblem>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceBuiltinKind {
    SecretSet,
    VariableSet,
}

impl ResourceBuiltinKind {
    pub fn as_resource_type(self) -> &'static str {
        match self {
            Self::SecretSet => SecretSetResource::RESOURCE_TYPE,
            Self::VariableSet => VariableSetResource::RESOURCE_TYPE,
        }
    }

    pub fn from_resource_type(value: &str) -> Option<Self> {
        match value {
            SecretSetResource::RESOURCE_TYPE => Some(Self::SecretSet),
            VariableSetResource::RESOURCE_TYPE => Some(Self::VariableSet),
            _ => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug, Clone)]
pub enum ResourceKindInput {
    Builtin(ResourceBuiltinKind),
    Custom(String),
}

impl ResourceKindInput {
    pub fn into_resource_type(self) -> String {
        match self {
            Self::Builtin(kind) => kind.as_resource_type().to_string(),
            Self::Custom(kind) => kind,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceSelectorInput {
    pub kind: ResourceKindInput,
    pub api_version: Option<String>,
    #[graphql(name = "ref")]
    pub resource_ref: ResourceRefInput,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceBatchSelectorInput {
    pub kind: ResourceKindInput,
    pub api_version: Option<String>,
    #[graphql(name = "refs")]
    pub resource_refs: Vec<ResourceRefInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug, Clone)]
pub enum ResourceRefInput {
    ById(ResourceID),
    ByName(ResourceByNameSelectorInput),
}

impl From<ResourceRefInput> for kamu_resources_facade::ResourceRef {
    fn from(value: ResourceRefInput) -> Self {
        match value {
            ResourceRefInput::ById(uid) => Self::ById(uid.into()),
            ResourceRefInput::ByName(by_name) => Self::ByName(by_name.name.clone()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceByNameSelectorInput {
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceKind {
    pub value: String,
    pub builtin: Option<ResourceBuiltinKind>,
}

impl ResourceKind {
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();

        Self {
            builtin: ResourceBuiltinKind::from_resource_type(&value),
            value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceKindDescriptor {
    pub name: String,
    pub short_names: Vec<String>,
    pub kind: ResourceKind,
    pub api_version: String,
    pub list_columns: Vec<ResourceListColumnDescriptor>,
}

impl From<kamu_resources::ResourceKindDescriptor> for ResourceKindDescriptor {
    fn from(value: kamu_resources::ResourceKindDescriptor) -> Self {
        Self {
            name: value.name,
            short_names: value.short_names,
            kind: ResourceKind::new(value.kind),
            api_version: value.api_version,
            list_columns: value.list_columns.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources_facade::ResourceManifestFormat")]
pub enum ResourceManifestFormat {
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceRenderManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct Resource {
    pub api_version: String,
    pub kind: ResourceKind,
    pub metadata: ResourceMetadata,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

impl From<kamu_resources::ResourceView> for Resource {
    fn from(value: kamu_resources::ResourceView) -> Self {
        let metadata = ResourceMetadata::from(value.clone());

        Self {
            api_version: value.api_version,
            kind: ResourceKind::new(value.kind),
            metadata,
            spec: value.spec,
            status: value.status,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourcesResult {
    pub resources: Vec<BatchResourceSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchGetResourcesResponse> for BatchResourcesResult {
    fn from(value: BatchGetResourcesResponse) -> Self {
        Self {
            resources: value
                .successes
                .into_iter()
                .map(|success| BatchResourceSuccess {
                    request_index: success.request_index,
                    resource: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceSuccess {
    pub request_index: usize,
    pub resource: Resource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceManifestsResult {
    pub manifests: Vec<BatchResourceManifestSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchRenderResourceManifestsResponse> for BatchResourceManifestsResult {
    fn from(value: BatchRenderResourceManifestsResponse) -> Self {
        Self {
            manifests: value
                .successes
                .into_iter()
                .map(|success| BatchResourceManifestSuccess {
                    request_index: success.request_index,
                    manifest: ResourceRenderManifestResult {
                        manifest: success.item.manifest,
                        format: success.item.format.into(),
                    },
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceManifestSuccess {
    pub request_index: usize,
    pub manifest: ResourceRenderManifestResult,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceIdentity {
    pub id: ResourceID,
    pub api_version: String,
    pub kind: ResourceKind,
    pub canonical_kind_name: String,
    pub name: String,
}

impl From<kamu_resources::ResourceIdentityView> for ResourceIdentity {
    fn from(value: kamu_resources::ResourceIdentityView) -> Self {
        Self {
            id: value.uid.into(),
            api_version: value.api_version,
            kind: ResourceKind::new(value.kind),
            canonical_kind_name: value.canonical_kind_name,
            name: value.name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceIdentitiesResult {
    pub identities: Vec<BatchResourceIdentitySuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchGetResourceIdentitiesResponse> for BatchResourceIdentitiesResult {
    fn from(value: BatchGetResourceIdentitiesResponse) -> Self {
        Self {
            identities: value
                .successes
                .into_iter()
                .map(|success| BatchResourceIdentitySuccess {
                    request_index: success.request_index,
                    identity: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceIdentitySuccess {
    pub request_index: usize,
    pub identity: ResourceIdentity,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceProblem {
    pub request_index: usize,
    pub code: BatchResourceProblemCode,
    pub message: String,
}

impl From<BatchGetResourceProblem> for BatchResourceProblem {
    fn from(value: BatchGetResourceProblem) -> Self {
        let code = BatchResourceProblemCode::from_lookup_problem(&value.error);
        Self {
            request_index: value.request_index,
            message: value.error.to_string(),
            code,
        }
    }
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchResourceProblemCode {
    UidNotFound,
    NameNotFound,
    ApiVersionMismatch,
    KindMismatch,
}

impl BatchResourceProblemCode {
    fn from_lookup_problem(error: &kamu_resources_facade::ResourceLookupProblem) -> Self {
        use kamu_resources_facade::ResourceLookupProblem as E;
        match error {
            E::UIDNotFound(_) => Self::UidNotFound,
            E::NameNotFound(_) => Self::NameNotFound,
            E::ApiVersionMismatch(_) => Self::ApiVersionMismatch,
            E::KindMismatch(_) => Self::KindMismatch,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceMetadata {
    pub id: ResourceID,
    pub account_id: AccountID<'static>,
    pub name: String,
    pub description: Option<String>,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

impl From<kamu_resources::ResourceView> for ResourceMetadata {
    fn from(value: kamu_resources::ResourceView) -> Self {
        let labels = serde_json::to_value(value.metadata.labels).unwrap();
        let annotations = serde_json::to_value(value.metadata.annotations).unwrap();

        Self {
            id: value.metadata.uid.into(),
            account_id: value.account.id.into(),
            name: value.metadata.name.clone(),
            description: value.metadata.description,
            labels,
            annotations,
            generation: value.metadata.generation,
            created_at: value.metadata.created_at,
            updated_at: value.metadata.updated_at,
            deleted_at: value.metadata.deleted_at,
            last_reconciled_at: value.last_reconciled_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceSummary {
    pub id: ResourceID,
    pub api_version: String,
    pub kind: ResourceKind,
    pub name: String,
    pub description: Option<String>,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummary>,
    pub list_values: Vec<ResourceListColumnValueView>,
}

impl From<kamu_resources::ResourceSummaryView> for ResourceSummary {
    fn from(value: kamu_resources::ResourceSummaryView) -> Self {
        Self {
            id: value.uid.into(),
            api_version: value.api_version,
            kind: ResourceKind::new(value.kind),
            name: value.name.clone(),
            description: value.description,
            generation: value.generation,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: value.status.map(Into::into),
            list_values: value.list_values.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceListColumnDescriptor {
    pub key: String,
    pub header: String,
    pub data_type: String,
    pub visibility: String,
}

impl From<kamu_resources::ResourceListColumnDescriptor> for ResourceListColumnDescriptor {
    fn from(value: kamu_resources::ResourceListColumnDescriptor) -> Self {
        Self {
            key: value.key,
            header: value.header,
            data_type: value.data_type.to_string(),
            visibility: value.visibility.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceListColumnValueView {
    pub key: String,
    pub string_value: Option<String>,
    pub uint64_value: Option<u64>,
    pub bool_value: Option<bool>,
}

impl From<kamu_resources::ResourceListColumnValueView> for ResourceListColumnValueView {
    fn from(value: kamu_resources::ResourceListColumnValueView) -> Self {
        let (string_value, uint64_value, bool_value) = match value.value {
            kamu_resources::ResourceListColumnValue::String(value) => (Some(value), None, None),
            kamu_resources::ResourceListColumnValue::UInt64(value) => (None, Some(value), None),
            kamu_resources::ResourceListColumnValue::Bool(value) => (None, None, Some(value)),
        };

        Self {
            key: value.key,
            string_value,
            uint64_value,
            bool_value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceStatusSummary {
    pub phase: Option<String>,
    pub observed_generation: Option<u64>,
    pub ready: Option<bool>,
}

impl From<kamu_resources::ResourceStatusSummaryView> for ResourceStatusSummary {
    fn from(value: kamu_resources::ResourceStatusSummaryView) -> Self {
        Self {
            phase: value.phase.map(|phase| phase.to_string()),
            observed_generation: value.observed_generation,
            ready: value.ready,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourcesSummary {
    pub resource_counts: Vec<ResourceTypeCountSummary>,
}

impl ResourcesSummary {
    pub fn from_domain(value: kamu_resources::ResourcesSummary) -> Self {
        Self {
            resource_counts: value.resource_counts.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceTypeCountSummary {
    pub kind: String,
    pub name: String,
    pub api_version: String,
    pub total_count: u64,
    pub phase_counts: ResourcePhaseCounts,
}

impl From<kamu_resources::ResourceTypeCountSummary> for ResourceTypeCountSummary {
    fn from(value: kamu_resources::ResourceTypeCountSummary) -> Self {
        Self {
            kind: value.kind,
            name: value.name,
            api_version: value.api_version,
            total_count: value.total_count,
            phase_counts: value.phase_counts.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourcePhaseCounts {
    pub pending: u64,
    pub reconciling: u64,
    pub ready: u64,
    pub degraded: u64,
    pub failed: u64,
}

impl From<kamu_resources::ResourcePhaseCounts> for ResourcePhaseCounts {
    fn from(value: kamu_resources::ResourcePhaseCounts) -> Self {
        Self {
            pending: value.pending,
            reconciling: value.reconciling,
            ready: value.ready,
            degraded: value.degraded,
            failed: value.failed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(ResourceSummary, ResourceConnection, ResourceEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    ResourceIdentity,
    ResourceIdentityConnection,
    ResourceIdentityEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
