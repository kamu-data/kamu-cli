// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::facade::graphql::cynic_api::scalars::{AccountName, Uint64};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceSummary {
    pub id: kamu_resources::ResourceUID,
    pub api_version: String,
    pub kind: ResourceKind,
    pub name: String,
    pub description: Option<String>,
    pub generation: Uint64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummary>,
    pub list_values: Vec<ResourceListColumnValueView>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceStatusSummary {
    pub phase: Option<ResourcePhase>,
    pub observed_generation: Option<Uint64>,
    pub ready: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourcePhase {
    Pending,
    Reconciling,
    Ready,
    Degraded,
    Failed,
}

impl From<ResourcePhase> for kamu_resources::ResourcePhase {
    fn from(value: ResourcePhase) -> Self {
        match value {
            ResourcePhase::Pending => Self::Pending,
            ResourcePhase::Reconciling => Self::Reconciling,
            ResourcePhase::Ready => Self::Ready,
            ResourcePhase::Degraded => Self::Degraded,
            ResourcePhase::Failed => Self::Failed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceListColumnValueView {
    pub key: String,
    pub string_value: Option<String>,
    pub uint64_value: Option<Uint64>,
    pub bool_value: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceConnection {
    pub nodes: Vec<ResourceSummary>,
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
pub(crate) struct ResourceUnsupportedDescriptorProblem {
    pub code: ResourceUnsupportedDescriptorProblemCode,
    pub kind: String,
    pub api_version: String,
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceUnsupportedDescriptorProblemCode {
    NotFound,
    Duplicate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceBadAccountProblem {
    pub code: ResourceBadAccountProblemCode,
    pub account_id: Option<odf::AccountID>,
    pub account_name: Option<AccountName>,
    pub expected_name: Option<AccountName>,
    pub actual_name: Option<AccountName>,
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceBadAccountProblemCode {
    EmptySelector,
    AccountNotFoundById,
    AccountNotFoundByName,
    IdNameMismatch,
    Other,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Copy, Clone)]
pub(crate) struct ResourceUIDNotFoundProblem {
    pub uid: kamu_resources::ResourceUID,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceNameNotFoundProblem {
    pub kind: String,
    pub name: String,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApiVersionMismatchProblem {
    pub expected_api_version: String,
    pub actual_api_version: String,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceKindMismatchProblem {
    pub uid: kamu_resources::ResourceUID,
    pub expected_kind: String,
    pub actual_kind: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceLookupProblem {
    ResourceUIDNotFoundProblem(ResourceUIDNotFoundProblem),
    ResourceNameNotFoundProblem(ResourceNameNotFoundProblem),
    ResourceApiVersionMismatchProblem(ResourceApiVersionMismatchProblem),
    ResourceKindMismatchProblem(ResourceKindMismatchProblem),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceLookupProblemResult {
    pub problem: ResourceLookupProblem,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceInvalidSearchQueryProblem {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceTypeCountSummary {
    pub kind: String,
    pub name: String,
    pub api_version: String,
    pub total_count: Uint64,
    pub phase_counts: ResourcePhaseCounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourcePhaseCounts {
    pub pending: Uint64,
    pub reconciling: Uint64,
    pub ready: Uint64,
    pub degraded: Uint64,
    pub failed: Uint64,
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
    pub account_name: Option<AccountName>,
    pub name: String,
    pub description: Option<String>,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub generation: Uint64,
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
    pub problem: ResourceLookupProblem,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
