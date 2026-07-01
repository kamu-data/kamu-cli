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
    pub id: kamu_resources::ResourceID,
    pub schema: String,
    pub name: kamu_resources::ResourceName,
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
    pub schema: String,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Copy, Clone)]
pub(crate) struct ResourceIDNotFoundProblem {
    pub id: kamu_resources::ResourceID,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceNameNotFoundProblem {
    pub kind: String,
    pub name: kamu_resources::ResourceName,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceSchemaMismatchProblem {
    pub id: kamu_resources::ResourceID,
    pub expected_schema: String,
    pub actual_schema: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceLookupProblem {
    ResourceIDNotFoundProblem(ResourceIDNotFoundProblem),
    ResourceNameNotFoundProblem(ResourceNameNotFoundProblem),
    ResourceSchemaMismatchProblem(ResourceSchemaMismatchProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceSelectorProblem {
    ResourceIDNotFoundProblem(ResourceIDNotFoundProblem),
    ResourceNameNotFoundProblem(ResourceNameNotFoundProblem),
    ResourceSchemaMismatchProblem(ResourceSchemaMismatchProblem),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceSelectorProblemResult {
    pub problem: ResourceSelectorProblem,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceInvalidSearchQueryProblem {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceTypeCountSummary {
    pub schema: String,
    pub name: String,
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
    pub schema: String,
    pub headers: ResourceHeaders,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceHeaders {
    pub id: kamu_resources::ResourceID,
    pub account: ResourceAccount,
    pub name: kamu_resources::ResourceName,
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
pub(crate) struct ResourceAccount {
    pub id: odf::AccountID,
    pub name: Option<AccountName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceIdentity {
    pub id: kamu_resources::ResourceID,
    pub schema: String,
    pub canonical_kind_name: String,
    pub name: kamu_resources::ResourceName,
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
