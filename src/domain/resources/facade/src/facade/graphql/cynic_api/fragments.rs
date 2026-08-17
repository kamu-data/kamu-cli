// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::facade::graphql::cynic_api::scalars::{
    AccountName,
    ResourceAnnotations,
    ResourceConditions,
    ResourceLabels,
    Uint64,
};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceSummary {
    pub id: kamu_resources::ResourceID,
    pub schema: kamu_resources::TypeUri,
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
    Failed,
}

impl From<ResourcePhase> for kamu_resources::ResourcePhase {
    fn from(value: ResourcePhase) -> Self {
        match value {
            ResourcePhase::Pending => Self::Pending,
            ResourcePhase::Reconciling => Self::Reconciling,
            ResourcePhase::Ready => Self::Ready,
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
    pub total_count: i32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceHandleConnection {
    pub nodes: Vec<ResourceHandle>,
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
    pub schema: kamu_resources::TypeUri,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceUnsupportedSelectorProblem {
    pub selector: kamu_resources::ResourceTypeSelectorRaw,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceBadAccountProblem {
    pub code: ResourceBadAccountProblemCode,
    pub account_id: Option<odf::AccountID>,
    pub account_name: Option<AccountName>,
    pub expected_resource_id: Option<kamu_resources::ResourceID>,
    pub expected_did: Option<odf::AccountID>,
    pub expected_name: Option<AccountName>,
    pub actual_name: Option<AccountName>,
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceBadAccountProblemCode {
    EmptySelector,
    AccountNotFoundById,
    AccountNotFoundByName,
    SelectorMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Copy, Clone)]
pub(crate) struct ResourceIDNotFoundProblem {
    pub id: kamu_resources::ResourceID,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceNameNotFoundProblem {
    pub type_name: kamu_resources::TypeName,
    pub name: kamu_resources::ResourceName,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceSchemaMismatchProblem {
    pub id: kamu_resources::ResourceID,
    pub expected_schema: kamu_resources::TypeUri,
    pub actual_schema: kamu_resources::TypeUri,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceNameMismatchProblem {
    pub id: kamu_resources::ResourceID,
    pub expected_name: kamu_resources::ResourceName,
    pub actual_name: kamu_resources::ResourceName,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceAnyTypeNameNotFoundProblem {
    pub name: kamu_resources::ResourceName,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceAmbiguousTypeProblem {
    pub name: kamu_resources::ResourceName,
    pub type_names: Vec<kamu_resources::TypeName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceLookupProblem {
    ResourceIDNotFoundProblem(ResourceIDNotFoundProblem),
    ResourceNameNotFoundProblem(ResourceNameNotFoundProblem),
    ResourceAnyTypeNameNotFoundProblem(ResourceAnyTypeNameNotFoundProblem),
    ResourceAmbiguousTypeProblem(ResourceAmbiguousTypeProblem),
    ResourceSchemaMismatchProblem(ResourceSchemaMismatchProblem),
    ResourceNameMismatchProblem(ResourceNameMismatchProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceInvalidLabelFilterProblem {
    pub code: ResourceLabelFilterProblemCode,
    pub message: String,
}

/// Mirrors the server enum so the remote facade can rebuild the same typed
/// error the local one raises, rather than degrading it to a message.
#[derive(cynic::Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceLabelFilterProblemCode {
    InvalidKey,
    ResourceExtensionSchema,
    NonStringValue,
    DuplicateAfterCanonicalization,
    UnsupportedExpression,
}

impl From<ResourceInvalidLabelFilterProblem> for crate::ResourceInvalidLabelFilterError {
    fn from(value: ResourceInvalidLabelFilterProblem) -> Self {
        use crate::ResourceLabelFilterProblemCode as C;

        let code = match value.code {
            ResourceLabelFilterProblemCode::InvalidKey => C::InvalidKey,
            ResourceLabelFilterProblemCode::ResourceExtensionSchema => C::ResourceExtensionSchema,
            ResourceLabelFilterProblemCode::NonStringValue => C::NonStringValue,
            ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization => {
                C::DuplicateAfterCanonicalization
            }
            ResourceLabelFilterProblemCode::UnsupportedExpression => C::UnsupportedExpression,
        };

        Self {
            code,
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceTypeCountSummary {
    pub schema: kamu_resources::TypeUri,
    pub type_name: kamu_resources::TypeName,
    pub total_count: Uint64,
    pub phase_counts: ResourcePhaseCounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourcePhaseCounts {
    pub pending: Uint64,
    pub reconciling: Uint64,
    pub ready: Uint64,
    pub failed: Uint64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct Resource {
    pub schema: kamu_resources::TypeUri,
    pub headers: ResourceHeaders,
    pub spec: serde_json::Value,
    pub status: ResourceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceStatus {
    pub phase: ResourcePhase,
    pub observed_generation: Option<Uint64>,
    pub reconciled_at: Option<DateTime<Utc>>,
    pub conditions: ResourceConditions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceHeaders {
    pub id: kamu_resources::ResourceID,
    pub account: AccountHandle,
    pub name: kamu_resources::ResourceName,
    pub labels: ResourceLabels,
    pub annotations: ResourceAnnotations,
    pub generation: Uint64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct AccountHandle {
    pub id: kamu_resources::ResourceID,
    pub did: odf::AccountID,
    pub name: AccountName,
}

impl From<AccountHandle> for odf::AccountHandle {
    fn from(value: AccountHandle) -> Self {
        Self {
            id: value.id,
            did: value.did,
            name: odf::AccountName::new_unchecked(&value.name.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceHandle {
    pub id: kamu_resources::ResourceID,
    #[cynic(rename = "type")]
    pub r#type: kamu_resources::TypeUri,
    pub did: Option<odf::metadata::formats::Did>,
    pub name: kamu_resources::ResourceName,
    pub account: AccountHandle,
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
