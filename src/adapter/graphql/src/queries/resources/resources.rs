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

use super::helpers as resource_helpers;
use crate::LoggedInGuard;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Resources;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Resources {
    const DEFAULT_PER_PAGE: usize = 15;

    /// Returns a resource by selector, if found
    #[tracing::instrument(level = "info", name = Resources_resource, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<Option<Resource>> {
        resource_helpers::get_resource(ctx, selector, None /* current subject */).await
    }

    /// Returns resources of the specified kind
    #[tracing::instrument(level = "info", name = Resources_list_by_kind, skip_all, fields(?kind, ?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_by_kind(
        &self,
        ctx: &Context<'_>,
        kind: ResourceKindInput,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        resource_helpers::list_resources_connection(
            ctx, kind, None, /* current subject */
            page, per_page,
        )
        .await
    }

    /// Returns resources across all kinds
    #[tracing::instrument(level = "info", name = Resources_list_all, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_all(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        resource_helpers::list_all_resources_connection(
            ctx, None, /* current subject */
            page, per_page,
        )
        .await
    }

    /// Validates a resource manifest without applying it
    #[tracing::instrument(level = "info", name = Resources_validate_manifest, skip_all)]
    async fn validate_manifest(
        &self,
        ctx: &Context<'_>,
        manifest: String,
        format: ResourceManifestFormat,
    ) -> Result<ResourceValidateManifestResult> {
        let _ = (ctx, manifest, format);
        todo!("Resources.validate_manifest is not implemented yet");
    }

    /// Renders a canonical manifest representation from a stored resource
    #[tracing::instrument(level = "info", name = Resources_render_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn render_manifest(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
        format: ResourceManifestFormat,
    ) -> Result<ResourceRenderManifestResult> {
        let _ = (ctx, selector, format);
        todo!("Resources.render_manifest is not implemented yet");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resource types
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

impl ResourceKindInput {
    pub fn into_resource_type(self) -> String {
        match self {
            Self::Builtin(kind) => kind.as_resource_type().to_string(),
            Self::Custom(kind) => kind,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug, Clone)]
pub enum ResourceKindInput {
    Builtin(ResourceBuiltinKind),
    Custom(String),
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

#[derive(OneofObject, Debug, Clone)]
pub enum ResourceRefInput {
    ById(ResourceID),
    ByName(ResourceByNameSelectorInput),
}

impl From<ResourceRefInput> for kamu_resources_facade::GetResourceRef {
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

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources_facade::ResourceManifestFormat")]
pub enum ResourceManifestFormat {
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceValidateManifestResult {
    pub valid: bool,
    pub issues: Vec<ResourceValidationIssue>,
    pub kind: Option<ResourceKind>,
    pub api_version: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceValidationIssue {
    pub severity: ResourceValidationIssueSeverity,
    pub code: Option<String>,
    pub path: Option<String>,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceValidationIssueSeverity {
    Error,
    Warning,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceRenderManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
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
pub struct ResourceStatusSummary {
    pub phase: Option<String>,
    pub observed_generation: Option<u64>,
    pub ready: Option<bool>,
}

impl From<kamu_resources::ResourceStatusSummaryView> for ResourceStatusSummary {
    fn from(value: kamu_resources::ResourceStatusSummaryView) -> Self {
        Self {
            phase: value.phase,
            observed_generation: value.observed_generation,
            ready: value.ready,
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
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(ResourceSummary, ResourceConnection, ResourceEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
