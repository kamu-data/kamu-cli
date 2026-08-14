// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use domain::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    Resource,
    ResourceAccountRef,
    ResourceHandle,
    ResourceID,
    ResourceLabelFilterInput,
    ResourceRef,
    ResourceSelector,
    ResourceSummaryView,
    ResourceTypeDescriptor,
    ResourcesSummary,
};
use internal_error::InternalError;
use kamu_resources as domain;

use crate::{
    ApplyManifestError,
    BatchResourceError,
    DeleteResourceError,
    GetResourceError,
    ListResourcesError,
    ListSupportedResourceTypesError,
    RenderResourceManifestError,
    ResourceLookupProblem,
    ResourcesSummaryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait ResourceFacade: Send + Sync {
    async fn list_supported_resource_types(
        &self,
    ) -> Result<Vec<ResourceTypeDescriptor>, ListSupportedResourceTypesError>;

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError>;

    /// Fetches one resource. Provided: delegates to
    /// [`ResourceFacade::get_many`] with a one-element batch.
    async fn get(
        &self,
        resource_ref: ResourceRef,
        spec_view_mode: SpecViewMode,
    ) -> Result<Resource, GetResourceError> {
        single_from_batch(
            self.get_many(vec![resource_ref], spec_view_mode).await?,
            "Get",
        )
    }

    async fn get_many(
        &self,
        resource_refs: Vec<ResourceRef>,
        spec_view_mode: SpecViewMode,
    ) -> Result<BatchResourceResponse<Resource, ResourceLookupProblem>, BatchResourceError>;

    /// Provided: delegates to [`ResourceFacade::get_handles`].
    async fn get_handle(
        &self,
        resource_ref: ResourceRef,
    ) -> Result<ResourceHandle, GetResourceError> {
        single_from_batch(self.get_handles(vec![resource_ref]).await?, "Get handle")
    }

    async fn get_handles(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceHandle, ResourceLookupProblem>, BatchResourceError>;

    /// Provided: delegates to [`ResourceFacade::render_manifests`].
    async fn render_manifest(
        &self,
        resource_ref: ResourceRef,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        single_from_batch(
            self.render_manifests(vec![resource_ref], format, spec_view_mode)
                .await?,
            "Render manifest",
        )
    }

    async fn render_manifests(
        &self,
        resource_refs: Vec<ResourceRef>,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    >;

    /// Lists resources matching the selectors, with typed columns rendered.
    ///
    /// Replaces the former `list`/`list_all` pair: `list` could render typed
    /// columns but only for one type, `list_all` could span types but rendered
    /// none. This spans types *and* renders columns for every result.
    async fn search(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourcesResponse, ListResourcesError>;

    /// The handle-only form of [`ResourceFacade::search`], for callers that
    /// need identity rather than presentation.
    async fn search_handles(
        &self,
        request: SearchResourceHandlesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError>;

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError>;

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyManifestError>;

    async fn plan_apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestPlanningDecision>, BatchResourceError>;

    async fn apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestApplicationDecision>, BatchResourceError>;

    /// Provided: delegates to [`ResourceFacade::delete_many`].
    async fn delete(&self, resource_ref: ResourceRef) -> Result<ResourceID, DeleteResourceError> {
        single_from_batch(self.delete_many(vec![resource_ref]).await?, "Delete")
    }

    async fn delete_many(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceID, ResourceLookupProblem>, BatchResourceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unwraps a one-element batch response into a scalar result.
///
/// The scalar operations are provided methods delegating to their batch form,
/// so this is where a batch of one becomes an `Ok(item)` or the item's own
/// error. An empty response means the batch neither succeeded nor reported a
/// problem, which is a bug in the batch implementation rather than a user
/// error — hence the internal error naming the operation.
fn single_from_batch<T, E, Err>(
    response: BatchResourceResponse<T, E>,
    operation: &str,
) -> Result<T, Err>
where
    Err: From<E> + From<InternalError>,
{
    if let Some(success) = response.successes.into_iter().next() {
        Ok(success.item)
    } else if let Some(problem) = response.problems.into_iter().next() {
        Err(problem.error.into())
    } else {
        Err(InternalError::new(format!("{operation} response did not contain an item")).into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct BatchResourceResponse<T, E> {
    pub successes: Vec<BatchResourceSuccess<T>>,
    pub problems: Vec<BatchResourceProblem<E>>,
}

impl<T, E> BatchResourceResponse<T, E> {
    pub fn empty() -> Self {
        Self {
            successes: Vec::new(),
            problems: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct BatchResourceSuccess<T> {
    pub request_index: usize,
    pub item: T,
}

#[derive(Debug)]
pub struct BatchResourceProblem<E> {
    pub request_index: usize,
    pub error: E,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestRequest {
    pub format: ResourceManifestFormat,
    pub manifest: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestBatchRequest {
    pub items: Vec<ApplyManifestRequest>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ApplyManifestBatchResponse<D> {
    pub items: Vec<ApplyManifestBatchItemResult<D>>,
    /// Positional indexes of successes hidden by a batch rollback.
    /// This is transport metadata, not a per-item error.
    pub rolled_back_successes: Vec<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ApplyManifestBatchItemResult<D> {
    pub request_index: usize,
    pub outcome: Result<D, ApplyManifestError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ResourceManifestFormat {
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchResourcesRequest {
    /// Which resources to span. Several selectors act as a logical OR; an empty
    /// list matches nothing, and a single type-less unnarrowed selector spans
    /// every type.
    pub selectors: Vec<ResourceSelector>,
    /// The account rows fall back to when a selector names none.
    pub account: Option<ResourceAccountRef>,
    pub label_filter: Option<ResourceLabelFilterInput>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchResourcesResponse {
    pub items: Vec<ResourceSummaryView>,
    /// Total matching the selectors, ignoring pagination.
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchResourceHandlesRequest {
    /// See [`SearchResourcesRequest::selectors`].
    pub selectors: Vec<ResourceSelector>,
    pub account: Option<ResourceAccountRef>,
    pub label_filter: Option<ResourceLabelFilterInput>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchResourceHandlesResponse {
    pub items: Vec<ResourceHandle>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct ResourcesSummaryRequest {
    pub account: Option<ResourceAccountRef>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RenderResourceManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpecViewMode {
    #[default]
    Encrypted,
    Revealed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
