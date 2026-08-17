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
    ResourceRef,
    ResourceSelector,
    ResourceSummaryView,
    ResourceTypeDescriptor,
    ResourcesSummary,
};
use kamu_resources as domain;

use crate::{
    ApplyManifestError,
    BatchResourceError,
    ListResourcesError,
    ListSupportedResourceTypesError,
    ResourceLookupProblem,
    ResourcesSummaryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The two families here are deliberately asymmetric about which form is the
/// primitive.
///
/// Ref-keyed reads and deletes (`get`, `get_handles`,
/// `render_manifests`, `delete`) exist *only* in batch form: the batch
/// pipeline — group refs by `(account, schema)`, resolve ids per group, merge
/// back by `request_index` — is the primitive, and a single-resource call is
/// simply a one-element batch. A scalar form would be a second implementation
/// of the same contract, kept honest only by tests.
///
/// Apply keeps its scalar form because there the scalar *is* the primitive:
/// `plan_apply_manifests`/`apply_manifests` are loops over
/// `plan_apply_manifest`/`apply_manifest`, and the per-item transaction
/// boundary belongs to the caller (the CLI's `--continue-on-error` path opens
/// one transaction per manifest), not to the facade.
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

    async fn get(
        &self,
        resource_refs: Vec<ResourceRef>,
        spec_view_mode: SpecViewMode,
    ) -> Result<BatchResourceResponse<Resource, ResourceLookupProblem>, BatchResourceError>;

    async fn get_handles(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceHandle, ResourceLookupProblem>, BatchResourceError>;

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
    /// Spans several resource types *and* renders columns for every result.
    ///
    /// Unlike the ref-keyed operations above, this is not a batch form of
    /// anything: [`ResourceFacade::search_handles`] answers the same request
    /// with a cheaper response, it is not a scalar counterpart.
    async fn search(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourcesResponse, ListResourcesError>;

    /// The handle-only form of [`ResourceFacade::search`], for callers that
    /// need identity rather than presentation. Takes the same request — only
    /// the response shape differs.
    async fn search_handles(
        &self,
        request: SearchResourcesRequest,
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

    async fn delete(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceID, ResourceLookupProblem>, BatchResourceError>;
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

/// What to search for, shared by [`ResourceFacade::search`] and
/// [`ResourceFacade::search_handles`] — only the response shape differs.
#[derive(Debug, Clone)]
pub struct SearchResourcesRequest {
    /// Which resources to span. Several selectors act as a logical OR; an empty
    /// list matches nothing, and a single type-less unnarrowed selector spans
    /// every type.
    ///
    /// Label filtering rides here too: each selector carries its own `labels`,
    /// so one call may filter differently per type. There is deliberately no
    /// call-level filter — one uniform filter is the special case where every
    /// selector carries the same labels.
    pub selectors: Vec<ResourceSelector>,
    /// The account rows fall back to when a selector names none.
    pub account: Option<ResourceAccountRef>,
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
