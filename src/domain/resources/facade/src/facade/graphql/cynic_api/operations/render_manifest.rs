// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;
use internal_error::InternalError;

use crate::facade::graphql::cynic_api::fragments::{
    BatchResourceProblem,
    ResourceManifestFormat,
    ResourceRenderManifestResult,
};
use crate::facade::graphql::cynic_api::inputs::{
    ResourceBatchSelectorInput,
    ResourceSelectorInput,
};
use crate::facade::graphql::cynic_api::schema;
use crate::{
    ResourceBatchSelector,
    ResourceManifestFormat as DomainFormat,
    ResourceSelector,
    SpecViewMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "RenderResourceManifestVariables")]
pub(crate) struct RenderManifestQuery {
    pub resources: RenderManifestResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "RenderResourceManifestVariables"
)]
pub(crate) struct RenderManifestResources {
    #[arguments(selector: $selector, format: $format, revealed: $revealed)]
    pub render_manifest: ResourceRenderManifestOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceRenderManifestOutcome {
    ResourceRenderManifestResult(ResourceRenderManifestResult),
    ResourceUIDNotFoundProblem(RenderResourceUIDNotFoundProblem),
    ResourceNameNotFoundProblem(RenderResourceNameNotFoundProblem),
    ResourceApiVersionMismatchProblem(RenderResourceApiVersionMismatchProblem),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceUIDNotFoundProblem")]
pub(crate) struct RenderResourceUIDNotFoundProblem {
    pub uid: kamu_resources::ResourceUID,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceNameNotFoundProblem")]
pub(crate) struct RenderResourceNameNotFoundProblem {
    pub kind: String,
    pub name: String,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceApiVersionMismatchProblem")]
pub(crate) struct RenderResourceApiVersionMismatchProblem {
    pub expected_api_version: String,
    pub actual_api_version: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "RenderResourceManifestsVariables")]
pub(crate) struct RenderManifestsQuery {
    pub resources: RenderManifestsResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "RenderResourceManifestsVariables"
)]
pub(crate) struct RenderManifestsResources {
    #[arguments(selector: $selector, format: $format, revealed: $revealed)]
    pub render_manifests: BatchResourceManifestsResult,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceManifestsResult {
    pub manifests: Vec<BatchResourceManifestSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceManifestSuccess {
    pub request_index: i32,
    pub manifest: ResourceRenderManifestResult,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct RenderResourceManifestVariables {
    pub selector: ResourceSelectorInput,
    pub format: ResourceManifestFormat,
    pub revealed: bool,
}

impl RenderResourceManifestVariables {
    pub(crate) fn new(
        selector: &ResourceSelector,
        format: DomainFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            format: format.into(),
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct RenderResourceManifestsVariables {
    pub selector: ResourceBatchSelectorInput,
    pub format: ResourceManifestFormat,
    pub revealed: bool,
}

impl RenderResourceManifestsVariables {
    pub(crate) fn new(
        selector: &ResourceBatchSelector,
        format: DomainFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            format: format.into(),
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_manifest_operation(
    variables: RenderResourceManifestVariables,
) -> cynic::Operation<RenderManifestQuery, RenderResourceManifestVariables> {
    RenderManifestQuery::build(variables)
}

pub(crate) fn build_manifests_operation(
    variables: RenderResourceManifestsVariables,
) -> cynic::Operation<RenderManifestsQuery, RenderResourceManifestsVariables> {
    RenderManifestsQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
