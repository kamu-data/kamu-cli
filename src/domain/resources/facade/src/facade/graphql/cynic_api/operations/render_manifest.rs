// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;
use kamu_resources::ResourceRef;

use crate::facade::graphql::cynic_api::fragments::{
    BatchResourceProblem,
    ResourceAccountResolutionProblem,
    ResourceManifestFormat,
    ResourceRenderManifestResult,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::{
    ResourceRefInput,
    SpecViewOptsInput,
    resource_ref_inputs,
};
use crate::facade::graphql::cynic_api::schema;
use crate::{ResourceManifestFormat as DomainFormat, SpecViewOpts};

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
    #[arguments(resourceRefs: $resource_refs, format: $format, opts: $opts)]
    pub render_manifests: BatchResourceManifestsOutcome,
}

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum BatchResourceManifestsOutcome {
    BatchResourceManifestsResult(BatchResourceManifestsResult),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceAccountResolutionProblem(ResourceAccountResolutionProblem),
    #[cynic(fallback)]
    Unknown,
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
pub(crate) struct RenderResourceManifestsVariables {
    pub resource_refs: Vec<ResourceRefInput>,
    pub format: ResourceManifestFormat,
    pub opts: SpecViewOptsInput,
}

impl RenderResourceManifestsVariables {
    pub(crate) fn new(
        resource_refs: &[ResourceRef],
        format: DomainFormat,
        spec_view: SpecViewOpts,
    ) -> Self {
        Self {
            resource_refs: resource_ref_inputs(resource_refs),
            format: format.into(),
            opts: spec_view.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_manifests_operation(
    variables: RenderResourceManifestsVariables,
) -> cynic::Operation<RenderManifestsQuery, RenderResourceManifestsVariables> {
    RenderManifestsQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
