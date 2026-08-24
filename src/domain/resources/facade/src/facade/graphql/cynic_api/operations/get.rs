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

use crate::SpecViewOpts;
use crate::facade::graphql::cynic_api::fragments::{
    BatchResourceProblem,
    Resource,
    ResourceAccountResolutionProblem,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::{
    ResourceRefInput,
    SpecViewOptsInput,
    resource_ref_inputs,
};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceRefsVariables")]
pub(crate) struct GetResourcesQuery {
    pub resources: GetResourcesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ResourceRefsVariables")]
pub(crate) struct GetResourcesResources {
    #[arguments(resourceRefs: $resource_refs, opts: $opts)]
    pub by_refs: BatchResourcesOutcome,
}

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum BatchResourcesOutcome {
    BatchResourcesResult(BatchResourcesResult),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceAccountResolutionProblem(ResourceAccountResolutionProblem),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourcesResult {
    pub resources: Vec<BatchResourceSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceSuccess {
    pub request_index: i32,
    pub resource: Resource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceRefsVariables {
    pub resource_refs: Vec<ResourceRefInput>,
    pub opts: SpecViewOptsInput,
}

impl ResourceRefsVariables {
    pub(crate) fn new(resource_refs: &[ResourceRef], spec_view: SpecViewOpts) -> Self {
        Self {
            resource_refs: resource_ref_inputs(resource_refs),
            opts: spec_view.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ResourceRefsVariables,
) -> cynic::Operation<GetResourcesQuery, ResourceRefsVariables> {
    GetResourcesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
