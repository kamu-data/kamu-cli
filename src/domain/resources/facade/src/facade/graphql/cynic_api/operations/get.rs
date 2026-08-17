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

use crate::SpecViewMode;
use crate::facade::graphql::cynic_api::fragments::{
    BatchResourceProblem,
    Resource,
    ResourceBadAccountProblem,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::{ResourceRefInput, resource_ref_inputs};
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
    #[arguments(resourceRefs: $resource_refs, revealed: $revealed)]
    pub resources: BatchResourcesOutcome,
}

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum BatchResourcesOutcome {
    BatchResourcesResult(BatchResourcesResult),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
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
    pub revealed: bool,
}

impl ResourceRefsVariables {
    pub(crate) fn new(resource_refs: &[ResourceRef], spec_view_mode: SpecViewMode) -> Self {
        Self {
            resource_refs: resource_ref_inputs(resource_refs),
            revealed: spec_view_mode == SpecViewMode::Revealed,
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
