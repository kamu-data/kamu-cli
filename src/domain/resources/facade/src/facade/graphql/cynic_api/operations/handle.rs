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
    ResourceBadAccountProblem,
    ResourceHandle,
    ResourceSelectorProblemResult,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::{ResourceRefInput, resource_ref_inputs};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceHandleRefVariables")]
pub(crate) struct GetResourceHandleQuery {
    pub resources: ResourceHandleResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ResourceHandleRefVariables")]
pub(crate) struct ResourceHandleResources {
    #[arguments(resourceRef: $resource_ref)]
    pub resource_handle: ResourceGetHandleOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceGetHandleOutcome {
    ResourceHandle(ResourceHandle),
    ResourceSelectorProblemResult(ResourceSelectorProblemResult),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceHandleRefsVariables")]
pub(crate) struct GetResourceHandlesQuery {
    pub resources: ResourceHandlesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ResourceHandleRefsVariables")]
pub(crate) struct ResourceHandlesResources {
    #[arguments(resourceRefs: $resource_refs)]
    pub resource_handles: BatchResourceHandlesOutcome,
}

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum BatchResourceHandlesOutcome {
    BatchResourceHandlesResult(BatchResourceHandlesResult),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceHandlesResult {
    pub handles: Vec<BatchResourceHandleSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceHandleSuccess {
    pub request_index: i32,
    pub handle: ResourceHandle,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceHandleRefVariables {
    pub resource_ref: ResourceRefInput,
}

impl ResourceHandleRefVariables {
    pub(crate) fn new(resource_ref: &ResourceRef) -> Self {
        Self {
            resource_ref: resource_ref.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceHandleRefsVariables {
    pub resource_refs: Vec<ResourceRefInput>,
}

impl ResourceHandleRefsVariables {
    pub(crate) fn new(resource_refs: &[ResourceRef]) -> Self {
        Self {
            resource_refs: resource_ref_inputs(resource_refs),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_handle_operation(
    variables: ResourceHandleRefVariables,
) -> cynic::Operation<GetResourceHandleQuery, ResourceHandleRefVariables> {
    GetResourceHandleQuery::build(variables)
}

pub(crate) fn build_handles_operation(
    variables: ResourceHandleRefsVariables,
) -> cynic::Operation<GetResourceHandlesQuery, ResourceHandleRefsVariables> {
    GetResourceHandlesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
