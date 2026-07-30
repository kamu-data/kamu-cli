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
    ResourceBadAccountProblem,
    ResourceHandle,
    ResourceInvalidLabelFilterProblem,
    ResourceSelectorProblemResult,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::{
    ResourceBatchSelectorInput,
    ResourceSelectorInput,
};
use crate::facade::graphql::cynic_api::schema;
use crate::{ResourceBatchSelector, ResourceSelector};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceHandleSelectorVariables")]
pub(crate) struct GetResourceHandleQuery {
    pub resources: ResourceHandleResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceHandleSelectorVariables"
)]
pub(crate) struct ResourceHandleResources {
    #[arguments(selector: $selector)]
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
#[cynic(
    graphql_type = "Query",
    variables = "ResourceHandleBatchSelectorVariables"
)]
pub(crate) struct GetResourceHandlesQuery {
    pub resources: ResourceHandlesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceHandleBatchSelectorVariables"
)]
pub(crate) struct ResourceHandlesResources {
    #[arguments(selector: $selector)]
    pub resource_handles: BatchResourceHandlesOutcome,
}

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum BatchResourceHandlesOutcome {
    BatchResourceHandlesResult(BatchResourceHandlesResult),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidLabelFilterProblem(ResourceInvalidLabelFilterProblem),
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
pub(crate) struct ResourceHandleSelectorVariables {
    pub selector: ResourceSelectorInput,
}

impl ResourceHandleSelectorVariables {
    pub(crate) fn new(selector: &ResourceSelector) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceHandleBatchSelectorVariables {
    pub selector: ResourceBatchSelectorInput,
}

impl ResourceHandleBatchSelectorVariables {
    pub(crate) fn new(selector: &ResourceBatchSelector) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_handle_operation(
    variables: ResourceHandleSelectorVariables,
) -> cynic::Operation<GetResourceHandleQuery, ResourceHandleSelectorVariables> {
    GetResourceHandleQuery::build(variables)
}

pub(crate) fn build_handles_operation(
    variables: ResourceHandleBatchSelectorVariables,
) -> cynic::Operation<GetResourceHandlesQuery, ResourceHandleBatchSelectorVariables> {
    GetResourceHandlesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
