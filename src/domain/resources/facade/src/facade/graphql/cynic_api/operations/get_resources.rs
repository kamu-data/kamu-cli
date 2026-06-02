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

use crate::facade::graphql::cynic_api::fragments::{BatchResourceProblem, Resource};
use crate::facade::graphql::cynic_api::inputs::ResourceBatchSelectorInput;
use crate::facade::graphql::cynic_api::schema;
use crate::{ResourceBatchSelector, SpecViewMode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceBatchSelectorVariables")]
pub(crate) struct GetResourcesQuery {
    pub resources: GetResourcesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceBatchSelectorVariables"
)]
pub(crate) struct GetResourcesResources {
    #[arguments(selector: $selector, revealed: $revealed)]
    pub resources: BatchResourcesResult,
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
pub(crate) struct ResourceBatchSelectorVariables {
    pub selector: ResourceBatchSelectorInput,
    pub revealed: bool,
}

impl ResourceBatchSelectorVariables {
    pub(crate) fn new(
        selector: &ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ResourceBatchSelectorVariables,
) -> cynic::Operation<GetResourcesQuery, ResourceBatchSelectorVariables> {
    GetResourcesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
