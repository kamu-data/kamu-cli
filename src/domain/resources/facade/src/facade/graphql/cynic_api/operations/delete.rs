// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::MutationBuilder;

use crate::facade::graphql::cynic_api::fragments::{
    BatchResourceProblem,
    ResourceAccountResolutionProblem,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::ResourceRefInput;
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Mutation", variables = "DeleteVariables")]
pub(crate) struct DeleteMutation {
    pub resources: ResourcesMutDelete,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourcesMut", variables = "DeleteVariables")]
pub(crate) struct ResourcesMutDelete {
    #[arguments(resourceRefs: $resource_refs)]
    pub delete: ResourceDeleteOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceDeleteOutcome {
    ResourceDeleteResult(ResourceDeleteResult),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceAccountResolutionProblem(ResourceAccountResolutionProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceDeleteResult {
    pub resources: Vec<ResourceDeleteSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceDeleteSuccess {
    pub request_index: i32,
    pub resource_id: kamu_resources::ResourceID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct DeleteVariables {
    pub resource_refs: Vec<ResourceRefInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_delete_operation(
    variables: DeleteVariables,
) -> cynic::Operation<DeleteMutation, DeleteVariables> {
    DeleteMutation::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
