// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::MutationBuilder;

use crate::facade::graphql::cynic_api::fragments::BatchResourceProblem;
use crate::facade::graphql::cynic_api::inputs::{
    ResourceBatchSelectorInput,
    ResourceSelectorInput,
};
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
    #[arguments(selector: $selector)]
    pub delete: ResourceDeleteResult,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Mutation", variables = "DeleteManyVariables")]
pub(crate) struct DeleteManyMutation {
    pub resources: ResourcesMutDeleteMany,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourcesMut", variables = "DeleteManyVariables")]
pub(crate) struct ResourcesMutDeleteMany {
    #[arguments(selector: $selector)]
    pub delete_many: ResourceDeleteManyResult,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceDeleteResult {
    pub resource_id: kamu_resources::ResourceUID,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceDeleteManyResult {
    pub resources: Vec<ResourceDeleteSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceDeleteSuccess {
    pub request_index: i32,
    pub resource_id: kamu_resources::ResourceUID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct DeleteVariables {
    pub selector: ResourceSelectorInput,
}

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct DeleteManyVariables {
    pub selector: ResourceBatchSelectorInput,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_delete_operation(
    variables: DeleteVariables,
) -> cynic::Operation<DeleteMutation, DeleteVariables> {
    DeleteMutation::build(variables)
}

pub(crate) fn build_delete_many_operation(
    variables: DeleteManyVariables,
) -> cynic::Operation<DeleteManyMutation, DeleteManyVariables> {
    DeleteManyMutation::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
