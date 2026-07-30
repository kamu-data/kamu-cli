// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;

use crate::facade::graphql::cynic_api::fragments::{
    ResourceBadAccountProblem,
    ResourceConnection,
    ResourceHandleConnection,
    ResourceInvalidLabelFilterProblem,
    ResourceInvalidSearchQueryProblem,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::schema;
use crate::facade::graphql::cynic_api::variables::{
    ListAllVariables,
    ListAllVariablesFields,
    ListByResourceTypeVariables,
    ListByResourceTypeVariablesFields,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListByResourceTypeVariables")]
pub(crate) struct ListByResourceTypeQuery {
    pub resources: ListByResourceTypeResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListByResourceTypeVariables")]
pub(crate) struct ListByResourceTypeResources {
    #[arguments(resourceType: $resource_type, account: $account, labelFilter: $label_filter, page: $page, perPage: $per_page)]
    pub list_by_resource_type: ResourceListOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListAllVariables")]
pub(crate) struct ListAllQuery {
    pub resources: ListAllResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListAllVariables")]
pub(crate) struct ListAllResources {
    #[arguments(account: $account, labelFilter: $label_filter, page: $page, perPage: $per_page)]
    pub list_all: ResourceListAllOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListByResourceTypeVariables")]
pub(crate) struct ListHandlesByResourceTypeQuery {
    pub resources: ListHandlesByResourceTypeResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListByResourceTypeVariables")]
pub(crate) struct ListHandlesByResourceTypeResources {
    #[arguments(resourceType: $resource_type, account: $account, labelFilter: $label_filter, page: $page, perPage: $per_page)]
    pub list_handles_by_resource_type: ResourceHandleListOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListAllVariables")]
pub(crate) struct ListAllHandlesQuery {
    pub resources: ListAllHandlesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListAllVariables")]
pub(crate) struct ListAllHandlesResources {
    #[arguments(account: $account, labelFilter: $label_filter, page: $page, perPage: $per_page)]
    pub list_all_handles: ResourceHandleListAllOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceListOutcome {
    ResourceConnection(ResourceConnection),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidLabelFilterProblem(ResourceInvalidLabelFilterProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceListAllOutcome {
    ResourceConnection(ResourceConnection),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidLabelFilterProblem(ResourceInvalidLabelFilterProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceHandleListOutcome {
    ResourceHandleConnection(ResourceHandleConnection),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidSearchQueryProblem(ResourceInvalidSearchQueryProblem),
    ResourceInvalidLabelFilterProblem(ResourceInvalidLabelFilterProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceHandleListAllOutcome {
    ResourceHandleConnection(ResourceHandleConnection),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidLabelFilterProblem(ResourceInvalidLabelFilterProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_list_by_resource_type_operation(
    variables: ListByResourceTypeVariables,
) -> cynic::Operation<ListByResourceTypeQuery, ListByResourceTypeVariables> {
    ListByResourceTypeQuery::build(variables)
}

pub(crate) fn build_list_all_operation(
    variables: ListAllVariables,
) -> cynic::Operation<ListAllQuery, ListAllVariables> {
    ListAllQuery::build(variables)
}

pub(crate) fn build_list_handles_by_resource_type_operation(
    variables: ListByResourceTypeVariables,
) -> cynic::Operation<ListHandlesByResourceTypeQuery, ListByResourceTypeVariables> {
    ListHandlesByResourceTypeQuery::build(variables)
}

pub(crate) fn build_list_all_handles_operation(
    variables: ListAllVariables,
) -> cynic::Operation<ListAllHandlesQuery, ListAllVariables> {
    ListAllHandlesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
