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
    ResourceIdentityConnection,
    ResourceInvalidSearchQueryProblem,
    ResourceUnsupportedDescriptorProblem,
};
use crate::facade::graphql::cynic_api::schema;
use crate::facade::graphql::cynic_api::variables::{
    ListAllVariables,
    ListAllVariablesFields,
    ListByKindVariables,
    ListByKindVariablesFields,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListByKindVariables")]
pub(crate) struct ListByKindQuery {
    pub resources: ListByKindResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListByKindVariables")]
pub(crate) struct ListByKindResources {
    #[arguments(kind: $kind, account: $account, page: $page, perPage: $per_page)]
    pub list_by_kind: ResourceListOutcome,
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
    #[arguments(account: $account, page: $page, perPage: $per_page)]
    pub list_all: ResourceListAllOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListByKindVariables")]
pub(crate) struct ListIdentitiesByKindQuery {
    pub resources: ListIdentitiesByKindResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListByKindVariables")]
pub(crate) struct ListIdentitiesByKindResources {
    #[arguments(kind: $kind, account: $account, page: $page, perPage: $per_page)]
    pub list_identities_by_kind: ResourceIdentityListOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ListAllVariables")]
pub(crate) struct ListAllIdentitiesQuery {
    pub resources: ListAllIdentitiesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ListAllVariables")]
pub(crate) struct ListAllIdentitiesResources {
    #[arguments(account: $account, page: $page, perPage: $per_page)]
    pub list_all_identities: ResourceIdentityListAllOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceListOutcome {
    ResourceConnection(ResourceConnection),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceListAllOutcome {
    ResourceConnection(ResourceConnection),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceIdentityListOutcome {
    ResourceIdentityConnection(ResourceIdentityConnection),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidSearchQueryProblem(ResourceInvalidSearchQueryProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceIdentityListAllOutcome {
    ResourceIdentityConnection(ResourceIdentityConnection),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_list_by_kind_operation(
    variables: ListByKindVariables,
) -> cynic::Operation<ListByKindQuery, ListByKindVariables> {
    ListByKindQuery::build(variables)
}

pub(crate) fn build_list_all_operation(
    variables: ListAllVariables,
) -> cynic::Operation<ListAllQuery, ListAllVariables> {
    ListAllQuery::build(variables)
}

pub(crate) fn build_list_identities_by_kind_operation(
    variables: ListByKindVariables,
) -> cynic::Operation<ListIdentitiesByKindQuery, ListByKindVariables> {
    ListIdentitiesByKindQuery::build(variables)
}

pub(crate) fn build_list_all_identities_operation(
    variables: ListAllVariables,
) -> cynic::Operation<ListAllIdentitiesQuery, ListAllVariables> {
    ListAllIdentitiesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
