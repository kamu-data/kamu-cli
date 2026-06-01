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
    ResourceConnection,
    ResourceIdentityConnection,
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
    pub list_by_kind: ResourceConnection,
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
    pub list_all: ResourceConnection,
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
    pub list_identities_by_kind: ResourceIdentityConnection,
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
    pub list_all_identities: ResourceIdentityConnection,
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
