use cynic::QueryBuilder;

use super::fragments::{ResourceConnection, ResourceIdentityConnection};
use super::schema;
use super::variables::{
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

pub(crate) fn build_list_by_kind_operation(
    variables: ListByKindVariables,
) -> cynic::Operation<ListByKindQuery, ListByKindVariables> {
    ListByKindQuery::build(variables)
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

pub(crate) fn build_list_all_operation(
    variables: ListAllVariables,
) -> cynic::Operation<ListAllQuery, ListAllVariables> {
    ListAllQuery::build(variables)
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

pub(crate) fn build_list_identities_by_kind_operation(
    variables: ListByKindVariables,
) -> cynic::Operation<ListIdentitiesByKindQuery, ListByKindVariables> {
    ListIdentitiesByKindQuery::build(variables)
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

pub(crate) fn build_list_all_identities_operation(
    variables: ListAllVariables,
) -> cynic::Operation<ListAllIdentitiesQuery, ListAllVariables> {
    ListAllIdentitiesQuery::build(variables)
}
