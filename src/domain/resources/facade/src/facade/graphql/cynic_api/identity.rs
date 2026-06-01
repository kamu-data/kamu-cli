use cynic::QueryBuilder;

use super::fragments::{BatchResourceProblem, ResourceIdentity};
use super::schema;
use super::variables::{
    ResourceIdentityBatchSelectorVariables,
    ResourceIdentityBatchSelectorVariablesFields,
    ResourceIdentitySelectorVariables,
    ResourceIdentitySelectorVariablesFields,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Query",
    variables = "ResourceIdentitySelectorVariables"
)]
pub(crate) struct GetResourceIdentityQuery {
    pub resources: ResourceIdentityResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceIdentitySelectorVariables"
)]
pub(crate) struct ResourceIdentityResources {
    #[arguments(selector: $selector)]
    pub resource_identity: Option<ResourceIdentity>,
}

pub(crate) fn build_identity_operation(
    variables: ResourceIdentitySelectorVariables,
) -> cynic::Operation<GetResourceIdentityQuery, ResourceIdentitySelectorVariables> {
    GetResourceIdentityQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Query",
    variables = "ResourceIdentityBatchSelectorVariables"
)]
pub(crate) struct GetResourceIdentitiesQuery {
    pub resources: ResourceIdentitiesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceIdentityBatchSelectorVariables"
)]
pub(crate) struct ResourceIdentitiesResources {
    #[arguments(selector: $selector)]
    pub resource_identities: BatchResourceIdentitiesResult,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceIdentitiesResult {
    pub identities: Vec<BatchResourceIdentitySuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceIdentitySuccess {
    pub request_index: i32,
    pub identity: ResourceIdentity,
}

pub(crate) fn build_identities_operation(
    variables: ResourceIdentityBatchSelectorVariables,
) -> cynic::Operation<GetResourceIdentitiesQuery, ResourceIdentityBatchSelectorVariables> {
    GetResourceIdentitiesQuery::build(variables)
}
