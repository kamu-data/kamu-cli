use cynic::QueryBuilder;

use super::fragments::{BatchResourceProblem, Resource};
use super::schema;
use super::variables::{ResourceBatchSelectorVariables, ResourceBatchSelectorVariablesFields};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceBatchSelectorVariables")]
pub(crate) struct GetResourcesQuery {
    pub resources: Resources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(variables = "ResourceBatchSelectorVariables")]
pub(crate) struct Resources {
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

pub(crate) fn build_operation(
    variables: ResourceBatchSelectorVariables,
) -> cynic::Operation<GetResourcesQuery, ResourceBatchSelectorVariables> {
    GetResourcesQuery::build(variables)
}
