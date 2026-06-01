use cynic::QueryBuilder;

use super::fragments::Resource;
use super::schema;
use super::variables::{ResourceSelectorVariables, ResourceSelectorVariablesFields};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceSelectorVariables")]
pub(crate) struct GetResourceQuery {
    pub resources: Resources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(variables = "ResourceSelectorVariables")]
pub(crate) struct Resources {
    #[arguments(selector: $selector, revealed: $revealed)]
    pub resource: Option<Resource>,
}

pub(crate) fn build_operation(
    variables: ResourceSelectorVariables,
) -> cynic::Operation<GetResourceQuery, ResourceSelectorVariables> {
    GetResourceQuery::build(variables)
}
