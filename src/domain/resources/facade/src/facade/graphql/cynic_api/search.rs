use cynic::QueryBuilder;

use super::fragments::ResourceIdentityConnection;
use super::schema;
use super::variables::{SearchIdentitiesVariables, SearchIdentitiesVariablesFields};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "SearchIdentitiesVariables")]
pub(crate) struct SearchIdentitiesQuery {
    pub resources: SearchIdentitiesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "SearchIdentitiesVariables")]
pub(crate) struct SearchIdentitiesResources {
    #[arguments(query: $query, page: $page, perPage: $per_page)]
    pub search_identities: ResourceIdentityConnection,
}

pub(crate) fn build_operation(
    variables: SearchIdentitiesVariables,
) -> cynic::Operation<SearchIdentitiesQuery, SearchIdentitiesVariables> {
    SearchIdentitiesQuery::build(variables)
}
