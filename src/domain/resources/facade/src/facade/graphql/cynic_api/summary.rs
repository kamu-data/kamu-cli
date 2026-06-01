use cynic::QueryBuilder;

use super::fragments::ResourcesSummary;
use super::inputs::ResourceAccountSelectorInput;
use super::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct SummaryVariables {
    pub account: Option<ResourceAccountSelectorInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "SummaryVariables")]
pub(crate) struct SummaryQuery {
    pub resources: SummaryResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "SummaryVariables")]
pub(crate) struct SummaryResources {
    #[arguments(account: $account)]
    pub summary: ResourcesSummary,
}

pub(crate) fn build_operation(
    variables: SummaryVariables,
) -> cynic::Operation<SummaryQuery, SummaryVariables> {
    SummaryQuery::build(variables)
}
