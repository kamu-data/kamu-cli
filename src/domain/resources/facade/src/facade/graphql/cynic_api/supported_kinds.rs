use cynic::QueryBuilder;

use super::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query")]
pub(crate) struct SupportedKindsQuery {
    pub resources: Resources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct Resources {
    pub supported_kinds: Vec<ResourceKindDescriptor>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceKindDescriptor {
    pub name: String,
    pub short_names: Vec<String>,
    pub kind: ResourceKind,
    pub api_version: String,
    pub list_columns: Vec<ResourceListColumnDescriptor>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceKind {
    pub value: String,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceListColumnDescriptor {
    pub key: String,
    pub header: String,
    pub data_type: String,
    pub visibility: String,
}

pub(crate) fn build_operation() -> cynic::Operation<SupportedKindsQuery, ()> {
    SupportedKindsQuery::build(())
}
