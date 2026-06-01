use cynic::QueryBuilder;

use super::fragments::{BatchResourceProblem, ResourceRenderManifestResult};
use super::schema;
use super::variables::{
    RenderResourceManifestVariables,
    RenderResourceManifestVariablesFields,
    RenderResourceManifestsVariables,
    RenderResourceManifestsVariablesFields,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "RenderResourceManifestVariables")]
pub(crate) struct RenderManifestQuery {
    pub resources: RenderManifestResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "RenderResourceManifestVariables"
)]
pub(crate) struct RenderManifestResources {
    #[arguments(selector: $selector, format: $format, revealed: $revealed)]
    pub render_manifest: ResourceRenderManifestResult,
}

pub(crate) fn build_manifest_operation(
    variables: RenderResourceManifestVariables,
) -> cynic::Operation<RenderManifestQuery, RenderResourceManifestVariables> {
    RenderManifestQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "RenderResourceManifestsVariables")]
pub(crate) struct RenderManifestsQuery {
    pub resources: RenderManifestsResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "RenderResourceManifestsVariables"
)]
pub(crate) struct RenderManifestsResources {
    #[arguments(selector: $selector, format: $format, revealed: $revealed)]
    pub render_manifests: BatchResourceManifestsResult,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceManifestsResult {
    pub manifests: Vec<BatchResourceManifestSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceManifestSuccess {
    pub request_index: i32,
    pub manifest: ResourceRenderManifestResult,
}

pub(crate) fn build_manifests_operation(
    variables: RenderResourceManifestsVariables,
) -> cynic::Operation<RenderManifestsQuery, RenderResourceManifestsVariables> {
    RenderManifestsQuery::build(variables)
}
