// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;
use kamu_resources::ResourceRef;

use crate::SpecViewMode;
use crate::facade::graphql::cynic_api::fragments::{Resource, ResourceSelectorProblemResult};
use crate::facade::graphql::cynic_api::inputs::ResourceRefInput;
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceRefVariables")]
pub(crate) struct GetResourceQuery {
    pub resources: GetResourceResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ResourceRefVariables")]
pub(crate) struct GetResourceResources {
    #[arguments(resourceRef: $resource_ref, revealed: $revealed)]
    pub resource: ResourceGetOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceGetOutcome {
    Resource(Resource),
    ResourceSelectorProblemResult(ResourceSelectorProblemResult),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceRefVariables {
    pub resource_ref: ResourceRefInput,
    pub revealed: bool,
}

impl ResourceRefVariables {
    pub(crate) fn new(resource_ref: &ResourceRef, spec_view_mode: SpecViewMode) -> Self {
        Self {
            resource_ref: resource_ref.into(),
            revealed: spec_view_mode == SpecViewMode::Revealed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ResourceRefVariables,
) -> cynic::Operation<GetResourceQuery, ResourceRefVariables> {
    GetResourceQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
