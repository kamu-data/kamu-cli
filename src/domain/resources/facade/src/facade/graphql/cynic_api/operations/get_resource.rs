// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;
use internal_error::InternalError;

use crate::facade::graphql::cynic_api::fragments::{
    Resource,
    ResourceBadAccountProblem,
    ResourceUnsupportedDescriptorProblem,
};
use crate::facade::graphql::cynic_api::inputs::ResourceSelectorInput;
use crate::facade::graphql::cynic_api::schema;
use crate::{ResourceSelector, SpecViewMode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "ResourceSelectorVariables")]
pub(crate) struct GetResourceQuery {
    pub resources: GetResourceResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "ResourceSelectorVariables")]
pub(crate) struct GetResourceResources {
    #[arguments(selector: $selector, revealed: $revealed)]
    pub resource: ResourceGetOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceGetOutcome {
    Resource(Resource),
    ResourceUIDNotFoundProblem(GetResourceUIDNotFoundProblem),
    ResourceNameNotFoundProblem(GetResourceNameNotFoundProblem),
    ResourceApiVersionMismatchProblem(GetResourceApiVersionMismatchProblem),
    ResourceKindMismatchProblem(GetResourceKindMismatchProblem),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceUIDNotFoundProblem")]
pub(crate) struct GetResourceUIDNotFoundProblem {
    pub uid: kamu_resources::ResourceUID,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceNameNotFoundProblem")]
pub(crate) struct GetResourceNameNotFoundProblem {
    pub kind: String,
    pub name: String,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceApiVersionMismatchProblem")]
pub(crate) struct GetResourceApiVersionMismatchProblem {
    pub expected_api_version: String,
    pub actual_api_version: String,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourceKindMismatchProblem")]
pub(crate) struct GetResourceKindMismatchProblem {
    pub uid: kamu_resources::ResourceUID,
    pub expected_kind: String,
    pub actual_kind: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceSelectorVariables {
    pub selector: ResourceSelectorInput,
    pub revealed: bool,
}

impl ResourceSelectorVariables {
    pub(crate) fn new(
        selector: &ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ResourceSelectorVariables,
) -> cynic::Operation<GetResourceQuery, ResourceSelectorVariables> {
    GetResourceQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
