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

use crate::SearchResourceIdentitiesRequest;
use crate::facade::graphql::cynic_api::fragments::{
    ResourceBadAccountProblem,
    ResourceIdentityConnection,
    ResourceInvalidSearchQueryProblem,
    ResourceUnsupportedDescriptorProblem,
};
use crate::facade::graphql::cynic_api::inputs::SearchResourceIdentitiesInput;
use crate::facade::graphql::cynic_api::schema;

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
    pub search_identities: ResourceIdentityListOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceIdentityListOutcome {
    ResourceIdentityConnection(ResourceIdentityConnection),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidSearchQueryProblem(ResourceInvalidSearchQueryProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct SearchIdentitiesVariables {
    pub query: SearchResourceIdentitiesInput,
    pub page: i32,
    pub per_page: i32,
}

impl SearchIdentitiesVariables {
    pub(crate) fn new(request: &SearchResourceIdentitiesRequest) -> Result<Self, InternalError> {
        let (page, per_page) = request.pagination.as_page_params(Self::DEFAULT_PAGE_SIZE)?;
        Ok(Self {
            query: request.try_into()?,
            page,
            per_page,
        })
    }

    const DEFAULT_PAGE_SIZE: usize = 100;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: SearchIdentitiesVariables,
) -> cynic::Operation<SearchIdentitiesQuery, SearchIdentitiesVariables> {
    SearchIdentitiesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
