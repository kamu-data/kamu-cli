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

use crate::SearchResourcesRequest;
use crate::facade::graphql::cynic_api::fragments::{
    ResourceBadAccountProblem,
    ResourceConnection,
    ResourceInvalidLabelFilterProblem,
    ResourceUnsupportedSelectorProblem,
};
use crate::facade::graphql::cynic_api::inputs::SearchResourcesInput;
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "SearchVariables")]
pub(crate) struct SearchQuery {
    pub resources: SearchResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "SearchVariables")]
pub(crate) struct SearchResources {
    #[arguments(query: $query, page: $page, perPage: $per_page)]
    pub by_selectors: ResourceListOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourceListOutcome {
    ResourceConnection(ResourceConnection),
    ResourceUnsupportedSelectorProblem(ResourceUnsupportedSelectorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidLabelFilterProblem(ResourceInvalidLabelFilterProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct SearchVariables {
    pub query: SearchResourcesInput,
    pub page: i32,
    pub per_page: i32,
}

impl SearchVariables {
    pub(crate) fn new(request: &SearchResourcesRequest) -> Result<Self, InternalError> {
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
    variables: SearchVariables,
) -> cynic::Operation<SearchQuery, SearchVariables> {
    SearchQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
