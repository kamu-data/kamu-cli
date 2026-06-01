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
use crate::facade::graphql::cynic_api::fragments::ResourceIdentityConnection;
use crate::facade::graphql::cynic_api::inputs::SearchResourceIdentitiesInput;
use crate::facade::graphql::cynic_api::schema;
use crate::facade::graphql::cynic_api::variables::graphql_page_params;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct SearchIdentitiesVariables {
    pub query: SearchResourceIdentitiesInput,
    pub page: i32,
    pub per_page: i32,
}

impl SearchIdentitiesVariables {
    pub(crate) fn new(request: &SearchResourceIdentitiesRequest) -> Result<Self, InternalError> {
        let (page, per_page) =
            graphql_page_params(request.pagination.offset, request.pagination.limit);
        Ok(Self {
            query: request.try_into()?,
            page,
            per_page,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: SearchIdentitiesVariables,
) -> cynic::Operation<SearchIdentitiesQuery, SearchIdentitiesVariables> {
    SearchIdentitiesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
