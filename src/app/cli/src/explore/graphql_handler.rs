// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Extension;
use axum::response::IntoResponse;
use database_common_macros::transactional_handler;
use dill::Catalog;
use internal_error::InternalError;
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An error type that wraps a GraphQL response containing errors.
/// When returned as `Err`, it triggers transaction rollback while still
/// returning the GraphQL response (with errors) to the client.
pub struct GqlResponseError(async_graphql_axum::GraphQLResponse);

impl std::fmt::Display for GqlResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GraphQL response contains errors")
    }
}

impl std::fmt::Debug for GqlResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GqlResponseError")
    }
}

impl std::error::Error for GqlResponseError {}

impl From<InternalError> for GqlResponseError {
    fn from(e: InternalError) -> Self {
        let response = async_graphql::Response::from_errors(vec![async_graphql::ServerError::new(
            e.to_string(),
            None,
        )]);
        GqlResponseError(response.into())
    }
}

impl IntoResponse for GqlResponseError {
    fn into_response(self) -> axum::response::Response {
        self.0.into_response()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn graphql_handler(
    Extension(schema): Extension<kamu_adapter_graphql::Schema>,
    Extension(catalog): Extension<Catalog>,
    req: async_graphql_axum::GraphQLRequest,
) -> Result<async_graphql_axum::GraphQLResponse, GqlResponseError> {
    let graphql_request = req
        .into_inner()
        .data(account_entity_data_loader(&catalog))
        .data(dataset_handle_data_loader(&catalog))
        .data(catalog);

    tracing::debug!(?graphql_request, "Incoming GraphQL request");

    let graphql_response: async_graphql_axum::GraphQLResponse =
        schema.execute(graphql_request).await.into();

    // Check if the response contains errors - if so, return Err to trigger
    // transaction rollback while still returning the GraphQL response to the client
    if !graphql_response.0.is_ok() {
        Err(GqlResponseError(graphql_response))
    } else {
        Ok(graphql_response)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
