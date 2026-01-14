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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Adds request-scoped data (e.g., data loaders) to the GraphQL request.
#[derive(Clone, Copy)]
pub struct GraphqlRequestContext {
    pub extend_request: fn(async_graphql::Request, &Catalog) -> async_graphql::Request,
}

impl GraphqlRequestContext {
    pub fn new(
        extend_request: fn(async_graphql::Request, &Catalog) -> async_graphql::Request,
    ) -> Self {
        Self { extend_request }
    }
}

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
pub async fn graphql_handler<Query, Mutation, Subscription>(
    Extension(schema): Extension<async_graphql::Schema<Query, Mutation, Subscription>>,
    Extension(catalog): Extension<Catalog>,
    Extension(request_context): Extension<GraphqlRequestContext>,
    req: async_graphql_axum::GraphQLRequest,
) -> Result<async_graphql_axum::GraphQLResponse, GqlResponseError>
where
    Query: async_graphql::ObjectType + Send + Sync + 'static,
    Mutation: async_graphql::ObjectType + Send + Sync + 'static,
    Subscription: async_graphql::SubscriptionType + Send + Sync + 'static,
{
    let graphql_request =
        (request_context.extend_request)(req.into_inner(), &catalog).data(catalog);
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
