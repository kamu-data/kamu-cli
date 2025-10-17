// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::response::Response;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const HEADER_TRACE_GRAPHQL: &str = "x-trace-graphql";
const HEADER_TRACE_GRAPHQL_ENABLED_VALUE: http::HeaderValue = http::HeaderValue::from_static("1");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GraphqlTracingLayer<Query, Mutation, Subscription> {
    // NOTE: We don't use `async_graphql::SchemaBuilder` here because it isn't cloneable.
    schema: async_graphql::Schema<Query, Mutation, Subscription>,
    schema_quiet: async_graphql::Schema<Query, Mutation, Subscription>,
}

// NOTE: Explicit implementation to avoid adding Clone requirement for generic
//       types.
impl<Query, Mutation, Subscription> Clone for GraphqlTracingLayer<Query, Mutation, Subscription> {
    fn clone(&self) -> Self {
        // Cheap clones
        Self {
            schema: self.schema.clone(),
            schema_quiet: self.schema_quiet.clone(),
        }
    }
}

impl<Query, Mutation, Subscription> GraphqlTracingLayer<Query, Mutation, Subscription> {
    pub fn new(
        schema: async_graphql::Schema<Query, Mutation, Subscription>,
        schema_quiet: async_graphql::Schema<Query, Mutation, Subscription>,
    ) -> Self {
        Self {
            schema,
            schema_quiet,
        }
    }
}

impl<Svc, Query, Mutation, Subscription> tower::Layer<Svc>
    for GraphqlTracingLayer<Query, Mutation, Subscription>
{
    type Service = GraphqlTracingMiddleware<Svc, Query, Mutation, Subscription>;

    fn layer(&self, inner: Svc) -> Self::Service {
        // Cheap clones
        GraphqlTracingMiddleware {
            inner,
            schema: self.schema.clone(),
            schema_quiet: self.schema_quiet.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GraphqlTracingMiddleware<Svc, Query, Mutation, Subscription> {
    inner: Svc,
    schema: async_graphql::Schema<Query, Mutation, Subscription>,
    schema_quiet: async_graphql::Schema<Query, Mutation, Subscription>,
}

// NOTE: Explicit implementation to avoid adding Clone requirement for generic
//       types.
impl<Svc, Query, Mutation, Subscription> Clone
    for GraphqlTracingMiddleware<Svc, Query, Mutation, Subscription>
where
    Svc: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            schema: self.schema.clone(),
            schema_quiet: self.schema_quiet.clone(),
        }
    }
}

impl<Svc, Query, Mutation, Subscription> tower::Service<http::Request<Body>>
    for GraphqlTracingMiddleware<Svc, Query, Mutation, Subscription>
where
    Svc: tower::Service<http::Request<Body>, Response = Response> + Send + Clone + 'static,
    Svc::Future: Send + 'static,
    Query: Sync + Send + 'static,
    Mutation: Sync + Send + 'static,
    Subscription: Sync + Send + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let mut inner = self.inner.clone();
        let schema = self.schema.clone();
        let schema_quiet = self.schema_quiet.clone();

        Box::pin(async move {
            let schema_to_insert = match request.headers().get(HEADER_TRACE_GRAPHQL) {
                Some(header_value) if header_value == HEADER_TRACE_GRAPHQL_ENABLED_VALUE => schema,
                _ => schema_quiet,
            };

            request.extensions_mut().insert(schema_to_insert);

            inner.call(request).await
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
