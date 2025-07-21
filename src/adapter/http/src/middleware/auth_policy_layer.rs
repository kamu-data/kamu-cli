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
use graphql_parser::query::{Definition, parse_query};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use kamu_accounts::CurrentAccountSubject;
use serde_json::Value;
use tower::{Layer, Service};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthPolicyLayer {}

impl AuthPolicyLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<Svc> Layer<Svc> for AuthPolicyLayer {
    type Service = AuthPolicyMiddleware<Svc>;

    fn layer(&self, inner: Svc) -> Self::Service {
        AuthPolicyMiddleware { inner }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthPolicyMiddleware<Svc> {
    inner: Svc,
}

impl<Svc> AuthPolicyMiddleware<Svc> {
    fn check_http_subject(base_catalog: &dill::Catalog) -> Result<(), Response> {
        let current_account_subject = base_catalog
            .get_one::<kamu_accounts::CurrentAccountSubject>()
            .expect("CurrentAccountSubject not found in HTTP server extensions");

        match current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(_) => Ok(()),
            CurrentAccountSubject::Anonymous(_) => Err(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("Anonymous account is restricted".into())
                .unwrap()),
        }
    }

    pub async fn is_auth_graphql_operation_request(
        request: Request<Body>,
    ) -> (Request<Body>, bool) {
        if request.method() == http::Method::GET {
            return (request, true);
        }

        let (parts, body) = request.into_parts();

        let body_bytes = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => return (Request::from_parts(parts, Body::empty()), false),
        };

        let json: Value = match serde_json::from_slice(&body_bytes) {
            Ok(v) => v,
            Err(_) => return (Request::from_parts(parts, Body::from(body_bytes)), false),
        };

        let Some(query_str) = json.get("query").and_then(|q| q.as_str()) else {
            return (Request::from_parts(parts, Body::from(body_bytes)), false);
        };

        let parsed_query = match parse_query::<&str>(query_str) {
            Ok(doc) => doc,
            Err(_) => return (Request::from_parts(parts, Body::from(body_bytes)), false),
        };

        let is_auth = parsed_query.definitions.iter().any(|def| match def {
            Definition::Operation(op) => {
                let selection_set = match op {
                    graphql_parser::query::OperationDefinition::Query(q) => &q.selection_set,
                    graphql_parser::query::OperationDefinition::Mutation(m) => &m.selection_set,
                    graphql_parser::query::OperationDefinition::Subscription(_) => return false,
                    graphql_parser::query::OperationDefinition::SelectionSet(s) => s,
                };

                selection_set.items.iter().any(|sel| {
                    if let graphql_parser::query::Selection::Field(field) = sel {
                        field.name.to_lowercase().contains("auth")
                    } else {
                        false
                    }
                })
            }
            _ => false,
        });

        (Request::from_parts(parts, Body::from(body_bytes)), is_auth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Svc> Service<http::Request<Body>> for AuthPolicyMiddleware<Svc>
where
    Svc: Service<http::Request<Body>, Response = Response> + Send + 'static + Clone,
    Svc::Future: Send + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let path = request.uri().path();

            let (request, skip_auth) = if path == "/graphql" {
                Self::is_auth_graphql_operation_request(request).await
            } else {
                (request, false)
            };

            if skip_auth {
                return inner.call(request).await;
            }

            let base_catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in HTTP server extensions");

            if let Err(err_response) = Self::check_http_subject(base_catalog) {
                return Ok(err_response);
            }

            inner.call(request).await
        })
    }
}
