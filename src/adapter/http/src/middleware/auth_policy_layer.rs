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
use http::StatusCode;
use kamu_accounts_services::AuthPolicyService;
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
    fn check(base_catalog: &dill::Catalog) -> Result<(), Response> {
        let auth_policy_service = base_catalog.get_one::<dyn AuthPolicyService>().unwrap();

        if let Err(err) = auth_policy_service.check() {
            tracing::warn!(error = ?err, "Auth policy check failed");
            return Err(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(err.to_string().into())
                .unwrap());
        }

        Ok(())
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
            let base_catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions");

            if let Err(err_response) = Self::check(base_catalog) {
                return Ok(err_response);
            }

            inner.call(request).await
        })
    }
}
