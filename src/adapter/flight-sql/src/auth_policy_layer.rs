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

use kamu_accounts_services::{AuthPolicyCheckError, AuthPolicyService};
use tonic::Status;
use tonic::body::BoxBody;
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
    fn check(base_catalog: &dill::Catalog) -> Result<(), AuthPolicyCheckError> {
        let auth_policy_service = base_catalog.get_one::<dyn AuthPolicyService>().unwrap();

        auth_policy_service.check()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Svc, ReqBody> Service<http::Request<ReqBody>> for AuthPolicyMiddleware<Svc>
where
    ReqBody: Send + 'static,
    Svc: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>>,
    Svc: Clone + Send + 'static,
    Svc::Future: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = Svc::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let base_catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions");

            if let Err(err_response) = Self::check(base_catalog) {
                return Ok(Status::unauthenticated(err_response.to_string()).into_http());
            }

            inner.call(request).await
        })
    }
}
