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

use kamu_accounts::CurrentAccountSubject;
use tonic::Status;
use tower::{Layer, Service};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthPolicyLayer {
    allow_anonymous: bool,
}

impl AuthPolicyLayer {
    pub fn new(allow_anonymous: bool) -> Self {
        Self { allow_anonymous }
    }
}

impl<Svc> Layer<Svc> for AuthPolicyLayer {
    type Service = AuthPolicyMiddleware<Svc>;

    fn layer(&self, inner: Svc) -> Self::Service {
        AuthPolicyMiddleware {
            inner,
            allow_anonymous: self.allow_anonymous,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthPolicyMiddleware<Svc> {
    inner: Svc,
    allow_anonymous: bool,
}

impl<Svc> AuthPolicyMiddleware<Svc> {
    fn check_tonic_subject(base_catalog: &dill::Catalog) -> Result<(), String> {
        let current_account_subject = base_catalog
            .get_one::<kamu_accounts::CurrentAccountSubject>()
            .expect("CurrentAccountSubject not found in HTTP server extensions");

        match current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(_) => Ok(()),
            CurrentAccountSubject::Anonymous(_) => {
                Err("Anonymous account is restricted".to_string())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<Svc, ReqBody, ResBody> Service<http::Request<ReqBody>> for AuthPolicyMiddleware<Svc>
where
    ReqBody: Send + 'static,
    ResBody: Default,
    Svc: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    Svc: Clone + Send + 'static,
    Svc::Future: Send + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let allow_anonymous = self.allow_anonymous;

        Box::pin(async move {
            // ToDo: Modify to not use this value in runtime
            if !allow_anonymous {
                let base_catalog = request
                    .extensions()
                    .get::<dill::Catalog>()
                    .expect("Catalog not found in http server extensions");

                if let Err(err_response) = Self::check_tonic_subject(base_catalog) {
                    return Ok(Status::unauthenticated(err_response.clone()).into_http());
                }
            }

            inner.call(request).await
        })
    }
}
