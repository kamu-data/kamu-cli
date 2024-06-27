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
use axum::response::{IntoResponse, Response};
use database_common::DatabaseTransactionRunner;
use futures::Future;
use kamu::domain::InternalError;
use tower::{Layer, Service};

use crate::api_error::IntoApiError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RunInDatabaseTransactionLayer {}

impl RunInDatabaseTransactionLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<InnerSvc> Layer<InnerSvc> for RunInDatabaseTransactionLayer {
    type Service = RunInDatabaseTransactionMiddleware<InnerSvc>;

    fn layer(&self, inner: InnerSvc) -> Self::Service {
        RunInDatabaseTransactionMiddleware { inner }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RunInDatabaseTransactionMiddleware<Svc> {
    inner: Svc,
}

impl<InnerSvc> Service<http::Request<Body>> for RunInDatabaseTransactionMiddleware<InnerSvc>
where
    InnerSvc: Service<http::Request<Body>, Response = Response> + Send + Clone + 'static,
    InnerSvc::Error: Send,
    InnerSvc::Future: Send,
{
    type Response = InnerSvc::Response;
    type Error = InnerSvc::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(ctx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        // Inspired by https://github.com/maxcountryman/axum-login/blob/5239b38b2698a3db3f92075b6ad430aea79c215a/axum-login/src/auth.rs
        // TODO: PERF: Is cloning a performance concern?
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let base_catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions")
                .clone();
            let transaction_runner = DatabaseTransactionRunner::new(base_catalog);

            transaction_runner
                .transactional(|updated_catalog| async move {
                    request.extensions_mut().insert(updated_catalog);

                    let inner_result = inner.call(request).await;

                    Ok(inner_result)
                })
                .await
                .unwrap_or_else(|e: InternalError| Ok(e.api_err().into_response()))
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
