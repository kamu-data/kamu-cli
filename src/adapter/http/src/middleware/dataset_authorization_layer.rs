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
use futures::Future;
use kamu::domain::{CurrentAccountSubject, GetDatasetError};
use opendatafabric::DatasetRef;
use tower::{Layer, Service};

use crate::axum_utils::*;

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetAuthorizationLayer {}

impl DatasetAuthorizationLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<Svc> Layer<Svc> for DatasetAuthorizationLayer {
    type Service = DatasetAuthorizationMiddleware<Svc>;

    fn layer(&self, inner: Svc) -> Self::Service {
        DatasetAuthorizationMiddleware { inner }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetAuthorizationMiddleware<Svc> {
    inner: Svc,
}

impl<Svc> DatasetAuthorizationMiddleware<Svc> {
    fn check_logged_in(catalog: &dill::Catalog) -> Result<(), axum::response::Response> {
        let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
        if let CurrentAccountSubject::Anonymous(_) = current_account_subject.as_ref() {
            Err(unauthorized_access_response())
        } else {
            Ok(())
        }
    }

    fn required_access_action(request: &http::Request<Body>) -> kamu::domain::auth::DatasetAction {
        if !request.method().is_safe() || Self::is_potential_creation(request) {
            kamu::domain::auth::DatasetAction::Write
        } else {
            kamu::domain::auth::DatasetAction::Read
        }
    }

    fn is_potential_creation(request: &http::Request<Body>) -> bool {
        let path = request.uri().path();
        "/push" == path
    }
}

impl<Svc> Service<http::Request<Body>> for DatasetAuthorizationMiddleware<Svc>
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
        // Inspired by https://github.com/maxcountryman/axum-login/blob/main/axum-login/src/auth.rs
        // TODO: PERF: Is cloning a performance concern?
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions");

            let dataset_action_authorizer = catalog
                .get_one::<dyn kamu::domain::auth::DatasetActionAuthorizer>()
                .unwrap();

            let dataset_repo = catalog
                .get_one::<dyn kamu::domain::DatasetRepository>()
                .unwrap();

            let dataset_ref = request
                .extensions()
                .get::<DatasetRef>()
                .expect("Dataset ref not found in http server extensions");

            let action = Self::required_access_action(&request);

            match dataset_repo.resolve_dataset_ref(&dataset_ref).await {
                Ok(dataset_handle) => {
                    if let Err(err) = dataset_action_authorizer
                        .check_action_allowed(&dataset_handle, action)
                        .await
                    {
                        tracing::error!(
                            "Dataset '{}' {} access denied: {:?}",
                            dataset_ref,
                            action,
                            err
                        );
                        return Ok(forbidden_access_response());
                    }
                }
                Err(e) => {
                    if let GetDatasetError::NotFound(_) = e {
                        if Self::is_potential_creation(&request) {
                            if let Err(err_result) = Self::check_logged_in(&catalog) {
                                return Ok(err_result);
                            }
                        }
                    } else {
                        tracing::error!("Could not get dataset: {:?}", e);
                        return Ok(internal_server_error_response());
                    }
                }
            }

            inner.call(request).await
        })
    }
}
