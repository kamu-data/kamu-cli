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
use database_common::DatabaseTransactionRunner;
use futures::Future;
use internal_error::InternalError;
use kamu_core::auth::DatasetActionAuthorizerExt;
use kamu_core::DatasetRegistry;
use tower::{Layer, Service};

use crate::axum_utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetAuthorizationLayer<DatasetActionQuery> {
    dataset_action_query: DatasetActionQuery,
}

impl<DatasetActionQuery> DatasetAuthorizationLayer<DatasetActionQuery>
where
    DatasetActionQuery: Fn(&http::Request<Body>) -> kamu_core::auth::DatasetAction,
{
    pub fn new(dataset_action_query: DatasetActionQuery) -> Self {
        Self {
            dataset_action_query,
        }
    }
}

impl<Svc, DatasetActionQuery> Layer<Svc> for DatasetAuthorizationLayer<DatasetActionQuery>
where
    DatasetActionQuery: Clone,
{
    type Service = DatasetAuthorizationMiddleware<Svc, DatasetActionQuery>;

    fn layer(&self, inner: Svc) -> Self::Service {
        DatasetAuthorizationMiddleware {
            inner,
            dataset_action_query: self.dataset_action_query.clone(),
        }
    }
}

impl Default
    for DatasetAuthorizationLayer<
        for<'a> fn(&'a http::Request<Body>) -> kamu_core::auth::DatasetAction,
    >
{
    fn default() -> Self {
        Self::new(get_dataset_action_for_request)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetAuthorizationMiddleware<Svc, DatasetActionQuery> {
    inner: Svc,
    dataset_action_query: DatasetActionQuery,
}

impl<Svc, DatasetActionQuery> Service<http::Request<Body>>
    for DatasetAuthorizationMiddleware<Svc, DatasetActionQuery>
where
    Svc: Service<http::Request<Body>, Response = Response> + Send + 'static + Clone,
    Svc::Future: Send + 'static,
    DatasetActionQuery: Send + Clone + 'static,
    DatasetActionQuery: Fn(&http::Request<Body>) -> kamu_core::auth::DatasetAction,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        // Inspired by https://github.com/maxcountryman/axum-login/blob/5239b38b2698a3db3f92075b6ad430aea79c215a/axum-login/src/auth.rs
        // TODO: PERF: Is cloning a performance concern?
        let mut inner = self.inner.clone();

        let dataset_action_query = self.dataset_action_query.clone();

        Box::pin(async move {
            let catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions");

            let dataset_ref = request
                .extensions()
                .get::<odf::DatasetRef>()
                .expect("Dataset ref not found in http server extensions");

            let action = dataset_action_query(&request);

            enum CheckResult {
                Proceed,
                ErrorResponse(Response<Body>),
            }

            let check_result: Result<CheckResult, InternalError> =
                DatabaseTransactionRunner::new(catalog.clone())
                    .transactional(|transaction_catalog| async move {
                        let dataset_registry = transaction_catalog
                            .get_one::<dyn DatasetRegistry>()
                            .unwrap();
                        let dataset_action_authorizer = transaction_catalog
                            .get_one::<dyn kamu_core::auth::DatasetActionAuthorizer>()
                            .unwrap();

                        match dataset_registry
                            .resolve_dataset_handle_by_ref(dataset_ref)
                            .await
                        {
                            Ok(dataset_handle) => {
                                // For anonymous or user, the dataset does not exist if there is no
                                // access
                                let not_accessible = !dataset_action_authorizer
                                    .is_action_allowed(&dataset_handle.id, action)
                                    .await?;

                                if not_accessible {
                                    Ok(CheckResult::ErrorResponse(not_found_response()))
                                } else {
                                    Ok(CheckResult::Proceed)
                                }
                            }
                            Err(odf::dataset::GetDatasetError::NotFound(_)) => {
                                Ok(CheckResult::Proceed)
                            }
                            Err(odf::dataset::GetDatasetError::Internal(_)) => {
                                Ok(CheckResult::ErrorResponse(internal_server_error_response()))
                            }
                        }
                    })
                    .await;

            match check_result {
                Ok(CheckResult::Proceed) => inner.call(request).await,
                Ok(CheckResult::ErrorResponse(r)) => Ok(r),
                Err(err) => {
                    tracing::error!(error=?err, error_msg=%err, "DatasetAuthorizationLayer failed");
                    Ok(internal_server_error_response())
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
