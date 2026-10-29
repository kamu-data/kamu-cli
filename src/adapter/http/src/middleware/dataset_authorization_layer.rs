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
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};
use kamu_datasets::{
    DatasetPermission,
    DatasetPermissionAccess,
    DatasetPermissionChecker,
    DatasetRegistry,
};
use tower::{Layer, Service};

use crate::axum_utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetAuthorizationLayer<DatasetPermissionQuery> {
    dataset_permission_query: DatasetPermissionQuery,
}

impl<DatasetPermissionQuery> DatasetAuthorizationLayer<DatasetPermissionQuery>
where
    DatasetPermissionQuery: Fn(&http::Request<Body>) -> kamu_datasets::DatasetPermission,
{
    pub fn new(dataset_permission_query: DatasetPermissionQuery) -> Self {
        Self {
            dataset_permission_query,
        }
    }
}

impl<Svc, DatasetPermissionQuery> Layer<Svc> for DatasetAuthorizationLayer<DatasetPermissionQuery>
where
    DatasetPermissionQuery: Copy,
{
    type Service = DatasetAuthorizationMiddleware<Svc, DatasetPermissionQuery>;

    fn layer(&self, inner: Svc) -> Self::Service {
        DatasetAuthorizationMiddleware {
            inner,
            dataset_permission_query: self.dataset_permission_query,
        }
    }
}

impl Default
    for DatasetAuthorizationLayer<
        for<'a> fn(&'a http::Request<Body>) -> kamu_datasets::DatasetPermission,
    >
{
    fn default() -> Self {
        Self::new(get_dataset_permission_for_request)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetAuthorizationMiddleware<Svc, DatasetPermissionQuery> {
    inner: Svc,
    dataset_permission_query: DatasetPermissionQuery,
}

impl<Svc, DatasetPermissionQuery> Service<http::Request<Body>>
    for DatasetAuthorizationMiddleware<Svc, DatasetPermissionQuery>
where
    Svc: Service<http::Request<Body>, Response = Response> + Send + 'static + Clone,
    Svc::Future: Send + 'static,
    DatasetPermissionQuery: Send + Copy + 'static,
    DatasetPermissionQuery: Fn(&http::Request<Body>) -> kamu_datasets::DatasetPermission,
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

        let dataset_permission_query = self.dataset_permission_query;

        Box::pin(async move {
            let catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions");

            let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
            if let CurrentAccountSubject::Anonymous(reason) = current_account_subject.as_ref() {
                let message = match reason {
                    AnonymousAccountReason::AuthenticationInvalid => {
                        Some("Authentication token invalid")
                    }
                    AnonymousAccountReason::AuthenticationExpired => {
                        Some("Authentication token expired")
                    }
                    AnonymousAccountReason::NoAuthenticationProvided => None,
                };

                if let Some(message) = message {
                    return Ok(unauthorized_access_response(Some(message)));
                }
            }

            let dataset_ref = request
                .extensions()
                .get::<odf::DatasetRef>()
                .expect("Dataset ref not found in http server extensions");

            let current_dataset_permission = dataset_permission_query(&request);

            enum CheckResult {
                Proceed,
                ErrorResponse(Response<Body>),
            }

            let check_result: Result<CheckResult, InternalError> =
                DatabaseTransactionRunner::new(catalog.clone())
                    .transactional(|transaction_catalog| async move {
                        use kamu_datasets::DatasetPermissionCheckerExt;

                        let dataset_registry = transaction_catalog
                            .get_one::<dyn DatasetRegistry>()
                            .unwrap();
                        let dataset_permission_checker = transaction_catalog
                            .get_one::<dyn DatasetPermissionChecker>()
                            .unwrap();

                        let dataset_handle = {
                            use odf::DatasetRefUnresolvedError as E;

                            match dataset_registry
                                .resolve_dataset_handle_by_ref(dataset_ref)
                                .await
                            {
                                Ok(dataset_handle) => dataset_handle,
                                Err(E::NotFound(_)) => {
                                    // In case of dataset initial pushing, the dataset does not
                                    // exist beforehand.
                                    return Ok(CheckResult::Proceed);
                                }
                                Err(e @ E::Internal(_)) => {
                                    return Err(e.int_err());
                                }
                            }
                        };
                        let granted_permissions = dataset_permission_checker
                            .get_granted_permissions_for_existing_dataset(&dataset_handle.id)
                            .await?;

                        match DatasetPermission::resolve_access(
                            &granted_permissions,
                            current_dataset_permission,
                        ) {
                            DatasetPermissionAccess::Allowed => Ok(CheckResult::Proceed),
                            DatasetPermissionAccess::Insufficient => {
                                Ok(CheckResult::ErrorResponse(forbidden_access_response()))
                            }
                            DatasetPermissionAccess::Forbidden => {
                                Ok(CheckResult::ErrorResponse(not_found_response()))
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
