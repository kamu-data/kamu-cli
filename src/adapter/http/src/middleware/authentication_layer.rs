// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::response::Response;
use axum::RequestExt;
use database_common::DatabaseTransactionRunner;
use futures::Future;
use kamu_accounts::{
    AccessTokenError,
    AnonymousAccountReason,
    AuthenticationService,
    CurrentAccountSubject,
    GetAccountInfoError,
};
use tower::{Layer, Service};

use crate::axum_utils::*;
use crate::{AccessToken, BearerHeader};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthenticationLayer {}

impl AuthenticationLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<Svc> Layer<Svc> for AuthenticationLayer {
    type Service = AuthenticationMiddleware<Svc>;

    fn layer(&self, inner: Svc) -> Self::Service {
        AuthenticationMiddleware { inner }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthenticationMiddleware<Svc> {
    inner: Svc,
}

impl<Svc> AuthenticationMiddleware<Svc> {
    async fn current_account_subject(
        base_catalog: &dill::Catalog,
        maybe_access_token: Option<AccessToken>,
    ) -> Result<CurrentAccountSubject, Response> {
        use tracing::Instrument;

        if let Some(access_token) = maybe_access_token {
            let account_res = DatabaseTransactionRunner::new(base_catalog.clone())
                .transactional_with(
                    |authentication_service: Arc<dyn AuthenticationService>| async move {
                        authentication_service
                            .account_by_token(access_token.token)
                            .await
                    },
                )
                .instrument(tracing::debug_span!(
                    "AuthenticationMiddleware::current_account_subject"
                ))
                .await;

            // TODO: PERF: Getting the full account info here is expensive while all we need
            // is the caller identity
            match account_res {
                Ok(account) => Ok(CurrentAccountSubject::logged(
                    account.id,
                    account.account_name,
                    account.is_admin,
                )),
                Err(GetAccountInfoError::AccessToken(e)) => match e {
                    AccessTokenError::Expired => Ok(CurrentAccountSubject::anonymous(
                        AnonymousAccountReason::AuthenticationExpired,
                    )),
                    AccessTokenError::Invalid(err) => {
                        tracing::warn!(error = err, "Ignoring invalid auth token",);
                        Ok(CurrentAccountSubject::anonymous(
                            AnonymousAccountReason::AuthenticationInvalid,
                        ))
                    }
                },
                Err(GetAccountInfoError::AccountUnresolved) => {
                    tracing::warn!("Ignoring auth token pointing to non-existing account");
                    Ok(CurrentAccountSubject::anonymous(
                        AnonymousAccountReason::AuthenticationInvalid,
                    ))
                }
                Err(GetAccountInfoError::Internal(err)) => {
                    tracing::error!(
                        error = ?err,
                        error_msg = %err,
                        "Internal error during authentication",
                    );
                    Err(internal_server_error_response())
                }
            }
        } else {
            Ok(CurrentAccountSubject::anonymous(
                AnonymousAccountReason::NoAuthenticationProvided,
            ))
        }
    }
}

impl<Svc> Service<http::Request<Body>> for AuthenticationMiddleware<Svc>
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

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        // Inspired by https://github.com/maxcountryman/axum-login/blob/5239b38b2698a3db3f92075b6ad430aea79c215a/axum-login/src/auth.rs
        // TODO: PERF: Is cloning a performance concern?
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let maybe_access_token = request
                .extract_parts::<Option<BearerHeader>>()
                .await
                .unwrap()
                .map(|th| AccessToken::new(th.token()));

            let base_catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in http server extensions");

            let current_account_subject =
                match Self::current_account_subject(base_catalog, maybe_access_token.clone()).await
                {
                    Ok(current_account_subject) => current_account_subject,
                    Err(response) => return Ok(response),
                };

            tracing::debug!(subject = ?current_account_subject, "Authenticated request");

            let mut derived_catalog_builder = dill::CatalogBuilder::new_chained(base_catalog);
            derived_catalog_builder.add_value(current_account_subject);
            if let Some(access_token) = maybe_access_token {
                derived_catalog_builder.add_value(access_token);
            }

            let derived_catalog = derived_catalog_builder.build();
            request.extensions_mut().insert(derived_catalog);

            inner.call(request).await
        })
    }
}
