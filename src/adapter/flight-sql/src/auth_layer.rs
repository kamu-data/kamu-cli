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

use database_common::DatabaseTransactionRunner;
use futures::Future;
use kamu_accounts::{
    Account,
    AnonymousAccountReason,
    AuthenticationService,
    CurrentAccountSubject,
    GetAccountInfoError,
};
use tonic::body::BoxBody;
use tonic::Status;
use tower::{Layer, Service};

use crate::SessionId;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SessionAuthConfig {
    pub allow_anonymous: bool,
}

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
    fn extract_service_method<T>(request: &http::Request<T>) -> (String, String) {
        let path = request.uri().path();
        let mut parts = path.split('/').filter(|x| !x.is_empty());
        let service = parts.next().unwrap_or_default();
        let method = parts.next().unwrap_or_default();
        (service.to_string(), method.to_string())
    }

    fn extract_bearer_token<T>(request: &http::Request<T>) -> Option<String> {
        let auth = request.headers().get(http::header::AUTHORIZATION)?;
        let auth = auth.to_str().ok()?;

        if auth.starts_with("Bearer ") || auth.starts_with("bearer ") {
            return Some(auth["Bearer ".len()..].to_string());
        }

        None
    }

    async fn get_account_by_token(
        base_catalog: &dill::Catalog,
        access_token: String,
    ) -> Result<Account, GetAccountInfoError> {
        use tracing::Instrument;

        DatabaseTransactionRunner::new(base_catalog.clone())
            .transactional_with(
                |authentication_service: Arc<dyn AuthenticationService>| async move {
                    authentication_service.account_by_token(access_token).await
                },
            )
            .instrument(tracing::debug_span!(
                "AuthenticationMiddleware::current_account_subject"
            ))
            .await
    }
}

impl<Svc, ReqBody> Service<http::Request<ReqBody>> for AuthenticationMiddleware<Svc>
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

    fn call(&mut self, mut request: http::Request<ReqBody>) -> Self::Future {
        // Inspired by https://github.com/maxcountryman/axum-login/blob/5239b38b2698a3db3f92075b6ad430aea79c215a/axum-login/src/auth.rs
        // TODO: PERF: Is cloning a performance concern?
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let base_catalog = request
                .extensions()
                .get::<dill::Catalog>()
                .expect("Catalog not found in request extensions");

            let conf: Arc<SessionAuthConfig> = base_catalog.get_one().unwrap();

            let token = Self::extract_bearer_token(&request);
            let (service, method) = Self::extract_service_method(&request);

            let subject = match &token {
                None if conf.allow_anonymous
                    && service == "arrow.flight.protocol.FlightService"
                    && method == "Handshake" =>
                {
                    CurrentAccountSubject::anonymous(
                        AnonymousAccountReason::NoAuthenticationProvided,
                    )
                }
                Some(token) if conf.allow_anonymous && token.starts_with("anon_") => {
                    // TODO: SEC: Anonymous session tokens have to be validated
                    CurrentAccountSubject::anonymous(
                        AnonymousAccountReason::NoAuthenticationProvided,
                    )
                }
                Some(token) => {
                    match Self::get_account_by_token(base_catalog, token.clone()).await {
                        Ok(account) => CurrentAccountSubject::logged(
                            account.id,
                            account.account_name,
                            account.is_admin,
                        ),
                        Err(e @ GetAccountInfoError::AccessToken(_)) => {
                            tracing::warn!("{e}");
                            return Ok(Status::unauthenticated(e.to_string()).into_http());
                        }
                        Err(e @ GetAccountInfoError::AccountUnresolved) => {
                            tracing::warn!("{e}");
                            return Ok(Status::unauthenticated(e.to_string()).into_http());
                        }
                        Err(e @ GetAccountInfoError::Internal(_)) => {
                            tracing::error!(
                                error = ?e,
                                error_msg = %e,
                                "Internal error during authentication",
                            );
                            return Ok(Status::internal("Internal error").into_http());
                        }
                    }
                }
                _ => {
                    // Disallow fully unauthorized access - anonymous users have to go through
                    // handshare procedure
                    return Ok(Status::unauthenticated(
                        "Unauthenticated access is not allowed. Provide a bearer token or use \
                         basic auth and handshake endpoint to login as anonymous.",
                    )
                    .into_http());
                }
            };

            let session_id = token.map(SessionId);

            tracing::debug!(?subject, ?session_id, "Authenticated request");

            let mut derived_catalog_builder = dill::CatalogBuilder::new_chained(base_catalog);
            if let Some(session_id) = session_id {
                derived_catalog_builder.add_value(session_id);
            }
            derived_catalog_builder.add_value(subject);

            let derived_catalog = derived_catalog_builder.build();
            request.extensions_mut().insert(derived_catalog);

            inner.call(request).await
        })
    }
}
