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
use axum::RequestExt;
use futures::Future;
use kamu::domain::{auth, AnonymousAccountReason, CurrentAccountSubject};
use tower::{Layer, Service};

use crate::access_token::AccessToken;
use crate::axum_utils::*;
use crate::smart_protocol::BearerHeader;

/////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthenticationMiddleware<Svc> {
    inner: Svc,
}

impl<Svc> AuthenticationMiddleware<Svc> {
    async fn current_account_subject(
        base_catalog: &dill::Catalog,
        maybe_access_token: Option<AccessToken>,
    ) -> Result<CurrentAccountSubject, Response> {
        if let Some(access_token) = maybe_access_token {
            let authentication_service = base_catalog
                .get_one::<dyn auth::AuthenticationService>()
                .unwrap();

            // TODO: Getting the full account info here is expensive while all we need is
            // the caller identity
            match authentication_service
                .account_info_by_token(access_token.token)
                .await
            {
                Ok(account_info) => Ok(CurrentAccountSubject::logged(account_info.account_name)),
                Err(auth::GetAccountInfoError::AccessToken(e)) => match e {
                    auth::AccessTokenError::Expired => Ok(CurrentAccountSubject::anonymous(
                        AnonymousAccountReason::AuthenticationExpired,
                    )),
                    auth::AccessTokenError::Invalid(_) => Ok(CurrentAccountSubject::anonymous(
                        AnonymousAccountReason::AuthenticationInvalid,
                    )),
                },
                Err(auth::GetAccountInfoError::Internal(_)) => {
                    return Err(internal_server_error_response());
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
        // Inspired by https://github.com/maxcountryman/axum-login/blob/main/axum-login/src/auth.rs
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
                match Self::current_account_subject(base_catalog, maybe_access_token).await {
                    Ok(current_account_subject) => current_account_subject,
                    Err(response) => return Ok(response),
                };

            tracing::debug!(subject = ?current_account_subject, "Authenticated request");

            let derived_catalog = dill::CatalogBuilder::new_chained(base_catalog)
                .add_value(current_account_subject)
                .build();

            request.extensions_mut().insert(derived_catalog);

            inner.call(request).await
        })
    }
}
