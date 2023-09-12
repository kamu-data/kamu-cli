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
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::RequestExt;
use futures::Future;
use kamu::domain::{auth, CurrentAccountSubject};
use tower::{Layer, Service};

use crate::AccessToken;

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CurrentAccountResolverLayer {}

impl CurrentAccountResolverLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<Svc> Layer<Svc> for CurrentAccountResolverLayer {
    type Service = CurrentAccountResolverMiddleware<Svc>;

    fn layer(&self, inner: Svc) -> Self::Service {
        CurrentAccountResolverMiddleware { inner }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CurrentAccountResolverMiddleware<Svc> {
    inner: Svc,
}

impl<Svc> CurrentAccountResolverMiddleware<Svc> {
    async fn current_account_subject(
        base_catalog: &dill::Catalog,
        maybe_access_token: Option<AccessToken>,
    ) -> Result<CurrentAccountSubject, Response> {
        if let Some(access_token) = maybe_access_token {
            let authentication_service = base_catalog
                .get_one::<dyn auth::AuthenticationService>()
                .unwrap();

            match authentication_service
                .get_account_info(access_token.token)
                .await
            {
                Ok(account_info) => {
                    Ok(CurrentAccountSubject::new(account_info.account_name, false))
                }
                Err(auth::GetAccountInfoError::AccessToken(_)) => Ok(CurrentAccountSubject::new(
                    opendatafabric::AccountName::new_unchecked(auth::ANONYMOUS_ACCOUNT_NAME),
                    true,
                )),
                Err(auth::GetAccountInfoError::Internal(_)) => {
                    return Err(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Default::default())
                        .unwrap());
                }
            }
        } else {
            Ok(CurrentAccountSubject::new(
                opendatafabric::AccountName::new_unchecked(auth::ANONYMOUS_ACCOUNT_NAME),
                true,
            ))
        }
    }
}

impl<Svc> Service<Request<Body>> for CurrentAccountResolverMiddleware<Svc>
where
    Svc: Service<Request<Body>, Response = Response> + Send + 'static + Clone,
    Svc::Future: Send + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        // Inspired by https://github.com/maxcountryman/axum-login/blob/main/axum-login/src/auth.rs
        // TODO: PERF: Is cloning a performance concern?
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let maybe_access_token = request
                .extract_parts::<Option<
                    axum::TypedHeader<
                        axum::headers::Authorization<axum::headers::authorization::Bearer>,
                    >,
                >>()
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

            let derived_catalog = dill::CatalogBuilder::new_chained(base_catalog)
                .add_value(current_account_subject)
                .build();

            request.extensions_mut().insert(derived_catalog.clone());

            inner.call(request).await
        })
    }
}
