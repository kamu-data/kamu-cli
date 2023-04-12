// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::FromRequestParts;
use axum::RequestExt;
use axum::{body::Body, http::Request, http::StatusCode, response::Response};
use kamu::domain::{DatasetRepository, GetDatasetError};
use opendatafabric::DatasetRef;
use std::future::Future;
use std::marker::PhantomData;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tower::{Layer, Service};

/////////////////////////////////////////////////////////////////////////////////

pub struct DatasetResolverLayer<IdExt, Extractor> {
    identity_extractor: IdExt,
    _ex: PhantomData<Extractor>,
}

// Implementing manually since derive macro thinks Extractor has to be Clone too
impl<IdExt, Extractor> Clone for DatasetResolverLayer<IdExt, Extractor>
where
    IdExt: Clone,
{
    fn clone(&self) -> Self {
        Self {
            identity_extractor: self.identity_extractor.clone(),
            _ex: PhantomData,
        }
    }
}

impl<IdExt, Extractor> DatasetResolverLayer<IdExt, Extractor>
where
    IdExt: Fn(Extractor) -> DatasetRef,
{
    pub fn new(identity_extractor: IdExt) -> Self {
        Self {
            identity_extractor,
            _ex: PhantomData,
        }
    }
}

impl<Svc, IdExt, Extractor> Layer<Svc> for DatasetResolverLayer<IdExt, Extractor>
where
    IdExt: Clone,
{
    type Service = DatasetResolverMiddleware<Svc, IdExt, Extractor>;

    fn layer(&self, inner: Svc) -> Self::Service {
        DatasetResolverMiddleware {
            inner,
            layer: self.clone(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub struct DatasetResolverMiddleware<Svc, IdExt, Extractor> {
    inner: Svc,
    layer: DatasetResolverLayer<IdExt, Extractor>,
}

impl<Svc, IdExt, Extractor> DatasetResolverMiddleware<Svc, IdExt, Extractor> {
    fn is_dataset_optional(request: &Request<Body>) -> bool {
        let path = request.uri().path();
        if "/push" == path {
            return true;
        }

        let method = request.method().as_str();
        method == "PUT" && (path.starts_with("/data/") || path.starts_with("/checkpoints/"))
    }
}

impl<Svc, IdExt, Extractor> Clone for DatasetResolverMiddleware<Svc, IdExt, Extractor>
where
    Svc: Clone,
    IdExt: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<Svc, IdExt, Extractor> Service<Request<Body>>
    for DatasetResolverMiddleware<Svc, IdExt, Extractor>
where
    IdExt: Send + Clone + 'static,
    IdExt: Fn(Extractor) -> DatasetRef,
    Extractor: FromRequestParts<()> + Send + 'static,
    <Extractor as FromRequestParts<()>>::Rejection: std::fmt::Debug,
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
        let layer = self.layer.clone();

        Box::pin(async move {
            let param1 = match request.extract_parts::<Extractor>().await {
                Ok(p) => p,
                Err(err) => {
                    tracing::warn!("Could not extract params: {:?}", err);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Default::default())
                        .unwrap());
                }
            };

            let dataset_ref = (layer.identity_extractor)(param1);
            if Self::is_dataset_optional(&request) {
                request.extensions_mut().insert(dataset_ref);
            } else {
                let catalog = request
                    .extensions()
                    .get::<dill::Catalog>()
                    .expect("Catalog not found in http server extensions");

                let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

                let dataset = match dataset_repo.get_dataset(&dataset_ref).await {
                    Ok(ds) => ds,
                    Err(GetDatasetError::NotFound(err)) => {
                        tracing::warn!("Dataset not found: {:?}", err);
                        return Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Default::default())
                            .unwrap());
                    }
                    Err(err) => {
                        tracing::error!("Could not get dataset: {:?}", err);
                        return Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Default::default())
                            .unwrap());
                    }
                };

                request.extensions_mut().insert(dataset);
            }

            inner.call(request).await
        })
    }
}
