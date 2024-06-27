// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::FromRequestParts;
use axum::response::Response;
use axum::RequestExt;
use kamu::domain::{DatasetRepository, GetDatasetError};
use opendatafabric::DatasetRef;
use tower::{Layer, Service};

use crate::axum_utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetResolverLayer<IdExt, Extractor, DatasetOptPred> {
    identity_extractor: IdExt,
    dataset_optionality_predicate: DatasetOptPred,
    _ex: PhantomData<Extractor>,
}

// Implementing manually since derive macro thinks Extractor has to be Clone too
impl<IdExt, Extractor, DatasetOptPred> Clone
    for DatasetResolverLayer<IdExt, Extractor, DatasetOptPred>
where
    IdExt: Clone,
    DatasetOptPred: Clone,
{
    fn clone(&self) -> Self {
        Self {
            identity_extractor: self.identity_extractor.clone(),
            dataset_optionality_predicate: self.dataset_optionality_predicate.clone(),
            _ex: PhantomData,
        }
    }
}

impl<IdExt, Extractor, DatasetOptPred> DatasetResolverLayer<IdExt, Extractor, DatasetOptPred>
where
    IdExt: Fn(Extractor) -> DatasetRef,
    DatasetOptPred: Fn(&http::Request<Body>) -> bool,
{
    pub fn new(identity_extractor: IdExt, dataset_optionality_predicate: DatasetOptPred) -> Self {
        Self {
            identity_extractor,
            dataset_optionality_predicate,
            _ex: PhantomData,
        }
    }
}

impl<Svc, IdExt, Extractor, DatasetOptPred> Layer<Svc>
    for DatasetResolverLayer<IdExt, Extractor, DatasetOptPred>
where
    IdExt: Clone,
    DatasetOptPred: Clone,
{
    type Service = DatasetResolverMiddleware<Svc, IdExt, Extractor, DatasetOptPred>;

    fn layer(&self, inner: Svc) -> Self::Service {
        DatasetResolverMiddleware {
            inner,
            layer: self.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetResolverMiddleware<Svc, IdExt, Extractor, DatasetOptPred> {
    inner: Svc,
    layer: DatasetResolverLayer<IdExt, Extractor, DatasetOptPred>,
}

impl<Svc, IdExt, Extractor, DatasetOptPred> Clone
    for DatasetResolverMiddleware<Svc, IdExt, Extractor, DatasetOptPred>
where
    Svc: Clone,
    IdExt: Clone,
    DatasetOptPred: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<Svc, IdExt, Extractor, DatasetOptPred> Service<http::Request<Body>>
    for DatasetResolverMiddleware<Svc, IdExt, Extractor, DatasetOptPred>
where
    IdExt: Send + Clone + 'static,
    IdExt: Fn(Extractor) -> DatasetRef,
    Extractor: FromRequestParts<()> + Send + 'static,
    <Extractor as FromRequestParts<()>>::Rejection: std::fmt::Debug,
    DatasetOptPred: Send + Clone + 'static,
    DatasetOptPred: Fn(&http::Request<Body>) -> bool,
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
        let layer = self.layer.clone();

        Box::pin(async move {
            let param1 = match request.extract_parts::<Extractor>().await {
                Ok(p) => p,
                Err(err) => {
                    tracing::warn!("Could not extract params: {:?}", err);
                    return Ok(bad_request_response());
                }
            };

            let dataset_ref = (layer.identity_extractor)(param1);
            if !(layer.dataset_optionality_predicate)(&request) {
                let catalog = request
                    .extensions()
                    .get::<dill::Catalog>()
                    .expect("Catalog not found in http server extensions");

                let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

                let dataset = match dataset_repo.get_dataset(&dataset_ref).await {
                    Ok(ds) => ds,
                    Err(GetDatasetError::NotFound(err)) => {
                        tracing::warn!("Dataset not found: {:?}", err);
                        return Ok(not_found_response());
                    }
                    Err(err) => {
                        tracing::error!("Could not get dataset: {:?}", err);
                        return Ok(internal_server_error_response());
                    }
                };

                request.extensions_mut().insert(dataset);
            }

            request.extensions_mut().insert(dataset_ref);

            inner.call(request).await
        })
    }
}
