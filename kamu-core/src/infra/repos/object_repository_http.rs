// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::Multihash;

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Client;
use tokio::io::AsyncRead;
use tracing::debug;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ObjectRepositoryHttp {
    client: Client,
    base_url: Url,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl ObjectRepositoryHttp {
    pub fn new(client: Client, base_url: Url) -> Self {
        assert!(
            !base_url.cannot_be_a_base()
                && (base_url.path().is_empty() || base_url.path().ends_with('/')),
            "Invalid base url: {}",
            base_url
        );
        Self { client, base_url }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl ObjectRepository for ObjectRepositoryHttp {
    async fn contains(&self, hash: &Multihash) -> Result<bool, InternalError> {
        let url = self
            .base_url
            .join(&hash.to_multibase_string())
            .into_internal_error()?;

        debug!(%url, "Checking for object");

        let response = self.client.head(url).send().await.into_internal_error()?;

        match response.error_for_status() {
            Ok(_) => Ok(true),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => Ok(false),
            Err(e) => Err(e.into_internal_error().into()),
        }
    }

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        let url = self
            .base_url
            .join(&hash.to_multibase_string())
            .into_internal_error()?;

        debug!(%url, "Reading object");

        let response = self.client.get(url).send().await.into_internal_error()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                }))
            }
            Err(e) => Err(e.into_internal_error().into()),
        }?;

        let data = response.bytes().await.into_internal_error()?;

        Ok(data)
    }

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let url = self
            .base_url
            .join(&hash.to_multibase_string())
            .into_internal_error()?;

        debug!(%url, "Reading object stream");

        let response = self.client.get(url).send().await.into_internal_error()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                }))
            }
            Err(e) => Err(e.into_internal_error().into()),
        }?;

        let stream = response.bytes_stream();

        use futures::TryStreamExt;
        use tokio_util::compat::FuturesAsyncReadCompatExt;
        let reader = stream
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .compat();

        Ok(Box::new(reader))
    }

    async fn insert_bytes<'a>(
        &'a self,
        _data: &'a [u8],
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        panic!("Http object repository is read-only")
    }

    async fn insert_stream<'a>(
        &'a self,
        _src: Box<AsyncReadObj>,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        panic!("Http object repository is read-only")
    }

    async fn delete(&self, _hash: &Multihash) -> Result<(), InternalError> {
        panic!("Http object repository is read-only")
    }
}
