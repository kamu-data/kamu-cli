// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use crate::domain::*;
use opendatafabric::Multihash;

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Client;
use tokio::io::AsyncRead;
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
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let url = self.base_url.join(&hash.to_multibase_string()).int_err()?;

        tracing::debug!(%url, "Checking for object");

        let response = self.client.head(url).send().await.int_err()?;

        match response.error_for_status() {
            Ok(_) => Ok(true),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => Ok(false),
            Err(e) if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED) => {
                Err(AccessError::Unauthorized(e.into()).into())
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::FORBIDDEN) => {
                Err(AccessError::Forbidden(e.into()).into())
            }
            Err(e) => Err(e.int_err().into()),
        }
    }

    async fn get_size(&self, _hash: &Multihash) -> Result<u64, GetError> {
        panic!("get_size unsupported for HTTP object repository");
    }

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        let url = self.base_url.join(&hash.to_multibase_string()).int_err()?;

        tracing::debug!(%url, "Reading object");

        let response = self.client.get(url).send().await.int_err()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                }))
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED) => {
                Err(AccessError::Unauthorized(e.into()).into())
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::FORBIDDEN) => {
                Err(AccessError::Forbidden(e.into()).into())
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        let data = response.bytes().await.int_err()?;

        Ok(data)
    }

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let url = self.base_url.join(&hash.to_multibase_string()).int_err()?;

        tracing::debug!(%url, "Reading object stream");

        let response = self.client.get(url).send().await.int_err()?;

        let response = match response.error_for_status() {
            Ok(resp) => Ok(resp),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                }))
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED) => {
                Err(AccessError::Unauthorized(e.into()).into())
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::FORBIDDEN) => {
                Err(AccessError::Forbidden(e.into()).into())
            }
            Err(e) => Err(e.int_err().into()),
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

    async fn get_download_url(
        &self,
        hash: &Multihash,
        _opts: TransferOpts,
    ) -> Result<GetTransferUrlResult, GetTransferUrlError> {
        match self.base_url.join(&hash.to_multibase_string()) {
            Ok(url) => Ok(GetTransferUrlResult {
                url,
                expires_at: None,
            }),
            Err(e) => Err(GetTransferUrlError::Internal(e.int_err())),
        }
    }

    async fn get_upload_url(
        &self,
        _hash: &Multihash,
        _opts: TransferOpts,
    ) -> Result<GetTransferUrlResult, GetTransferUrlError> {
        Err(AccessError::ReadOnly(None).into())
    }

    async fn insert_bytes<'a>(
        &'a self,
        _data: &'a [u8],
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        Err(AccessError::ReadOnly(None).into())
    }

    async fn insert_stream<'a>(
        &'a self,
        _src: Box<AsyncReadObj>,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        Err(AccessError::ReadOnly(None).into())
    }

    async fn insert_file_move<'a>(
        &'a self,
        _src: &Path,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        Err(AccessError::ReadOnly(None).into())
    }

    async fn delete(&self, _hash: &Multihash) -> Result<(), DeleteError> {
        Err(AccessError::ReadOnly(None).into())
    }
}
