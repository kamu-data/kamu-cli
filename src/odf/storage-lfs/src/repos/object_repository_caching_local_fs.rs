// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use async_utils::AsyncReadObj;
use bytes::Bytes;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_metadata::*;
use odf_storage::*;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A read-through and (partially) a write-through caching layer for
/// [`ObjectRepository`] using a local file system.
///
/// Currently, caches objects forever, so a cache directory cleanup has to be
/// handled separately.
pub struct ObjectRepositoryCachingLocalFs<WrappedRepo> {
    wrapped: WrappedRepo,
    cache_dir: Arc<PathBuf>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<WrappedRepo> ObjectRepositoryCachingLocalFs<WrappedRepo> {
    pub fn new(wrapped: WrappedRepo, cache_dir: Arc<PathBuf>) -> Self {
        Self { wrapped, cache_dir }
    }

    fn cache_path(&self, hash: &Multihash) -> PathBuf {
        self.cache_dir.join(hash.as_multibase().to_stack_string())
    }

    fn ensure_cache_dir(&self) -> Result<(), std::io::Error> {
        if !self.cache_dir.exists() {
            std::fs::create_dir_all(self.cache_dir.as_ref())?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<WrappedRepo> ObjectRepository for ObjectRepositoryCachingLocalFs<WrappedRepo>
where
    WrappedRepo: ObjectRepository,
{
    fn protocol(&self) -> ObjectRepositoryProtocol {
        self.wrapped.protocol()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%hash))]
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let cache_path = self.cache_path(hash);
        if cache_path.is_file() {
            Ok(true)
        } else {
            self.wrapped.contains(hash).await
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%hash))]
    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError> {
        let cache_path = self.cache_path(hash);
        match std::fs::metadata(&cache_path) {
            Ok(meta) => Ok(meta.len()),
            Err(err) if err.kind() == ErrorKind::NotFound => self.wrapped.get_size(hash).await,
            Err(err) => Err(err.int_err().into()),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%hash))]
    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        let cache_path = self.cache_path(hash);
        match std::fs::read(&cache_path) {
            Ok(data) => Ok(Bytes::from(data)),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let bytes = self.wrapped.get_bytes(hash).await?;
                self.ensure_cache_dir().int_err()?;
                std::fs::write(&cache_path, &bytes).int_err()?;
                Ok(bytes)
            }
            Err(err) => Err(err.int_err().into()),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%hash))]
    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let cache_path = self.cache_path(hash);

        match tokio::fs::File::open(&cache_path).await {
            Ok(file) => Ok(Box::new(file)),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let mut stream = self.wrapped.get_stream(hash).await?;

                self.ensure_cache_dir().int_err()?;

                let mut file = tokio::fs::File::create(cache_path).await.int_err()?;
                tokio::io::copy(&mut stream, &mut file).await.int_err()?;
                file.flush().await.int_err()?;

                file.seek(std::io::SeekFrom::Start(0)).await.int_err()?;
                Ok(Box::new(file))
            }
            Err(err) => Err(err.int_err().into()),
        }
    }

    async fn get_internal_url(&self, hash: &Multihash) -> Url {
        self.wrapped.get_internal_url(hash).await
    }

    async fn get_external_download_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError> {
        self.wrapped.get_external_download_url(hash, opts).await
    }

    async fn get_external_upload_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError> {
        self.wrapped.get_external_upload_url(hash, opts).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let res = self.wrapped.insert_bytes(data, options).await?;
        self.ensure_cache_dir().int_err()?;
        let cache_path = self.cache_path(&res.hash);
        std::fs::write(cache_path, data).int_err()?;
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_stream<'a>(
        &'a self,
        src: Box<AsyncReadObj>,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        // Will cache upon next read, for simplicity
        self.wrapped.insert_stream(src, options).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_file_move<'a>(
        &'a self,
        src: &Path,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        // Will cache upon next read, for simplicity
        self.wrapped.insert_file_move(src, options).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%hash))]
    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        let cache_path = self.cache_path(hash);
        match std::fs::remove_file(&cache_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.int_err()),
        }?;
        self.wrapped.delete(hash).await
    }
}
