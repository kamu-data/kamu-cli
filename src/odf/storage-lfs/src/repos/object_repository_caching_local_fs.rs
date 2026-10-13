// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use async_trait::async_trait;
use async_utils::AsyncReadObj;
use bytes::Bytes;
use internal_error::ErrorIntoInternal;
use odf_metadata::*;
use odf_storage::*;
use url::Url;

use crate::ObjectRepositoryLocalFSSha3;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A read-through and (partially) a write-through caching layer for
/// [`ObjectRepository`] using a local file system.
///
/// Cache entries are written atomically: a failed or interrupted fill leaves
/// no entry behind, so a later read goes to the wrapped repository again.
///
/// Currently, caches objects forever, so a cache directory cleanup has to be
/// handled separately.
pub struct ObjectRepositoryCachingLocalFs<WrappedRepo> {
    wrapped: WrappedRepo,
    cache: ObjectRepositoryLocalFSSha3,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<WrappedRepo> ObjectRepositoryCachingLocalFs<WrappedRepo> {
    pub fn new(wrapped: WrappedRepo, cache_dir: &Path) -> Self {
        Self {
            wrapped,
            cache: ObjectRepositoryLocalFSSha3::new(cache_dir),
        }
    }

    // The hash comes from the wrapped repository, so the cache must not
    // recompute it with its own digest algorithm
    fn cache_insert_opts(hash: &Multihash) -> InsertOpts<'_> {
        InsertOpts {
            precomputed_hash: Some(hash),
            ..Default::default()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl<WrappedRepo> ObjectRepository for ObjectRepositoryCachingLocalFs<WrappedRepo>
where
    WrappedRepo: ObjectRepository,
{
    fn protocol(&self) -> ObjectRepositoryProtocol {
        self.wrapped.protocol()
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_contains, skip_all, fields(%hash))]
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        if self.cache.contains(hash).await? {
            Ok(true)
        } else {
            self.wrapped.contains(hash).await
        }
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_get_size, skip_all, fields(%hash))]
    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError> {
        match self.cache.get_size(hash).await {
            Err(GetError::NotFound(_)) => self.wrapped.get_size(hash).await,
            res @ (Ok(_) | Err(GetError::Access(_) | GetError::Internal(_))) => res,
        }
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_get_bytes, skip_all, fields(%hash))]
    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        match self.cache.get_bytes(hash).await {
            Err(GetError::NotFound(_)) => {
                let bytes = self.wrapped.get_bytes(hash).await?;
                self.cache
                    .insert_bytes(&bytes, Self::cache_insert_opts(hash))
                    .await
                    .map_err(ErrorIntoInternal::int_err)?;
                Ok(bytes)
            }
            res @ (Ok(_) | Err(GetError::Access(_) | GetError::Internal(_))) => res,
        }
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_get_stream, skip_all, fields(%hash))]
    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        match self.cache.get_stream(hash).await {
            Err(GetError::NotFound(_)) => {
                let stream = self.wrapped.get_stream(hash).await?;
                self.cache
                    .insert_stream(stream, Self::cache_insert_opts(hash))
                    .await
                    .map_err(ErrorIntoInternal::int_err)?;
                match self.cache.get_stream(hash).await {
                    // Removed right after the fill, e.g. by a cache cleanup
                    Err(GetError::NotFound(_)) => self.wrapped.get_stream(hash).await,
                    res @ (Ok(_) | Err(GetError::Access(_) | GetError::Internal(_))) => res,
                }
            }
            res @ (Ok(_) | Err(GetError::Access(_) | GetError::Internal(_))) => res,
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

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_insert_bytes, skip_all)]
    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let res = self.wrapped.insert_bytes(data, options).await?;
        self.cache
            .insert_bytes(data, Self::cache_insert_opts(&res.hash))
            .await
            .map_err(ErrorIntoInternal::int_err)?;
        Ok(res)
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_insert_stream, skip_all)]
    async fn insert_stream<'a>(
        &'a self,
        src: Box<AsyncReadObj>,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        // Will cache upon next read, for simplicity
        self.wrapped.insert_stream(src, options).await
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_insert_file_move, skip_all)]
    async fn insert_file_move<'a>(
        &'a self,
        src: &Path,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        // Will cache upon next read, for simplicity
        self.wrapped.insert_file_move(src, options).await
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryCachingLocalFs_delete, skip_all, fields(%hash))]
    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        self.cache.delete(hash).await?;
        self.wrapped.delete(hash).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
