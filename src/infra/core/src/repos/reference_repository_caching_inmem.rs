// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use async_trait::async_trait;
use kamu_core::repos::reference_repository::SetRefError;
use kamu_core::{
    BlockRef,
    DeleteRefError,
    GetRefError,
    NamedObjectRepository,
    ReferenceRepository,
};
use opendatafabric::Multihash;
use tokio::sync::Mutex;

use crate::ReferenceRepositoryImpl;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ReferenceRepositoryCachingInMem<R> {
    wrapped: ReferenceRepositoryImpl<R>,
    cache: Mutex<HashMap<BlockRef, Multihash>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<R> ReferenceRepositoryCachingInMem<R> {
    pub fn new(repo: R) -> Self {
        let wrapped = ReferenceRepositoryImpl::new(repo);

        Self {
            wrapped,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<R> ReferenceRepository for ReferenceRepositoryCachingInMem<R>
where
    R: NamedObjectRepository + Send + Sync,
{
    async fn get(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        let mut cache = self.cache.lock().await;

        if let Some(cached_result) = cache.get(r) {
            return Ok(cached_result.clone());
        };

        let get_res = self.wrapped.get(r).await;

        if let Ok(hash) = &get_res {
            cache.insert(r.clone(), hash.clone());
        }

        get_res
    }

    async fn set(&self, r: &BlockRef, hash: &Multihash) -> Result<(), SetRefError> {
        let mut cache = self.cache.lock().await;
        let set_res = self.wrapped.set(r, hash).await;

        if set_res.is_ok() {
            cache.insert(r.clone(), hash.clone());
        }

        set_res
    }

    async fn delete(&self, r: &BlockRef) -> Result<(), DeleteRefError> {
        let mut cache = self.cache.lock().await;
        let delete_res = self.wrapped.delete(r).await;

        if delete_res.is_ok() {
            cache.remove(r);
        }

        delete_res
    }
}
