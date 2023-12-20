// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use kamu_core::repos::reference_repository::SetRefError;
use kamu_core::*;
use opendatafabric::Multihash;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ReferenceRepositoryImpl<R> {
    repo: R,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<R> ReferenceRepositoryImpl<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<R> ReferenceRepository for ReferenceRepositoryImpl<R>
where
    R: NamedObjectRepository + Send + Sync,
{
    async fn get(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        let data = match self.repo.get(&r.as_str()).await {
            Ok(data) => Ok(data),
            Err(GetNamedError::NotFound(_)) => Err(GetRefError::NotFound(RefNotFoundError {
                block_ref: r.clone(),
            })),
            Err(GetNamedError::Access(e)) => Err(GetRefError::Access(e)),
            Err(GetNamedError::Internal(e)) => Err(GetRefError::Internal(e)),
        }?;
        let text = std::str::from_utf8(&data[..]).int_err()?;
        let hash = Multihash::from_multibase(&text).int_err()?;
        Ok(hash)
    }

    async fn set(&self, r: &BlockRef, hash: &Multihash) -> Result<(), SetRefError> {
        let multibase = hash.as_multibase().to_stack_string();
        match self.repo.set(&r.as_str(), multibase.as_bytes()).await {
            Ok(()) => Ok(()),
            Err(SetNamedError::Access(e)) => Err(SetRefError::Access(e)),
            Err(SetNamedError::Internal(e)) => Err(SetRefError::Internal(e)),
        }
    }

    async fn delete(&self, r: &BlockRef) -> Result<(), DeleteRefError> {
        match self.repo.delete(r.as_str()).await {
            Ok(()) => Ok(()),
            Err(DeleteNamedError::Access(e)) => Err(DeleteRefError::Access(e)),
            Err(DeleteNamedError::Internal(e)) => Err(DeleteRefError::Internal(e)),
        }
    }
}
